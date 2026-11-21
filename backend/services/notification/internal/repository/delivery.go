package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/notification/internal/domain"
)

func (r *PostgresNotificationRepository) ClaimDeliveries(ctx context.Context, limit int, lease time.Duration) ([]domain.DeliveryClaim, error) {
	if limit < 1 || limit > 4 || lease < time.Second || lease > time.Minute {
		return nil, fmt.Errorf("invalid delivery claim bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackNotification(tx)
	// Expired sends and outcomes left UNKNOWN by older workers are terminalized.
	// SMTP has no idempotency key, so these rows must not be redispatched.
	// Snapshot visibility keeps both recovery groups out of this sweep's claim set.
	rows, err := tx.Query(ctx, `WITH expired AS (
 SELECT id FROM notifications WHERE managed AND status='DELIVERING' AND delivery_lease_until<=clock_timestamp()
 ORDER BY delivery_lease_until,id LIMIT $1 FOR UPDATE SKIP LOCKED
 ), recovered AS (
 UPDATE notifications n SET status='ATTENTION',delivery_due_at=NULL,
 delivery_token=NULL,delivery_lease_until=NULL,last_error='delivery_lease_expired'
 FROM expired e WHERE n.id=e.id RETURNING n.id)
	, prior_unknown AS (
	 SELECT id FROM notifications WHERE managed AND status='UNKNOWN'
	 ORDER BY delivery_due_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
	), terminalized AS (
	 UPDATE notifications n SET status='ATTENTION',delivery_due_at=NULL,
	 delivery_token=NULL,delivery_lease_until=NULL,last_error=COALESCE(NULLIF(last_error,''),'delivery_outcome_unknown')
	 FROM prior_unknown u WHERE n.id=u.id RETURNING n.id)
	 SELECT id,type,recipient,channel,locale,payload,status,sent_at,created_at
 FROM notifications WHERE managed AND status IN ('PENDING','RETRYABLE')
 AND delivery_attempts<8 AND delivery_due_at<=clock_timestamp()
 ORDER BY delivery_due_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*domain.Notification
	for rows.Next() {
		n := &domain.Notification{Managed: true}
		var raw []byte
		if err := rows.Scan(&n.ID, &n.Type, &n.Recipient, &n.Channel, &n.Locale, &raw, &n.Status, &n.SentAt, &n.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &n.Payload); err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	claims := make([]domain.DeliveryClaim, 0, len(items))
	for _, n := range items {
		c := domain.DeliveryClaim{Notification: n, Token: uuid.New()}
		err := tx.QueryRow(ctx, `UPDATE notifications SET status='DELIVERING',delivery_token=$2,
 delivery_fence=delivery_fence+1,delivery_attempts=delivery_attempts+1,
 delivery_lease_until=clock_timestamp()+$3::double precision*interval '1 second'
 WHERE id=$1 RETURNING delivery_fence,delivery_attempts,delivery_lease_until`, n.ID, c.Token, lease.Seconds()).Scan(&c.Fence, &c.Attempts, &c.LeaseUntil)
		if err != nil {
			return nil, err
		}
		claims = append(claims, c)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claims, nil
}

func validDeliveryResult(c domain.DeliveryClaim, v domain.DeliveryResult) bool {
	if c.Notification == nil || c.Notification.ID == uuid.Nil || c.Token == uuid.Nil || c.Fence < 1 {
		return false
	}
	switch v.State {
	case "SENT", "ATTENTION", "CANCELLED":
		return true
	case "RETRYABLE":
		return !v.NextAttemptAt.IsZero()
	}
	return false
}

type DeliveryStat struct {
	State     string
	Count     int64
	OldestAge float64
}

func (r *PostgresNotificationRepository) DeliveryStats(ctx context.Context) ([]DeliveryStat, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT status,count(*),GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)))::double precision FROM notifications WHERE managed AND status<>'SENT' GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []DeliveryStat
	for rows.Next() {
		var s DeliveryStat
		if err := rows.Scan(&s.State, &s.Count, &s.OldestAge); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

// FinishDelivery is replayable with the original claim and result. This lets a
// caller resolve an applied-but-unacknowledged marker without invoking Send again.
func (r *PostgresNotificationRepository) FinishDelivery(ctx context.Context, c domain.DeliveryClaim, v domain.DeliveryResult) (bool, error) {
	if !validDeliveryResult(c, v) {
		return false, fmt.Errorf("invalid delivery result")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackNotification(tx)
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM notifications WHERE id=$1 AND managed FOR UPDATE`, c.Notification.ID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	// Evaluate clock_timestamp only AFTER obtaining the row lock.
	var owned, applied bool
	if err := tx.QueryRow(ctx, `SELECT status='DELIVERING' AND delivery_token=$2 AND delivery_fence=$3 AND delivery_lease_until>clock_timestamp(),
 status=$4::varchar AND delivery_fence=$3 AND delivery_token IS NULL
 AND COALESCE(provider_id,'')=$5 AND COALESCE(last_error,'')=$6
 AND ($4::varchar IN ('SENT','ATTENTION','CANCELLED') OR delivery_due_at=$7::timestamptz)
 FROM notifications WHERE id=$1`, id, c.Token, c.Fence, v.State, v.ProviderID, v.ErrorCategory, v.NextAttemptAt).Scan(&owned, &applied); err != nil {
		return false, err
	}
	if applied {
		return true, nil
	}
	if !owned {
		return false, nil
	}
	tag, err := tx.Exec(ctx, `UPDATE notifications SET status=$4::varchar,provider_id=NULLIF($5::text,''),last_error=NULLIF($6::text,''),
 sent_at=CASE WHEN $4::varchar='SENT' THEN clock_timestamp() ELSE sent_at END,
 delivery_token=NULL,delivery_lease_until=NULL,
 delivery_due_at=CASE WHEN $4::varchar IN ('SENT','ATTENTION','CANCELLED') THEN NULL ELSE $7::timestamptz END
 WHERE id=$1 AND managed AND status='DELIVERING' AND delivery_token=$2 AND delivery_fence=$3 AND delivery_lease_until>clock_timestamp()`, id, c.Token, c.Fence, v.State, v.ProviderID, v.ErrorCategory, v.NextAttemptAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		// Fresh bounded read even when the original operation context expired.
		readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		var confirmed bool
		readErr := r.pool.QueryRow(readCtx, `SELECT status=$2::varchar AND delivery_fence=$3 AND delivery_token IS NULL
 AND COALESCE(provider_id,'')=$4 AND COALESCE(last_error,'')=$5
 AND ($2::varchar IN ('SENT','ATTENTION','CANCELLED') OR delivery_due_at=$6::timestamptz)
 FROM notifications WHERE id=$1`, id, v.State, c.Fence, v.ProviderID, v.ErrorCategory, v.NextAttemptAt).Scan(&confirmed)
		if readErr == nil && confirmed {
			return true, nil
		}
		return false, err
	}
	return true, nil
}
