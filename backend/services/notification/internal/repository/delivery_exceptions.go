package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (r *PostgresNotificationRepository) ReadDeliveryExceptions(ctx context.Context, from, to time.Time, limit, offset int32) ([]DeliveryException, bool, time.Time, error) {
	if r == nil || r.pool == nil || from.IsZero() || to.IsZero() || !from.Before(to) || to.Sub(from) > 31*24*time.Hour || limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		return nil, false, time.Time{}, ErrInvalidInboxQuery
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, false, time.Time{}, err
	}
	defer rollbackNotification(tx)
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout='5s'`); err != nil {
		return nil, false, time.Time{}, err
	}
	var observedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&observedAt); err != nil {
		return nil, false, time.Time{}, err
	}
	rows, err := tx.Query(ctx, `SELECT type,status,created_at,payload->>'booking_id',payload->>'event_id'
		FROM notifications
		WHERE managed AND type IN ('tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention')
		  AND created_at >= $1 AND created_at < $2
		  AND (status IN ('UNKNOWN','ATTENTION')
		       OR status='PENDING' AND created_at <= clock_timestamp()-interval '15 minutes'
		       OR status='RETRYABLE' AND delivery_attempts >= 8)
		ORDER BY created_at DESC,id DESC LIMIT $3 OFFSET $4`, from, to, limit+1, offset)
	if err != nil {
		return nil, false, time.Time{}, err
	}
	items := make([]DeliveryException, 0, limit+1)
	for rows.Next() {
		var typ, state, bookingIDRaw, eventIDRaw string
		var createdAt time.Time
		if err := rows.Scan(&typ, &state, &createdAt, &bookingIDRaw, &eventIDRaw); err != nil {
			rows.Close()
			return nil, false, time.Time{}, err
		}
		bookingID, err := uuid.Parse(bookingIDRaw)
		if err != nil || bookingID == uuid.Nil {
			rows.Close()
			return nil, false, time.Time{}, fmt.Errorf("invalid persisted notification booking identity: %w", ErrSemanticConflict)
		}
		eventID, err := uuid.Parse(eventIDRaw)
		if err != nil || eventID == uuid.Nil {
			rows.Close()
			return nil, false, time.Time{}, fmt.Errorf("invalid persisted notification event identity: %w", ErrSemanticConflict)
		}
		items = append(items, DeliveryException{BookingID: bookingID, EventID: eventID, Type: typ, State: state, CreatedAt: createdAt})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, false, time.Time{}, err
	}
	rows.Close()
	more := len(items) > int(limit)
	if more {
		items = items[:limit]
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, time.Time{}, err
	}
	return items, more, observedAt.UTC(), nil
}

var _ interface {
	ReadDeliveryExceptions(context.Context, time.Time, time.Time, int32, int32) ([]DeliveryException, bool, time.Time, error)
} = (*PostgresNotificationRepository)(nil)
