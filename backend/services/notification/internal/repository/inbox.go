package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ticketbox/notification/internal/domain"
)

const inboxQueryTimeout = 5 * time.Second

func (r *PostgresNotificationRepository) AcceptBusinessNotification(ctx context.Context, event *domain.BusinessNotificationV1, hash [32]byte, rendered domain.InboxTemplate, recipient string, emailVerified bool) (bool, error) {
	if event == nil || event.NotificationID == uuid.Nil || event.UserID == uuid.Nil || rendered.Locale != "en" && rendered.Locale != "zh" || rendered.Title == "" || rendered.Body == "" || emailVerified && recipient == "" {
		return false, domain.ErrInvalidBusinessNotification
	}
	canonical, err := json.Marshal(event)
	if err != nil {
		return false, domain.ErrInvalidBusinessNotification
	}
	_, actualHash, err := domain.ParseBusinessNotificationV1(canonical)
	if err != nil || actualHash != hash {
		return false, domain.ErrInvalidBusinessNotification
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackNotification(tx)
	tag, err := tx.Exec(ctx, `INSERT INTO notification_inbox(notification_id,user_id,booking_id,event_id,type,locale,title,body,payload_hash,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp()) ON CONFLICT(notification_id) DO NOTHING`,
		event.NotificationID, event.UserID, event.BookingID, event.EventID, event.Type, rendered.Locale,
		rendered.Title, rendered.Body, hash[:])
	if err != nil {
		return false, fmt.Errorf("persist Notification inbox intent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var owner uuid.UUID
		var existingHash []byte
		if err := tx.QueryRow(ctx, `SELECT user_id,payload_hash FROM notification_inbox WHERE notification_id=$1 FOR UPDATE`, event.NotificationID).Scan(&owner, &existingHash); err != nil {
			return false, fmt.Errorf("reload Notification inbox intent: %w", err)
		}
		if owner != event.UserID || !bytes.Equal(existingHash, hash[:]) {
			return false, ErrSemanticConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate Notification inbox read: %w", err)
		}
		return false, nil
	}

	if emailVerified {
		payload, err := json.Marshal(event)
		if err != nil {
			return false, domain.ErrInvalidBusinessNotification
		}
		_, err = tx.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,locale,payload,status,created_at,managed,entity_id,transition_generation,payload_hash,delivery_id,delivery_fence,delivery_due_at,delivery_attempts)
			VALUES($1,$2,$3,'email',$4,$5,'PENDING',clock_timestamp(),true,$1,1,$6,$1,0,clock_timestamp(),0)`,
			event.NotificationID, event.Type, recipient, rendered.Locale, payload, hash[:])
		if err != nil {
			return false, fmt.Errorf("persist Notification email delivery intent: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit Notification business intent: %w", err)
	}
	return true, nil
}

func (r *PostgresNotificationRepository) ListInbox(ctx context.Context, userID uuid.UUID, page, pageSize int32, unreadOnly bool) (InboxPage, error) {
	if userID == uuid.Nil || page < 1 || page > 1000 || pageSize < 1 || pageSize > 100 {
		return InboxPage{}, ErrInvalidInboxQuery
	}
	offset := int64(page-1) * int64(pageSize)
	ctx, cancel := context.WithTimeout(ctx, inboxQueryTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return InboxPage{}, err
	}
	defer rollbackNotification(tx)
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout='3s'`); err != nil {
		return InboxPage{}, err
	}
	var result InboxPage
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM notification_inbox WHERE user_id=$1 AND (NOT $2 OR read_at IS NULL)`, userID, unreadOnly).Scan(&result.Total); err != nil {
		return InboxPage{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM notification_inbox WHERE user_id=$1 AND read_at IS NULL`, userID).Scan(&result.UnreadCount); err != nil {
		return InboxPage{}, err
	}
	rows, err := tx.Query(ctx, `SELECT notification_id,user_id,booking_id,event_id,type,locale,title,body,created_at,read_at
		FROM notification_inbox WHERE user_id=$1 AND (NOT $2 OR read_at IS NULL)
		ORDER BY created_at DESC,notification_id DESC LIMIT $3 OFFSET $4`, userID, unreadOnly, pageSize, offset)
	if err != nil {
		return InboxPage{}, err
	}
	result.Items = make([]domain.BusinessInboxRecord, 0, pageSize)
	for rows.Next() {
		var item domain.BusinessInboxRecord
		var owner uuid.UUID
		var bookingID pgtype.UUID
		if err := rows.Scan(&item.ID, &owner, &bookingID, &item.EventID, &item.Type, &item.Locale, &item.Title, &item.Body, &item.CreatedAt, &item.ReadAt); err != nil {
			rows.Close()
			return InboxPage{}, err
		}
		if bookingID.Valid {
			item.BookingID = uuid.UUID(bookingID.Bytes)
		}
		if owner != userID {
			rows.Close()
			return InboxPage{}, errors.New("Notification inbox owner projection mismatch")
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return InboxPage{}, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return InboxPage{}, err
	}
	return result, nil
}

func (r *PostgresNotificationRepository) MarkInboxRead(ctx context.Context, userID, notificationID uuid.UUID) (time.Time, error) {
	if userID == uuid.Nil || notificationID == uuid.Nil {
		return time.Time{}, ErrInboxNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var readAt time.Time
	err := r.pool.QueryRow(ctx, `UPDATE notification_inbox SET read_at=COALESCE(read_at,clock_timestamp())
		WHERE notification_id=$1 AND user_id=$2 RETURNING read_at`, notificationID, userID).Scan(&readAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrInboxNotFound
	}
	return readAt, err
}

var _ BusinessNotificationRepository = (*PostgresNotificationRepository)(nil)
