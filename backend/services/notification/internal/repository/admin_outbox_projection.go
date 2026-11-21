package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	AdminOutboxProjectionCap       = 5000
	adminOutboxProjectionReadLimit = AdminOutboxProjectionCap + 1
	adminOutboxReadTimeout         = 8 * time.Second
)

var ErrAdminOutboxCapacityExceeded = errors.New("notification admin outbox projection exceeds capacity")

// AdminOutboxProjection contains only durable, low-risk Notification fields
// approved for the read-only Admin view. Recipient, payload, provider identity,
// attempts, leases, and raw errors are deliberately excluded.
type AdminOutboxProjection struct {
	ID        uuid.UUID
	EventType string
	Status    string
	CreatedAt time.Time
	Managed   bool
}

// AdminOutboxProjectionReader is an additive query capability kept separate
// from NotificationRepository so existing repository doubles stay compatible.
type AdminOutboxProjectionReader interface {
	ReadAdminOutboxProjection(context.Context) ([]AdminOutboxProjection, error)
}

func (r *PostgresNotificationRepository) ReadAdminOutboxProjection(ctx context.Context) ([]AdminOutboxProjection, error) {
	if r == nil || r.pool == nil {
		return nil, errors.New("Notification admin outbox reader unavailable")
	}
	bounded, cancel := context.WithTimeout(ctx, adminOutboxReadTimeout)
	defer cancel()

	tx, err := r.pool.BeginTx(bounded, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollbackNotification(tx)
	if _, err := tx.Exec(bounded, `SET LOCAL statement_timeout = '5s'`); err != nil {
		return nil, err
	}

	// Intentionally scan the whole source in primary-key order. Legacy rows
	// consume the same source-wide capacity bound, so managed is filtered only
	// after the 5,001-row sentinel has been observed.
	rows, err := tx.Query(bounded, `
		SELECT id, type, status, created_at, managed
		FROM notifications
		ORDER BY id
		LIMIT $1`, adminOutboxProjectionReadLimit)
	if err != nil {
		return nil, err
	}
	projection := make([]AdminOutboxProjection, 0, adminOutboxProjectionReadLimit)
	for rows.Next() {
		var record AdminOutboxProjection
		if err := rows.Scan(&record.ID, &record.EventType, &record.Status, &record.CreatedAt, &record.Managed); err != nil {
			rows.Close()
			return nil, err
		}
		projection = append(projection, record)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, rowsErr
	}
	if len(projection) > AdminOutboxProjectionCap {
		return nil, ErrAdminOutboxCapacityExceeded
	}
	if err := tx.Commit(bounded); err != nil {
		return nil, err
	}
	return projection, nil
}
