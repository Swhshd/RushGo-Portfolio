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

var ErrAdminOutboxCapacityExceeded = errors.New("admin outbox projection exceeds capacity")

// AdminOutboxProjection contains only the durable fields authorized for the
// read-only Admin view. Event keys, payloads, and error details are excluded.
type AdminOutboxProjection struct {
	ID        uuid.UUID
	EventType string
	Published bool
	CreatedAt time.Time
}

// AdminOutboxProjectionReader is an additive capability kept separate from
// BookingRepository so existing repository doubles remain source-compatible.
type AdminOutboxProjectionReader interface {
	ReadAdminOutboxProjection(context.Context) ([]AdminOutboxProjection, error)
}

func (r *PostgresBookingRepository) ReadAdminOutboxProjection(ctx context.Context) ([]AdminOutboxProjection, error) {
	if r == nil || r.pool == nil {
		return nil, errors.New("Booking outbox reader unavailable")
	}
	bounded, cancel := context.WithTimeout(ctx, adminOutboxReadTimeout)
	defer cancel()

	tx, err := r.pool.BeginTx(bounded, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(bounded, `SET LOCAL statement_timeout = '5s'`); err != nil {
		return nil, err
	}

	rows, err := tx.Query(bounded, `
		SELECT id, event_type, published, created_at
		FROM outbox
		ORDER BY id
		LIMIT $1`, adminOutboxProjectionReadLimit)
	if err != nil {
		return nil, err
	}
	projection := make([]AdminOutboxProjection, 0, adminOutboxProjectionReadLimit)
	for rows.Next() {
		var record AdminOutboxProjection
		if err := rows.Scan(&record.ID, &record.EventType, &record.Published, &record.CreatedAt); err != nil {
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
	if err := tx.Commit(bounded); err != nil {
		return nil, err
	}
	if len(projection) > AdminOutboxProjectionCap {
		return nil, ErrAdminOutboxCapacityExceeded
	}
	return projection, nil
}
