package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
)

type PostgresNotificationRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresNotificationRepository(pool *pgxpool.Pool) *PostgresNotificationRepository {
	return &PostgresNotificationRepository{pool: pool}
}

func (r *PostgresNotificationRepository) Create(ctx context.Context, n *domain.Notification) error {
	if n.Managed {
		payload, err := payloadWithRecipient(n.Payload, n.Recipient)
		if err != nil {
			return err
		}
		key, _, hash, err := domain.PrepareIntent(n.Type, payload)
		if err != nil {
			return err
		}
		_, _, err = r.AcceptIntent(ctx, key, n, hash)
		return err
	}
	payload, err := json.Marshal(n.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	query := `INSERT INTO notifications (id, type, recipient, channel, payload, status, created_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err = r.pool.Exec(ctx, query, n.ID, n.Type, n.Recipient, n.Channel, payload, n.Status, n.CreatedAt)
	if err != nil {
		return fmt.Errorf("create notification: %w", err)
	}
	return nil
}

func (r *PostgresNotificationRepository) ProcessOnce(ctx context.Context, n *domain.Notification, send func(context.Context, *domain.Notification) error) (bool, error) {
	payload, err := payloadWithRecipient(n.Payload, n.Recipient)
	if err != nil {
		return false, fmt.Errorf("marshal payload: %w", err)
	}

	key, _, hash, err := domain.PrepareIntent(n.Type, payload)
	if err != nil {
		return false, err
	}
	_, created, err := r.AcceptIntent(ctx, key, n, hash)
	return created, err
}

func (r *PostgresNotificationRepository) MarkSent(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE notifications SET status = 'SENT', sent_at = NOW() WHERE id = $1 AND NOT managed`
	result, err := r.pool.Exec(ctx, query, id)
	if err == nil && result.RowsAffected() == 0 {
		return ErrLegacyAttention
	}
	return err
}
