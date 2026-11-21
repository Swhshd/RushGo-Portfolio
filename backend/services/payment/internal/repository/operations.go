package repository

import (
	"context"
	"github.com/google/uuid"
)

type OperationsRepository interface {
	RefundPreparedStale(context.Context, uuid.UUID) (bool, error)
}

func (r *PostgresPaymentRepository) RefundPreparedStale(ctx context.Context, id uuid.UUID) (bool, error) {
	var stale bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_refund_audit p WHERE p.payment_id=$1 AND p.record_kind='PREPARED' AND p.recorded_at<clock_timestamp()-interval '15 minutes' AND NOT EXISTS(SELECT 1 FROM payment_refund_audit r WHERE r.payment_id=p.payment_id AND r.application_attempt_id=p.application_attempt_id AND r.record_kind='RESPONSE'))`, id).Scan(&stale)
	return stale, err
}
