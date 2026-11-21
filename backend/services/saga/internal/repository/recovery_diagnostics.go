package repository

import (
	"context"
	"time"
)

func (r *SagaRepository) RecoveryStats(ctx context.Context, maxAttempts int) (due, attention int64, oldest float64, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err = r.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE recovery_state='ready' AND status IN ('PENDING','PROCESSING','ROLLING_BACK','FAIL','WAIT_FOR_PAYMENT') AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())),count(*) FILTER(WHERE recovery_state='attention' OR (recovery_state='ready' AND status IN ('PENDING','PROCESSING','ROLLING_BACK','FAIL','WAIT_FOR_PAYMENT') AND failure_attempts>=$1)),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(next_attempt_at) FILTER(WHERE recovery_state='ready' AND status IN ('PENDING','PROCESSING','ROLLING_BACK','FAIL','WAIT_FOR_PAYMENT') AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()))),0)::double precision FROM sagas WHERE recovery_managed`, maxAttempts).Scan(&due, &attention, &oldest)
	return
}
