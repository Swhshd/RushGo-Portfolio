package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

// SwitchToCompensation retains the live claim but changes its direction under
// the same fence. The caller has first obtained Booking's winning terminal CAS.
func (r *SagaRepository) SwitchToCompensation(ctx context.Context, c domain.SagaClaim) (*domain.SagaClaim, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	v, owned, err := ownedSagaClaim(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	if !owned {
		return nil, ErrRecoveryConflict
	}
	if v.Direction != "forward" {
		return &c, nil
	}
	index := v.CurrentStepIndex
	if index > 4 {
		index = 4
	}
	if index < 2 {
		index = 2
	}
	// Include the failing/intended effect: an absent reserve closes to a tombstone,
	// and an uncertain create remains financial responsibility until proven safe.
	tag, err := tx.Exec(ctx, `UPDATE sagas SET direction='compensate',status='ROLLING_BACK',current_step_index=$2,next_attempt_at=clock_timestamp() WHERE id=$1 AND lease_until>clock_timestamp()`, v.ID, index)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrRecoveryConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("Saga direction commit outcome unknown: %w", err)
	}
	c.Direction = "compensate"
	v.Direction = "compensate"
	v.Status = domain.SAGA_ROLLING_BACK
	v.CurrentStepIndex = index
	c.Saga = v
	return &c, nil
}

// PersistClaimFailure releases a live claim when execution fails before a
// step result can be written. Ownership is checked after the row lock so a
// stale worker cannot release or reschedule a newer owner's work.
func (r *SagaRepository) PersistClaimFailure(ctx context.Context, c domain.SagaClaim, category string, next time.Time, attention bool) (bool, error) {
	if category == "" || next.IsZero() {
		return false, ErrInvalidRecoveryInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var owned bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(claim_token=$2 AND claim_fence=$3 AND lease_until>clock_timestamp() AND recovery_state='ready' AND direction=$4,FALSE) FROM sagas WHERE id=$1 FOR UPDATE`, c.SagaID, c.Token, c.Fence, c.Direction).Scan(&owned); err != nil {
		return false, err
	}
	if !owned {
		return false, nil
	}
	recoveryState := "ready"
	if attention {
		recoveryState = "attention"
	}
	tag, err := tx.Exec(ctx, `UPDATE sagas SET recovery_state=$2,error_category=$3,next_attempt_at=$4,claim_token=NULL,lease_until=NULL,failure_attempts=failure_attempts+1 WHERE id=$1 AND claim_token=$5 AND claim_fence=$6 AND lease_until>clock_timestamp() AND recovery_state='ready' AND direction=$7`, c.SagaID, recoveryState, category, next, c.Token, c.Fence, c.Direction)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("Saga claim failure commit outcome unknown: %w", err)
	}
	return true, nil
}

// SettleSaga accepts a service-verified current Booking disposition and durable
// Payment obligation identity. It never reopens the forward direction.
func (r *SagaRepository) SettleSaga(ctx context.Context, booking uuid.UUID, generation int64, payment, obligation *uuid.UUID, terminal bool, reason string) error {
	if booking == uuid.Nil || generation != 1 || reason == "" || (payment != nil && *payment == uuid.Nil) || (obligation != nil && *obligation == uuid.Nil) {
		return ErrInvalidRecoveryInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var id uuid.UUID
	var savedPayment, last *uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT id,payment_id,last_obligation_id FROM sagas WHERE booking_id=$1 AND lifecycle_generation=$2 AND recovery_managed FOR UPDATE`, booking, generation).Scan(&id, &savedPayment, &last); err != nil {
		return err
	}
	v, err := loadSaga(ctx, tx, id)
	if err != nil {
		return err
	}
	if err = validateManagedGraph(v); err != nil {
		return err
	}
	if payment != nil && savedPayment != nil && *payment != *savedPayment {
		return ErrRecoveryConflict
	}
	if v.Status == domain.SAGA_COMPLETED {
		if terminal {
			return ErrRecoveryConflict
		}
		return tx.Commit(ctx)
	}
	newObligation := terminal && obligation != nil && (last == nil || *last != *obligation)
	if v.Status == domain.SAGA_ROLLED_BACK && !newObligation {
		return tx.Commit(ctx)
	}
	if newObligation && v.Direction == "compensate" && v.CurrentStepIndex <= 1 {
		// A genuinely new collection only reopens original S1 resolution. S0 is
		// already closed and every higher original step retains its durable history.
		if _, err = tx.Exec(ctx, `UPDATE saga_steps SET compensate_state='pending',compensated_at=NULL,status=CASE execute_state WHEN 'completed' THEN 'COMPLETED'::step_status WHEN 'intended' THEN 'EXECUTING'::step_status WHEN 'pending' THEN 'PENDING'::step_status ELSE 'FAILED'::step_status END WHERE saga_id=$1 AND "order"=1`, id); err != nil {
			return err
		}
		// Revoke an in-flight S0 close result too: it cannot terminate a graph
		// whose newly collected Payment still needs original S1 resolution.
		if v.CurrentStepIndex == 0 {
			if _, err = tx.Exec(ctx, `UPDATE saga_steps SET compensate_state='pending',status=CASE execute_state WHEN 'completed' THEN 'COMPLETED'::step_status WHEN 'intended' THEN 'EXECUTING'::step_status WHEN 'pending' THEN 'PENDING'::step_status ELSE 'FAILED'::step_status END WHERE saga_id=$1 AND "order"=0`, id); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE sagas SET status='ROLLING_BACK',direction='compensate',current_step_index=1,last_obligation_id=$2,claim_token=NULL,lease_until=NULL WHERE id=$1`, id, obligation); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE sagas SET payment_id=COALESCE(payment_id,$2),last_obligation_id=COALESCE($3,last_obligation_id),wake_reason=$4,wake_version=wake_version+1,next_attempt_at=clock_timestamp(),status=CASE WHEN status='WAIT_FOR_PAYMENT' THEN 'PROCESSING'::saga_status ELSE status END WHERE id=$1`, id, payment, obligation, reason); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("Saga settlement commit outcome unknown: %w", err)
	}
	return nil
}

var _ ReplayRepository = (*SagaRepository)(nil)
