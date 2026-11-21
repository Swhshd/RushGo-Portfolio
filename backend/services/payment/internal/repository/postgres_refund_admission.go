package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

// DeferRefundForAdmission neutrally releases only a current refund claim whose
// final guard returned a committed positive coordination-wait proof. It restores
// the exact preclaim attempt count and refund horizon without touching late
// financial facts or a successor's lease.
func (p *PostgresPaymentRepository) DeferRefundForAdmission(ctx context.Context, claim domain.FinancialClaim, guard RefundPreSendResult, nextAttempt time.Time) (bool, error) {
	if claim.Kind != "refund" || claim.PaymentID == uuid.Nil || claim.Token == uuid.Nil || claim.Fence <= 0 ||
		guard.Decision != RefundPreSendCoordinationWait || !guard.coordinationWaitProof || claim.PriorAttempts < 0 ||
		!validPriorRefundClaimState(claim.PriorRefundState) {
		return false, ErrInvalidFinancialIdentity
	}
	now := time.Now().UTC()
	if !nextAttempt.After(now) || nextAttempt.After(now.Add(5*time.Minute)) {
		return false, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("%w: begin neutral refund defer: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())
	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return false, err
	}
	if !active {
		return false, fmt.Errorf("%w: Payment admission is inactive during neutral defer", admission.ErrUnavailable)
	}
	var lockedID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE`, claim.PaymentID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, RecordNotFound
	}
	if err != nil {
		return false, fmt.Errorf("%w: lock Payment for neutral refund defer: %v", admission.ErrUnavailable, err)
	}
	var currentToken *uuid.UUID
	var currentFence int64
	var leaseValid bool
	var currentKind *string
	var refundState string
	err = tx.QueryRow(ctx, `SELECT claim_token,claim_fence,COALESCE(lease_until>clock_timestamp(),false),operation_kind,refund_state
		FROM payments WHERE id=$1`, claim.PaymentID).Scan(&currentToken, &currentFence, &leaseValid, &currentKind, &refundState)
	if err != nil {
		return false, fmt.Errorf("%w: reload refund owner for neutral defer: %v", admission.ErrUnavailable, err)
	}
	if currentToken == nil || *currentToken != claim.Token || currentFence != claim.Fence || !leaseValid || currentKind == nil || *currentKind != "refund" {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("%w: stale neutral defer read commit: %v", admission.ErrUnavailable, err)
		}
		return false, nil
	}

	var tag pgconn.CommandTag
	if refundState == "succeeded" {
		// An observation that completed the refund wins over this neutral defer.
		// Release only this still-current lease; keep all observed financial facts.
		tag, err = tx.Exec(ctx, `UPDATE payments SET claim_token=NULL,lease_until=NULL,operation_kind=NULL
			WHERE id=$1 AND claim_token=$2 AND claim_fence=$3 AND lease_until>clock_timestamp()
			AND operation_kind='refund' AND refund_state='succeeded'`, claim.PaymentID, claim.Token, claim.Fence)
	} else if refundState == "claimed" {
		tag, err = tx.Exec(ctx, `UPDATE payments SET refund_state=$4,attempts=$5,refund_first_attempt_at=$6,
			claim_token=NULL,lease_until=NULL,operation_kind=NULL,due_at=$7,updated_at=clock_timestamp()
			WHERE id=$1 AND claim_token=$2 AND claim_fence=$3 AND lease_until>clock_timestamp()
			AND operation_kind='refund' AND refund_state='claimed'`, claim.PaymentID, claim.Token, claim.Fence,
			claim.PriorRefundState, claim.PriorAttempts, claim.PriorRefundFirstAttemptAt, nextAttempt)
	} else {
		// A late outcome may have changed the state while the worker was running.
		// Do not restore stale bookkeeping over it.
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("%w: late refund observation read commit: %v", admission.ErrUnavailable, err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: restore preclaim refund bookkeeping: %v", admission.ErrUnavailable, err)
	}
	owned := tag.RowsAffected() == 1
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: neutral refund defer commit outcome unknown: %v", admission.ErrUnavailable, err)
	}
	return owned, nil
}

func validPriorRefundClaimState(state string) bool {
	switch state {
	case "pending", "claimed", "retryable", "provider_pending", "unknown":
		return true
	default:
		return false
	}
}
