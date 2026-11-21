package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/payment/internal/domain"
)

func (p *PostgresPaymentRepository) ClaimFinancialWork(ctx context.Context, limit int, lease time.Duration) ([]domain.FinancialClaim, error) {
	return p.claimFinancialWork(ctx, nil, limit, lease, false)
}
func (p *PostgresPaymentRepository) ClaimCreatePayment(ctx context.Context, id uuid.UUID, lease time.Duration) (*domain.FinancialClaim, error) {
	claims, err := p.claimFinancialWork(ctx, &id, 1, lease, true)
	if err != nil || len(claims) == 0 {
		return nil, err
	}
	return &claims[0], nil
}
func (p *PostgresPaymentRepository) claimFinancialWork(ctx context.Context, id *uuid.UUID, limit int, lease time.Duration, createOnly bool) ([]domain.FinancialClaim, error) {
	if limit < 1 || limit > 16 || lease <= 0 || lease > time.Minute {
		return nil, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Protected worker writes share the same activation barrier as permit grants
	// and refund PREPARED. This lock is acquired before selecting Payment rows.
	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id FROM payments WHERE managed AND ($1::uuid IS NULL OR id=$1)
	 AND (create_state IN ('create_pending','create_unknown','create_claimed') OR (NOT $3 AND (
	 (refund_state IN ('pending','claimed','retryable','provider_pending','unknown') AND (NOT $4 OR NOT EXISTS (
		SELECT 1 FROM payment_admission_permits ap WHERE ap.payment_id=payments.id AND ap.state='PENDING'))) OR
	 (provider_outcome<>'collected' AND cancel_state IN ('cancel_pending','cancel_claimed','cancel_unknown')) OR
	 (create_state IN ('create_ready','create_attention') AND reconciliation_state IN ('pending','claimed')))))
	 AND due_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY due_at,id LIMIT $2 FOR UPDATE SKIP LOCKED`, id, limit, createOnly, active)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var value uuid.UUID
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var claims []domain.FinancialClaim
	for _, id := range ids {
		c := domain.FinancialClaim{PaymentID: id, Token: uuid.New(), Kind: "create"}
		var state, refund, cancelState, outcome string
		var priorAttempts int
		var priorRefundFirstAttemptAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT create_state,refund_state,cancel_state,provider_outcome,attempts,refund_first_attempt_at FROM payments WHERE id=$1`, id).
			Scan(&state, &refund, &cancelState, &outcome, &priorAttempts, &priorRefundFirstAttemptAt); err != nil {
			return nil, err
		}
		if state == "create_ready" || state == "create_attention" {
			c.Kind = "reconcile"
		}
		if !createOnly {
			if refund == "pending" || refund == "claimed" || refund == "retryable" || refund == "provider_pending" || refund == "unknown" {
				c.Kind = "refund"
			} else if outcome != "collected" && (cancelState == "cancel_pending" || cancelState == "cancel_claimed" || cancelState == "cancel_unknown") {
				c.Kind = "cancel"
			}
		}
		if active && c.Kind == "refund" {
			// Recheck after the Payment FOR UPDATE lock, using a fresh statement
			// snapshot before touching attempts or refund_first_attempt_at.
			var pendingAdmission bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_admission_permits WHERE payment_id=$1 AND state='PENDING')`, id).Scan(&pendingAdmission); err != nil {
				return nil, fmt.Errorf("recheck pending Payment admission before refund claim: %w", err)
			}
			if pendingAdmission {
				continue
			}
		}
		c.PriorAttempts = priorAttempts
		c.PriorRefundState = refund
		if priorRefundFirstAttemptAt != nil {
			prior := *priorRefundFirstAttemptAt
			c.PriorRefundFirstAttemptAt = &prior
		}
		c.Operation.PaymentID = id
		var hash []byte
		err := tx.QueryRow(ctx, `UPDATE payments SET claim_token=$2,claim_fence=claim_fence+1,operation_kind=$4,lease_until=clock_timestamp()+$3::interval,
		 create_first_attempt_at=CASE WHEN $4='create' THEN COALESCE(create_first_attempt_at,clock_timestamp()) ELSE create_first_attempt_at END,
		 cancel_first_attempt_at=CASE WHEN $4='cancel' THEN COALESCE(cancel_first_attempt_at,clock_timestamp()) ELSE cancel_first_attempt_at END,
		 refund_first_attempt_at=CASE WHEN $4='refund' THEN COALESCE(refund_first_attempt_at,clock_timestamp()) ELSE refund_first_attempt_at END,
		 create_state=CASE WHEN $4='create' THEN 'create_claimed' ELSE create_state END,reconciliation_state=CASE WHEN $4='reconcile' THEN 'claimed' ELSE reconciliation_state END,
		 cancel_state=CASE WHEN $4='cancel' THEN 'cancel_claimed' ELSE cancel_state END,refund_state=CASE WHEN $4='refund' THEN 'claimed' ELSE refund_state END,
		 attempts=CASE WHEN $4='reconcile' THEN attempts WHEN ($4='refund' AND refund_first_attempt_at IS NULL) OR ($4='cancel' AND cancel_first_attempt_at IS NULL) THEN 1 ELSE attempts+1 END
		 WHERE id=$1 RETURNING claim_fence,lease_until,CASE WHEN $4='refund' THEN refund_first_attempt_at WHEN $4='cancel' THEN cancel_first_attempt_at ELSE create_first_attempt_at END,
		 CASE WHEN $4='refund' THEN refund_key WHEN $4='cancel' THEN cancel_key ELSE create_key END,booking_id,user_id,payment_generation,provider_account,livemode,price,currency,request_hash,frozen_create,attempts`, id, c.Token, fmt.Sprintf("%f seconds", lease.Seconds()), c.Kind).
			Scan(&c.Fence, &c.LeaseUntil, &c.FirstAttemptAt, &c.Key, &c.Operation.Identity.BookingID, &c.Operation.Identity.UserID, &c.Operation.Identity.Generation, &c.Operation.Identity.ProviderAccount, &c.Operation.Identity.LiveMode, &c.Operation.Identity.AmountCents, &c.Operation.Identity.Currency, &hash, &c.Operation.FrozenParams, &c.Attempts)
		if err != nil {
			return nil, err
		}
		copy(c.Operation.Identity.Fingerprint[:], hash)
		c.Operation.Key = c.Key
		if c.Kind == "reconcile" {
			c.Key = ""
			c.FirstAttemptAt = nil
		}
		if c.Kind == "refund" {
			c.Refund = &domain.RefundObligation{PaymentID: id, Generation: c.Operation.Identity.Generation, Key: c.Key}
			if err := tx.QueryRow(ctx, `SELECT refund_decision_id,refund_amount_cents,refund_currency,refund_reason FROM payments WHERE id=$1`, id).Scan(&c.Refund.DecisionID, &c.Refund.AmountCents, &c.Refund.Currency, &c.Refund.Reason); err != nil {
				return nil, err
			}
		}
		c.View, err = scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE id=$1`, id))
		if err != nil {
			return nil, err
		}
		claims = append(claims, c)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("create claim outcome unknown: %w", err)
	}
	return claims, nil
}

func (p *PostgresPaymentRepository) FinishFinancialWork(ctx context.Context, c domain.FinancialClaim, result domain.FinancialResult) (bool, error) {
	if (c.Kind != "create" && c.Kind != "reconcile" && c.Kind != "refund" && c.Kind != "cancel") || c.Token == uuid.Nil || c.Fence < 1 {
		return false, ErrInvalidFinancialIdentity
	}
	if c.Kind == "create" && result.State != "create_ready" && result.State != "create_unknown" && result.State != "create_attention" {
		return false, ErrInvalidFinancialIdentity
	}
	if result.State == "create_ready" && result.Observation == nil {
		return false, ErrProviderEvidenceConflict
	}
	if c.Kind == "reconcile" && result.State != "pending" && result.State != "sale_verified" && result.State != "refund_required" && result.State != "resolved" && result.State != "attention" {
		return false, ErrInvalidFinancialIdentity
	}
	if c.Kind == "refund" && result.State != "retryable" && result.State != "provider_pending" && result.State != "succeeded" && result.State != "attention" {
		return false, ErrInvalidFinancialIdentity
	}
	if c.Kind == "cancel" && result.State != "cancel_unknown" && result.State != "cancelled" && result.State != "cancel_attention" {
		return false, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var token *uuid.UUID
	var fence int64
	var leaseValid bool
	var kind *string
	err = tx.QueryRow(ctx, `SELECT claim_token,claim_fence,COALESCE(lease_until>clock_timestamp(),false),operation_kind FROM payments WHERE id=$1 AND managed FOR UPDATE`, c.PaymentID).Scan(&token, &fence, &leaseValid, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, RecordNotFound
	}
	if err != nil {
		return false, err
	}
	owned := token != nil && *token == c.Token && fence == c.Fence && leaseValid && kind != nil && *kind == c.Kind
	if result.Observation != nil {
		if result.Observation.PaymentID != c.PaymentID {
			return false, ErrProviderEvidenceConflict
		}
		// Trusted late evidence survives a stale execution lease, while the
		// current worker's lease, due state and result remain fenced.
		if c.Kind == "refund" {
			if !result.Observation.Collected {
				return false, ErrProviderEvidenceConflict
			}
			if err := retainRefundObservation(ctx, tx, c, *result.Observation, owned); err != nil {
				return false, err
			}
		} else if err := recordFinancialObservation(ctx, tx, *result.Observation); err != nil {
			return false, err
		}
	}
	if owned {
		next := result.NextAttemptAt
		if next.IsZero() {
			next = time.Now().Add(time.Second)
		}
		if c.Kind == "create" {
			_, err = tx.Exec(ctx, `UPDATE payments SET create_state=CASE WHEN payment_intent_id IS NOT NULL AND payment_intent_id<>'' THEN 'create_ready' ELSE $2 END,
		 claim_token=NULL,lease_until=NULL,operation_kind=NULL,last_error=$3,due_at=$4,updated_at=clock_timestamp() WHERE id=$1`, c.PaymentID, result.State, result.ErrorCategory, next)
		} else if c.Kind == "refund" {
			if result.State == "succeeded" && (result.Observation == nil || result.Observation.SucceededRefundCents != c.Operation.Identity.AmountCents) {
				return false, ErrProviderEvidenceConflict
			}
			_, err = tx.Exec(ctx, `UPDATE payments SET refund_state=CASE WHEN refund_state='succeeded' THEN 'succeeded' ELSE $2 END,
			 reconciliation_state=CASE WHEN refund_state='succeeded' OR $2='succeeded' THEN 'resolved' WHEN $2='attention' THEN 'attention' ELSE 'refund_required' END,
			 claim_token=NULL,lease_until=NULL,operation_kind=NULL,last_error=$3,due_at=$4,updated_at=clock_timestamp() WHERE id=$1`, c.PaymentID, result.State, result.ErrorCategory, next)
		} else if c.Kind == "cancel" {
			_, err = tx.Exec(ctx, `UPDATE payments SET cancel_state=CASE WHEN cancel_state='cancelled' THEN cancel_state ELSE $2 END,
			 reconciliation_state=CASE WHEN provider_outcome='collected' THEN 'pending' WHEN provider_status='canceled' AND $2='cancelled' THEN 'resolved' WHEN $2='cancel_attention' THEN 'attention' ELSE 'pending' END,
			 claim_token=NULL,lease_until=NULL,operation_kind=NULL,last_error=$3,due_at=$4,updated_at=clock_timestamp() WHERE id=$1`, c.PaymentID, result.State, result.ErrorCategory, next)
		} else {
			if result.BookingDisposition != "" {
				if result.BookingDisposition == "CONFIRMED" {
					if result.DecisionID != uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+c.Operation.Identity.BookingID.String()+":g1:confirm")) {
						return false, ErrFinancialConflict
					}
				} else if TerminalDisposition(c.Operation.Identity.BookingID, result.DecisionID) != result.BookingDisposition {
					return false, ErrFinancialConflict
				}
			}
			_, err = tx.Exec(ctx, `UPDATE payments SET reconciliation_state=CASE
			 WHEN refund_state='succeeded' THEN 'resolved'
			 WHEN refund_state NOT IN ('not_required','succeeded') THEN 'refund_required'
			 WHEN $2='resolved' AND provider_outcome='collected' AND refund_state<>'succeeded' THEN 'pending'
			 WHEN $2='resolved' AND provider_outcome<>'collected' AND provider_status<>'canceled' THEN 'pending'
			 WHEN $2='sale_verified' AND (provider_outcome<>'collected' OR refund_state<>'not_required' OR $5<>'CONFIRMED') THEN 'pending'
			 WHEN $2='refund_required' AND refund_state='not_required' THEN 'pending' ELSE $2 END,
			 booking_disposition=CASE WHEN $5='' THEN booking_disposition ELSE $5 END,claim_token=NULL,lease_until=NULL,operation_kind=NULL,last_error=$3,due_at=$4,updated_at=clock_timestamp() WHERE id=$1`, c.PaymentID, result.State, result.ErrorCategory, next, result.BookingDisposition)
		}
		if err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("create result commit outcome unknown: %w", err)
	}
	return owned, nil
}

func retainRefundObservation(ctx context.Context, tx pgx.Tx, c domain.FinancialClaim, o domain.ProviderObservation, owned bool) error {
	var amount int64
	var currency, account, intent string
	var booking uuid.UUID
	var generation int64
	var live bool
	var providerID *string
	if err := tx.QueryRow(ctx, `SELECT refund_amount_cents,refund_currency,refund_provider_id,booking_id,payment_generation,provider_account,livemode,payment_intent_id FROM payments WHERE id=$1`, c.PaymentID).Scan(&amount, &currency, &providerID, &booking, &generation, &account, &live, &intent); err != nil {
		return err
	}
	if o.PaymentID != c.PaymentID || o.BookingID != booking || o.Generation != generation || o.Account != account || o.LiveMode != live || o.IntentID != intent || o.ObservedAt.IsZero() || o.AmountCents != amount || o.Currency != currency || o.SucceededRefundCents < 0 || o.SucceededRefundCents > amount {
		return ErrProviderEvidenceConflict
	}
	if providerID != nil && o.RefundID != "" && *providerID != o.RefundID && o.SucceededRefundCents != amount {
		return ErrProviderEvidenceConflict
	}
	_, err := tx.Exec(ctx, `UPDATE payments SET refund_provider_id=COALESCE(refund_provider_id,NULLIF($2,'')),
	 refund_provider_status=CASE WHEN $4=refund_amount_cents THEN 'succeeded' WHEN refund_state='succeeded' OR NOT $5 THEN refund_provider_status ELSE $3 END,
	 refund_state=CASE WHEN refund_state='succeeded' OR $4=refund_amount_cents THEN 'succeeded' ELSE refund_state END,
	 refund_completed_at=CASE WHEN $4=refund_amount_cents THEN COALESCE(refund_completed_at,clock_timestamp()) ELSE refund_completed_at END,
	 reconciliation_state=CASE WHEN refund_state='succeeded' OR $4=refund_amount_cents THEN 'resolved' ELSE reconciliation_state END WHERE id=$1`, c.PaymentID, o.RefundID, o.Status, o.SucceededRefundCents, owned)
	return err
}

// FinancialBacklog exposes aggregates without identities or frozen parameters.
func (p *PostgresPaymentRepository) FinancialBacklog(ctx context.Context) (due, attention int64, oldestSeconds float64, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	err = p.pool.QueryRow(ctx, `SELECT
	 COUNT(*) FILTER (WHERE due_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) AND (
	 create_state IN ('create_pending','create_unknown','create_claimed') OR refund_state IN ('pending','claimed','retryable','provider_pending','unknown') OR
	 (provider_outcome<>'collected' AND cancel_state IN ('cancel_pending','cancel_claimed','cancel_unknown')) OR
	 (create_state IN ('create_ready','create_attention') AND reconciliation_state IN ('pending','claimed')))),
	 COUNT(*) FILTER (WHERE create_state='create_attention' OR cancel_state='cancel_attention' OR refund_state='attention' OR reconciliation_state='attention'),
	 COALESCE(MAX(EXTRACT(EPOCH FROM clock_timestamp()-COALESCE(refund_requested_at,created_at))) FILTER (WHERE reconciliation_state NOT IN ('sale_verified','resolved')),0)
	 FROM payments WHERE managed`).Scan(&due, &attention, &oldestSeconds)
	return
}
