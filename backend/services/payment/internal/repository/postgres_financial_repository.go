package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/payment/internal/domain"
)

const financialColumns = paymentColumns + `,p.managed,p.provider_account,p.livemode,p.payment_generation,p.provider_outcome,p.provider_status,
	p.booking_disposition,p.create_state,p.cancel_state,p.reconciliation_state,p.refund_state,p.refund_reason,
	p.refund_provider_id,p.refund_provider_status,p.refund_amount_cents,p.refund_decision_id,p.due_at,p.provider_observed_at,p.last_error`

func scanFinancial(row pgx.Row) (*domain.FinancialView, error) {
	p := &domain.Payment{}
	v := &domain.FinancialView{Payment: p}
	var status, intent, method *string
	var providerAccount *string
	var generation, refundAmount *int64
	var refundDecisionID *uuid.UUID
	var outcome, providerStatus, disposition, create, cancel, reconciliation, refund, refundReason, refundID, refundStatus, lastError *string
	var managed bool
	targets := paymentScanTargets(p, &status, &intent, &method)
	targets = append(targets, &managed, &providerAccount, &v.LiveMode, &generation, &outcome, &providerStatus, &disposition, &create, &cancel, &reconciliation,
		&refund, &refundReason, &refundID, &refundStatus, &refundAmount, &refundDecisionID, &v.NextAttemptAt, &v.ObservedAt, &lastError)
	if err := row.Scan(targets...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, RecordNotFound
		}
		return nil, fmt.Errorf("scan financial payment: %w", err)
	}
	finishPaymentScan(p, status, intent, method)
	v.Managed = managed
	if providerAccount != nil {
		v.ProviderAccount = *providerAccount
	}
	if generation != nil {
		v.Generation = *generation
	}
	// Legacy absence stays unknown; never fabricate a managed lifecycle or completed refund.
	v.ProviderOutcome, v.CreateState, v.CancelState, v.ReconciliationState, v.RefundState = "unknown", "unknown", "unknown", "unknown", "unknown"
	for _, field := range []struct {
		target *string
		source *string
	}{
		{&v.ProviderOutcome, outcome}, {&v.ProviderStatus, providerStatus}, {&v.BookingDisposition, disposition},
		{&v.CreateState, create}, {&v.CancelState, cancel}, {&v.ReconciliationState, reconciliation}, {&v.RefundState, refund}, {&v.RefundReason, refundReason},
		{&v.RefundID, refundID}, {&v.RefundStatus, refundStatus}, {&v.ErrorCategory, lastError},
	} {
		if field.source != nil {
			*field.target = *field.source
		}
	}
	if refundAmount != nil {
		v.RefundAmountCents = *refundAmount
	}
	v.RefundDecisionID = refundDecisionID
	return v, nil
}

func (p *PostgresPaymentRepository) GetFinancial(ctx context.Context, id uuid.UUID) (*domain.FinancialView, error) {
	return scanFinancial(p.pool.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE p.id=$1`, id))
}

func (p *PostgresPaymentRepository) GetByLogicalIdentity(ctx context.Context, bookingID uuid.UUID, generation int64) (*domain.FinancialView, error) {
	return scanFinancial(p.pool.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE p.managed AND p.booking_id=$1 AND p.payment_generation=$2`, bookingID, generation))
}

func validateCreateIdentity(op domain.CreateOperation) error {
	i := op.Identity
	if op.PaymentID == uuid.Nil || i.BookingID == uuid.Nil || i.UserID == uuid.Nil || i.Generation != 1 ||
		i.AmountCents < 1 || i.AmountCents > 2147483647 || i.Currency != "usd" || i.ProviderAccount == "" ||
		i.Fingerprint == [32]byte{} || op.Key != "ticketbox:payment:"+op.PaymentID.String()+":g1:create" {
		return ErrInvalidFinancialIdentity
	}
	fingerprint, err := domain.FinancialFingerprint(i, op.FrozenParams)
	if err != nil || fingerprint != i.Fingerprint {
		return ErrInvalidFinancialIdentity
	}
	return nil
}

func (p *PostgresPaymentRepository) CreateOrGet(ctx context.Context, op domain.CreateOperation) (*domain.FinancialView, bool, error) {
	if err := validateCreateIdentity(op); err != nil {
		return nil, false, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(context.Background())
	i := op.Identity
	tag, err := tx.Exec(ctx, `INSERT INTO payments(id,user_id,booking_id,status,price,currency,managed,
		payment_generation,provider_account,livemode,request_hash,frozen_create,create_key,create_state,
		provider_outcome,cancel_key,cancel_state,reconciliation_state,refund_key,refund_state,claim_fence,due_at,attempts)
		VALUES($1,$2,$3,'pending',$4,$5,true,$6,$7,$8,$9,$10,$11,'create_pending','unknown',$12,'not_requested',
		'pending',$13,'not_required',0,NOW(),0)
		ON CONFLICT (booking_id,payment_generation) WHERE managed DO NOTHING`,
		op.PaymentID, i.UserID, i.BookingID, i.AmountCents, i.Currency, i.Generation, i.ProviderAccount, i.LiveMode,
		i.Fingerprint[:], op.FrozenParams, op.Key, "ticketbox:payment:"+op.PaymentID.String()+":g1:cancel",
		"ticketbox:payment:"+op.PaymentID.String()+":g1:refund")
	if err != nil {
		return nil, false, fmt.Errorf("persist financial identity: %w", err)
	}
	// Separate READ COMMITTED statement sees a concurrent winner after uniqueness wait.
	var stored domain.FinancialIdentity
	var hash, frozen []byte
	err = tx.QueryRow(ctx, `SELECT booking_id,user_id,payment_generation,provider_account,livemode,price,currency,request_hash,frozen_create
		FROM payments WHERE managed AND booking_id=$1 AND payment_generation=$2 FOR UPDATE`, i.BookingID, i.Generation).
		Scan(&stored.BookingID, &stored.UserID, &stored.Generation, &stored.ProviderAccount, &stored.LiveMode, &stored.AmountCents, &stored.Currency, &hash, &frozen)
	if err != nil {
		return nil, false, fmt.Errorf("reload financial identity: %w", err)
	}
	copy(stored.Fingerprint[:], hash)
	var original, requested any
	if json.Unmarshal(frozen, &original) != nil || json.Unmarshal(op.FrozenParams, &requested) != nil ||
		stored != i || !bytes.Equal(hash, i.Fingerprint[:]) || !reflect.DeepEqual(original, requested) {
		return nil, false, ErrFinancialConflict
	}
	view, err := scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE p.managed AND p.booking_id=$1 AND p.payment_generation=$2`, i.BookingID, i.Generation))
	if err != nil {
		return nil, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		// Caller must re-read/replay this same logical identity; COMMIT error is not a failed payment.
		return nil, false, fmt.Errorf("financial identity commit outcome unknown: %w", err)
	}
	return view, tag.RowsAffected() == 1, nil
}

func (p *PostgresPaymentRepository) RecordObservation(ctx context.Context, o domain.ProviderObservation) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err := recordFinancialObservation(ctx, tx, o); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func recordFinancialObservation(ctx context.Context, tx pgx.Tx, o domain.ProviderObservation) error {
	if o.PaymentID == uuid.Nil || o.IntentID == "" || o.ObservedAt.IsZero() {
		return ErrProviderEvidenceConflict
	}
	var booking uuid.UUID
	var generation, amount int64
	var account, currency string
	var live bool
	var intent *string
	err := tx.QueryRow(ctx, `SELECT booking_id,payment_generation,price,provider_account,livemode,currency,payment_intent_id
		FROM payments WHERE id=$1 AND managed FOR UPDATE`, o.PaymentID).
		Scan(&booking, &generation, &amount, &account, &live, &currency, &intent)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecordNotFound
	}
	if err != nil {
		return err
	}
	if booking != o.BookingID || generation != o.Generation || amount != o.AmountCents || currency != o.Currency ||
		account != o.Account || live != o.LiveMode || (intent != nil && *intent != "" && *intent != o.IntentID) {
		return ErrProviderEvidenceConflict
	}
	_, err = tx.Exec(ctx, `UPDATE payments SET payment_intent_id=$2,
		provider_outcome=CASE WHEN $3 OR provider_outcome='collected' THEN 'collected'
		WHEN $4='canceled' AND (provider_observed_at IS NULL OR provider_observed_at <= $5) THEN 'not_collected' ELSE provider_outcome END,
		status=CASE WHEN $3 OR provider_outcome='collected' THEN 'success'::payment_status ELSE status END,
		provider_status=CASE WHEN $3 THEN 'succeeded' WHEN provider_outcome='collected' THEN provider_status
		WHEN provider_observed_at IS NULL OR provider_observed_at <= $5 THEN $4 ELSE provider_status END,
		provider_observed_at=CASE WHEN provider_observed_at IS NULL OR provider_observed_at < $5 THEN $5 ELSE provider_observed_at END,
		create_state='create_ready',reconciliation_state=CASE WHEN reconciliation_state IN ('sale_verified','resolved') AND NOT $3
		THEN reconciliation_state ELSE 'pending' END,due_at=CASE WHEN $3 OR $4='canceled' OR due_at<=NOW() THEN NOW() ELSE due_at END,updated_at=NOW() WHERE id=$1`,
		o.PaymentID, o.IntentID, o.Collected, o.Status, o.ObservedAt)
	if err != nil {
		return fmt.Errorf("retain provider observation: %w", err)
	}
	return nil
}
