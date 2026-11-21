package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

// RequireApprovedUnusedRefund persists the new Booking-owned refund type
// without routing it through the legacy terminal-disposition contract or
// rewriting booking_disposition. The Payment row lock makes the decision a
// single refund obligation and makes duplicate delivery idempotent.
func (p *PostgresPaymentRepository) RequireApprovedUnusedRefund(ctx context.Context, obligation domain.RefundObligation, identity ApprovedUnusedRefundIdentity) (*domain.FinancialView, error) {
	if obligation.PaymentID == uuid.Nil || obligation.DecisionID == uuid.Nil || obligation.Generation != 1 ||
		obligation.Reason != domain.RefundReasonConfirmedUnusedTicket || obligation.AmountCents < 1 ||
		obligation.Currency != "usd" || obligation.Key != "ticketbox:payment:"+obligation.PaymentID.String()+":g1:refund" ||
		identity.BookingID == uuid.Nil || identity.UserID == uuid.Nil || identity.Generation != 1 || identity.LiveMode ||
		identity.ProviderAccount == "" || identity.PaymentIntentID == "" || identity.AmountCents != obligation.AmountCents ||
		identity.Currency != obligation.Currency {
		return nil, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	admissionActive, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !admissionActive {
		return nil, admission.ErrUnavailable
	}

	var bookingID, userID uuid.UUID
	var generation, amount int64
	var account, currency, paymentIntent, status, outcome, disposition string
	var liveMode bool
	var createState, cancelState, reconciliationState, refundState string
	var refundKey string
	var savedDecision *uuid.UUID
	var savedAmount *int64
	var savedCurrency, savedReason, providerID, providerStatus *string
	err = tx.QueryRow(ctx, `SELECT booking_id,user_id,payment_generation,provider_account,livemode,price,currency,
		COALESCE(payment_intent_id,''),status,provider_outcome,booking_disposition,create_state,cancel_state,
		reconciliation_state,refund_state,refund_key,refund_decision_id,refund_amount_cents,refund_currency,
		refund_reason,refund_provider_id,refund_provider_status
		FROM payments WHERE managed AND id=$1 FOR UPDATE`, obligation.PaymentID).Scan(
		&bookingID, &userID, &generation, &account, &liveMode, &amount, &currency, &paymentIntent,
		&status, &outcome, &disposition, &createState, &cancelState, &reconciliationState, &refundState,
		&refundKey, &savedDecision, &savedAmount, &savedCurrency, &savedReason, &providerID, &providerStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, RecordNotFound
	}
	if err != nil {
		return nil, err
	}
	if bookingID != identity.BookingID || userID != identity.UserID || generation != identity.Generation ||
		account != identity.ProviderAccount || liveMode != identity.LiveMode || amount != identity.AmountCents ||
		currency != identity.Currency || paymentIntent == "" || paymentIntent != identity.PaymentIntentID ||
		status != string(domain.PaymentSuccess) || outcome != "collected" || disposition != "CONFIRMED" ||
		createState != "create_ready" || cancelState != "not_requested" {
		return nil, ErrFinancialConflict
	}
	if savedDecision != nil {
		if *savedDecision != obligation.DecisionID || savedAmount == nil || *savedAmount != obligation.AmountCents ||
			savedCurrency == nil || *savedCurrency != obligation.Currency || savedReason == nil || *savedReason != obligation.Reason ||
			refundKey != obligation.Key || refundState == "not_required" {
			return nil, ErrFinancialConflict
		}
		view, scanErr := scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE id=$1`, obligation.PaymentID))
		if scanErr != nil {
			return nil, scanErr
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return view, nil
	}
	if refundState != "not_required" || savedAmount != nil || savedCurrency != nil || savedReason != nil || providerID != nil || providerStatus != nil ||
		reconciliationState != "sale_verified" {
		return nil, ErrFinancialConflict
	}
	var committedAdmission bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_admission_permits WHERE payment_id=$1 AND state='COMMITTED')`, obligation.PaymentID).Scan(&committedAdmission); err != nil {
		return nil, err
	}
	if committedAdmission {
		return nil, ErrFinancialConflict
	}
	var priorRefundAudit bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_refund_audit WHERE payment_id=$1)`, obligation.PaymentID).Scan(&priorRefundAudit); err != nil {
		return nil, err
	}
	if priorRefundAudit {
		return nil, ErrFinancialConflict
	}
	_, err = tx.Exec(ctx, `UPDATE payments SET refund_decision_id=$2,refund_amount_cents=$3,refund_currency=$4,
		refund_reason=$5,refund_requested_at=clock_timestamp(),refund_state='pending',
		reconciliation_state='refund_required',due_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`,
		obligation.PaymentID, obligation.DecisionID, obligation.AmountCents, obligation.Currency, obligation.Reason)
	if err != nil {
		return nil, err
	}
	view, err := scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE id=$1`, obligation.PaymentID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return view, nil
}
