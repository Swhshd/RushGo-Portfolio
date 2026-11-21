package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/payment/internal/domain"
)

func TerminalDisposition(booking, decision uuid.UUID) string {
	for _, pair := range []struct{ kind, status string }{{"cancel", "CANCELLED"}, {"expire", "EXPIRED"}, {"fail", "FAILED"}, {"abort_confirmation", "FAILED"}} {
		if decision == uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:"+pair.kind)) {
			return pair.status
		}
	}
	return ""
}

func (p *PostgresPaymentRepository) RequireRefund(ctx context.Context, o domain.RefundObligation) (*domain.FinancialView, error) {
	if o.PaymentID == uuid.Nil || o.DecisionID == uuid.Nil || o.Generation != 1 || o.AmountCents < 1 || o.Currency != "usd" || o.Key != "ticketbox:payment:"+o.PaymentID.String()+":g1:refund" {
		return nil, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var booking uuid.UUID
	var generation, amount int64
	var currency, outcome, state, key string
	var decision *uuid.UUID
	var target *int64
	var savedCurrency, reason *string
	err = tx.QueryRow(ctx, `SELECT booking_id,payment_generation,price,currency,provider_outcome,refund_state,refund_key,refund_decision_id,refund_amount_cents,refund_currency,refund_reason FROM payments WHERE managed AND id=$1 FOR UPDATE`, o.PaymentID).
		Scan(&booking, &generation, &amount, &currency, &outcome, &state, &key, &decision, &target, &savedCurrency, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, RecordNotFound
	}
	if err != nil {
		return nil, err
	}
	disposition := TerminalDisposition(booking, o.DecisionID)
	if generation != o.Generation || amount != o.AmountCents || currency != o.Currency || key != o.Key || outcome != "collected" || disposition == "" || disposition != o.Reason {
		return nil, ErrFinancialConflict
	}
	if decision != nil {
		if *decision != o.DecisionID || target == nil || *target != o.AmountCents || savedCurrency == nil || *savedCurrency != o.Currency || reason == nil || *reason != o.Reason {
			return nil, ErrFinancialConflict
		}
	} else {
		if state != "not_required" {
			return nil, ErrFinancialConflict
		}
		_, err = tx.Exec(ctx, `UPDATE payments SET refund_decision_id=$2,refund_amount_cents=$3,refund_currency=$4,refund_reason=$5,refund_requested_at=clock_timestamp(),
		 refund_state='pending',reconciliation_state='refund_required',booking_disposition=$5,due_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, o.PaymentID, o.DecisionID, o.AmountCents, o.Currency, o.Reason)
		if err != nil {
			return nil, err
		}
	}
	view, err := scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE id=$1`, o.PaymentID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return view, nil
}

func (p *PostgresPaymentRepository) RequireCancel(ctx context.Context, payment, decision uuid.UUID) (*domain.FinancialView, error) {
	if payment == uuid.Nil || decision == uuid.Nil {
		return nil, ErrInvalidFinancialIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var booking uuid.UUID
	var outcome, providerStatus string
	if err := tx.QueryRow(ctx, `SELECT booking_id,provider_outcome,COALESCE(provider_status,'') FROM payments WHERE managed AND id=$1 FOR UPDATE`, payment).Scan(&booking, &outcome, &providerStatus); err != nil {
		return nil, err
	}
	disposition := TerminalDisposition(booking, decision)
	if disposition == "" {
		return nil, ErrFinancialConflict
	}
	if outcome == "collected" {
		return nil, ErrFinancialConflict
	}
	_, err = tx.Exec(ctx, `UPDATE payments SET booking_disposition=$2,
	 cancel_state=CASE WHEN cancel_state IN ('cancelled','cancel_attention') THEN cancel_state WHEN provider_status='canceled' THEN 'cancelled' ELSE 'cancel_pending' END,
	 reconciliation_state=CASE WHEN provider_status='canceled' THEN 'resolved' ELSE 'pending' END,due_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, payment, disposition)
	if err != nil {
		return nil, err
	}
	view, err := scanFinancial(tx.QueryRow(ctx, `SELECT `+financialColumns+` FROM payments p WHERE id=$1`, payment))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return view, nil
}
