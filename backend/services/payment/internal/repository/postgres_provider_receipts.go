package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/payment/internal/domain"
)

const receiptColumns = `id,provider_account,livemode,event_id,event_type,intent_id,refund_id,payment_id,booking_id,payment_generation,normalized_object,evidence_hash,amount_cents,currency,provider_status,collected,observed_at,received_at,correlation_state,relay_state,envelope_id`

func scanReceipt(row pgx.Row) (*domain.ProviderReceipt, error) {
	r := &domain.ProviderReceipt{}
	o := &r.Observation
	var intent, refund *string
	var payment, booking *uuid.UUID
	var generation *int64
	var hash []byte
	err := row.Scan(&r.ID, &o.Account, &o.LiveMode, &o.EventID, &r.EventType, &intent, &refund, &payment, &booking, &generation, &o.SafeObject, &hash, &o.AmountCents, &o.Currency, &o.Status, &o.Collected, &o.ObservedAt, &r.ReceivedAt, &r.CorrelationState, &r.RelayState, &r.EnvelopeID)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		o.IntentID = *intent
	}
	if refund != nil {
		o.RefundID = *refund
	}
	if payment != nil {
		o.PaymentID = *payment
	}
	if booking != nil {
		o.BookingID = *booking
	}
	if generation != nil {
		o.Generation = *generation
	}
	copy(o.EvidenceHash[:], hash)
	return r, nil
}

func (p *PostgresPaymentRepository) AcceptProviderReceipt(ctx context.Context, r domain.ProviderReceipt) (*domain.ProviderReceipt, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	o := r.Observation
	if r.ID == uuid.Nil || o.EventID == "" || o.Account == "" || o.LiveMode || o.ObservedAt.IsZero() || o.EvidenceHash == [32]byte{} || len(o.SafeObject) == 0 || (r.CorrelationState != "pending" && r.CorrelationState != "attention") {
		return nil, false, ErrProviderEvidenceConflict
	}
	expected := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+o.Account+":test:"+o.EventID))
	if r.ID != expected || r.EnvelopeID != expected.String() {
		return nil, false, ErrProviderEvidenceConflict
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	var payment, booking *uuid.UUID
	var generation *int64
	if o.PaymentID != uuid.Nil {
		payment = &o.PaymentID
	}
	if o.BookingID != uuid.Nil {
		booking = &o.BookingID
	}
	if o.Generation != 0 {
		generation = &o.Generation
	}
	tag, err := tx.Exec(ctx, `INSERT INTO payment_provider_events(id,provider_account,livemode,event_id,event_type,intent_id,refund_id,payment_id,booking_id,payment_generation,normalized_object,evidence_hash,amount_cents,currency,provider_status,collected,observed_at,correlation_state,relay_state,envelope_id)
	 VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) ON CONFLICT(provider_account,livemode,event_id) DO NOTHING`, r.ID, o.Account, o.LiveMode, o.EventID, r.EventType, o.IntentID, o.RefundID, payment, booking, generation, o.SafeObject, o.EvidenceHash[:], o.AmountCents, o.Currency, o.Status, o.Collected, o.ObservedAt, r.CorrelationState, r.RelayState, r.EnvelopeID)
	if err != nil {
		return nil, false, err
	}
	stored, err := scanReceipt(tx.QueryRow(ctx, `SELECT `+receiptColumns+` FROM payment_provider_events WHERE provider_account=$1 AND livemode=$2 AND event_id=$3 FOR UPDATE`, o.Account, o.LiveMode, o.EventID))
	if err != nil {
		return nil, false, err
	}
	if stored.EventType != r.EventType || !bytes.Equal(stored.Observation.EvidenceHash[:], o.EvidenceHash[:]) {
		return nil, false, ErrProviderEvidenceConflict
	}
	if stored.CorrelationState == "pending" {
		if err := applyStoredReceipt(ctx, tx, stored); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("receipt commit outcome unknown: %w", err)
	}
	return stored, tag.RowsAffected() == 1, nil
}

// Caller holds the receipt first; only this path locks its Payment afterward.
func applyStoredReceipt(ctx context.Context, tx pgx.Tx, r *domain.ProviderReceipt) error {
	o := r.Observation
	var id, booking uuid.UUID
	var account, currency string
	var amount, generation int64
	var live bool
	var intent *string
	err := tx.QueryRow(ctx, `SELECT id,booking_id,provider_account,livemode,price,currency,payment_generation,payment_intent_id FROM payments
	 WHERE managed AND (($1::uuid IS NOT NULL AND id=$1) OR ($1::uuid IS NULL AND provider_account=$2 AND livemode=$3 AND payment_intent_id=$4)) FOR UPDATE`, optionalUUID(o.PaymentID), o.Account, o.LiveMode, o.IntentID).
		Scan(&id, &booking, &account, &live, &amount, &currency, &generation, &intent)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	mismatch := account != o.Account || live != o.LiveMode || (o.BookingID != uuid.Nil && booking != o.BookingID) || (o.Generation != 0 && generation != o.Generation) || (intent != nil && *intent != "" && *intent != o.IntentID) || currency != o.Currency
	if r.EventType == "payment_intent.succeeded" || r.EventType == "payment_intent.payment_failed" || r.EventType == "payment_intent.canceled" {
		mismatch = mismatch || amount != o.AmountCents || o.PaymentID == uuid.Nil || o.BookingID == uuid.Nil || o.Generation != 1
	}
	if mismatch {
		r.CorrelationState, r.RelayState = "attention", "not_applicable"
		_, err := tx.Exec(ctx, `UPDATE payment_provider_events SET correlation_state='attention',relay_state='not_applicable',last_error='provider_identity_conflict' WHERE id=$1`, r.ID)
		return err
	}
	o.PaymentID, o.BookingID, o.Generation = id, booking, generation
	if o.RefundID != "" || r.EventType == "charge.refunded" {
		// A refund callback is durable observation and reconciliation work, not
		// proof of the cumulative succeeded full amount (verified by T08).
		_, err = tx.Exec(ctx, `UPDATE payments SET reconciliation_state='pending',due_at=clock_timestamp(),updated_at=clock_timestamp(),
		 refund_provider_status=CASE WHEN refund_state<>'succeeded' AND refund_provider_id=$2 THEN $3 ELSE refund_provider_status END WHERE id=$1`, id, o.RefundID, o.Status)
	} else {
		// A contradictory provider association must retain the receipt rather
		// than lose responsibility to a uniqueness error in the fact merge.
		merge, beginErr := tx.Begin(ctx)
		if beginErr != nil {
			return beginErr
		}
		err = recordFinancialObservation(ctx, merge, o)
		if err != nil {
			_ = merge.Rollback(ctx)
			var pgErr *pgconn.PgError
			if errors.Is(err, ErrProviderEvidenceConflict) || (errors.As(err, &pgErr) && pgErr.Code == "23505") {
				r.CorrelationState, r.RelayState = "attention", "not_applicable"
				_, err = tx.Exec(ctx, `UPDATE payment_provider_events SET correlation_state='attention',relay_state='not_applicable',last_error='provider_association_conflict' WHERE id=$1`, r.ID)
				return err
			}
			return err
		}
		err = merge.Commit(ctx)
	}
	if err != nil {
		return err
	}
	r.Observation = o
	r.CorrelationState = "applied"
	_, err = tx.Exec(ctx, `UPDATE payment_provider_events SET payment_id=$2,booking_id=$3,payment_generation=$4,correlation_state='applied',handled_at=clock_timestamp(),due_at=clock_timestamp() WHERE id=$1`, r.ID, id, booking, generation)
	return err
}

func optionalUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func (p *PostgresPaymentRepository) ClaimReceiptWork(ctx context.Context, limit int, lease time.Duration) ([]domain.ReceiptClaim, error) {
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
	rows, err := tx.Query(ctx, `SELECT id FROM payment_provider_events WHERE due_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
	 AND (correlation_state='pending' OR (correlation_state='applied' AND relay_state IN ('pending','claimed'))) ORDER BY due_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var claims []domain.ReceiptClaim
	for _, id := range ids {
		c := domain.ReceiptClaim{ReceiptID: id, Token: uuid.New()}
		err := tx.QueryRow(ctx, `UPDATE payment_provider_events SET claim_token=$2,claim_fence=claim_fence+1,lease_until=clock_timestamp()+$3::interval,attempts=attempts+1,
		 relay_state=CASE WHEN correlation_state='applied' AND relay_state='pending' THEN 'claimed' ELSE relay_state END WHERE id=$1 RETURNING claim_fence,lease_until`, id, c.Token, fmt.Sprintf("%f seconds", lease.Seconds())).Scan(&c.Fence, &c.LeaseUntil)
		if err != nil {
			return nil, err
		}
		r, err := scanReceipt(tx.QueryRow(ctx, `SELECT `+receiptColumns+` FROM payment_provider_events WHERE id=$1`, id))
		if err != nil {
			return nil, err
		}
		c.Receipt = *r
		claims = append(claims, c)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claims, nil
}

func (p *PostgresPaymentRepository) ApplyReceiptObservation(ctx context.Context, c domain.ReceiptClaim, o domain.ProviderObservation) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	r, owned, err := lockReceiptClaim(ctx, tx, c)
	if err != nil || !owned {
		return false, err
	}
	if o.EventID != r.Observation.EventID || o.EvidenceHash != r.Observation.EvidenceHash {
		return false, ErrProviderEvidenceConflict
	}
	if r.CorrelationState == "pending" {
		if err := applyStoredReceipt(ctx, tx, r); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return r.CorrelationState == "applied", nil
}

func lockReceiptClaim(ctx context.Context, tx pgx.Tx, c domain.ReceiptClaim) (*domain.ProviderReceipt, bool, error) {
	var token *uuid.UUID
	var fence int64
	var active bool
	if err := tx.QueryRow(ctx, `SELECT claim_token,claim_fence,COALESCE(lease_until>clock_timestamp(),false) FROM payment_provider_events WHERE id=$1 FOR UPDATE`, c.ReceiptID).Scan(&token, &fence, &active); err != nil {
		return nil, false, err
	}
	r, err := scanReceipt(tx.QueryRow(ctx, `SELECT `+receiptColumns+` FROM payment_provider_events WHERE id=$1`, c.ReceiptID))
	return r, err == nil && token != nil && *token == c.Token && c.Fence == fence && active, err
}

func (p *PostgresPaymentRepository) FinishReceiptWork(ctx context.Context, c domain.ReceiptClaim, correlation, relay, category string, next time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	r, owned, err := lockReceiptClaim(ctx, tx, c)
	if err != nil || !owned {
		return false, err
	}
	if (correlation != r.CorrelationState && !(correlation == "pending" && r.CorrelationState == "attention")) || (relay != "pending" && relay != "published" && relay != "not_applicable") || (relay == "published" && (r.CorrelationState != "applied" || r.RelayState == "not_applicable")) {
		return false, ErrProviderEvidenceConflict
	}
	if r.RelayState == "published" {
		relay = "published"
	}
	if r.RelayState == "not_applicable" {
		relay = "not_applicable"
	}
	if next.IsZero() {
		next = time.Now().Add(time.Second)
	}
	_, err = tx.Exec(ctx, `UPDATE payment_provider_events SET claim_token=NULL,lease_until=NULL,relay_state=$2,last_error=NULLIF($3,''),due_at=$4 WHERE id=$1`, c.ReceiptID, relay, category, next)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
