package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ticketbox/pkg/providerevidence"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

const paymentAdmissionPermitColumns = `request_id,payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,
	key_digest,fingerprint,identity_digest,token,fence,state,evidence_source,observed_at,created_at,next_attempt_at`

const maxPendingAdmissionsPerPayment = 16

func (p *PostgresPaymentRepository) GetAdmission(ctx context.Context, identity admission.Identity) (admission.Permit, error) {
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	permit, err := scanPaymentAdmissionPermit(p.pool.QueryRow(ctx,
		`SELECT `+paymentAdmissionPermitColumns+` FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID))
	if err != nil {
		return admission.Permit{}, err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	return permit, nil
}

// ActivatePaymentAdmission is an explicit operator action. Run it only after
// Booking's admission guard is active and upgraded Booking/Payment binaries
// are serving; it is monotonic and must never be reset to legacy mode.
func (p *PostgresPaymentRepository) ActivatePaymentAdmission(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: begin Payment admission activation: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, `UPDATE payment_admission_mode SET active=TRUE WHERE singleton=TRUE`)
	if err != nil {
		return fmt.Errorf("%w: activate Payment admission latch: %v", admission.ErrUnavailable, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: Payment admission latch is missing", admission.ErrUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: Payment admission activation commit outcome unknown: %v", admission.ErrUnavailable, err)
	}
	return nil
}

func (p *PostgresPaymentRepository) GrantAdmission(ctx context.Context, identity admission.Identity, evidence domain.AdmissionProviderEvidence) (admission.Permit, error) {
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	if err := validateAdmissionEvidence(evidence); err != nil {
		return admission.Permit{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: begin admission grant: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())

	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return admission.Permit{}, err
	}
	if !active {
		return admission.Permit{}, fmt.Errorf("%w: payment admission is inactive", admission.ErrUnavailable)
	}
	var lockedID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE`, identity.PaymentID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Permit{}, fmt.Errorf("%w: managed Payment is missing", admission.ErrBlocked)
	}
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: lock Payment for admission: %v", admission.ErrUnavailable, err)
	}

	// A same-identity retry returns the existing immutable decision before any
	// new eligibility decision or financial-state check.
	permit, err := scanPaymentAdmissionPermit(tx.QueryRow(ctx,
		`SELECT `+paymentAdmissionPermitColumns+` FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID))
	if err == nil {
		if err := permit.ValidateFor(identity); err != nil {
			return admission.Permit{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return admission.Permit{}, fmt.Errorf("%w: existing permit read commit outcome unknown: %v", admission.ErrUnavailable, err)
		}
		return permit, nil
	}
	if !errors.Is(err, admission.ErrNotFound) {
		return admission.Permit{}, err
	}

	var liveMode bool
	var paymentID, bookingID, userID uuid.UUID
	var generation, amountCents, refundAmountCents int64
	var providerAccount, currency, paymentIntentID, paymentStatus, providerOutcome, createState, cancelState, reconciliationState, refundState string
	err = tx.QueryRow(ctx, `SELECT id,booking_id,user_id,payment_generation,provider_account,livemode,
		price,currency,COALESCE(payment_intent_id,''),status::text,provider_outcome,create_state,cancel_state,
		reconciliation_state,refund_state,COALESCE(refund_amount_cents,0)
		FROM payments WHERE id=$1 AND managed`, identity.PaymentID).Scan(
		&paymentID, &bookingID, &userID, &generation, &providerAccount, &liveMode,
		&amountCents, &currency, &paymentIntentID, &paymentStatus, &providerOutcome, &createState,
		&cancelState, &reconciliationState, &refundState, &refundAmountCents)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Permit{}, fmt.Errorf("%w: Payment financial record is unmanaged", admission.ErrBlocked)
	}
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: reload Payment eligibility state: %v", admission.ErrUnavailable, err)
	}
	if paymentID != identity.PaymentID || bookingID != identity.BookingID || userID != identity.UserID || generation != identity.Generation {
		return admission.Permit{}, fmt.Errorf("%w: Payment financial identity differs from Booking proof", admission.ErrBlocked)
	}
	if providerAccount == "" || liveMode || amountCents < 1 || currency != "usd" || paymentIntentID == "" ||
		paymentStatus != string(domain.PaymentSuccess) || providerOutcome != "collected" || createState != "create_ready" ||
		cancelState != "not_requested" || reconciliationState != "sale_verified" || refundState != "not_required" || refundAmountCents > 0 {
		return admission.Permit{}, fmt.Errorf("%w: Payment is not in the admission-eligible financial state", admission.ErrBlocked)
	}
	var refundPrepared bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='PREPARED')`, identity.PaymentID).Scan(&refundPrepared); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: read refund preparation history: %v", admission.ErrUnavailable, err)
	}
	if refundPrepared {
		return admission.Permit{}, fmt.Errorf("%w: a refund was already prepared for this Payment", admission.ErrBlocked)
	}
	var pendingCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE payment_id=$1 AND state='PENDING'`, identity.PaymentID).Scan(&pendingCount); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: count pending Payment permits: %v", admission.ErrUnavailable, err)
	}
	if pendingCount >= maxPendingAdmissionsPerPayment {
		return admission.Permit{}, fmt.Errorf("%w: Payment has reached its pending admission limit", admission.ErrBlocked)
	}
	var evidenceFresh bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz <= clock_timestamp()
		AND $1::timestamptz >= clock_timestamp()-INTERVAL '10 seconds'`, evidence.ObservedAt).Scan(&evidenceFresh); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: verify provider evidence freshness: %v", admission.ErrUnavailable, err)
	}
	if !evidenceFresh {
		return admission.Permit{}, fmt.Errorf("%w: provider evidence is stale", admission.ErrUnavailable)
	}

	var fence int64
	if err := tx.QueryRow(ctx, `SELECT nextval('payment_admission_fence_seq')`).Scan(&fence); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: allocate admission fence: %v", admission.ErrUnavailable, err)
	}
	token := uuid.New()
	digest := identity.Digest()
	permit, err = scanPaymentAdmissionPermit(tx.QueryRow(ctx,
		`INSERT INTO payment_admission_permits(
			request_id,payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,key_digest,fingerprint,identity_digest,
			token,fence,state,evidence_source,observed_at,next_attempt_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING',$14,$15,clock_timestamp()+INTERVAL '15 seconds')
		RETURNING `+paymentAdmissionPermitColumns,
		identity.RequestID, identity.PaymentID, identity.BookingID, identity.TicketID, identity.EventID, identity.UserID, identity.ActorID,
		identity.Generation, identity.KeyDigest[:], identity.Fingerprint[:], digest[:], token, fence, evidence.Source, evidence.ObservedAt))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			if pgErr.ConstraintName == "payment_admission_one_pending_ticket" || pgErr.ConstraintName == "payment_admission_permits_pkey" {
				return admission.Permit{}, fmt.Errorf("%w: request or Ticket already has a different admission binding", admission.ErrConflict)
			}
		}
		return admission.Permit{}, fmt.Errorf("%w: insert admission permit: %v", admission.ErrUnavailable, err)
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		// Never infer success from a lost COMMIT acknowledgement. The caller can
		// retry this same RequestID and read the immutable result.
		return admission.Permit{}, fmt.Errorf("%w: admission grant commit outcome unknown: %v", admission.ErrUnavailable, err)
	}
	return permit, nil
}

func (p *PostgresPaymentRepository) SettleAdmission(ctx context.Context, identity admission.Identity, token uuid.UUID, fence int64, state admission.State) (admission.Permit, error) {
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	if token == uuid.Nil || fence <= 0 || (state != admission.Committed && state != admission.Aborted) {
		return admission.Permit{}, admission.ErrStale
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: begin admission settlement: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())
	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return admission.Permit{}, err
	}
	if !active {
		return admission.Permit{}, fmt.Errorf("%w: payment admission is inactive", admission.ErrUnavailable)
	}
	var lockedID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE`, identity.PaymentID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Permit{}, admission.ErrNotFound
	}
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: lock Payment for settlement: %v", admission.ErrUnavailable, err)
	}
	var permit admission.Permit
	digest := identity.Digest()
	_, err = tx.Exec(ctx, `UPDATE payment_admission_permits SET state=$5
		WHERE request_id=$1 AND payment_id=$2 AND identity_digest=$3 AND token=$4 AND fence=$6 AND state='PENDING'
		`,
		identity.RequestID, identity.PaymentID, digest[:], token, string(state), fence)
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: settle admission permit: %v", admission.ErrUnavailable, err)
	}
	permit, err = scanPaymentAdmissionPermit(tx.QueryRow(ctx,
		`SELECT `+paymentAdmissionPermitColumns+` FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID))
	if err != nil {
		return admission.Permit{}, err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	if permit.Token != token || permit.Fence != fence || permit.State != state {
		return admission.Permit{}, admission.ErrStale
	}
	if err := tx.Commit(ctx); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: admission settlement commit outcome unknown: %v", admission.ErrUnavailable, err)
	}
	return permit, nil
}

func (p *PostgresPaymentRepository) ScheduleAdmissionRetry(ctx context.Context, identity admission.Identity, token uuid.UUID, fence int64, attempt int, reason string) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if token == uuid.Nil || fence <= 0 || attempt < 0 {
		return admission.ErrStale
	}
	switch reason {
	case "booking_unavailable", "booking_outcome_mismatch", "booking_intent_pending", "outcome_unavailable":
	default:
		// Persist only bounded categories, never an RPC or provider error string.
		reason = "outcome_unavailable"
	}
	delay := admissionRetryDelay(attempt)
	digest := identity.Digest()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: begin admission retry scheduling: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())
	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("%w: Payment admission is inactive during retry scheduling", admission.ErrUnavailable)
	}
	tag, err := tx.Exec(ctx, `UPDATE payment_admission_permits SET
		next_attempt_at=clock_timestamp()+($7 * INTERVAL '1 second'),
		recovery_attempts=GREATEST(recovery_attempts,$6),last_error=$8
		WHERE request_id=$1 AND payment_id=$2 AND identity_digest=$3 AND token=$4 AND fence=$5 AND state='PENDING'`,
		identity.RequestID, identity.PaymentID, digest[:], token, fence, attempt, int64(delay/time.Second), reason)
	if err != nil {
		return fmt.Errorf("%w: schedule pending admission recovery: %v", admission.ErrUnavailable, err)
	}
	if tag.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("%w: admission retry schedule commit outcome unknown: %v", admission.ErrUnavailable, err)
		}
		return nil
	}
	permit, err := scanPaymentAdmissionPermit(tx.QueryRow(ctx,
		`SELECT `+paymentAdmissionPermitColumns+` FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID))
	if err != nil {
		return err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return err
	}
	if permit.Token != token || permit.Fence != fence {
		return admission.ErrStale
	}
	// A concurrent exact reconciliation may have made the permit terminal.
	// Scheduling cannot alter that immutable result.
	if permit.State.Terminal() {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("%w: terminal retry read commit outcome unknown: %v", admission.ErrUnavailable, err)
		}
		return nil
	}
	return admission.ErrStale
}

func (p *PostgresPaymentRepository) ClaimDueAdmissions(ctx context.Context, limit int) ([]AdmissionRecoveryClaim, error) {
	if limit < 1 || limit > 16 {
		return nil, admission.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: begin admission recovery batch: %v", admission.ErrUnavailable, err)
	}
	defer tx.Rollback(context.Background())
	active, err := lockPaymentAdmissionMode(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !active {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("%w: inactive recovery batch commit: %v", admission.ErrUnavailable, err)
		}
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT request_id FROM payment_admission_permits
		WHERE state='PENDING' AND next_attempt_at<=clock_timestamp()
		ORDER BY next_attempt_at,created_at,request_id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: select due admission permits: %v", admission.ErrUnavailable, err)
	}
	var requestIDs []uuid.UUID
	for rows.Next() {
		var requestID uuid.UUID
		if err := rows.Scan(&requestID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: scan due admission identity: %v", admission.ErrUnavailable, err)
		}
		requestIDs = append(requestIDs, requestID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("%w: iterate due admission permits: %v", admission.ErrUnavailable, err)
	}
	rows.Close()
	claims := make([]AdmissionRecoveryClaim, 0, len(requestIDs))
	for _, requestID := range requestIDs {
		row := tx.QueryRow(ctx, `UPDATE payment_admission_permits SET
			next_attempt_at=clock_timestamp()+INTERVAL '2 minutes', recovery_attempts=recovery_attempts+1
			WHERE request_id=$1 AND state='PENDING' RETURNING `+paymentAdmissionPermitColumns+`,recovery_attempts`, requestID)
		var claim AdmissionRecoveryClaim
		permit, attempt, err := scanPaymentAdmissionRecoveryClaim(row)
		if err != nil {
			return nil, fmt.Errorf("%w: claim due admission permit: %v", admission.ErrUnavailable, err)
		}
		claim.Permit, claim.Attempt = permit, attempt
		claims = append(claims, claim)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: admission recovery claim commit outcome unknown: %v", admission.ErrUnavailable, err)
	}
	return claims, nil
}

func lockPaymentAdmissionMode(ctx context.Context, tx pgx.Tx) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT active FROM payment_admission_mode WHERE singleton=TRUE FOR SHARE`).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("%w: payment admission mode latch is missing", admission.ErrUnavailable)
	}
	if err != nil {
		return false, fmt.Errorf("%w: lock payment admission mode: %v", admission.ErrUnavailable, err)
	}
	return active, nil
}

func validateAdmissionEvidence(evidence domain.AdmissionProviderEvidence) error {
	if evidence.EligibilityState == domain.AdmissionEligibilityDenied {
		return fmt.Errorf("%w: Payment provider evidence denies admission", admission.ErrBlocked)
	}
	if evidence.EligibilityState != domain.AdmissionEligibilityEligible || evidence.DisputeState != domain.DisputeEvidenceNoDispute ||
		evidence.RefundState != domain.RefundEvidenceNoRefund || !providerevidence.AllowedSource(evidence.Source) || evidence.ObservedAt.IsZero() {
		return fmt.Errorf("%w: fresh decision-grade Payment evidence is unavailable", admission.ErrUnavailable)
	}
	return nil
}

func scanPaymentAdmissionPermit(row pgx.Row) (admission.Permit, error) {
	var permit admission.Permit
	var generation int64
	var keyDigest, fingerprint, identityDigest []byte
	var state string
	err := row.Scan(&permit.Identity.RequestID, &permit.Identity.PaymentID, &permit.Identity.BookingID,
		&permit.Identity.TicketID, &permit.Identity.EventID, &permit.Identity.UserID, &permit.Identity.ActorID,
		&generation, &keyDigest, &fingerprint, &identityDigest, &permit.Token, &permit.Fence, &state,
		&permit.Source, &permit.ObservedAt, &permit.CreatedAt, &permit.NextAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Permit{}, admission.ErrNotFound
	}
	if err != nil {
		return admission.Permit{}, fmt.Errorf("%w: read Payment admission permit: %w", admission.ErrUnavailable, err)
	}
	permit.Identity.Generation = generation
	if len(keyDigest) != len(permit.Identity.KeyDigest) || len(fingerprint) != len(permit.Identity.Fingerprint) || len(identityDigest) != len(permit.Identity.Digest()) {
		return admission.Permit{}, fmt.Errorf("%w: stored Payment admission digest is malformed", admission.ErrConflict)
	}
	copy(permit.Identity.KeyDigest[:], keyDigest)
	copy(permit.Identity.Fingerprint[:], fingerprint)
	permit.State = admission.State(state)
	if err := permit.Identity.Validate(); err != nil {
		return admission.Permit{}, fmt.Errorf("%w: stored Payment admission binding is invalid", admission.ErrConflict)
	}
	digest := permit.Identity.Digest()
	if !bytes.Equal(identityDigest, digest[:]) {
		return admission.Permit{}, admission.ErrConflict
	}
	if permit.Token == uuid.Nil || permit.Fence <= 0 || !permit.State.Valid() {
		return admission.Permit{}, admission.ErrConflict
	}
	return permit, nil
}

func scanPaymentAdmissionRecoveryClaim(row pgx.Row) (admission.Permit, int, error) {
	var permit admission.Permit
	var generation int64
	var keyDigest, fingerprint, identityDigest []byte
	var state string
	var attempt int
	err := row.Scan(&permit.Identity.RequestID, &permit.Identity.PaymentID, &permit.Identity.BookingID,
		&permit.Identity.TicketID, &permit.Identity.EventID, &permit.Identity.UserID, &permit.Identity.ActorID,
		&generation, &keyDigest, &fingerprint, &identityDigest, &permit.Token, &permit.Fence, &state,
		&permit.Source, &permit.ObservedAt, &permit.CreatedAt, &permit.NextAttemptAt, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Permit{}, 0, admission.ErrNotFound
	}
	if err != nil {
		return admission.Permit{}, 0, err
	}
	permit.Identity.Generation = generation
	if len(keyDigest) != len(permit.Identity.KeyDigest) || len(fingerprint) != len(permit.Identity.Fingerprint) || len(identityDigest) != len(permit.Identity.Digest()) {
		return admission.Permit{}, 0, admission.ErrConflict
	}
	copy(permit.Identity.KeyDigest[:], keyDigest)
	copy(permit.Identity.Fingerprint[:], fingerprint)
	permit.State = admission.State(state)
	if err := permit.ValidateFor(permit.Identity); err != nil {
		return admission.Permit{}, 0, err
	}
	digest := permit.Identity.Digest()
	if !bytes.Equal(identityDigest, digest[:]) {
		return admission.Permit{}, 0, admission.ErrConflict
	}
	return permit, attempt, nil
}

func admissionRetryDelay(attempt int) time.Duration {
	shift := max(0, min(attempt-1, 8))
	delay := time.Second * time.Duration(1<<shift)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
