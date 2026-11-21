package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/admission"
)

var (
	ErrAdmissionInactive          = errors.New("Booking admission coordination is inactive")
	ErrAdmissionRequired          = errors.New("active Booking admission requires a permit")
	ErrAdmissionTerminal          = errors.New("Booking admission intent is terminal")
	ErrAdmissionAlreadyCheckedIn  = errors.New("Ticket was checked in by another operation")
	ErrAdmissionTicketRevoked     = errors.New("Ticket was revoked before admission")
	ErrAdmissionBookingIneligible = errors.New("Booking became ineligible before admission")
	ErrAdmissionRefundApproved    = errors.New("approved Booking refund blocks Ticket admission")
)

// TicketAdmissionRepository is an optional capability so existing in-memory
// TicketRepository fakes and older test adapters do not need to implement the
// admission boundary. The production PostgreSQL repository implements every
// method and fails closed if the mode row is missing or unreadable.
type TicketAdmissionRepository interface {
	AdmissionEnabled(context.Context) (bool, error)
	EnsureAdmissionIntent(context.Context, admission.Identity) error
	AttachAdmissionPermit(context.Context, admission.Permit) error
	ResolveAdmission(context.Context, admission.Identity, uuid.UUID, int64, bool) (admission.Outcome, error)
	LookupAdmissionIntent(context.Context, uuid.UUID, [32]byte, []byte, uuid.UUID) (admission.Identity, bool, error)
}

func (r *PostgresBookingRepository) AdmissionEnabled(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var active bool
	err := r.pool.QueryRow(ctx, `SELECT active FROM booking_admission_mode WHERE singleton=TRUE`).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, errors.New("Booking admission mode row is missing")
	}
	if err != nil {
		return false, fmt.Errorf("read Booking admission mode: %w", err)
	}
	return active, nil
}

func (r *PostgresBookingRepository) EnsureAdmissionIntent(ctx context.Context, identity admission.Identity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBooking(tx)
	active, err := lockBookingAdmissionMode(ctx, tx)
	if err != nil {
		return err
	} else if !active {
		return ErrAdmissionInactive
	}
	var refundDecisionID *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, identity.BookingID).Scan(&refundDecisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTicketCredentialMissing
	}
	if err != nil {
		return fmt.Errorf("lock Booking before admission intent: %w", err)
	}
	if refundDecisionID != nil {
		return ErrAdmissionRefundApproved
	}

	var assigned bool
	err = tx.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2 FOR SHARE`, identity.EventID, identity.ActorID).Scan(&assigned)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !assigned {
		return ErrScannerNotAssigned
	}
	if err != nil {
		return fmt.Errorf("verify scanner assignment for admission: %w", err)
	}

	var bookingID, paymentID, ticketEventID, bookingEventID, userID uuid.UUID
	var ticketGeneration, bookingGeneration int64
	var ticketStatus, bookingStatus string
	var managed bool
	var itemID, tierID, seatID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT t.booking_id,t.payment_id,t.event_id,b.event_id,b.user_id,t.lifecycle_generation,b.lifecycle_generation,
		t.status,b.status,b.managed,t.booking_item_id,t.ticket_tier_id,t.seat_id
		FROM tickets t JOIN bookings b ON b.id=t.booking_id
		WHERE t.id=$1 FOR SHARE OF t`, identity.TicketID).Scan(&bookingID, &paymentID, &ticketEventID, &bookingEventID, &userID,
		&ticketGeneration, &bookingGeneration, &ticketStatus, &bookingStatus, &managed, &itemID, &tierID, &seatID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTicketCredentialMissing
	}
	if err != nil {
		return fmt.Errorf("verify authoritative Ticket admission binding: %w", err)
	}
	if bookingID != identity.BookingID || paymentID != identity.PaymentID || ticketEventID != identity.EventID || bookingEventID != identity.EventID ||
		userID != identity.UserID || ticketGeneration != identity.Generation || bookingGeneration != identity.Generation {
		return admission.ErrConflict
	}
	if ticketStatus == "CHECKED_IN" {
		return ErrAdmissionAlreadyCheckedIn
	}
	if ticketStatus == "REVOKED" {
		return ErrAdmissionTicketRevoked
	}
	if bookingStatus != "CONFIRMED" || !managed {
		return ErrAdmissionBookingIneligible
	}
	if ticketStatus != "ISSUED" {
		return ErrAdmissionTerminal
	}
	var itemBound bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM booking_items WHERE id=$1 AND booking_id=$2 AND ticket_tier_id=$3 AND $4=ANY(seat_ids))`, itemID, bookingID, tierID, seatID).Scan(&itemBound); err != nil {
		return fmt.Errorf("verify Ticket seat binding for admission: %w", err)
	}
	if !itemBound {
		return admission.ErrConflict
	}

	digest := identity.Digest()
	_, err = tx.Exec(ctx, `INSERT INTO ticket_admission_intents
		(request_id,payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,key_digest,request_fingerprint,identity_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT(request_id) DO NOTHING`, identity.RequestID, identity.PaymentID, identity.BookingID, identity.TicketID, identity.EventID,
		identity.UserID, identity.ActorID, identity.Generation, identity.KeyDigest[:], identity.Fingerprint[:], digest[:])
	if err != nil {
		return fmt.Errorf("persist Ticket admission intent: %w", err)
	}
	stored, state, _, _, err := loadBookingAdmissionIntent(ctx, tx, identity.RequestID, true)
	if err != nil {
		return err
	}
	if stored != identity {
		return admission.ErrConflict
	}
	if state != admission.Pending {
		return ErrAdmissionTerminal
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("persist Ticket admission intent commit outcome unknown: %w", err)
	}
	return nil
}

func (r *PostgresBookingRepository) AttachAdmissionPermit(ctx context.Context, permit admission.Permit) error {
	if err := permit.ValidateFor(permit.Identity); err != nil {
		return err
	}
	if permit.State != admission.Pending {
		return ErrAdmissionTerminal
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBooking(tx)
	active, err := lockBookingAdmissionMode(ctx, tx)
	if err != nil {
		return err
	}
	if !active {
		return ErrAdmissionInactive
	}
	identity, state, storedToken, storedFence, err := loadBookingAdmissionIntent(ctx, tx, permit.Identity.RequestID, true)
	if err != nil {
		return err
	}
	if identity != permit.Identity {
		return admission.ErrConflict
	}
	if state != admission.Pending {
		return ErrAdmissionTerminal
	}
	if storedToken != uuid.Nil || storedFence != 0 {
		if storedToken != permit.Token || storedFence != permit.Fence {
			return admission.ErrStale
		}
	} else {
		_, err = tx.Exec(ctx, `UPDATE ticket_admission_intents SET permit_token=$2,permit_fence=$3,updated_at=clock_timestamp()
			WHERE request_id=$1 AND state='PENDING' AND permit_token IS NULL AND permit_fence IS NULL`, permit.Identity.RequestID, permit.Token, permit.Fence)
		if err != nil {
			return fmt.Errorf("attach Payment admission permit: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("attach Payment admission permit commit outcome unknown: %w", err)
	}
	return nil
}

func (r *PostgresBookingRepository) ResolveAdmission(ctx context.Context, identity admission.Identity, token uuid.UUID, fence int64, abortPending bool) (admission.Outcome, error) {
	if err := identity.Validate(); err != nil {
		return admission.Outcome{}, err
	}
	if token == uuid.Nil && fence != 0 || token != uuid.Nil && fence <= 0 || abortPending && (token == uuid.Nil || fence <= 0) {
		return admission.Outcome{}, admission.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return admission.Outcome{}, err
	}
	defer rollbackBooking(tx)
	active, err := lockBookingAdmissionMode(ctx, tx)
	if err != nil {
		return admission.Outcome{}, err
	}
	if !active {
		return admission.Outcome{}, ErrAdmissionInactive
	}
	stored, state, storedToken, storedFence, err := loadBookingAdmissionIntent(ctx, tx, identity.RequestID, true)
	if err != nil {
		return admission.Outcome{}, err
	}
	if stored != identity {
		return admission.Outcome{}, admission.ErrConflict
	}
	if token == uuid.Nil {
		if state != admission.Pending {
			return admission.Outcome{}, admission.ErrStale
		}
		if err = tx.Commit(ctx); err != nil {
			return admission.Outcome{}, fmt.Errorf("read pre-permit Booking admission outcome: %w", err)
		}
		return admission.Outcome{Identity: identity, State: admission.Pending}, nil
	}
	if storedToken != uuid.Nil && (storedToken != token || storedFence != fence) {
		return admission.Outcome{}, admission.ErrStale
	}
	if state.Terminal() {
		if storedToken == uuid.Nil {
			return admission.Outcome{}, admission.ErrStale
		}
		outcome := admission.Outcome{Identity: identity, Token: storedToken, Fence: storedFence, State: state}
		if state == admission.Committed {
			var resultCode string
			if err = tx.QueryRow(ctx, `SELECT result_code FROM ticket_admission_intents WHERE request_id=$1`, identity.RequestID).Scan(&resultCode); err != nil {
				return admission.Outcome{}, err
			}
			outcome.ResultCode = resultCode
		}
		if err = tx.Commit(ctx); err != nil {
			return admission.Outcome{}, fmt.Errorf("read terminal Booking admission outcome: %w", err)
		}
		return outcome, nil
	}
	if abortPending {
		if storedToken == uuid.Nil {
			_, err = tx.Exec(ctx, `UPDATE ticket_admission_intents SET permit_token=$2,permit_fence=$3,state='ABORTED',updated_at=clock_timestamp()
				WHERE request_id=$1 AND state='PENDING' AND permit_token IS NULL AND permit_fence IS NULL`, identity.RequestID, token, fence)
		} else {
			_, err = tx.Exec(ctx, `UPDATE ticket_admission_intents SET state='ABORTED',updated_at=clock_timestamp()
				WHERE request_id=$1 AND state='PENDING' AND permit_token=$2 AND permit_fence=$3`, identity.RequestID, token, fence)
		}
		if err != nil {
			return admission.Outcome{}, fmt.Errorf("persist Booking admission abort tombstone: %w", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return admission.Outcome{}, fmt.Errorf("persist Booking admission abort commit outcome unknown: %w", err)
		}
		return admission.Outcome{Identity: identity, Token: token, Fence: fence, State: admission.Aborted}, nil
	}
	if err = tx.Commit(ctx); err != nil {
		return admission.Outcome{}, fmt.Errorf("read pending Booking admission outcome: %w", err)
	}
	return admission.Outcome{Identity: identity, Token: token, Fence: fence, State: admission.Pending}, nil
}

func (r *PostgresBookingRepository) LookupAdmissionIntent(ctx context.Context, actorID uuid.UUID, keyDigest [32]byte, fingerprint []byte, eventID uuid.UUID) (admission.Identity, bool, error) {
	if actorID == uuid.Nil || keyDigest == [32]byte{} || len(fingerprint) != 32 || eventID == uuid.Nil {
		return admission.Identity{}, false, fmt.Errorf("invalid Ticket admission replay lookup")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var identity admission.Identity
	var keyBytes, fingerprintBytes, bindingDigest []byte
	err := r.pool.QueryRow(ctx, `SELECT request_id,payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,key_digest,request_fingerprint,identity_digest
		FROM ticket_admission_intents WHERE actor_id=$1 AND key_digest=$2`, actorID, keyDigest[:]).Scan(
		&identity.RequestID, &identity.PaymentID, &identity.BookingID, &identity.TicketID, &identity.EventID, &identity.UserID, &identity.ActorID,
		&identity.Generation, &keyBytes, &fingerprintBytes, &bindingDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Identity{}, false, nil
	}
	if err != nil {
		return admission.Identity{}, false, fmt.Errorf("lookup Ticket admission replay intent: %w", err)
	}
	if identity.EventID != eventID || len(keyBytes) != 32 || len(fingerprintBytes) != 32 || len(bindingDigest) != 32 {
		return admission.Identity{}, false, ErrTicketIdempotencyConflict
	}
	copy(identity.KeyDigest[:], keyBytes)
	copy(identity.Fingerprint[:], fingerprintBytes)
	computed := identity.Digest()
	if identity.ActorID != actorID || identity.KeyDigest != keyDigest || !sameFingerprint(identity.Fingerprint[:], fingerprint) || !sameFingerprint(bindingDigest, computed[:]) {
		return admission.Identity{}, false, ErrTicketIdempotencyConflict
	}
	if err = identity.Validate(); err != nil {
		return admission.Identity{}, false, err
	}
	return identity, true, nil
}

func lockBookingAdmissionMode(ctx context.Context, tx pgx.Tx) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT active FROM booking_admission_mode WHERE singleton=TRUE FOR SHARE`).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, errors.New("Booking admission mode row is missing")
	}
	if err != nil {
		return false, fmt.Errorf("lock Booking admission mode: %w", err)
	}
	return active, nil
}

func loadBookingAdmissionIntent(ctx context.Context, tx pgx.Tx, requestID uuid.UUID, forUpdate bool) (admission.Identity, admission.State, uuid.UUID, int64, error) {
	query := `SELECT request_id,payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,key_digest,request_fingerprint,identity_digest,state,permit_token,permit_fence
		FROM ticket_admission_intents WHERE request_id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var identity admission.Identity
	var keyBytes, fingerprintBytes, bindingDigest []byte
	var state string
	var token *uuid.UUID
	var fence *int64
	err := tx.QueryRow(ctx, query, requestID).Scan(&identity.RequestID, &identity.PaymentID, &identity.BookingID, &identity.TicketID, &identity.EventID,
		&identity.UserID, &identity.ActorID, &identity.Generation, &keyBytes, &fingerprintBytes, &bindingDigest, &state, &token, &fence)
	if errors.Is(err, pgx.ErrNoRows) {
		return admission.Identity{}, "", uuid.Nil, 0, admission.ErrNotFound
	}
	if err != nil {
		return admission.Identity{}, "", uuid.Nil, 0, fmt.Errorf("read Booking admission intent: %w", err)
	}
	if len(keyBytes) != 32 || len(fingerprintBytes) != 32 || len(bindingDigest) != 32 {
		return admission.Identity{}, "", uuid.Nil, 0, admission.ErrInvalid
	}
	copy(identity.KeyDigest[:], keyBytes)
	copy(identity.Fingerprint[:], fingerprintBytes)
	computed := identity.Digest()
	if !sameFingerprint(bindingDigest, computed[:]) {
		return admission.Identity{}, "", uuid.Nil, 0, admission.ErrConflict
	}
	if err = identity.Validate(); err != nil {
		return admission.Identity{}, "", uuid.Nil, 0, err
	}
	parsedState := admission.State(state)
	if !parsedState.Valid() {
		return admission.Identity{}, "", uuid.Nil, 0, admission.ErrInvalid
	}
	if (token == nil) != (fence == nil) {
		return admission.Identity{}, "", uuid.Nil, 0, admission.ErrInvalid
	}
	var parsedToken uuid.UUID
	var parsedFence int64
	if token != nil {
		parsedToken, parsedFence = *token, *fence
	}
	return identity, parsedState, parsedToken, parsedFence, nil
}

var _ TicketAdmissionRepository = (*PostgresBookingRepository)(nil)
