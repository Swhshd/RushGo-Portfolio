package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

func (r *PostgresBookingRepository) ListOwnedTickets(ctx context.Context, userID uuid.UUID) ([]domain.Ticket, error) {
	if userID == uuid.Nil {
		return nil, ErrTicketOwnerDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT t.id,t.booking_id,t.booking_item_id,t.event_id,t.seat_id,t.ticket_tier_id,t.status,t.issued_at,t.checked_in_at,b.status
		FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE b.user_id=$1 ORDER BY t.issued_at DESC,t.id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tickets := make([]domain.Ticket, 0)
	for rows.Next() {
		var ticket domain.Ticket
		var status, bookingStatus string
		if err := rows.Scan(&ticket.ID, &ticket.BookingID, &ticket.BookingItemID, &ticket.EventID, &ticket.SeatID, &ticket.TicketTierID, &status, &ticket.IssuedAt, &ticket.CheckedInAt, &bookingStatus); err != nil {
			return nil, err
		}
		ticket.Status = domain.TicketStatus(status)
		ticket.BookingStatus = domain.BookingStatus(bookingStatus)
		tickets = append(tickets, ticket)
	}
	return tickets, rows.Err()
}

func (r *PostgresBookingRepository) LoadTicketForCredential(ctx context.Context, userID, ticketID uuid.UUID) (domain.Ticket, domain.TicketIssuanceCandidate, error) {
	if userID == uuid.Nil || ticketID == uuid.Nil {
		return domain.Ticket{}, domain.TicketIssuanceCandidate{}, ErrTicketOwnerDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var ticket domain.Ticket
	var candidate domain.TicketIssuanceCandidate
	var ticketStatus, bookingStatus string
	var generation *int64
	var sagaID *uuid.UUID
	var paymentID *uuid.UUID
	var currency *string
	err := r.pool.QueryRow(ctx, `SELECT t.id,t.booking_id,t.booking_item_id,t.event_id,t.seat_id,t.ticket_tier_id,t.status,t.issued_at,t.checked_in_at,
		b.user_id,b.event_id,b.status,b.total_amount_cents,b.managed,b.lifecycle_generation,t.saga_id,t.payment_id,b.currency
		FROM tickets t JOIN bookings b ON b.id=t.booking_id WHERE t.id=$1`, ticketID).
		Scan(&ticket.ID, &ticket.BookingID, &ticket.BookingItemID, &ticket.EventID, &ticket.SeatID, &ticket.TicketTierID, &ticketStatus, &ticket.IssuedAt, &ticket.CheckedInAt,
			&candidate.UserID, &candidate.EventID, &bookingStatus, &candidate.TotalAmountCents, &candidate.Managed, &generation, &sagaID, &paymentID, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Ticket{}, domain.TicketIssuanceCandidate{}, ErrTicketCredentialMissing
	}
	if err != nil {
		return domain.Ticket{}, domain.TicketIssuanceCandidate{}, fmt.Errorf("load Ticket credential candidate: %w", err)
	}
	if candidate.UserID != userID {
		return domain.Ticket{}, domain.TicketIssuanceCandidate{}, ErrTicketOwnerDenied
	}
	ticket.Status = domain.TicketStatus(ticketStatus)
	ticket.BookingStatus = domain.BookingStatus(bookingStatus)
	candidate.BookingID = ticket.BookingID
	candidate.Status = domain.BookingStatus(bookingStatus)
	if generation != nil {
		candidate.Generation = *generation
	}
	if sagaID != nil {
		candidate.SagaID = *sagaID
	}
	if paymentID != nil {
		candidate.PaymentID = *paymentID
	}
	if currency != nil {
		candidate.Currency = *currency
	}
	booking := &domain.Booking{ID: candidate.BookingID}
	if err = loadLifecycleItems(ctx, r.pool, booking); err != nil {
		return domain.Ticket{}, domain.TicketIssuanceCandidate{}, fmt.Errorf("load Ticket credential Booking items: %w", err)
	}
	candidate.Items = booking.Items
	return ticket, candidate, nil
}

func (r *PostgresBookingRepository) LookupTicketCredentialRotation(ctx context.Context, userID, ticketID uuid.UUID, idempotencyKey string, fingerprint []byte) (domain.TicketCredentialRotationResult, bool, error) {
	if userID == uuid.Nil || ticketID == uuid.Nil || !ValidTicketIdempotencyKey(idempotencyKey) || len(fingerprint) != 32 {
		return domain.TicketCredentialRotationResult{}, false, ErrTicketIdempotencyKeyInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	keyDigest := sha256.Sum256([]byte(idempotencyKey))
	var operation string
	var storedFingerprint []byte
	var storedTicket uuid.UUID
	var resultCode string
	var credentialVersion *int64
	var expiresAt, completedAt *time.Time
	err := r.pool.QueryRow(ctx, `SELECT operation,request_fingerprint,ticket_id,result_code,credential_version,credential_expires_at,completed_at
		FROM ticket_operation_idempotency WHERE actor_user_id=$1 AND idempotency_key_digest=$2`, userID, keyDigest[:]).
		Scan(&operation, &storedFingerprint, &storedTicket, &resultCode, &credentialVersion, &expiresAt, &completedAt)
	if err == nil {
		if operation != "ROTATE_CREDENTIAL" || storedTicket != ticketID || !sameFingerprint(storedFingerprint, fingerprint) {
			return domain.TicketCredentialRotationResult{}, false, ErrTicketIdempotencyConflict
		}
		if completedAt == nil {
			return domain.TicketCredentialRotationResult{}, false, ErrTicketIdempotencyInProgress
		}
		if resultCode != string(domain.TicketCredentialRotationRotated) || credentialVersion == nil || expiresAt == nil {
			return domain.TicketCredentialRotationResult{}, false, errors.New("stored Ticket credential rotation receipt is incomplete")
		}
		return domain.TicketCredentialRotationResult{Outcome: domain.TicketCredentialRotationAlreadyApplied, CredentialVersion: *credentialVersion, ExpiresAt: expiresAt.UTC()}, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketCredentialRotationResult{}, false, err
	}

	// Check pre-migration scan receipts too. New check-ins claim the shared
	// digest-keyed table, while older rows retain their original key text.
	var legacyScanExists bool
	err = r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ticket_scan_requests WHERE staff_user_id=$1 AND idempotency_key IN ($2,$3))`, userID, idempotencyKey, ticketScanKeyStorageValue(idempotencyKey)).Scan(&legacyScanExists)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, false, err
	}
	if legacyScanExists {
		return domain.TicketCredentialRotationResult{}, false, ErrTicketIdempotencyConflict
	}
	return domain.TicketCredentialRotationResult{}, false, nil
}

func (r *PostgresBookingRepository) RotateTicketCredential(ctx context.Context, command domain.TicketCredentialRotationCommand) (domain.TicketCredentialRotationResult, error) {
	if command.UserID == uuid.Nil || command.TicketID == uuid.Nil || !ValidTicketIdempotencyKey(command.IdempotencyKey) || len(command.RequestFingerprint) != 32 || len(command.CredentialDigest) != 32 || !command.ExpiresAt.After(time.Now()) || command.ExpiresAt.After(time.Now().Add(10*time.Minute+5*time.Second)) {
		return domain.TicketCredentialRotationResult{}, ErrTicketIdempotencyKeyInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, err
	}
	defer rollbackBooking(tx)
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='1500ms'`); err != nil {
		return domain.TicketCredentialRotationResult{}, err
	}
	var bookingID uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT booking_id FROM tickets WHERE id=$1`, command.TicketID).Scan(&bookingID); errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketCredentialRotationResult{}, ErrTicketCredentialMissing
	} else if err != nil {
		return domain.TicketCredentialRotationResult{}, ticketOperationWaitError(err)
	}
	var bookingStatus string
	var owner uuid.UUID
	var refundDecisionID *uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT status,user_id,refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).
		Scan(&bookingStatus, &owner, &refundDecisionID); errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketCredentialRotationResult{}, ErrTicketCredentialMissing
	} else if err != nil {
		return domain.TicketCredentialRotationResult{}, ticketOperationWaitError(err)
	}
	if owner != command.UserID {
		return domain.TicketCredentialRotationResult{}, ErrTicketOwnerDenied
	}
	if refundDecisionID != nil {
		return domain.TicketCredentialRotationResult{}, ErrTicketNotEligible
	}
	keyDigest := sha256.Sum256([]byte(command.IdempotencyKey))
	tag, err := tx.Exec(ctx, `INSERT INTO ticket_operation_idempotency(actor_user_id,idempotency_key_digest,operation,request_fingerprint,ticket_id)
		VALUES($1,$2,'ROTATE_CREDENTIAL',$3,$4) ON CONFLICT(actor_user_id,idempotency_key_digest) DO NOTHING`,
		command.UserID, keyDigest[:], command.RequestFingerprint, command.TicketID)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, ticketOperationWaitError(err)
	}
	if tag.RowsAffected() == 0 {
		var operation string
		var storedFingerprint []byte
		var storedTicket uuid.UUID
		var resultCode string
		var credentialVersion *int64
		var expiresAt, completedAt *time.Time
		err = tx.QueryRow(ctx, `SELECT operation,request_fingerprint,ticket_id,result_code,credential_version,credential_expires_at,completed_at
			FROM ticket_operation_idempotency WHERE actor_user_id=$1 AND idempotency_key_digest=$2 FOR UPDATE`, command.UserID, keyDigest[:]).
			Scan(&operation, &storedFingerprint, &storedTicket, &resultCode, &credentialVersion, &expiresAt, &completedAt)
		if err != nil {
			return domain.TicketCredentialRotationResult{}, ticketOperationWaitError(err)
		}
		if operation != "ROTATE_CREDENTIAL" || storedTicket != command.TicketID || !sameFingerprint(storedFingerprint, command.RequestFingerprint) {
			return domain.TicketCredentialRotationResult{}, ErrTicketIdempotencyConflict
		}
		if completedAt == nil {
			return domain.TicketCredentialRotationResult{}, ErrTicketIdempotencyInProgress
		}
		if resultCode != string(domain.TicketCredentialRotationRotated) || credentialVersion == nil || expiresAt == nil {
			return domain.TicketCredentialRotationResult{}, errors.New("stored Ticket credential rotation receipt is incomplete")
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.TicketCredentialRotationResult{}, fmt.Errorf("Ticket credential idempotency replay commit: %w", err)
		}
		return domain.TicketCredentialRotationResult{Outcome: domain.TicketCredentialRotationAlreadyApplied, CredentialVersion: *credentialVersion, ExpiresAt: expiresAt.UTC()}, nil
	}

	// Historical Check-in requests predate the shared operation-key table.
	var legacyScanExists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ticket_scan_requests WHERE staff_user_id=$1 AND idempotency_key IN ($2,$3))`, command.UserID, command.IdempotencyKey, ticketScanKeyStorageValue(command.IdempotencyKey)).Scan(&legacyScanExists)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, err
	}
	if legacyScanExists {
		return domain.TicketCredentialRotationResult{}, ErrTicketIdempotencyConflict
	}

	var status string
	if err = tx.QueryRow(ctx, `SELECT t.status FROM tickets t WHERE t.id=$1 FOR UPDATE OF t`, command.TicketID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketCredentialRotationResult{}, ErrTicketCredentialMissing
	} else if err != nil {
		return domain.TicketCredentialRotationResult{}, ticketOperationWaitError(err)
	}
	if status != string(domain.TicketIssued) || bookingStatus != string(domain.StatusConfirmed) {
		return domain.TicketCredentialRotationResult{}, ErrTicketNotEligible
	}
	var version int64
	err = tx.QueryRow(ctx, `INSERT INTO ticket_credentials(ticket_id,token_digest,expires_at,credential_version,rotated_at)
		VALUES($1,$2,$3,1,clock_timestamp())
		ON CONFLICT(ticket_id) DO UPDATE SET token_digest=EXCLUDED.token_digest,expires_at=EXCLUDED.expires_at,credential_version=ticket_credentials.credential_version+1,rotated_at=clock_timestamp()
		RETURNING credential_version`, command.TicketID, command.CredentialDigest, command.ExpiresAt.UTC()).Scan(&version)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, fmt.Errorf("rotate Ticket credential: %w", err)
	}
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `UPDATE ticket_operation_idempotency SET result_code='ROTATED',credential_version=$3,credential_expires_at=$4,completed_at=clock_timestamp()
		WHERE actor_user_id=$1 AND idempotency_key_digest=$2 AND operation='ROTATE_CREDENTIAL' AND completed_at IS NULL
		RETURNING credential_expires_at`, command.UserID, keyDigest[:], version, command.ExpiresAt.UTC()).Scan(&expiresAt)
	if err != nil {
		return domain.TicketCredentialRotationResult{}, fmt.Errorf("complete Ticket credential idempotency receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.TicketCredentialRotationResult{}, fmt.Errorf("Ticket credential commit outcome unknown: %w", err)
	}
	return domain.TicketCredentialRotationResult{Outcome: domain.TicketCredentialRotationRotated, CredentialVersion: version, ExpiresAt: expiresAt.UTC()}, nil
}

func ticketOperationWaitError(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "55P03" {
		return ErrTicketIdempotencyInProgress
	}
	return err
}

func (r *PostgresBookingRepository) IsScannerAssigned(ctx context.Context, eventID, staffID uuid.UUID) (bool, error) {
	if eventID == uuid.Nil || staffID == uuid.Nil {
		return false, ErrScannerNotAssigned
	}
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	var active bool
	err := r.pool.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2`, eventID, staffID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return active, err
}

func (r *PostgresBookingRepository) SetScannerAssignment(ctx context.Context, eventID, staffID, actorID uuid.UUID, active bool) (domain.EventScannerAssignment, error) {
	if eventID == uuid.Nil || staffID == uuid.Nil || actorID == uuid.Nil {
		return domain.EventScannerAssignment{}, fmt.Errorf("invalid scanner assignment identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.EventScannerAssignment{}, err
	}
	defer rollbackBooking(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('rushgo:event-scanner:' || $1::text || ':' || $2::text, 0))`, eventID, staffID); err != nil {
		return domain.EventScannerAssignment{}, fmt.Errorf("lock event scanner assignment identity: %w", err)
	}
	var current bool
	err = tx.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2 FOR UPDATE`, eventID, staffID).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return domain.EventScannerAssignment{}, err
	}
	noRow := errors.Is(err, pgx.ErrNoRows)
	changed := noRow || current != active
	now := time.Now().UTC()
	if changed {
		if active {
			_, err = tx.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at,revoked_by,revoked_at)
				VALUES($1,$2,true,$3,clock_timestamp(),NULL,NULL) ON CONFLICT(event_id,staff_user_id) DO UPDATE SET active=true,granted_by=EXCLUDED.granted_by,granted_at=clock_timestamp(),revoked_by=NULL,revoked_at=NULL`, eventID, staffID, actorID)
			if err != nil {
				return domain.EventScannerAssignment{}, fmt.Errorf("grant scanner assignment: %w", err)
			}
		} else if !noRow {
			_, err = tx.Exec(ctx, `UPDATE event_scanner_assignments SET active=false,revoked_by=$3,revoked_at=clock_timestamp() WHERE event_id=$1 AND staff_user_id=$2`, eventID, staffID, actorID)
			if err != nil {
				return domain.EventScannerAssignment{}, fmt.Errorf("revoke scanner assignment: %w", err)
			}
		} else {
			changed = false
		}
		action := "revoked"
		if active {
			action = "granted"
		}
		if changed {
			if _, err = tx.Exec(ctx, `INSERT INTO event_scanner_assignment_audit(id,event_id,staff_user_id,action,actor_user_id) VALUES($1,$2,$3,$4,$5)`, uuid.New(), eventID, staffID, action, actorID); err != nil {
				return domain.EventScannerAssignment{}, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.EventScannerAssignment{}, err
	}
	return domain.EventScannerAssignment{EventID: eventID, StaffUserID: staffID, Active: active, ChangedAt: now}, nil
}

func (r *PostgresBookingRepository) ListScannerAssignments(ctx context.Context, eventID uuid.UUID) ([]domain.EventScannerAssignment, error) {
	if eventID == uuid.Nil {
		return nil, fmt.Errorf("invalid scanner event")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT event_id,staff_user_id,active,GREATEST(granted_at,COALESCE(revoked_at,granted_at))
		FROM event_scanner_assignments WHERE event_id=$1 ORDER BY staff_user_id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assignments := make([]domain.EventScannerAssignment, 0)
	for rows.Next() {
		var v domain.EventScannerAssignment
		if err := rows.Scan(&v.EventID, &v.StaffUserID, &v.Active, &v.ChangedAt); err != nil {
			return nil, err
		}
		assignments = append(assignments, v)
	}
	return assignments, rows.Err()
}

func (r *PostgresBookingRepository) LoadTicketScanCandidate(ctx context.Context, digest []byte) (domain.TicketScanCandidate, error) {
	if len(digest) != 32 {
		return domain.TicketScanCandidate{}, ErrTicketCredentialMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var c domain.TicketScanCandidate
	var ticketStatus, bookingStatus string
	var generation *int64
	var sagaID *uuid.UUID
	var currency *string
	var bookingEventID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT t.id,t.booking_id,t.booking_item_id,t.event_id,t.seat_id,t.ticket_tier_id,t.status,t.issued_at,t.checked_in_at,c.expires_at,
		b.user_id,b.event_id,b.status,b.total_amount_cents,b.managed,b.lifecycle_generation,t.saga_id,t.payment_id,b.currency,b.refund_decision_id
		FROM ticket_credentials c JOIN tickets t ON t.id=c.ticket_id JOIN bookings b ON b.id=t.booking_id WHERE c.token_digest=$1`, digest).
		Scan(&c.Ticket.ID, &c.Ticket.BookingID, &c.Ticket.BookingItemID, &c.Ticket.EventID, &c.Ticket.SeatID, &c.Ticket.TicketTierID, &ticketStatus, &c.Ticket.IssuedAt, &c.Ticket.CheckedInAt, &c.CredentialExpiresAt,
			&c.Booking.UserID, &bookingEventID, &bookingStatus, &c.Booking.TotalAmountCents, &c.Booking.Managed, &generation, &sagaID, &c.Booking.PaymentID, &currency, &c.BookingRefundDecisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanCandidate{}, ErrTicketCredentialMissing
	}
	if err != nil {
		return domain.TicketScanCandidate{}, err
	}
	c.Ticket.Status = domain.TicketStatus(ticketStatus)
	c.Booking.Status = domain.BookingStatus(bookingStatus)
	c.Booking.BookingID = c.Ticket.BookingID
	c.Booking.EventID = bookingEventID
	if generation != nil {
		c.Booking.Generation = *generation
	}
	if sagaID != nil {
		c.Booking.SagaID = *sagaID
	}
	if currency != nil {
		c.Booking.Currency = *currency
	}
	booking := &domain.Booking{ID: c.Booking.BookingID}
	if err = loadLifecycleItems(ctx, r.pool, booking); err != nil {
		return domain.TicketScanCandidate{}, err
	}
	c.Booking.Items = booking.Items
	return c, nil
}

func (r *PostgresBookingRepository) LookupTicketScanRequest(ctx context.Context, staffID, eventID uuid.UUID, idempotencyKey string, fingerprint []byte) (domain.TicketScanResult, bool, error) {
	if staffID == uuid.Nil || eventID == uuid.Nil || !validTicketScanKey(idempotencyKey) || len(fingerprint) != 32 {
		return domain.TicketScanResult{}, false, fmt.Errorf("invalid Ticket scan idempotency lookup")
	}
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.TicketScanResult{}, false, err
	}
	defer rollbackBooking(tx)
	var assigned bool
	err = tx.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2 FOR SHARE`, eventID, staffID).Scan(&assigned)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !assigned {
		return domain.TicketScanResult{}, false, ErrScannerNotAssigned
	}
	if err != nil {
		return domain.TicketScanResult{}, false, err
	}
	keyDigest := sha256.Sum256([]byte(idempotencyKey))
	var operation string
	var operationFingerprint []byte
	var operationCompletedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT operation,request_fingerprint,completed_at FROM ticket_operation_idempotency
		WHERE actor_user_id=$1 AND idempotency_key_digest=$2`, staffID, keyDigest[:]).
		Scan(&operation, &operationFingerprint, &operationCompletedAt)
	if err == nil {
		if operation != "CHECK_IN" || !sameFingerprint(operationFingerprint, fingerprint) {
			return domain.TicketScanResult{}, false, ErrTicketIdempotencyConflict
		}
		if operationCompletedAt == nil {
			return domain.TicketScanResult{}, false, ErrTicketIdempotencyInProgress
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanResult{}, false, err
	}
	var storedFingerprint []byte
	var storedEvent uuid.UUID
	var result domain.TicketScanResult
	err = tx.QueryRow(ctx, `SELECT request_fingerprint,event_id,result_code,reason_code,checked_in_at,evidence_source,observed_at
		FROM ticket_scan_requests WHERE staff_user_id=$1 AND idempotency_key IN ($2,$3)`, staffID, idempotencyKey, ticketScanKeyStorageValue(idempotencyKey)).
		Scan(&storedFingerprint, &storedEvent, &result.ResultCode, &result.ReasonCode, &result.CheckedInAt, &result.EvidenceSource, &result.ObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = tx.Commit(ctx); err != nil {
			return domain.TicketScanResult{}, false, err
		}
		return domain.TicketScanResult{}, false, nil
	}
	if err != nil {
		return domain.TicketScanResult{}, false, fmt.Errorf("lookup Ticket scan idempotency result: %w", err)
	}
	if storedEvent != eventID || !sameFingerprint(storedFingerprint, fingerprint) {
		return domain.TicketScanResult{}, false, ErrTicketIdempotencyConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.TicketScanResult{}, false, err
	}
	result.Replayed = true
	return result, true, nil
}

func (r *PostgresBookingRepository) CommitTicketCheckIn(ctx context.Context, c domain.TicketScanCommand) (domain.TicketScanResult, error) {
	if err := validTicketScanCommand(c); err != nil {
		return domain.TicketScanResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	defer rollbackBooking(tx)
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='1500ms'`); err != nil {
		return domain.TicketScanResult{}, ticketOperationWaitError(err)
	}
	admissionActive, err := lockBookingAdmissionMode(ctx, tx)
	if err != nil {
		return domain.TicketScanResult{}, ticketOperationWaitError(err)
	}
	if admissionActive && c.Decision.Eligible && c.Admission == nil {
		return domain.TicketScanResult{}, ErrAdmissionRequired
	}
	if c.Admission != nil {
		if !admissionActive {
			return domain.TicketScanResult{}, ErrAdmissionInactive
		}
		if err = validateScanAdmissionPermit(c); err != nil {
			return domain.TicketScanResult{}, err
		}
	}
	var bookingID uuid.UUID
	var ticketEventID uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT booking_id,event_id FROM tickets WHERE id=$1`, c.TicketID).Scan(&bookingID, &ticketEventID); errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanResult{}, ErrTicketCredentialMissing
	} else if err != nil {
		return domain.TicketScanResult{}, ticketOperationWaitError(err)
	}
	var bookingEventID uuid.UUID
	var refundDecisionID *uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT event_id,refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).Scan(&bookingEventID, &refundDecisionID); errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanResult{}, ErrTicketCredentialMissing
	} else if err != nil {
		return domain.TicketScanResult{}, ticketOperationWaitError(err)
	}
	if ticketEventID != c.EventID || bookingEventID != c.EventID {
		return domain.TicketScanResult{}, ErrTicketIdentityConflict
	}
	var assigned bool
	err = tx.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2 FOR UPDATE`, c.EventID, c.StaffUserID).Scan(&assigned)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !assigned {
		return domain.TicketScanResult{}, ErrScannerNotAssigned
	}
	if err != nil {
		return domain.TicketScanResult{}, err
	}

	var oldFingerprint []byte
	var oldEvent uuid.UUID
	var oldTicket *uuid.UUID
	var oldResult, oldReason string
	var oldAt *time.Time
	var oldSource string
	var oldObserved *time.Time
	err = tx.QueryRow(ctx, `SELECT request_fingerprint,event_id,ticket_id,result_code,reason_code,checked_in_at,evidence_source,observed_at FROM ticket_scan_requests WHERE staff_user_id=$1 AND idempotency_key IN ($2,$3) FOR UPDATE`, c.StaffUserID, c.IdempotencyKey, ticketScanKeyStorageValue(c.IdempotencyKey)).
		Scan(&oldFingerprint, &oldEvent, &oldTicket, &oldResult, &oldReason, &oldAt, &oldSource, &oldObserved)
	if err == nil {
		if oldTicket == nil || *oldTicket != c.TicketID || !sameFingerprint(oldFingerprint, c.RequestFingerprint) || oldEvent != c.EventID {
			return domain.TicketScanResult{}, ErrTicketIdempotencyConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.TicketScanResult{}, err
		}
		return domain.TicketScanResult{ResultCode: oldResult, ReasonCode: oldReason, CheckedInAt: oldAt, Replayed: true, EvidenceSource: oldSource, ObservedAt: oldObserved}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanResult{}, err
	}
	keyDigest := sha256.Sum256([]byte(c.IdempotencyKey))
	if err = claimTicketOperationIdempotency(ctx, tx, c.StaffUserID, keyDigest[:], "CHECK_IN", c.TicketID, &c.EventID, c.RequestFingerprint); err != nil {
		return domain.TicketScanResult{}, err
	}
	if c.Admission != nil {
		identity, state, token, fence, loadErr := loadBookingAdmissionIntent(ctx, tx, c.Admission.Identity.RequestID, true)
		if loadErr != nil {
			return domain.TicketScanResult{}, loadErr
		}
		if identity != c.Admission.Identity {
			return domain.TicketScanResult{}, admission.ErrConflict
		}
		if state != admission.Pending {
			return domain.TicketScanResult{}, ErrAdmissionTerminal
		}
		if token != c.Admission.Token || fence != c.Admission.Fence {
			return domain.TicketScanResult{}, admission.ErrStale
		}
	}

	var status, bookingStatus string
	var owner, paymentID, eventID, seatID uuid.UUID
	var managed bool
	var generation, ticketGeneration int64
	var digest []byte
	var expiry time.Time
	var existingCheckedInAt *time.Time
	err = tx.QueryRow(ctx, `SELECT t.status,b.status,b.user_id,t.booking_id,t.payment_id,t.event_id,b.event_id,t.seat_id,b.managed,b.lifecycle_generation,t.lifecycle_generation,c.token_digest,c.expires_at,t.checked_in_at
		FROM tickets t JOIN bookings b ON b.id=t.booking_id JOIN ticket_credentials c ON c.ticket_id=t.id
		WHERE t.id=$1 FOR UPDATE OF t,c`, c.TicketID).Scan(&status, &bookingStatus, &owner, &bookingID, &paymentID, &eventID, &bookingEventID, &seatID, &managed, &generation, &ticketGeneration, &digest, &expiry, &existingCheckedInAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketScanResult{}, ErrTicketCredentialMissing
	}
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	if c.Admission != nil && (c.Admission.Identity.BookingID != bookingID || c.Admission.Identity.PaymentID != paymentID ||
		c.Admission.Identity.EventID != eventID || c.Admission.Identity.EventID != bookingEventID || c.Admission.Identity.UserID != owner ||
		c.Admission.Identity.Generation != generation || c.Admission.Identity.Generation != ticketGeneration) {
		return domain.TicketScanResult{}, admission.ErrConflict
	}
	result := domain.TicketScanResult{ResultCode: "denied", ReasonCode: c.Decision.ReasonCode, EvidenceSource: c.Evidence.Source}
	if !c.Evidence.ObservedAt.IsZero() {
		observed := c.Evidence.ObservedAt.UTC()
		result.ObservedAt = &observed
	}
	if c.EventID != eventID || eventID != bookingEventID {
		result.ReasonCode = "event_mismatch"
	} else if !bytes.Equal(digest, c.CredentialDigest) {
		result.ReasonCode = "credential_rotated"
	} else if !expiry.After(time.Now()) {
		result.ReasonCode = "credential_expired"
	} else if refundDecisionID != nil {
		result.ReasonCode = "refund_approved"
		if c.Evidence.ObservedAt.IsZero() {
			result.EvidenceSource = "booking_refund_fence"
		}
	} else if status == string(domain.TicketRevoked) {
		result.ReasonCode = "ticket_revoked"
	} else if status == string(domain.TicketCheckedIn) {
		if existingCheckedInAt == nil {
			return domain.TicketScanResult{}, errors.New("checked-in Ticket has no check-in timestamp")
		}
		result.ResultCode = "already_checked_in"
		result.ReasonCode = "already_checked_in"
		result.CheckedInAt = existingCheckedInAt
	} else if bookingStatus != string(domain.StatusConfirmed) || !managed || generation != 1 {
		result.ReasonCode = "booking_not_eligible"
	} else if c.Decision.Eligible {
		var decisionAt time.Time
		if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&decisionAt); err != nil {
			return domain.TicketScanResult{}, err
		}
		paymentQueryStartedAt := c.Evidence.PaymentQueryStartedAt
		paymentQueryWallStartedAt := c.Evidence.PaymentQueryWallStartedAt
		if paymentQueryStartedAt.IsZero() || paymentQueryWallStartedAt.IsZero() ||
			!domain.PaymentEvidenceFreshAt(c.Evidence.ObservedAt, decisionAt) ||
			!domain.PaymentEvidenceElapsedWithinBound(
				time.Since(paymentQueryStartedAt),
				time.Since(paymentQueryWallStartedAt),
			) {
			result.ReasonCode = "payment_evidence_stale"
		} else {
			checkedInAt := decisionAt
			var admissionRequest any
			if c.Admission != nil {
				admissionRequest = c.Admission.Identity.RequestID
			}
			if err = tx.QueryRow(ctx, `UPDATE tickets SET status='CHECKED_IN',checked_in_at=$2,admission_request_id=$3 WHERE id=$1 AND status='ISSUED' RETURNING checked_in_at`, c.TicketID, decisionAt, admissionRequest).Scan(&checkedInAt); err != nil {
				return domain.TicketScanResult{}, err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO ticket_checkins(id,ticket_id,event_id,staff_user_id,result_code,dispute_state,refund_state,evidence_source,observed_at,checked_in_at)
				VALUES($1,$2,$3,$4,'accepted',$5,$6,$7,$8,$9)`, uuid.New(), c.TicketID, c.EventID, c.StaffUserID, string(c.Evidence.DisputeState), string(c.Evidence.RefundState), c.Evidence.Source, c.Evidence.ObservedAt.UTC(), checkedInAt); err != nil {
				return domain.TicketScanResult{}, err
			}
			result.ResultCode = "accepted"
			result.ReasonCode = "checked_in"
			result.CheckedInAt = &checkedInAt
		}
	}
	if c.Admission != nil {
		state, resultCode := string(admission.Aborted), result.ResultCode
		if result.ResultCode == "accepted" {
			state = string(admission.Committed)
			resultCode = "accepted"
		} else if result.ResultCode != "already_checked_in" {
			resultCode = "denied"
		}
		completion, updateErr := tx.Exec(ctx, `UPDATE ticket_admission_intents SET state=$4,result_code=$5,updated_at=clock_timestamp()
			WHERE request_id=$1 AND state='PENDING' AND permit_token=$2 AND permit_fence=$3`, c.Admission.Identity.RequestID, c.Admission.Token, c.Admission.Fence, state, resultCode)
		if updateErr != nil {
			return domain.TicketScanResult{}, fmt.Errorf("persist Ticket admission outcome: %w", updateErr)
		}
		if completion.RowsAffected() != 1 {
			return domain.TicketScanResult{}, admission.ErrStale
		}
	}
	var observed any
	if result.ObservedAt != nil {
		observed = *result.ObservedAt
	}
	disputeState := string(c.Evidence.DisputeState)
	if disputeState == "" {
		disputeState = string(domain.EvidenceUnknown)
	}
	refundState := string(c.Evidence.RefundState)
	if refundState == "" {
		refundState = string(domain.RefundUnknown)
	}
	var checked any
	if result.CheckedInAt != nil {
		checked = *result.CheckedInAt
	}
	// Return the persisted timestamp precision on the first response as well as
	// replay. The input evidence can contain nanoseconds; PostgreSQL stores microseconds.
	err = tx.QueryRow(ctx, `INSERT INTO ticket_scan_requests(staff_user_id,idempotency_key,request_fingerprint,event_id,ticket_id,result_code,reason_code,checked_in_at,dispute_state,refund_state,evidence_source,observed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING observed_at`, c.StaffUserID, ticketScanKeyStorageValue(c.IdempotencyKey), c.RequestFingerprint, c.EventID, c.TicketID, result.ResultCode, result.ReasonCode, checked, disputeState, refundState, result.EvidenceSource, observed).Scan(&result.ObservedAt)
	if err != nil {
		return domain.TicketScanResult{}, fmt.Errorf("persist Ticket scan idempotency record: %w", err)
	}
	completion, err := tx.Exec(ctx, `UPDATE ticket_operation_idempotency SET result_code=$3,completed_at=clock_timestamp()
		WHERE actor_user_id=$1 AND idempotency_key_digest=$2 AND operation='CHECK_IN' AND completed_at IS NULL`, c.StaffUserID, keyDigest[:], result.ResultCode)
	if err != nil {
		return domain.TicketScanResult{}, fmt.Errorf("complete Ticket operation idempotency record: %w", err)
	}
	if completion.RowsAffected() != 1 {
		return domain.TicketScanResult{}, errors.New("Ticket operation idempotency record did not complete")
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.TicketScanResult{}, fmt.Errorf("Ticket check-in transaction outcome unknown: %w", err)
	}
	return result, nil
}

func validateScanAdmissionPermit(c domain.TicketScanCommand) error {
	if c.Admission == nil {
		return nil
	}
	permit := c.Admission
	if err := permit.ValidateFor(permit.Identity); err != nil {
		return err
	}
	keyDigest := sha256.Sum256([]byte(c.IdempotencyKey))
	if permit.State != admission.Pending || permit.Identity.ActorID != c.StaffUserID || permit.Identity.EventID != c.EventID ||
		permit.Identity.TicketID != c.TicketID || permit.Identity.KeyDigest != keyDigest ||
		!sameFingerprint(permit.Identity.Fingerprint[:], c.RequestFingerprint) ||
		permit.Identity.RequestID != admission.RequestIDFor(c.StaffUserID, keyDigest) {
		return admission.ErrConflict
	}
	return nil
}

func claimTicketOperationIdempotency(ctx context.Context, tx pgx.Tx, actorID uuid.UUID, keyDigest []byte, operation string, ticketID uuid.UUID, eventID *uuid.UUID, fingerprint []byte) error {
	var event any
	if eventID != nil {
		event = *eventID
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ticket_operation_idempotency(actor_user_id,idempotency_key_digest,operation,request_fingerprint,ticket_id,event_id)
		VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(actor_user_id,idempotency_key_digest) DO NOTHING`, actorID, keyDigest, operation, fingerprint, ticketID, event)
	if err != nil {
		return ticketOperationWaitError(err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var storedOperation string
	var storedFingerprint []byte
	var storedTicket uuid.UUID
	var storedEventID *uuid.UUID
	var completedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT operation,request_fingerprint,ticket_id,event_id,completed_at FROM ticket_operation_idempotency
		WHERE actor_user_id=$1 AND idempotency_key_digest=$2 FOR UPDATE`, actorID, keyDigest).Scan(&storedOperation, &storedFingerprint, &storedTicket, &storedEventID, &completedAt)
	if err != nil {
		return ticketOperationWaitError(err)
	}
	if storedOperation != operation || storedTicket != ticketID || !sameFingerprint(storedFingerprint, fingerprint) || (eventID == nil) != (storedEventID == nil) || eventID != nil && *eventID != *storedEventID {
		return ErrTicketIdempotencyConflict
	}
	return ErrTicketIdempotencyInProgress
}

func ticketScanKeyStorageValue(idempotencyKey string) string {
	digest := sha256.Sum256([]byte(idempotencyKey))
	return fmt.Sprintf("sha256:%x", digest[:])
}

var _ TicketRepository = (*PostgresBookingRepository)(nil)
