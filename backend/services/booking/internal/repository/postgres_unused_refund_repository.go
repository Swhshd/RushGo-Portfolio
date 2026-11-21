package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/booking/internal/domain"
)

const bookingRefundRequestSelect = `SELECT r.request_id,r.booking_id,r.event_id,r.buyer_user_id,r.payment_id,r.generation,
	r.amount_cents,r.currency,r.status,r.reason,r.decision_reason,r.decision_id,r.approver_user_id,
	d.payment_intent_id,d.provider_account,d.livemode,d.event_starts_at,d.event_authority_version,
	COALESCE(d.qualification_attempt_id,r.qualification_attempt_id),COALESCE(d.event_qualified_at,r.event_qualified_at),
	r.requested_at,r.decided_at,r.payment_refund_state,r.payment_refund_amount_cents,r.payment_refund_status,
	r.ticket_revocation_state,r.ticket_revoked_at
	FROM booking_refund_requests r LEFT JOIN booking_refund_decisions d ON d.decision_id=r.decision_id`

func scanBookingRefundRequest(row pgx.Row) (domain.BookingRefundRequest, error) {
	var result domain.BookingRefundRequest
	var status string
	var decisionID, approverID *uuid.UUID
	var qualificationAttemptID *uuid.UUID
	var paymentIntentID, providerAccount, authorityVersion *string
	var livemode *bool
	var eventQualifiedAt *time.Time
	err := row.Scan(&result.RequestID, &result.BookingID, &result.EventID, &result.BuyerID, &result.PaymentID, &result.Generation,
		&result.AmountCents, &result.Currency, &status, &result.Reason, &result.DecisionReason, &decisionID, &approverID,
		&paymentIntentID, &providerAccount, &livemode, &result.EventStartsAt, &authorityVersion, &qualificationAttemptID, &eventQualifiedAt,
		&result.RequestedAt, &result.DecidedAt, &result.PaymentRefundState, &result.PaymentRefundAmountCents, &result.PaymentRefundStatus,
		&result.TicketRevocationState, &result.TicketRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	result.Status = domain.RefundRequestStatus(status)
	if decisionID != nil {
		result.DecisionID = *decisionID
	}
	if approverID != nil {
		result.ApproverID = *approverID
	}
	if paymentIntentID != nil {
		result.PaymentIntentID = *paymentIntentID
	}
	if providerAccount != nil {
		result.ProviderAccount = *providerAccount
	}
	if livemode != nil {
		result.Livemode = *livemode
	}
	if authorityVersion != nil {
		result.EventAuthorityVersion = *authorityVersion
	}
	if qualificationAttemptID != nil {
		result.QualificationAttemptID = *qualificationAttemptID
	}
	if eventQualifiedAt != nil {
		result.EventQualifiedAt = eventQualifiedAt.UTC()
	}
	return result, nil
}

func (r *PostgresBookingRepository) PreviewUnusedRefund(ctx context.Context, buyerID, bookingID uuid.UUID) (domain.RefundApprovalCandidate, string, error) {
	if r == nil || r.pool == nil || buyerID == uuid.Nil || bookingID == uuid.Nil {
		return domain.RefundApprovalCandidate{}, "invalid_identity", ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", err
	}
	defer rollbackBooking(tx)
	candidate, reason, err := loadUnusedRefundCandidate(ctx, tx, buyerID, bookingID, "FOR SHARE")
	if err != nil {
		return candidate, "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.RefundApprovalCandidate{}, "", err
	}
	return candidate, reason, nil
}

func (r *PostgresBookingRepository) RequestUnusedRefund(ctx context.Context, command domain.RequestUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || command.RequestID == uuid.Nil || command.BookingID == uuid.Nil || command.BuyerID == uuid.Nil ||
		command.KeyDigest == [32]byte{} || command.Fingerprint == [32]byte{} || !domain.ValidRefundRequestReason(command.Reason) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	defer rollbackBooking(tx)
	var owner uuid.UUID
	err = tx.QueryRow(ctx, `SELECT user_id FROM bookings WHERE id=$1 FOR UPDATE`, command.BookingID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if owner != command.BuyerID {
		return domain.BookingRefundRequest{}, ErrRefundRequestOwnerDenied
	}
	if request, found, err := refundIdempotencyReplay(ctx, tx, command.BuyerID, "request", command.KeyDigest, command.Fingerprint); err != nil {
		return domain.BookingRefundRequest{}, err
	} else if found {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, nil
	}
	candidate, reason, err := loadUnusedRefundCandidate(ctx, tx, command.BuyerID, command.BookingID, "")
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if reason != "" {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotEligible
	}
	if request, inserted, err := claimRefundIdempotency(ctx, tx, command.BuyerID, "request", command.KeyDigest, command.Fingerprint, command.RequestID); err != nil {
		return domain.BookingRefundRequest{}, err
	} else if !inserted {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, nil
	}
	_, err = tx.Exec(ctx, `INSERT INTO booking_refund_requests
		(request_id,booking_id,event_id,buyer_user_id,payment_id,generation,amount_cents,currency,reason,status)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'REQUESTED')`, command.RequestID, candidate.BookingID, candidate.EventID, candidate.BuyerID,
		candidate.PaymentID, candidate.Generation, candidate.AmountCents, candidate.Currency, command.Reason)
	if err != nil {
		if isUniqueViolation(err, "booking_refund_one_open_request") {
			return domain.BookingRefundRequest{}, ErrRefundRequestConflict
		}
		return domain.BookingRefundRequest{}, fmt.Errorf("persist Booking refund request: %w", err)
	}
	request, err := getRefundRequestByID(ctx, tx, command.RequestID, false)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("commit Booking refund request: %w", err)
	}
	return request, nil
}

func (r *PostgresBookingRepository) GetUnusedRefundByBooking(ctx context.Context, buyerID, bookingID uuid.UUID) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || buyerID == uuid.Nil || bookingID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	result, err := scanBookingRefundRequest(r.pool.QueryRow(ctx, bookingRefundRequestSelect+`
		WHERE r.booking_id=$1 AND r.buyer_user_id=$2 ORDER BY r.requested_at DESC,r.request_id DESC LIMIT 1`, bookingID, buyerID))
	if errors.Is(err, ErrRefundRequestNotFound) {
		return domain.BookingRefundRequest{}, err
	}
	if err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("read Booking refund request: %w", err)
	}
	return result, nil
}

func (r *PostgresBookingRepository) GetUnusedRefundByRequest(ctx context.Context, buyerID, requestID uuid.UUID) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || buyerID == uuid.Nil || requestID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	result, err := scanBookingRefundRequest(r.pool.QueryRow(ctx, bookingRefundRequestSelect+` WHERE r.request_id=$1 AND r.buyer_user_id=$2`, requestID, buyerID))
	if err != nil {
		if errors.Is(err, ErrRefundRequestNotFound) {
			return domain.BookingRefundRequest{}, err
		}
		return domain.BookingRefundRequest{}, fmt.Errorf("read Booking refund request: %w", err)
	}
	return result, nil
}

func (r *PostgresBookingRepository) GetEventUnusedRefundByID(ctx context.Context, eventID, requestID uuid.UUID) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil || requestID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return scanBookingRefundRequest(r.pool.QueryRow(ctx, bookingRefundRequestSelect+` WHERE r.request_id=$1 AND r.event_id=$2`, requestID, eventID))
}

func (r *PostgresBookingRepository) HasPendingBookingAdmission(ctx context.Context, bookingID uuid.UUID) (bool, error) {
	if r == nil || r.pool == nil || bookingID == uuid.Nil {
		return false, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return pendingBookingAdmission(ctx, r.pool, bookingID)
}

func (r *PostgresBookingRepository) WithdrawUnusedRefund(ctx context.Context, command domain.WithdrawUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || command.RequestID == uuid.Nil || command.BuyerID == uuid.Nil || command.KeyDigest == [32]byte{} || command.Fingerprint == [32]byte{} {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	defer rollbackBooking(tx)
	if _, err = lockBookingAdmissionMode(ctx, tx); err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("lock Booking Admission mode for withdrawal: %w", err)
	}
	var bookingID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT booking_id FROM booking_refund_requests WHERE request_id=$1 AND buyer_user_id=$2`, command.RequestID, command.BuyerID).Scan(&bookingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	var lockedBookingID uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).Scan(&lockedBookingID); errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	} else if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	pendingAttempt, attemptErr := scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE request_id=$1 AND state='PENDING' FOR UPDATE`, command.RequestID))
	hasPendingAttempt := attemptErr == nil
	if attemptErr != nil && !errors.Is(attemptErr, ErrRefundRequestNotFound) {
		return domain.BookingRefundRequest{}, attemptErr
	}
	request, err := getRefundRequestByID(ctx, tx, command.RequestID, true)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if replay, found, replayErr := refundIdempotencyReplay(ctx, tx, command.BuyerID, "withdraw", command.KeyDigest, command.Fingerprint); replayErr != nil {
		return domain.BookingRefundRequest{}, replayErr
	} else if found {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return replay, nil
	}
	if request.Status == domain.RefundRequestWithdrawn {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, nil
	}
	if request.Status != domain.RefundRequestRequested && request.Status != domain.RefundRequestWaitingCheckInResolution {
		return domain.BookingRefundRequest{}, ErrRefundRequestFinal
	}
	if _, inserted, err := claimRefundIdempotency(ctx, tx, command.BuyerID, "withdraw", command.KeyDigest, command.Fingerprint, request.RequestID); err != nil {
		return domain.BookingRefundRequest{}, err
	} else if !inserted {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return getRefundRequestByID(ctx, r.pool, request.RequestID, false)
	}
	if hasPendingAttempt {
		if err = terminalizeRefundApprovalAttempt(ctx, tx, pendingAttempt.AttemptID, domain.RefundApprovalAttemptAborted); err != nil {
			return domain.BookingRefundRequest{}, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='WITHDRAWN' WHERE request_id=$1 AND status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION')`, request.RequestID); err != nil {
		return domain.BookingRefundRequest{}, err
	}
	request, err = getRefundRequestByID(ctx, tx, request.RequestID, false)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.BookingRefundRequest{}, err
	}
	return request, nil
}

func (r *PostgresBookingRepository) DecideUnusedRefund(ctx context.Context, command domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	return r.decideUnusedRefund(ctx, command, false)
}

func (r *PostgresBookingRepository) decideUnusedRefund(ctx context.Context, command domain.DecideUnusedRefundCommand, coordinated bool) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || command.DecisionID == uuid.Nil || command.RequestID == uuid.Nil || command.EventID == uuid.Nil || command.ApproverID == uuid.Nil ||
		command.KeyDigest == [32]byte{} || command.Fingerprint == [32]byte{} || (command.Kind != domain.RefundDecisionApproved && command.Kind != domain.RefundDecisionRejected) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotEligible
	}
	if coordinated && (command.Kind != domain.RefundDecisionApproved || command.QualificationAttemptID == uuid.Nil || command.QualificationAttemptID != command.DecisionID) {
		return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptConflict
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	defer rollbackBooking(tx)
	admissionActive, modeErr := lockBookingAdmissionMode(ctx, tx)
	if modeErr != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("lock Booking Admission mode for refund decision: %w", modeErr)
	}
	if command.Kind == domain.RefundDecisionApproved && !admissionActive {
		return domain.BookingRefundRequest{}, ErrRefundRequestAdmissionInactive
	}
	var bookingID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT booking_id FROM booking_refund_requests WHERE request_id=$1`, command.RequestID).Scan(&bookingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	var bookingEventID uuid.UUID
	var bookingRefundDecisionID *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT event_id,refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).Scan(&bookingEventID, &bookingRefundDecisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	var attempt domain.RefundApprovalAttempt
	hasPendingAttempt := false
	hasApprovalAttempt := false
	if coordinated {
		attempt, err = scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE attempt_id=$1 FOR UPDATE`, command.QualificationAttemptID))
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
		if attempt.State != domain.RefundApprovalAttemptPending {
			return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptFinal
		}
		hasPendingAttempt = true
		hasApprovalAttempt = true
	} else {
		attempt, err = scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE request_id=$1 FOR UPDATE`, command.RequestID))
		if err == nil {
			hasApprovalAttempt = true
			hasPendingAttempt = attempt.State == domain.RefundApprovalAttemptPending
		} else if !errors.Is(err, ErrRefundRequestNotFound) {
			return domain.BookingRefundRequest{}, err
		}
	}
	request, err := getRefundRequestByID(ctx, tx, command.RequestID, true)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if request.EventID != command.EventID || bookingEventID != request.EventID {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotEligible
	}
	if coordinated && (attempt.AttemptID != command.DecisionID || attempt.RequestID != request.RequestID || attempt.BookingID != request.BookingID ||
		attempt.EventID != request.EventID || attempt.BuyerID != request.BuyerID || attempt.PaymentID != request.PaymentID ||
		attempt.Generation != request.Generation || attempt.ApproverID != command.ApproverID || attempt.KeyDigest != command.KeyDigest ||
		attempt.Fingerprint != command.Fingerprint || attempt.Reason != command.Reason || command.Event.QualificationAttemptID != attempt.AttemptID) {
		return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptConflict
	}
	if !coordinated && hasApprovalAttempt && command.Kind == domain.RefundDecisionApproved &&
		(attempt.AttemptID != command.DecisionID || attempt.ApproverID != command.ApproverID || attempt.KeyDigest != command.KeyDigest ||
			attempt.Fingerprint != command.Fingerprint || attempt.Reason != command.Reason) {
		return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptConflict
	}
	continuePendingAttemptWait := !coordinated && hasPendingAttempt && command.Kind == domain.RefundDecisionApproved
	abortAttemptForError := func(cause error) (domain.BookingRefundRequest, error) {
		if !coordinated {
			return domain.BookingRefundRequest{}, cause
		}
		if terminalErr := terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptAborted); terminalErr != nil {
			return domain.BookingRefundRequest{}, terminalErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return domain.BookingRefundRequest{}, fmt.Errorf("commit Booking refund approval abort after %v: %w", cause, commitErr)
		}
		return domain.BookingRefundRequest{}, cause
	}
	if request.BuyerID == command.ApproverID {
		return abortAttemptForError(ErrRefundRequestApproverDenied)
	}
	if err = lockEventRefundApprover(ctx, tx, command.EventID, command.ApproverID); err != nil {
		if errors.Is(err, ErrRefundRequestApproverDenied) {
			return abortAttemptForError(err)
		}
		return domain.BookingRefundRequest{}, err
	}
	if err = lockAndRequireNonScanner(ctx, tx, command.EventID, command.ApproverID); err != nil {
		if errors.Is(err, ErrRefundRequestScannerDenied) {
			return abortAttemptForError(err)
		}
		return domain.BookingRefundRequest{}, err
	}
	replay, replayFound, replayErr := refundIdempotencyReplay(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint)
	if replayErr != nil {
		return domain.BookingRefundRequest{}, replayErr
	}
	if replayFound && !coordinated && !continuePendingAttemptWait &&
		(request.Status != domain.RefundRequestWaitingCheckInResolution || command.Kind != domain.RefundDecisionApproved) {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return replay, nil
	}
	if request.Status == domain.RefundRequestRejected || request.Status == domain.RefundRequestApproved ||
		request.Status == domain.RefundRequestProcessing || request.Status == domain.RefundRequestRefunded || request.Status == domain.RefundRequestNeedsAttention {
		return abortAttemptForError(ErrRefundRequestFinal)
	}
	if request.Status != domain.RefundRequestRequested && request.Status != domain.RefundRequestWaitingCheckInResolution {
		return abortAttemptForError(ErrRefundRequestFinal)
	}
	if coordinated && (command.Event.EventID != request.EventID || command.Event.StartsAt.IsZero() || command.Event.EventQualifiedAt.IsZero() ||
		!command.Event.EventQualifiedAt.Before(command.Event.StartsAt) || command.Event.AuthorityVersion == "") {
		return abortAttemptForError(ErrRefundRequestEventAuthorityUnavailable)
	}
	if !replayFound {
		_, inserted, claimErr := claimRefundIdempotency(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint, request.RequestID)
		if claimErr != nil {
			return domain.BookingRefundRequest{}, claimErr
		}
		if !inserted {
			replay, replayFound, replayErr = refundIdempotencyReplay(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint)
			if replayErr != nil {
				return domain.BookingRefundRequest{}, replayErr
			}
			if !replayFound || request.Status != domain.RefundRequestWaitingCheckInResolution || command.Kind != domain.RefundDecisionApproved {
				return domain.BookingRefundRequest{}, ErrRefundRequestIdempotencyConflict
			}
		}
	}
	if command.Kind == domain.RefundDecisionRejected {
		if hasPendingAttempt {
			if err = terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptAborted); err != nil {
				return domain.BookingRefundRequest{}, err
			}
		}
		if err = insertRefundDecision(ctx, tx, command, request, nil); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='REJECTED',decision_reason=$2,decision_id=$3,approver_user_id=$4,decided_at=clock_timestamp()
			WHERE request_id=$1 AND status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION')`, request.RequestID, command.Reason, command.DecisionID, command.ApproverID)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
	} else {
		if bookingRefundDecisionID != nil {
			return abortAttemptForError(ErrRefundRequestFinal)
		}
		candidate, reason, candidateErr := loadUnusedRefundCandidate(ctx, tx, request.BuyerID, request.BookingID, "")
		if candidateErr != nil {
			return domain.BookingRefundRequest{}, candidateErr
		}
		if reason != "" {
			if coordinated {
				if err = terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptRejected); err != nil {
					return domain.BookingRefundRequest{}, err
				}
			} else if hasPendingAttempt {
				if err = terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptRejected); err != nil {
					return domain.BookingRefundRequest{}, err
				}
			}
			if err = persistUnusedRefundRejection(ctx, tx, command, request, reason); err != nil {
				return domain.BookingRefundRequest{}, err
			}
		} else if candidate.EventID != request.EventID || candidate.BookingID != request.BookingID || candidate.PaymentID != request.PaymentID ||
			candidate.BuyerID != request.BuyerID || candidate.Generation != request.Generation || candidate.AmountCents != request.AmountCents || candidate.Currency != request.Currency {
			if coordinated {
				if err = terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptRejected); err != nil {
					return domain.BookingRefundRequest{}, err
				}
			} else if hasPendingAttempt {
				if err = terminalizeRefundApprovalAttempt(ctx, tx, attempt.AttemptID, domain.RefundApprovalAttemptRejected); err != nil {
					return domain.BookingRefundRequest{}, err
				}
			}
			if err = persistUnusedRefundRejection(ctx, tx, command, request, "booking_identity_changed"); err != nil {
				return domain.BookingRefundRequest{}, err
			}
		} else if pending, pendingErr := pendingBookingAdmission(ctx, tx, request.BookingID); pendingErr != nil {
			return domain.BookingRefundRequest{}, pendingErr
		} else if pending {
			_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='WAITING_CHECKIN_RESOLUTION' WHERE request_id=$1 AND status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION')`, request.RequestID)
			if err != nil {
				return domain.BookingRefundRequest{}, err
			}
		} else {
			if hasApprovalAttempt && !coordinated {
				return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptConflict
			}
			if command.Event.EventID != request.EventID || command.Event.StartsAt.IsZero() {
				return abortAttemptForError(ErrRefundRequestEventAuthorityUnavailable)
			}
			var beforeEventStart bool
			if coordinated {
				beforeEventStart = command.Event.EventQualifiedAt.Before(command.Event.StartsAt)
			} else {
				var decisionAt time.Time
				if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&decisionAt); err != nil {
					return domain.BookingRefundRequest{}, err
				}
				beforeEventStart = decisionAt.Before(command.Event.StartsAt)
			}
			if !beforeEventStart {
				if coordinated {
					return abortAttemptForError(ErrRefundRequestEventAuthorityUnavailable)
				}
				if err = persistUnusedRefundRejection(ctx, tx, command, request, "event_started_or_time_untrusted"); err != nil {
					return domain.BookingRefundRequest{}, err
				}
			} else if err = domain.ValidateUnusedRefundPaymentSnapshot(candidate, command.Payment); err != nil {
				if coordinated {
					return abortAttemptForError(fmt.Errorf("%w: %v", ErrRefundRequestPaymentConflict, err))
				}
				return domain.BookingRefundRequest{}, fmt.Errorf("%w: %v", ErrRefundRequestPaymentConflict, err)
			} else if err = commitUnusedRefundApproval(ctx, tx, command, request); err != nil {
				return domain.BookingRefundRequest{}, err
			}
		}
	}
	result, err := getRefundRequestByID(ctx, tx, request.RequestID, false)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("commit Booking refund decision: %w", err)
	}
	return result, nil
}

func (r *PostgresBookingRepository) ListEventUnusedRefunds(ctx context.Context, query domain.RefundEventListQuery) ([]domain.BookingRefundRequest, bool, error) {
	if r == nil || r.pool == nil || query.EventID == uuid.Nil || query.Limit < 1 || query.Limit > 100 || (query.BeforeAt == nil) != (query.BeforeID == uuid.Nil) {
		return nil, false, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, bookingRefundRequestSelect+`
		WHERE r.event_id=$1 AND ($2::timestamptz IS NULL OR r.requested_at<$2 OR (r.requested_at=$2 AND r.request_id<$3))
		ORDER BY r.requested_at DESC,r.request_id DESC LIMIT $4`, query.EventID, query.BeforeAt, query.BeforeID, query.Limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := make([]domain.BookingRefundRequest, 0, query.Limit)
	for rows.Next() {
		item, scanErr := scanBookingRefundRequest(rows)
		if scanErr != nil {
			return nil, false, scanErr
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(items) > query.Limit
	if hasMore {
		items = items[:query.Limit]
	}
	return items, hasMore, nil
}

func (r *PostgresBookingRepository) SetEventRefundApprover(ctx context.Context, eventID, staffID, actorID uuid.UUID, active bool) (domain.RefundApproverAssignment, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil || staffID == uuid.Nil || actorID == uuid.Nil || (active && staffID == actorID) {
		return domain.RefundApproverAssignment{}, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RefundApproverAssignment{}, err
	}
	defer rollbackBooking(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('rushgo:event-refund-approver:' || $1::text || ':' || $2::text,0))`, eventID, staffID); err != nil {
		return domain.RefundApproverAssignment{}, err
	}
	var current bool
	err = tx.QueryRow(ctx, `SELECT active FROM event_refund_approvers WHERE event_id=$1 AND staff_user_id=$2 FOR UPDATE`, eventID, staffID).Scan(&current)
	noRow := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !noRow {
		return domain.RefundApproverAssignment{}, err
	}
	changed := (noRow && active) || (!noRow && current != active)
	if changed && active {
		_, err = tx.Exec(ctx, `INSERT INTO event_refund_approvers(event_id,staff_user_id,active,granted_by,granted_at,revoked_by,revoked_at)
			VALUES($1,$2,TRUE,$3,clock_timestamp(),NULL,NULL)
			ON CONFLICT(event_id,staff_user_id) DO UPDATE SET active=TRUE,granted_by=EXCLUDED.granted_by,granted_at=clock_timestamp(),revoked_by=NULL,revoked_at=NULL`, eventID, staffID, actorID)
	} else if changed && !noRow {
		_, err = tx.Exec(ctx, `UPDATE event_refund_approvers SET active=FALSE,revoked_by=$3,revoked_at=clock_timestamp() WHERE event_id=$1 AND staff_user_id=$2`, eventID, staffID, actorID)
	}
	if err != nil {
		return domain.RefundApproverAssignment{}, fmt.Errorf("update event refund approver: %w", err)
	}
	if changed {
		action := "revoked"
		if active {
			action = "granted"
		}
		if _, err = tx.Exec(ctx, `INSERT INTO event_refund_approver_audit(id,event_id,staff_user_id,action,actor_user_id) VALUES($1,$2,$3,$4,$5)`, uuid.New(), eventID, staffID, action, actorID); err != nil {
			return domain.RefundApproverAssignment{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.RefundApproverAssignment{}, err
	}
	return domain.RefundApproverAssignment{EventID: eventID, StaffUserID: staffID, Active: active, ChangedAt: time.Now().UTC()}, nil
}

func (r *PostgresBookingRepository) ListEventRefundApprovers(ctx context.Context, eventID uuid.UUID) ([]domain.RefundApproverAssignment, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil {
		return nil, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT event_id,staff_user_id,active,CASE WHEN active THEN granted_at ELSE revoked_at END
		FROM event_refund_approvers WHERE event_id=$1 ORDER BY staff_user_id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assignments := make([]domain.RefundApproverAssignment, 0)
	for rows.Next() {
		var assignment domain.RefundApproverAssignment
		if err = rows.Scan(&assignment.EventID, &assignment.StaffUserID, &assignment.Active, &assignment.ChangedAt); err != nil {
			return nil, err
		}
		assignments = append(assignments, assignment)
	}
	return assignments, rows.Err()
}

func (r *PostgresBookingRepository) IsEventRefundApprover(ctx context.Context, eventID, userID uuid.UUID) (bool, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil || userID == uuid.Nil {
		return false, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var active bool
	err := r.pool.QueryRow(ctx, `SELECT active FROM event_refund_approvers WHERE event_id=$1 AND staff_user_id=$2`, eventID, userID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return active, err
}

func (r *PostgresBookingRepository) CheckEventRefundDecisionActor(ctx context.Context, eventID, actorID, buyerID uuid.UUID) error {
	if r == nil || r.pool == nil || eventID == uuid.Nil || actorID == uuid.Nil || buyerID == uuid.Nil {
		return ErrRefundRequestApproverDenied
	}
	if actorID == buyerID {
		return ErrRefundRequestApproverDenied
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var approverActive bool
	err := r.pool.QueryRow(ctx, `SELECT active FROM event_refund_approvers WHERE event_id=$1 AND staff_user_id=$2`, eventID, actorID).Scan(&approverActive)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !approverActive) {
		return ErrRefundRequestApproverDenied
	}
	if err != nil {
		return err
	}
	var scannerActive bool
	err = r.pool.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2`, eventID, actorID).Scan(&scannerActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if scannerActive {
		return ErrRefundRequestScannerDenied
	}
	return nil
}

func (r *PostgresBookingRepository) GetApprovedUnusedRefundDecision(ctx context.Context, bookingID, decisionID uuid.UUID) (domain.RefundDecisionSnapshot, error) {
	if r == nil || r.pool == nil || bookingID == uuid.Nil || decisionID == uuid.Nil {
		return domain.RefundDecisionSnapshot{}, ErrRefundRequestNotApproved
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return approvedUnusedRefundDecision(ctx, r.pool, bookingID, decisionID)
}

func (r *PostgresBookingRepository) ListUnusedRefundsForRecovery(ctx context.Context, limit int) ([]domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil {
		return nil, ErrRefundRequestNotEligible
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `WITH picked AS MATERIALIZED (
		SELECT r.request_id,r.recovery_checked_at,r.requested_at
		FROM booking_refund_requests r
		WHERE r.status IN ('APPROVED','REFUND_PROCESSING','NEEDS_ATTENTION')
			OR (r.status='REFUNDED' AND r.ticket_revocation_state<>'revoked')
		ORDER BY r.recovery_checked_at,r.requested_at,r.request_id
		LIMIT $1 FOR UPDATE OF r SKIP LOCKED
	), touched AS (
		UPDATE booking_refund_requests r
		SET recovery_checked_at=GREATEST(clock_timestamp(),r.recovery_checked_at+interval '1 microsecond')
		FROM picked p WHERE r.request_id=p.request_id
		RETURNING r.request_id
	)
	SELECT r.request_id,r.booking_id,r.event_id,r.buyer_user_id,r.payment_id,r.generation,
		r.amount_cents,r.currency,r.status,r.reason,r.decision_reason,r.decision_id,r.approver_user_id,
		d.payment_intent_id,d.provider_account,d.livemode,d.event_starts_at,d.event_authority_version,
		d.qualification_attempt_id,d.event_qualified_at,
		r.requested_at,r.decided_at,r.payment_refund_state,r.payment_refund_amount_cents,r.payment_refund_status,
		r.ticket_revocation_state,r.ticket_revoked_at
	FROM touched t
	JOIN picked p ON p.request_id=t.request_id
	JOIN booking_refund_requests r ON r.request_id=t.request_id
	LEFT JOIN booking_refund_decisions d ON d.decision_id=r.decision_id
	ORDER BY p.recovery_checked_at,p.requested_at,p.request_id`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.BookingRefundRequest, 0, limit)
	for rows.Next() {
		item, scanErr := scanBookingRefundRequest(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *PostgresBookingRepository) ApplyUnusedRefundPaymentSnapshot(ctx context.Context, expected domain.BookingRefundRequest, snapshot domain.UnusedRefundPaymentSnapshot) (domain.BookingRefundRequest, error) {
	if r == nil || r.pool == nil || expected.BookingID == uuid.Nil || expected.DecisionID == uuid.Nil || expected.RequestID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotApproved
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	defer rollbackBooking(tx)
	var currentDecisionID *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, expected.BookingID).Scan(&currentDecisionID)
	if errors.Is(err, pgx.ErrNoRows) || currentDecisionID == nil || *currentDecisionID != expected.DecisionID {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotApproved
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	request, err := getRefundRequestByID(ctx, tx, expected.RequestID, true)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if request.DecisionID != expected.DecisionID || request.BookingID != expected.BookingID || request.Status == domain.RefundRequestWithdrawn || request.DecisionID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotApproved
	}
	if request.Status == domain.RefundRequestRefunded && request.TicketRevocationState == "revoked" {
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, nil
	}
	if request.Status == domain.RefundRequestRefunded && request.PaymentRefundState == "succeeded" && request.PaymentRefundAmountCents == request.AmountCents {
		// A durable exact-success projection is monotonic. A stale in-flight
		// snapshot from another poller must not downgrade it during the crash gap.
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return revokeUnusedRefundTickets(ctx, r, request)
	}
	if request.Status != domain.RefundRequestApproved && request.Status != domain.RefundRequestProcessing && request.Status != domain.RefundRequestNeedsAttention && request.Status != domain.RefundRequestRefunded {
		return domain.BookingRefundRequest{}, ErrRefundRequestNotApproved
	}
	if snapshot.RefundDecisionID == uuid.Nil {
		noRefundRecorded := (snapshot.RefundState == "" || snapshot.RefundState == "not_required") && snapshot.RefundAmountCents == 0 && snapshot.RefundStatus == ""
		if sameRefundPaymentIdentity(request, snapshot) && snapshot.Collected && noRefundRecorded {
			// Booking approval is durable before the outbox reaches Payment. Keep
			// the request pending until the same decision is accepted; this path is
			// intentionally local-only and never treats missing receipt as success.
			if err = tx.Commit(ctx); err != nil {
				return domain.BookingRefundRequest{}, err
			}
			return request, nil
		}
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='NEEDS_ATTENTION',payment_refund_state='unknown',
			payment_refund_amount_cents=0,payment_refund_status='',ticket_revocation_state='needs_attention'
			WHERE request_id=$1 AND decision_id=$2 AND status<>'REFUNDED'`, request.RequestID, request.DecisionID)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
		request, err = getRefundRequestByID(ctx, tx, request.RequestID, false)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, ErrRefundRequestPaymentConflict
	}
	if !sameApprovedRefundIdentity(request, snapshot) {
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='NEEDS_ATTENTION',payment_refund_state='unknown',
			payment_refund_amount_cents=0,payment_refund_status='',ticket_revocation_state='needs_attention'
			WHERE request_id=$1 AND decision_id=$2 AND status<>'REFUNDED'`, request.RequestID, request.DecisionID)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
		request, err = getRefundRequestByID(ctx, tx, request.RequestID, false)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.BookingRefundRequest{}, err
		}
		return request, ErrRefundRequestPaymentConflict
	}
	if paymentRefundIsExactlySucceeded(request, snapshot) {
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='REFUNDED',payment_refund_state='succeeded',
			payment_refund_amount_cents=$3,payment_refund_status=$4,ticket_revocation_state='pending_payment',ticket_revoked_at=NULL
			WHERE request_id=$1 AND decision_id=$2`, request.RequestID, request.DecisionID, snapshot.RefundAmountCents, snapshot.RefundStatus)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
	} else {
		attention := isRefundAttentionState(snapshot.RefundState) || !isRefundPendingState(snapshot.RefundState)
		request, err = updateUnusedRefundProjection(ctx, tx, request, snapshot, attention)
		if err != nil {
			return domain.BookingRefundRequest{}, err
		}
	}
	if paymentRefundIsExactlySucceeded(request, snapshot) {
		request.Status = domain.RefundRequestRefunded
		request.PaymentRefundState = snapshot.RefundState
		request.PaymentRefundAmountCents = snapshot.RefundAmountCents
		request.PaymentRefundStatus = snapshot.RefundStatus
		request.TicketRevocationState = "pending_payment"
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if request.PaymentRefundState != "succeeded" || request.TicketRevocationState == "revoked" {
		return request, nil
	}
	// Commit the authoritative refund projection before touching Ticket rows.
	// This avoids holding Booking while waiting for a legacy Ticket writer; the
	// immutable Booking refund pointer blocks that writer when it resumes.
	return revokeUnusedRefundTickets(ctx, r, request)
}
