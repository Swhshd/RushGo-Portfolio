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

const refundApprovalAttemptSelect = `SELECT attempt_id,request_id,booking_id,event_id,buyer_user_id,payment_id,generation,
	approver_user_id,decision_key_digest,request_fingerprint,reason,state,event_starts_at,event_qualified_at,
	event_authority_version,created_at,terminal_at,release_acknowledged_at
	FROM booking_refund_approval_attempts`

func scanRefundApprovalAttempt(row pgx.Row) (domain.RefundApprovalAttempt, error) {
	var result domain.RefundApprovalAttempt
	var keyDigest, fingerprint []byte
	var state string
	var startsAt, qualifiedAt, terminalAt, releaseAcknowledgedAt *time.Time
	err := row.Scan(&result.AttemptID, &result.RequestID, &result.BookingID, &result.EventID, &result.BuyerID, &result.PaymentID, &result.Generation,
		&result.ApproverID, &keyDigest, &fingerprint, &result.Reason, &state, &startsAt, &qualifiedAt,
		&result.EventAuthorityVersion, &result.CreatedAt, &terminalAt, &releaseAcknowledgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if len(keyDigest) != len(result.KeyDigest) || len(fingerprint) != len(result.Fingerprint) {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("invalid persisted Booking refund approval attempt digest length")
	}
	copy(result.KeyDigest[:], keyDigest)
	copy(result.Fingerprint[:], fingerprint)
	result.State = domain.RefundApprovalAttemptState(state)
	if startsAt != nil {
		result.StartsAt = startsAt.UTC()
	}
	if qualifiedAt != nil {
		result.EventQualifiedAt = qualifiedAt.UTC()
	}
	if terminalAt != nil {
		result.TerminalAt = terminalAt.UTC()
	}
	result.ReleaseAcknowledged = releaseAcknowledgedAt != nil
	return result, nil
}

func (r *PostgresBookingRepository) BeginUnusedRefundApproval(ctx context.Context, command domain.BeginUnusedRefundApprovalCommand) (domain.RefundApprovalAttempt, error) {
	if r == nil || r.pool == nil || command.AttemptID == uuid.Nil || command.RequestID == uuid.Nil || command.EventID == uuid.Nil || command.ApproverID == uuid.Nil ||
		command.KeyDigest == [32]byte{} || command.Fingerprint == [32]byte{} || len([]rune(command.Reason)) > 500 {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotEligible
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	defer rollbackBooking(tx)
	if _, err = lockBookingAdmissionMode(ctx, tx); err != nil {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("lock Booking Admission mode for refund approval attempt: %w", err)
	}
	var bookingID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT booking_id FROM booking_refund_requests WHERE request_id=$1 AND event_id=$2`, command.RequestID, command.EventID).Scan(&bookingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	var bookingEventID, ownerID uuid.UUID
	var refundDecisionID *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT event_id,user_id,refund_decision_id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).
		Scan(&bookingEventID, &ownerID, &refundDecisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if bookingEventID != command.EventID || refundDecisionID != nil {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotEligible
	}
	existingAttempt, existingErr := scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE request_id=$1 FOR UPDATE`, command.RequestID))
	if existingErr != nil && !errors.Is(existingErr, ErrRefundRequestNotFound) {
		return domain.RefundApprovalAttempt{}, existingErr
	}
	request, err := getRefundRequestByID(ctx, tx, command.RequestID, true)
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if request.EventID != command.EventID || request.BuyerID != ownerID {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotEligible
	}
	if request.Status != domain.RefundRequestRequested && request.Status != domain.RefundRequestWaitingCheckInResolution {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestFinal
	}
	if request.BuyerID == command.ApproverID {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestApproverDenied
	}
	if err = lockEventRefundApprover(ctx, tx, command.EventID, command.ApproverID); err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if err = lockAndRequireNonScanner(ctx, tx, command.EventID, command.ApproverID); err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	replay, replayFound, replayErr := refundIdempotencyReplay(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint)
	if replayErr != nil {
		return domain.RefundApprovalAttempt{}, replayErr
	} else if replayFound && replay.RequestID != command.RequestID {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestIdempotencyConflict
	}
	if existingErr == nil {
		if existingAttempt.RequestID != request.RequestID || existingAttempt.BookingID != request.BookingID || existingAttempt.EventID != request.EventID ||
			existingAttempt.BuyerID != request.BuyerID || existingAttempt.PaymentID != request.PaymentID || existingAttempt.Generation != request.Generation ||
			request.QualificationAttemptID != existingAttempt.AttemptID || existingAttempt.ApproverID != command.ApproverID ||
			existingAttempt.KeyDigest != command.KeyDigest || existingAttempt.Fingerprint != command.Fingerprint || existingAttempt.Reason != command.Reason {
			return domain.RefundApprovalAttempt{}, ErrRefundApprovalAttemptConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.RefundApprovalAttempt{}, fmt.Errorf("commit Booking refund approval attempt replay: %w", err)
		}
		return existingAttempt, nil
	}
	if pending, pendingErr := pendingBookingAdmission(ctx, tx, request.BookingID); pendingErr != nil {
		return domain.RefundApprovalAttempt{}, pendingErr
	} else if pending {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestPendingAdmission
	}
	candidate, reasonCode, err := loadUnusedRefundCandidate(ctx, tx, request.BuyerID, request.BookingID, "")
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if reasonCode != "" || candidate.EventID != request.EventID || candidate.BookingID != request.BookingID || candidate.PaymentID != request.PaymentID ||
		candidate.BuyerID != request.BuyerID || candidate.Generation != request.Generation || candidate.AmountCents != request.AmountCents || candidate.Currency != request.Currency {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotEligible
	}
	if !replayFound {
		if _, inserted, claimErr := claimRefundIdempotency(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint, request.RequestID); claimErr != nil {
			return domain.RefundApprovalAttempt{}, claimErr
		} else if !inserted {
			replay, found, replayErr := refundIdempotencyReplay(ctx, tx, command.ApproverID, "decide", command.KeyDigest, command.Fingerprint)
			if replayErr != nil {
				return domain.RefundApprovalAttempt{}, replayErr
			}
			if !found || replay.RequestID != request.RequestID {
				return domain.RefundApprovalAttempt{}, ErrRefundRequestIdempotencyConflict
			}
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO booking_refund_approval_attempts
		(attempt_id,request_id,booking_id,event_id,buyer_user_id,payment_id,generation,approver_user_id,decision_key_digest,request_fingerprint,reason,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'PENDING')`, command.AttemptID, request.RequestID, request.BookingID, request.EventID,
		request.BuyerID, request.PaymentID, request.Generation, command.ApproverID, command.KeyDigest[:], command.Fingerprint[:], command.Reason)
	if err != nil {
		if isUniqueViolation(err, "booking_refund_one_approval_attempt_per_request") || isUniqueViolation(err, "booking_refund_approval_attempts_pkey") {
			return domain.RefundApprovalAttempt{}, ErrRefundApprovalAttemptConflict
		}
		return domain.RefundApprovalAttempt{}, fmt.Errorf("persist Booking refund approval attempt: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET qualification_attempt_id=$2,event_qualified_at=NULL WHERE request_id=$1 AND decision_id IS NULL`, request.RequestID, command.AttemptID); err != nil {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("bind Booking refund request to approval attempt: %w", err)
	}
	attempt, err := scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE attempt_id=$1`, command.AttemptID))
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("commit Booking refund approval attempt: %w", err)
	}
	return attempt, nil
}

func (r *PostgresBookingRepository) FinalizeUnusedRefundApproval(ctx context.Context, command domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	if command.QualificationAttemptID == uuid.Nil || command.DecisionID != command.QualificationAttemptID {
		return domain.BookingRefundRequest{}, ErrRefundApprovalAttemptConflict
	}
	return r.decideUnusedRefund(ctx, command, true)
}

func (r *PostgresBookingRepository) GetUnusedRefundApprovalAttempt(ctx context.Context, attemptID uuid.UUID) (domain.RefundApprovalAttempt, error) {
	if r == nil || r.pool == nil || attemptID == uuid.Nil {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return scanRefundApprovalAttempt(r.pool.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE attempt_id=$1`, attemptID))
}

func (r *PostgresBookingRepository) AbortUnusedRefundApprovalAttempt(ctx context.Context, attemptID uuid.UUID, _ string) (domain.RefundApprovalAttempt, error) {
	if r == nil || r.pool == nil || attemptID == uuid.Nil {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	defer rollbackBooking(tx)
	if _, err = lockBookingAdmissionMode(ctx, tx); err != nil {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("lock Booking Admission mode to abort refund approval attempt: %w", err)
	}
	var bookingID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT booking_id FROM booking_refund_approval_attempts WHERE attempt_id=$1`, attemptID).Scan(&bookingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundApprovalAttempt{}, ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).Scan(&bookingID); err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	attempt, err := scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE attempt_id=$1 FOR UPDATE`, attemptID))
	if err != nil {
		return domain.RefundApprovalAttempt{}, err
	}
	if attempt.State == domain.RefundApprovalAttemptPending {
		if _, err = tx.Exec(ctx, `UPDATE booking_refund_approval_attempts SET state='ABORTED',terminal_at=clock_timestamp() WHERE attempt_id=$1 AND state='PENDING'`, attemptID); err != nil {
			return domain.RefundApprovalAttempt{}, err
		}
		attempt.State = domain.RefundApprovalAttemptAborted
		attempt.TerminalAt = time.Now().UTC()
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.RefundApprovalAttempt{}, fmt.Errorf("commit Booking refund approval abort: %w", err)
	}
	return attempt, nil
}

func (r *PostgresBookingRepository) ListUnusedRefundApprovalAttemptsForRecovery(ctx context.Context, limit int) ([]domain.RefundApprovalAttempt, error) {
	if r == nil || r.pool == nil {
		return nil, ErrRefundRequestNotEligible
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackBooking(tx)
	rows, err := tx.Query(ctx, `WITH picked AS MATERIALIZED (
		SELECT attempt_id,recovery_checked_at,created_at
		FROM booking_refund_approval_attempts
		WHERE state='PENDING' OR (state<>'PENDING' AND release_acknowledged_at IS NULL)
		ORDER BY recovery_checked_at,created_at,attempt_id
		LIMIT $1 FOR UPDATE SKIP LOCKED
	), touched AS (
		UPDATE booking_refund_approval_attempts a
		SET recovery_checked_at=GREATEST(clock_timestamp(),a.recovery_checked_at+interval '1 microsecond')
		FROM picked p WHERE a.attempt_id=p.attempt_id
		RETURNING a.attempt_id
	)
	SELECT a.attempt_id,a.request_id,a.booking_id,a.event_id,a.buyer_user_id,a.payment_id,a.generation,
		a.approver_user_id,a.decision_key_digest,a.request_fingerprint,a.reason,a.state,a.event_starts_at,a.event_qualified_at,
		a.event_authority_version,a.created_at,a.terminal_at,a.release_acknowledged_at
	FROM touched t JOIN picked p ON p.attempt_id=t.attempt_id
	JOIN booking_refund_approval_attempts a ON a.attempt_id=t.attempt_id
	ORDER BY p.recovery_checked_at,p.created_at,p.attempt_id`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.RefundApprovalAttempt, 0, limit)
	for rows.Next() {
		item, scanErr := scanRefundApprovalAttempt(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *PostgresBookingRepository) AcknowledgeUnusedRefundAttemptRelease(ctx context.Context, attemptID uuid.UUID, outcome string) error {
	if r == nil || r.pool == nil || attemptID == uuid.Nil {
		return ErrRefundRequestNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBooking(tx)
	if _, err = lockBookingAdmissionMode(ctx, tx); err != nil {
		return fmt.Errorf("lock Booking Admission mode to acknowledge refund attempt release: %w", err)
	}
	var bookingID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT booking_id FROM booking_refund_approval_attempts WHERE attempt_id=$1`, attemptID).Scan(&bookingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrRefundRequestNotFound
	}
	if err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, bookingID).Scan(&bookingID); err != nil {
		return err
	}
	attempt, err := scanRefundApprovalAttempt(tx.QueryRow(ctx, refundApprovalAttemptSelect+` WHERE attempt_id=$1 FOR UPDATE`, attemptID))
	if err != nil {
		return err
	}
	if attempt.State == domain.RefundApprovalAttemptPending || string(attempt.State) != outcome {
		return ErrRefundApprovalAttemptFinal
	}
	if !attempt.ReleaseAcknowledged {
		if _, err = tx.Exec(ctx, `UPDATE booking_refund_approval_attempts SET release_acknowledged_at=clock_timestamp() WHERE attempt_id=$1 AND release_acknowledged_at IS NULL`, attemptID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
