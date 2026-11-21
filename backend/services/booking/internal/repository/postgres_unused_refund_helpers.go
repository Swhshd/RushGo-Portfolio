package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/booking/internal/domain"
)

type refundQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func getRefundRequestByID(ctx context.Context, q refundQueryer, id uuid.UUID, forUpdate bool) (domain.BookingRefundRequest, error) {
	query := bookingRefundRequestSelect + ` WHERE r.request_id=$1`
	if forUpdate {
		query += ` FOR UPDATE OF r`
	}
	return scanBookingRefundRequest(q.QueryRow(ctx, query, id))
}

func loadUnusedRefundCandidate(ctx context.Context, q refundQueryer, buyerID, bookingID uuid.UUID, lock string) (domain.RefundApprovalCandidate, string, error) {
	if lock != "" && lock != "FOR SHARE" && lock != "FOR UPDATE" {
		return domain.RefundApprovalCandidate{}, "invalid_lock", ErrRefundRequestNotEligible
	}
	query := `SELECT b.id,b.user_id,b.event_id,b.status,b.total_amount_cents,b.managed,
		b.lifecycle_generation,b.lifecycle_phase,b.currency,b.refund_decision_id,
		COALESCE((SELECT sum(bi.quantity)::bigint FROM booking_items bi WHERE bi.booking_id=b.id),0)
		FROM bookings b WHERE b.id=$1`
	if lock != "" {
		query += " " + lock
	}
	var gotBooking, owner, eventID uuid.UUID
	var status, lifecyclePhase string
	var amountCents, generation, expectedTickets int64
	var managed bool
	var currency *string
	var refundDecisionID *uuid.UUID
	err := q.QueryRow(ctx, query, bookingID).Scan(&gotBooking, &owner, &eventID, &status, &amountCents, &managed,
		&generation, &lifecyclePhase, &currency, &refundDecisionID, &expectedTickets)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundApprovalCandidate{}, "booking_missing", ErrRefundRequestNotFound
	}
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", fmt.Errorf("read Booking refund candidate: %w", err)
	}
	if owner != buyerID {
		return domain.RefundApprovalCandidate{}, "owner_mismatch", ErrRefundRequestOwnerDenied
	}
	if !managed || status != string(domain.StatusConfirmed) || lifecyclePhase != "TERMINAL" || generation != 1 ||
		currency == nil || *currency != "usd" || amountCents <= 0 || expectedTickets <= 0 || refundDecisionID != nil {
		return domain.RefundApprovalCandidate{}, "booking_not_eligible", nil
	}

	var receiptCount, matchingReceiptCount, receiptTicketCount int64
	err = q.QueryRow(ctx, `SELECT count(*) FILTER (WHERE outcome='issued'),
		count(*) FILTER (WHERE outcome='issued' AND event_ref_id=$2 AND lifecycle_generation=1),
		COALESCE(max(ticket_count) FILTER (WHERE outcome='issued' AND event_ref_id=$2 AND lifecycle_generation=1),0)
		FROM ticket_issuance_receipts WHERE booking_id=$1 AND lifecycle_generation=1`, bookingID, eventID).
		Scan(&receiptCount, &matchingReceiptCount, &receiptTicketCount)
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", fmt.Errorf("read Ticket issuance receipt: %w", err)
	}
	if receiptCount != 1 || matchingReceiptCount != 1 || receiptTicketCount != expectedTickets {
		return domain.RefundApprovalCandidate{}, "issuance_receipt_mismatch", nil
	}

	var ticketCount, issuedCount, distinctPayments, distinctEvents, distinctGenerations, distinctSagas int64
	var paymentID, sagaID *uuid.UUID
	var malformedTickets bool
	err = q.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE t.status='ISSUED'),
		count(DISTINCT t.payment_id),count(DISTINCT t.event_id),count(DISTINCT t.lifecycle_generation),
		count(DISTINCT t.saga_id),(array_agg(DISTINCT t.payment_id))[1],(array_agg(DISTINCT t.saga_id))[1],
		COALESCE(bool_or(bi.id IS NULL OR bi.seat_ids IS NULL OR t.event_id<>$2 OR t.lifecycle_generation<>1 OR
			t.ticket_tier_id<>bi.ticket_tier_id OR NOT (t.seat_id=ANY(bi.seat_ids))),FALSE)
		FROM tickets t LEFT JOIN booking_items bi ON bi.id=t.booking_item_id AND bi.booking_id=t.booking_id
		WHERE t.booking_id=$1`, bookingID, eventID).
		Scan(&ticketCount, &issuedCount, &distinctPayments, &distinctEvents, &distinctGenerations, &distinctSagas, &paymentID, &sagaID, &malformedTickets)
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", fmt.Errorf("read Ticket refund eligibility: %w", err)
	}
	if ticketCount != expectedTickets || issuedCount != expectedTickets || distinctPayments != 1 || distinctEvents != 1 || distinctGenerations != 1 || distinctSagas != 1 || paymentID == nil || sagaID == nil || malformedTickets {
		return domain.RefundApprovalCandidate{}, "ticket_set_mismatch", nil
	}
	var fullyMatchedReceiptCount int64
	err = q.QueryRow(ctx, `SELECT count(*) FROM ticket_issuance_receipts
		WHERE booking_id=$1 AND lifecycle_generation=1 AND outcome='issued' AND event_ref_id=$2
		AND payment_id=$3 AND saga_id=$4 AND ticket_count=$5`, bookingID, eventID, *paymentID, *sagaID, expectedTickets).Scan(&fullyMatchedReceiptCount)
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", fmt.Errorf("verify Ticket issuance receipt identity: %w", err)
	}
	if fullyMatchedReceiptCount != 1 {
		return domain.RefundApprovalCandidate{}, "issuance_receipt_mismatch", nil
	}

	var used bool
	err = q.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM ticket_checkins c JOIN tickets t ON t.id=c.ticket_id WHERE t.booking_id=$1
		UNION ALL
		SELECT 1 FROM ticket_scan_requests s JOIN tickets t ON t.id=s.ticket_id
			WHERE t.booking_id=$1 AND s.result_code='accepted'
		UNION ALL
		SELECT 1 FROM tickets t WHERE t.booking_id=$1 AND (t.status<>'ISSUED' OR t.checked_in_at IS NOT NULL)
		UNION ALL
		SELECT 1 FROM ticket_admission_intents i WHERE i.booking_id=$1 AND i.state='COMMITTED'
	)`, bookingID).Scan(&used)
	if err != nil {
		return domain.RefundApprovalCandidate{}, "", fmt.Errorf("read Ticket use history: %w", err)
	}
	if used {
		return domain.RefundApprovalCandidate{}, "ticket_used", nil
	}

	return domain.RefundApprovalCandidate{BookingID: gotBooking, BuyerID: owner, EventID: eventID, PaymentID: *paymentID,
		Generation: generation, AmountCents: amountCents, Currency: *currency}, "", nil
}

func refundIdempotencyReplay(ctx context.Context, q refundQueryer, actorID uuid.UUID, operation string, keyDigest, fingerprint [32]byte) (domain.BookingRefundRequest, bool, error) {
	var storedFingerprint []byte
	var requestID uuid.UUID
	err := q.QueryRow(ctx, `SELECT request_fingerprint,request_id FROM booking_refund_idempotency
		WHERE actor_user_id=$1 AND operation=$2 AND key_digest=$3`, actorID, operation, keyDigest[:]).Scan(&storedFingerprint, &requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingRefundRequest{}, false, nil
	}
	if err != nil {
		return domain.BookingRefundRequest{}, false, err
	}
	if len(storedFingerprint) != sha256.Size || !equalRefundFingerprint(storedFingerprint, fingerprint[:]) {
		return domain.BookingRefundRequest{}, false, ErrRefundRequestIdempotencyConflict
	}
	request, err := getRefundRequestByID(ctx, q, requestID, false)
	if errors.Is(err, ErrRefundRequestNotFound) {
		return domain.BookingRefundRequest{}, false, fmt.Errorf("refund idempotency record has no request: %w", err)
	}
	if err != nil {
		return domain.BookingRefundRequest{}, false, err
	}
	return request, true, nil
}

func claimRefundIdempotency(ctx context.Context, q refundQueryer, actorID uuid.UUID, operation string, keyDigest, fingerprint [32]byte, requestID uuid.UUID) (domain.BookingRefundRequest, bool, error) {
	result, err := q.Exec(ctx, `INSERT INTO booking_refund_idempotency(actor_user_id,operation,key_digest,request_fingerprint,request_id)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT(actor_user_id,operation,key_digest) DO NOTHING`,
		actorID, operation, keyDigest[:], fingerprint[:], requestID)
	if err != nil {
		return domain.BookingRefundRequest{}, false, err
	}
	if result.RowsAffected() == 1 {
		return domain.BookingRefundRequest{}, true, nil
	}
	request, found, err := refundIdempotencyReplay(ctx, q, actorID, operation, keyDigest, fingerprint)
	if err != nil {
		return domain.BookingRefundRequest{}, false, err
	}
	if !found {
		return domain.BookingRefundRequest{}, false, ErrRefundRequestIdempotencyConflict
	}
	return request, false, nil
}

func equalRefundFingerprint(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var diff byte
	for i := range left {
		diff |= left[i] ^ right[i]
	}
	return diff == 0
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}

func lockEventRefundApprover(ctx context.Context, q refundQueryer, eventID, userID uuid.UUID) error {
	var active bool
	err := q.QueryRow(ctx, `SELECT active FROM event_refund_approvers WHERE event_id=$1 AND staff_user_id=$2 FOR SHARE`, eventID, userID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
		return ErrRefundRequestApproverDenied
	}
	return err
}

func lockAndRequireNonScanner(ctx context.Context, q refundQueryer, eventID, userID uuid.UUID) error {
	// Share the exact advisory key used by SetScannerAssignment. The row lock
	// then prevents a concurrent assignment change from crossing this decision.
	if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('rushgo:event-scanner:' || $1::text || ':' || $2::text,0))`, eventID, userID); err != nil {
		return err
	}
	var active bool
	err := q.QueryRow(ctx, `SELECT active FROM event_scanner_assignments WHERE event_id=$1 AND staff_user_id=$2 FOR SHARE`, eventID, userID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if active {
		return ErrRefundRequestScannerDenied
	}
	return nil
}

func pendingBookingAdmission(ctx context.Context, q refundQueryer, bookingID uuid.UUID) (bool, error) {
	var pending bool
	err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ticket_admission_intents WHERE booking_id=$1 AND state='PENDING')`, bookingID).Scan(&pending)
	return pending, err
}

func insertRefundDecision(ctx context.Context, tx pgx.Tx, command domain.DecideUnusedRefundCommand, request domain.BookingRefundRequest, _ *domain.RefundApprovalCandidate) error {
	if command.Kind == domain.RefundDecisionRejected {
		_, err := tx.Exec(ctx, `INSERT INTO booking_refund_decisions
			(decision_id,request_id,booking_id,event_id,buyer_user_id,payment_id,generation,decision,decision_reason,approver_user_id,amount_cents,currency)
			VALUES($1,$2,$3,$4,$5,$6,$7,'REJECTED',$8,$9,$10,$11)`, command.DecisionID, request.RequestID, request.BookingID,
			request.EventID, request.BuyerID, request.PaymentID, request.Generation, command.Reason, command.ApproverID, request.AmountCents, request.Currency)
		return err
	}
	if command.Event.EventID != request.EventID || command.Event.StartsAt.IsZero() || command.Payment.PaymentIntentID == "" || command.Payment.ProviderAccount == "" {
		return ErrRefundRequestNotEligible
	}
	var qualificationAttemptID any
	var eventQualifiedAt any
	if command.QualificationAttemptID != uuid.Nil {
		if command.QualificationAttemptID != command.DecisionID || command.Event.QualificationAttemptID != command.QualificationAttemptID ||
			command.Event.EventQualifiedAt.IsZero() || !command.Event.EventQualifiedAt.Before(command.Event.StartsAt) {
			return ErrRefundRequestEventAuthorityUnavailable
		}
		qualificationAttemptID = command.QualificationAttemptID
		eventQualifiedAt = command.Event.EventQualifiedAt.UTC()
	}
	_, err := tx.Exec(ctx, `INSERT INTO booking_refund_decisions
		(decision_id,request_id,booking_id,event_id,buyer_user_id,payment_id,generation,decision,decision_reason,approver_user_id,amount_cents,currency,
		 payment_intent_id,provider_account,livemode,event_starts_at,event_authority_version,qualification_attempt_id,event_qualified_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,'APPROVED',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, command.DecisionID, request.RequestID,
		request.BookingID, request.EventID, request.BuyerID, request.PaymentID, request.Generation, command.Reason, command.ApproverID,
		request.AmountCents, request.Currency, command.Payment.PaymentIntentID, command.Payment.ProviderAccount, command.Payment.Livemode,
		command.Event.StartsAt.UTC(), strings.TrimSpace(command.Event.AuthorityVersion), qualificationAttemptID, eventQualifiedAt)
	return err
}

func persistUnusedRefundRejection(ctx context.Context, tx pgx.Tx, command domain.DecideUnusedRefundCommand, request domain.BookingRefundRequest, reason string) error {
	command.Kind = domain.RefundDecisionRejected
	command.Reason = reason
	if err := insertRefundDecision(ctx, tx, command, request, nil); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE booking_refund_requests SET status='REJECTED',decision_reason=$2,decision_id=$3,
		approver_user_id=$4,decided_at=clock_timestamp() WHERE request_id=$1 AND decision_id IS NULL AND status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION')`,
		request.RequestID, reason, command.DecisionID, command.ApproverID)
	return err
}

func terminalizeRefundApprovalAttempt(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID, state domain.RefundApprovalAttemptState) error {
	if attemptID == uuid.Nil || (state != domain.RefundApprovalAttemptRejected && state != domain.RefundApprovalAttemptAborted) {
		return ErrRefundApprovalAttemptConflict
	}
	result, err := tx.Exec(ctx, `UPDATE booking_refund_approval_attempts SET state=$2,terminal_at=clock_timestamp()
		WHERE attempt_id=$1 AND state='PENDING'`, attemptID, string(state))
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrRefundApprovalAttemptFinal
	}
	return nil
}

func commitUnusedRefundApproval(ctx context.Context, tx pgx.Tx, command domain.DecideUnusedRefundCommand, request domain.BookingRefundRequest) error {
	if command.QualificationAttemptID != uuid.Nil {
		result, err := tx.Exec(ctx, `UPDATE booking_refund_approval_attempts SET state='APPROVED',event_starts_at=$2,event_qualified_at=$3,
			event_authority_version=$4,terminal_at=clock_timestamp() WHERE attempt_id=$1 AND state='PENDING'`, command.QualificationAttemptID,
			command.Event.StartsAt.UTC(), command.Event.EventQualifiedAt.UTC(), strings.TrimSpace(command.Event.AuthorityVersion))
		if err != nil {
			return fmt.Errorf("persist Event qualification on Booking attempt: %w", err)
		}
		if result.RowsAffected() != 1 {
			return ErrRefundApprovalAttemptFinal
		}
	}
	if err := insertRefundDecision(ctx, tx, command, request, nil); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE bookings SET refund_decision_id=$2 WHERE id=$1 AND refund_decision_id IS NULL`, request.BookingID, command.DecisionID)
	if err != nil {
		return fmt.Errorf("persist Booking refund block: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrRefundRequestFinal
	}
	result, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='APPROVED',decision_reason=$2,decision_id=$3,
		approver_user_id=$4,decided_at=clock_timestamp(),payment_refund_state='pending',payment_refund_amount_cents=0,
		payment_refund_status='',ticket_revocation_state='pending_payment',ticket_revoked_at=NULL,
		qualification_attempt_id=COALESCE($5,qualification_attempt_id),event_qualified_at=$6
		WHERE request_id=$1 AND decision_id IS NULL AND status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION')`,
		request.RequestID, command.Reason, command.DecisionID, command.ApproverID, nullableQualificationAttemptID(command.QualificationAttemptID), nullableQualifiedAt(command.Event.EventQualifiedAt, command.QualificationAttemptID))
	if err != nil {
		return fmt.Errorf("persist Booking refund approval: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrRefundRequestFinal
	}
	payload, err := json.Marshal(struct {
		BookingID  string `json:"booking_id"`
		DecisionID string `json:"refund_decision_id"`
		PaymentID  string `json:"payment_id"`
		Generation int64  `json:"generation"`
	}{request.BookingID.String(), command.DecisionID.String(), request.PaymentID.String(), request.Generation})
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload,published,created_at)
		VALUES($1,$2,$3,$4,FALSE,clock_timestamp())`, command.DecisionID, domain.TypeUnusedTicketRefundApprovedV1, request.BookingID.String(), payload); err != nil {
		return fmt.Errorf("insert Booking refund outbox: %w", err)
	}
	return nil
}

func approvedUnusedRefundDecision(ctx context.Context, q refundQueryer, bookingID, decisionID uuid.UUID) (domain.RefundDecisionSnapshot, error) {
	var result domain.RefundDecisionSnapshot
	var decision, status string
	var livemode *bool
	var qualificationAttemptID *uuid.UUID
	var eventQualifiedAt *time.Time
	err := q.QueryRow(ctx, `SELECT r.request_id,r.booking_id,r.event_id,r.buyer_user_id,r.payment_id,r.generation,
		r.amount_cents,r.currency,r.status,r.reason,r.decision_reason,r.decision_id,r.approver_user_id,
		d.payment_intent_id,d.provider_account,d.livemode,d.event_starts_at,d.event_authority_version,d.qualification_attempt_id,d.event_qualified_at,
		r.requested_at,r.decided_at,r.payment_refund_state,r.payment_refund_amount_cents,r.payment_refund_status,
		r.ticket_revocation_state,r.ticket_revoked_at,d.decision
		FROM booking_refund_requests r JOIN booking_refund_decisions d ON d.decision_id=r.decision_id
		JOIN bookings b ON b.id=r.booking_id AND b.refund_decision_id=d.decision_id
		WHERE r.booking_id=$1 AND d.decision_id=$2 AND d.decision='APPROVED'`, bookingID, decisionID).Scan(
		&result.RequestID, &result.BookingID, &result.EventID, &result.BuyerID, &result.PaymentID, &result.Generation,
		&result.AmountCents, &result.Currency, &status, &result.Reason, &result.DecisionReason, &result.DecisionID, &result.ApproverID,
		&result.PaymentIntentID, &result.ProviderAccount, &livemode, &result.EventStartsAt, &result.EventAuthorityVersion, &qualificationAttemptID, &eventQualifiedAt,
		&result.RequestedAt, &result.DecidedAt, &result.PaymentRefundState, &result.PaymentRefundAmountCents, &result.PaymentRefundStatus,
		&result.TicketRevocationState, &result.TicketRevokedAt, &decision)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RefundDecisionSnapshot{}, ErrRefundRequestNotApproved
	}
	if err != nil {
		return domain.RefundDecisionSnapshot{}, err
	}
	if livemode == nil || *livemode {
		return domain.RefundDecisionSnapshot{}, ErrRefundRequestNotApproved
	}
	result.Status = domain.RefundRequestStatus(status)
	result.Livemode = *livemode
	result.DecisionKind = domain.RefundDecisionKind(decision)
	if qualificationAttemptID != nil {
		result.QualificationAttemptID = *qualificationAttemptID
	}
	if eventQualifiedAt != nil {
		result.EventQualifiedAt = eventQualifiedAt.UTC()
	}
	return result, nil
}

func nullableQualificationAttemptID(value uuid.UUID) any {
	if value == uuid.Nil {
		return nil
	}
	return value
}

func nullableQualifiedAt(value time.Time, attemptID uuid.UUID) any {
	if attemptID == uuid.Nil || value.IsZero() {
		return nil
	}
	return value.UTC()
}

func sameApprovedRefundIdentity(request domain.BookingRefundRequest, snapshot domain.UnusedRefundPaymentSnapshot) bool {
	return sameRefundPaymentIdentity(request, snapshot) && snapshot.RefundDecisionID == request.DecisionID
}

func sameRefundPaymentIdentity(request domain.BookingRefundRequest, snapshot domain.UnusedRefundPaymentSnapshot) bool {
	return snapshot.BookingID == request.BookingID && snapshot.BuyerID == request.BuyerID && snapshot.PaymentID == request.PaymentID &&
		snapshot.Generation == request.Generation && snapshot.AmountCents == request.AmountCents && snapshot.Currency == request.Currency &&
		snapshot.PaymentIntentID == request.PaymentIntentID && snapshot.ProviderAccount == request.ProviderAccount &&
		snapshot.Livemode == request.Livemode
}

func paymentRefundIsExactlySucceeded(request domain.BookingRefundRequest, snapshot domain.UnusedRefundPaymentSnapshot) bool {
	return sameApprovedRefundIdentity(request, snapshot) && snapshot.Collected && snapshot.RefundState == "succeeded" &&
		snapshot.RefundAmountCents == request.AmountCents && request.AmountCents > 0
}

func isRefundPendingState(value string) bool {
	switch value {
	case "pending", "claimed", "provider_pending", "retryable", "unknown":
		return true
	default:
		return false
	}
}

func isRefundAttentionState(value string) bool {
	return value == "attention" || value == "failed" || value == "canceled" || value == "cancelled"
}

func updateUnusedRefundProjection(ctx context.Context, tx pgx.Tx, request domain.BookingRefundRequest, snapshot domain.UnusedRefundPaymentSnapshot, attention bool) (domain.BookingRefundRequest, error) {
	status := domain.RefundRequestProcessing
	ticketState := "pending_payment"
	if attention {
		status = domain.RefundRequestNeedsAttention
		ticketState = "needs_attention"
	} else if isRefundPendingState(snapshot.RefundState) {
		status = domain.RefundRequestProcessing
	} else {
		status = domain.RefundRequestNeedsAttention
		ticketState = "needs_attention"
	}
	_, err := tx.Exec(ctx, `UPDATE booking_refund_requests SET status=$2,payment_refund_state=$3,payment_refund_amount_cents=$4,
		payment_refund_status=$5,ticket_revocation_state=$6 WHERE request_id=$1 AND decision_id=$7`, request.RequestID,
		string(status), snapshot.RefundState, snapshot.RefundAmountCents, snapshot.RefundStatus, ticketState, request.DecisionID)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	return getRefundRequestByID(ctx, tx, request.RequestID, false)
}

func revokeUnusedRefundTickets(ctx context.Context, r *PostgresBookingRepository, request domain.BookingRefundRequest) (domain.BookingRefundRequest, error) {
	if _, err := r.pool.Exec(ctx, `UPDATE tickets SET status='REVOKED',revoked_at=COALESCE(revoked_at,clock_timestamp())
		WHERE booking_id=$1 AND event_id=$2 AND payment_id=$3 AND lifecycle_generation=$4 AND status='ISSUED'`,
		request.BookingID, request.EventID, request.PaymentID, request.Generation); err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("revoke successfully refunded Tickets: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	defer rollbackBooking(tx)
	var unresolved, ticketCount, expectedTicketCount int64
	err = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE status<>'REVOKED'),
		COALESCE((SELECT sum(quantity)::bigint FROM booking_items WHERE booking_id=$1),0)
		FROM tickets WHERE booking_id=$1 AND lifecycle_generation=$2`, request.BookingID, request.Generation).
		Scan(&ticketCount, &unresolved, &expectedTicketCount)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if unresolved == 0 && ticketCount == expectedTicketCount && expectedTicketCount > 0 {
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status='REFUNDED',payment_refund_state='succeeded',
			ticket_revocation_state='revoked',ticket_revoked_at=COALESCE(ticket_revoked_at,clock_timestamp())
			WHERE request_id=$1 AND decision_id=$2 AND payment_refund_state='succeeded'`, request.RequestID, request.DecisionID)
	} else {
		_, err = tx.Exec(ctx, `UPDATE booking_refund_requests SET status=CASE WHEN status='REFUNDED' THEN 'REFUNDED' ELSE 'NEEDS_ATTENTION' END,
			ticket_revocation_state='needs_attention'
			WHERE request_id=$1 AND decision_id=$2 AND payment_refund_state='succeeded'`, request.RequestID, request.DecisionID)
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	result, err := getRefundRequestByID(ctx, tx, request.RequestID, false)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.BookingRefundRequest{}, err
	}
	return result, nil
}
