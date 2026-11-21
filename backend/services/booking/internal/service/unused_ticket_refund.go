package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/refundtime"
)

var (
	ErrUnusedRefundApprovalDisabled   = errors.New("unused-ticket refund approval is disabled")
	ErrUnusedRefundEventUnavailable   = errors.New("trusted Event time authority is unavailable")
	ErrUnusedRefundPaymentUnavailable = errors.New("Payment refund authority is unavailable")
	ErrUnusedRefundInvalidRequest     = errors.New("invalid unused-ticket refund request")
	ErrRefundApprovalAttemptPending   = errors.New("Booking refund approval attempt is still pending")
)

type RefundPaymentAuthority interface {
	GetUnusedRefundPaymentSnapshot(context.Context, domain.RefundApprovalCandidate) (domain.UnusedRefundPaymentSnapshot, error)
}

type UnusedTicketRefundService struct {
	repository       repository.UnusedTicketRefundRepository
	eventAuthority   refundtime.Authority
	paymentAuthority RefundPaymentAuthority
}

func NewUnusedTicketRefundService(repo repository.UnusedTicketRefundRepository, event refundtime.Authority, payment RefundPaymentAuthority) *UnusedTicketRefundService {
	return &UnusedTicketRefundService{repository: repo, eventAuthority: event, paymentAuthority: payment}
}

func (s *UnusedTicketRefundService) ApprovalEnabled() bool {
	return s != nil && s.repository != nil && s.eventAuthority != nil && s.paymentAuthority != nil
}

func (s *UnusedTicketRefundService) ApprovalBlockReason() string {
	if s == nil || s.repository == nil {
		return "booking_refund_repository_unavailable"
	}
	if s.eventAuthority == nil {
		return "event_time_authority_unavailable"
	}
	if s.paymentAuthority == nil {
		return "payment_authority_unavailable"
	}
	return ""
}

func (s *UnusedTicketRefundService) Request(ctx context.Context, buyerID, bookingID uuid.UUID, reason, key string) (domain.BookingRefundRequest, error) {
	if s == nil || s.repository == nil || buyerID == uuid.Nil || bookingID == uuid.Nil || !validRefundIdempotencyKey(key) || !domain.ValidRefundRequestReason(reason) {
		return domain.BookingRefundRequest{}, ErrUnusedRefundInvalidRequest
	}
	reason = strings.TrimSpace(reason)
	fingerprint := refundFingerprint(struct {
		Operation string    `json:"operation"`
		BuyerID   uuid.UUID `json:"buyer_id"`
		BookingID uuid.UUID `json:"booking_id"`
		Reason    string    `json:"reason"`
	}{"request", buyerID, bookingID, reason})
	return s.repository.RequestUnusedRefund(ctx, domain.RequestUnusedRefundCommand{
		RequestID: uuid.New(), BookingID: bookingID, BuyerID: buyerID, Reason: reason,
		KeyDigest: sha256.Sum256([]byte(key)), Fingerprint: fingerprint,
	})
}

func (s *UnusedTicketRefundService) Get(ctx context.Context, buyerID, bookingID uuid.UUID) (domain.BookingRefundRequest, error) {
	if s == nil || s.repository == nil || buyerID == uuid.Nil || bookingID == uuid.Nil {
		return domain.BookingRefundRequest{}, ErrUnusedRefundInvalidRequest
	}
	return s.repository.GetUnusedRefundByBooking(ctx, buyerID, bookingID)
}

func (s *UnusedTicketRefundService) Withdraw(ctx context.Context, buyerID, requestID uuid.UUID, key string) (domain.BookingRefundRequest, error) {
	if s == nil || s.repository == nil || buyerID == uuid.Nil || requestID == uuid.Nil || !validRefundIdempotencyKey(key) {
		return domain.BookingRefundRequest{}, ErrUnusedRefundInvalidRequest
	}
	fingerprint := refundFingerprint(struct {
		Operation string    `json:"operation"`
		BuyerID   uuid.UUID `json:"buyer_id"`
		RequestID uuid.UUID `json:"request_id"`
	}{"withdraw", buyerID, requestID})
	request, err := s.repository.WithdrawUnusedRefund(ctx, domain.WithdrawUnusedRefundCommand{
		RequestID: requestID, BuyerID: buyerID, KeyDigest: sha256.Sum256([]byte(key)), Fingerprint: fingerprint,
	})
	if err == nil && request.QualificationAttemptID != uuid.Nil {
		if attempt, attemptErr := s.repository.GetUnusedRefundApprovalAttempt(ctx, request.QualificationAttemptID); attemptErr == nil && attempt.State != domain.RefundApprovalAttemptPending {
			_ = s.releaseTerminalApprovalAttempt(ctx, attempt)
		}
	}
	return request, err
}

func (s *UnusedTicketRefundService) Decide(ctx context.Context, approverID, eventID, requestID uuid.UUID, kind domain.RefundDecisionKind, reason, key string) (domain.BookingRefundRequest, error) {
	if s == nil || s.repository == nil || approverID == uuid.Nil || eventID == uuid.Nil || requestID == uuid.Nil || !validRefundIdempotencyKey(key) ||
		(kind != domain.RefundDecisionApproved && kind != domain.RefundDecisionRejected) || len([]rune(strings.TrimSpace(reason))) > 500 ||
		(kind == domain.RefundDecisionRejected && !domain.ValidRefundRequestReason(reason)) {
		return domain.BookingRefundRequest{}, ErrUnusedRefundInvalidRequest
	}
	reason = strings.TrimSpace(reason)
	fingerprint := refundFingerprint(struct {
		Operation string                    `json:"operation"`
		Approver  uuid.UUID                 `json:"approver_id"`
		Event     uuid.UUID                 `json:"event_id"`
		Request   uuid.UUID                 `json:"request_id"`
		Kind      domain.RefundDecisionKind `json:"kind"`
		Reason    string                    `json:"reason"`
	}{"decide", approverID, eventID, requestID, kind, reason})
	command := domain.DecideUnusedRefundCommand{DecisionID: uuid.New(), RequestID: requestID, EventID: eventID, ApproverID: approverID,
		Kind: kind, Reason: reason, KeyDigest: sha256.Sum256([]byte(key)), Fingerprint: fingerprint}
	if kind == domain.RefundDecisionRejected {
		request, err := s.repository.DecideUnusedRefund(ctx, command)
		if err == nil && request.QualificationAttemptID != uuid.Nil {
			if attempt, attemptErr := s.repository.GetUnusedRefundApprovalAttempt(ctx, request.QualificationAttemptID); attemptErr == nil && attempt.State != domain.RefundApprovalAttemptPending {
				_ = s.releaseTerminalApprovalAttempt(ctx, attempt)
			}
		}
		return request, err
	}
	request, err := s.repository.GetEventUnusedRefundByID(ctx, eventID, requestID)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if request.QualificationAttemptID != uuid.Nil {
		command.DecisionID = request.QualificationAttemptID
	}
	// Exact retries of a final result need no fresh upstream reads. The
	// repository still authenticates the assigned actor and checks the exact key.
	if request.Status != domain.RefundRequestRequested && request.Status != domain.RefundRequestWaitingCheckInResolution {
		return s.repository.DecideUnusedRefund(ctx, command)
	}
	if err = s.repository.CheckEventRefundDecisionActor(ctx, eventID, approverID, request.BuyerID); err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if s.eventAuthority == nil || s.paymentAuthority == nil {
		waiting, waitErr := s.repository.DecideUnusedRefund(ctx, command)
		if waitErr == nil && waiting.Status == domain.RefundRequestWaitingCheckInResolution {
			return waiting, nil
		}
		if s.eventAuthority == nil {
			return domain.BookingRefundRequest{}, ErrUnusedRefundApprovalDisabled
		}
		return domain.BookingRefundRequest{}, ErrUnusedRefundPaymentUnavailable
	}
	pending, err := s.repository.HasPendingBookingAdmission(ctx, request.BookingID)
	if err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("read Booking Admission pending state: %w", err)
	}
	if pending {
		return s.repository.DecideUnusedRefund(ctx, command)
	}
	candidate, reasonCode, err := s.repository.PreviewUnusedRefund(ctx, request.BuyerID, request.BookingID)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if reasonCode != "" {
		// Let the locked transaction persist the final rejection after rechecking
		// the full Ticket set against the Booking row. A prior pending attempt
		// is terminated atomically with that result by the repository.
		return s.repository.DecideUnusedRefund(ctx, command)
	}
	candidate.ForApproval = true
	paymentCtx, cancelPayment := context.WithTimeout(ctx, 3*time.Second)
	payment, err := s.paymentAuthority.GetUnusedRefundPaymentSnapshot(paymentCtx, candidate)
	cancelPayment()
	if err != nil {
		return domain.BookingRefundRequest{}, ErrUnusedRefundPaymentUnavailable
	}
	if err = domain.ValidateUnusedRefundPaymentSnapshot(candidate, payment); err != nil {
		return domain.BookingRefundRequest{}, fmt.Errorf("%w: %v", repository.ErrRefundRequestPaymentConflict, err)
	}
	attempt, err := s.repository.BeginUnusedRefundApproval(ctx, domain.BeginUnusedRefundApprovalCommand{
		AttemptID: command.DecisionID, RequestID: command.RequestID, EventID: command.EventID, ApproverID: command.ApproverID,
		Reason: command.Reason, KeyDigest: command.KeyDigest, Fingerprint: command.Fingerprint,
	})
	if errors.Is(err, repository.ErrRefundRequestPendingAdmission) {
		return s.repository.DecideUnusedRefund(ctx, command)
	}
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	if attempt.State != domain.RefundApprovalAttemptPending {
		_ = s.releaseTerminalApprovalAttempt(ctx, attempt)
		return domain.BookingRefundRequest{}, repository.ErrRefundApprovalAttemptFinal
	}
	binding, err := refundApprovalAttemptBinding(attempt)
	if err != nil {
		return domain.BookingRefundRequest{}, err
	}
	eventCtx, cancelEvent := context.WithTimeout(ctx, 3*time.Second)
	permit, err := s.eventAuthority.Acquire(eventCtx, binding)
	cancelEvent()
	if err != nil {
		// Acquire may have persisted even when its response was lost. Keep the
		// attempt pending so a retry or recovery uses the same stable identity.
		return domain.BookingRefundRequest{}, ErrUnusedRefundEventUnavailable
	}
	if !permit.QualifiedFor(binding) {
		if permit.Binding != binding {
			return domain.BookingRefundRequest{}, ErrUnusedRefundEventUnavailable
		}
		if aborted, abortErr := s.repository.AbortUnusedRefundApprovalAttempt(ctx, attempt.AttemptID, "event_qualification_invalid"); abortErr == nil {
			_ = s.releaseTerminalApprovalAttempt(ctx, aborted)
		}
		return domain.BookingRefundRequest{}, ErrUnusedRefundEventUnavailable
	}
	command.DecisionID = attempt.AttemptID
	command.QualificationAttemptID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{EventID: permit.Binding.EventID, StartsAt: permit.StartsAt,
		AuthorityVersion: strconv.FormatInt(permit.Version, 10), QualificationAttemptID: permit.Binding.AttemptID,
		EventQualifiedAt: permit.QualifiedAt}
	command.Payment = payment
	result, err := s.repository.FinalizeUnusedRefundApproval(ctx, command)
	if err != nil {
		if persisted, readErr := s.repository.GetUnusedRefundApprovalAttempt(ctx, attempt.AttemptID); readErr == nil && persisted.State != domain.RefundApprovalAttemptPending {
			_ = s.releaseTerminalApprovalAttempt(ctx, persisted)
		}
		return domain.BookingRefundRequest{}, err
	}
	if persisted, readErr := s.repository.GetUnusedRefundApprovalAttempt(ctx, attempt.AttemptID); readErr == nil && persisted.State != domain.RefundApprovalAttemptPending {
		_ = s.releaseTerminalApprovalAttempt(ctx, persisted)
	}
	return result, nil
}

func (s *UnusedTicketRefundService) List(ctx context.Context, actorID, eventID uuid.UUID, cursor string, limit int) ([]domain.BookingRefundRequest, string, bool, error) {
	if s == nil || s.repository == nil || actorID == uuid.Nil || eventID == uuid.Nil {
		return nil, "", false, ErrUnusedRefundInvalidRequest
	}
	allowed, err := s.repository.IsEventRefundApprover(ctx, eventID, actorID)
	if err != nil {
		return nil, "", false, err
	}
	if !allowed {
		return nil, "", false, repository.ErrRefundRequestApproverDenied
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}
	query := domain.RefundEventListQuery{EventID: eventID, Limit: limit}
	if cursor != "" {
		payload, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
		if decodeErr != nil {
			return nil, "", false, ErrUnusedRefundInvalidRequest
		}
		var decoded struct {
			RequestedAt string `json:"requested_at"`
			RequestID   string `json:"request_id"`
		}
		if json.Unmarshal(payload, &decoded) != nil {
			return nil, "", false, ErrUnusedRefundInvalidRequest
		}
		at, timeErr := time.Parse(time.RFC3339Nano, decoded.RequestedAt)
		id, idErr := uuid.Parse(decoded.RequestID)
		if timeErr != nil || idErr != nil || id == uuid.Nil {
			return nil, "", false, ErrUnusedRefundInvalidRequest
		}
		query.BeforeAt, query.BeforeID = &at, id
	}
	items, hasMore, err := s.repository.ListEventUnusedRefunds(ctx, query)
	if err != nil {
		return nil, "", false, err
	}
	next := ""
	if hasMore && len(items) > 0 && items[len(items)-1].RequestedAt != nil {
		payload, marshalErr := json.Marshal(struct {
			RequestedAt string `json:"requested_at"`
			RequestID   string `json:"request_id"`
		}{items[len(items)-1].RequestedAt.UTC().Format(time.RFC3339Nano), items[len(items)-1].RequestID.String()})
		if marshalErr != nil {
			return nil, "", false, marshalErr
		}
		next = base64.RawURLEncoding.EncodeToString(payload)
	}
	return items, next, hasMore, nil
}

func (s *UnusedTicketRefundService) SetApprover(ctx context.Context, actorID, eventID, staffID uuid.UUID, active bool) (domain.RefundApproverAssignment, error) {
	if s == nil || s.repository == nil || actorID == uuid.Nil || eventID == uuid.Nil || staffID == uuid.Nil {
		return domain.RefundApproverAssignment{}, ErrUnusedRefundInvalidRequest
	}
	return s.repository.SetEventRefundApprover(ctx, eventID, staffID, actorID, active)
}

func (s *UnusedTicketRefundService) ListApprovers(ctx context.Context, eventID uuid.UUID) ([]domain.RefundApproverAssignment, error) {
	if s == nil || s.repository == nil || eventID == uuid.Nil {
		return nil, ErrUnusedRefundInvalidRequest
	}
	return s.repository.ListEventRefundApprovers(ctx, eventID)
}

func (s *UnusedTicketRefundService) ApprovedDecision(ctx context.Context, bookingID, decisionID uuid.UUID) (domain.RefundDecisionSnapshot, error) {
	if s == nil || s.repository == nil || bookingID == uuid.Nil || decisionID == uuid.Nil {
		return domain.RefundDecisionSnapshot{}, ErrUnusedRefundInvalidRequest
	}
	return s.repository.GetApprovedUnusedRefundDecision(ctx, bookingID, decisionID)
}

func (s *UnusedTicketRefundService) RecoverBatch(ctx context.Context, limit int) error {
	if s == nil || s.repository == nil {
		return ErrUnusedRefundPaymentUnavailable
	}
	var failures []error
	if s.eventAuthority != nil {
		attempts, attemptErr := s.repository.ListUnusedRefundApprovalAttemptsForRecovery(ctx, limit)
		if attemptErr != nil {
			failures = append(failures, attemptErr)
		} else {
			for _, attempt := range attempts {
				if recoverErr := s.recoverUnusedRefundApprovalAttempt(ctx, attempt); recoverErr != nil {
					failures = append(failures, recoverErr)
				}
			}
		}
	} else {
		attempts, attemptErr := s.repository.ListUnusedRefundApprovalAttemptsForRecovery(ctx, limit)
		if attemptErr != nil {
			failures = append(failures, attemptErr)
		} else if len(attempts) > 0 {
			failures = append(failures, ErrUnusedRefundEventUnavailable)
		}
	}
	if s.paymentAuthority == nil {
		return errors.Join(append(failures, ErrUnusedRefundPaymentUnavailable)...)
	}
	items, err := s.repository.ListUnusedRefundsForRecovery(ctx, limit)
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	for _, item := range items {
		candidate := domain.RefundApprovalCandidate{BookingID: item.BookingID, BuyerID: item.BuyerID, EventID: item.EventID, PaymentID: item.PaymentID,
			Generation: item.Generation, AmountCents: item.AmountCents, Currency: item.Currency}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		snapshot, snapshotErr := s.paymentAuthority.GetUnusedRefundPaymentSnapshot(callCtx, candidate)
		cancel()
		if snapshotErr != nil {
			failures = append(failures, snapshotErr)
			continue
		}
		if _, applyErr := s.repository.ApplyUnusedRefundPaymentSnapshot(ctx, item, snapshot); applyErr != nil {
			failures = append(failures, applyErr)
		}
	}
	return errors.Join(failures...)
}

func (s *UnusedTicketRefundService) recoverUnusedRefundApprovalAttempt(ctx context.Context, attempt domain.RefundApprovalAttempt) error {
	if attempt.State != domain.RefundApprovalAttemptPending {
		return s.releaseTerminalApprovalAttempt(ctx, attempt)
	}
	if s.eventAuthority == nil || s.paymentAuthority == nil {
		return ErrUnusedRefundApprovalDisabled
	}
	if err := s.repository.CheckEventRefundDecisionActor(ctx, attempt.EventID, attempt.ApproverID, attempt.BuyerID); err != nil {
		if errors.Is(err, repository.ErrRefundRequestApproverDenied) || errors.Is(err, repository.ErrRefundRequestScannerDenied) {
			aborted, abortErr := s.repository.AbortUnusedRefundApprovalAttempt(ctx, attempt.AttemptID, "approver_no_longer_authorized")
			if abortErr != nil {
				return abortErr
			}
			return s.releaseTerminalApprovalAttempt(ctx, aborted)
		}
		return err
	}
	pending, err := s.repository.HasPendingBookingAdmission(ctx, attempt.BookingID)
	if err != nil {
		return err
	}
	if pending {
		_, waitErr := s.repository.DecideUnusedRefund(ctx, refundApprovalAttemptWaitingCommand(attempt))
		return waitErr
	}
	candidate, reasonCode, err := s.repository.PreviewUnusedRefund(ctx, attempt.BuyerID, attempt.BookingID)
	if err != nil {
		return err
	}
	if reasonCode != "" || candidate.BookingID != attempt.BookingID || candidate.BuyerID != attempt.BuyerID || candidate.EventID != attempt.EventID ||
		candidate.PaymentID != attempt.PaymentID || candidate.Generation != attempt.Generation {
		aborted, abortErr := s.repository.AbortUnusedRefundApprovalAttempt(ctx, attempt.AttemptID, "booking_no_longer_eligible")
		if abortErr != nil {
			return abortErr
		}
		return s.releaseTerminalApprovalAttempt(ctx, aborted)
	}
	candidate.ForApproval = true
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	payment, err := s.paymentAuthority.GetUnusedRefundPaymentSnapshot(callCtx, candidate)
	cancel()
	if err != nil {
		return fmt.Errorf("refresh Payment approval snapshot for attempt %s: %w", attempt.AttemptID, err)
	}
	if err = domain.ValidateUnusedRefundPaymentSnapshot(candidate, payment); err != nil {
		aborted, abortErr := s.repository.AbortUnusedRefundApprovalAttempt(ctx, attempt.AttemptID, "payment_identity_no_longer_eligible")
		if abortErr != nil {
			return abortErr
		}
		if releaseErr := s.releaseTerminalApprovalAttempt(ctx, aborted); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}
	binding, err := refundApprovalAttemptBinding(attempt)
	if err != nil {
		return err
	}
	callCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
	permit, err := s.eventAuthority.Acquire(callCtx, binding)
	cancel()
	if err != nil {
		return fmt.Errorf("recover Event qualification for attempt %s: %w", attempt.AttemptID, err)
	}
	if !permit.QualifiedFor(binding) {
		if permit.Binding != binding {
			return ErrUnusedRefundEventUnavailable
		}
		aborted, abortErr := s.repository.AbortUnusedRefundApprovalAttempt(ctx, attempt.AttemptID, "event_qualification_invalid")
		if abortErr != nil {
			return abortErr
		}
		return s.releaseTerminalApprovalAttempt(ctx, aborted)
	}
	command := domain.DecideUnusedRefundCommand{DecisionID: attempt.AttemptID, QualificationAttemptID: attempt.AttemptID,
		RequestID: attempt.RequestID, EventID: attempt.EventID, ApproverID: attempt.ApproverID, Kind: domain.RefundDecisionApproved,
		Reason: attempt.Reason, KeyDigest: attempt.KeyDigest, Fingerprint: attempt.Fingerprint,
		Event: domain.RefundEventSnapshot{EventID: permit.Binding.EventID, StartsAt: permit.StartsAt,
			AuthorityVersion: strconv.FormatInt(permit.Version, 10), QualificationAttemptID: permit.Binding.AttemptID,
			EventQualifiedAt: permit.QualifiedAt}, Payment: payment}
	if _, err = s.repository.FinalizeUnusedRefundApproval(ctx, command); err != nil {
		persisted, readErr := s.repository.GetUnusedRefundApprovalAttempt(ctx, attempt.AttemptID)
		if readErr == nil && persisted.State != domain.RefundApprovalAttemptPending {
			if releaseErr := s.releaseTerminalApprovalAttempt(ctx, persisted); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
		}
		return fmt.Errorf("finalize recovered Booking refund approval attempt %s: %w", attempt.AttemptID, err)
	}
	persisted, err := s.repository.GetUnusedRefundApprovalAttempt(ctx, attempt.AttemptID)
	if err != nil {
		return err
	}
	if persisted.State == domain.RefundApprovalAttemptPending {
		return nil
	}
	return s.releaseTerminalApprovalAttempt(ctx, persisted)
}

func (s *UnusedTicketRefundService) releaseTerminalApprovalAttempt(ctx context.Context, attempt domain.RefundApprovalAttempt) error {
	if attempt.State == domain.RefundApprovalAttemptPending {
		return ErrRefundApprovalAttemptPending
	}
	if attempt.ReleaseAcknowledged {
		return nil
	}
	if s == nil || s.repository == nil || s.eventAuthority == nil {
		return ErrUnusedRefundEventUnavailable
	}
	binding, err := refundApprovalAttemptBinding(attempt)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	permit, err := s.eventAuthority.Release(callCtx, binding, string(attempt.State))
	cancel()
	if err != nil {
		return fmt.Errorf("release Event permit for attempt %s: %w", attempt.AttemptID, err)
	}
	if permit.Binding != binding {
		return ErrUnusedRefundEventUnavailable
	}
	if err := s.repository.AcknowledgeUnusedRefundAttemptRelease(ctx, attempt.AttemptID, string(attempt.State)); err != nil {
		return fmt.Errorf("acknowledge Event permit release for attempt %s: %w", attempt.AttemptID, err)
	}
	return nil
}

func refundApprovalAttemptBinding(attempt domain.RefundApprovalAttempt) (refundtime.Binding, error) {
	binding := refundtime.Binding{AttemptID: attempt.AttemptID, RequestID: attempt.RequestID, BookingID: attempt.BookingID,
		PaymentID: attempt.PaymentID, EventID: attempt.EventID, ApproverID: attempt.ApproverID, Generation: attempt.Generation,
		Fingerprint: hex.EncodeToString(attempt.Fingerprint[:])}
	if !binding.Valid() || attempt.KeyDigest == [32]byte{} || attempt.Fingerprint == [32]byte{} {
		return refundtime.Binding{}, ErrUnusedRefundInvalidRequest
	}
	return binding, nil
}

func refundApprovalAttemptWaitingCommand(attempt domain.RefundApprovalAttempt) domain.DecideUnusedRefundCommand {
	return domain.DecideUnusedRefundCommand{DecisionID: attempt.AttemptID, RequestID: attempt.RequestID, EventID: attempt.EventID,
		ApproverID: attempt.ApproverID, Kind: domain.RefundDecisionApproved, Reason: attempt.Reason,
		KeyDigest: attempt.KeyDigest, Fingerprint: attempt.Fingerprint}
}

func (s *UnusedTicketRefundService) Run(ctx context.Context) {
	if s == nil || s.paymentAuthority == nil || s.repository == nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		_ = s.RecoverBatch(ctx, 50)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validRefundIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func refundFingerprint(value any) [32]byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(encoded)
}
