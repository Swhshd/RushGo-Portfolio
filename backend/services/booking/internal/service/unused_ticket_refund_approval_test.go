package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/refundtime"
)

type coordinatedRefundRepoStub struct {
	unusedRefundServiceRepoStub
	request             domain.BookingRefundRequest
	attempt             domain.RefundApprovalAttempt
	beginCalls          int
	finalizeErr         error
	finalizeCalls       int
	listCalls           int
	releaseAcks         int
	abortCalls          int
	pendingAdmission    bool
	waitingCalls        int
	lastWaitCommand     domain.DecideUnusedRefundCommand
	lastDecisionCommand domain.DecideUnusedRefundCommand
}

func (r *coordinatedRefundRepoStub) GetEventUnusedRefundByID(context.Context, uuid.UUID, uuid.UUID) (domain.BookingRefundRequest, error) {
	return r.request, nil
}

func (r *coordinatedRefundRepoStub) HasPendingBookingAdmission(context.Context, uuid.UUID) (bool, error) {
	return r.pendingAdmission, nil
}

func (r *coordinatedRefundRepoStub) CheckEventRefundDecisionActor(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return nil
}

func (r *coordinatedRefundRepoStub) PreviewUnusedRefund(context.Context, uuid.UUID, uuid.UUID) (domain.RefundApprovalCandidate, string, error) {
	return domain.RefundApprovalCandidate{BookingID: r.request.BookingID, BuyerID: r.request.BuyerID, EventID: r.request.EventID,
		PaymentID: r.request.PaymentID, Generation: r.request.Generation, AmountCents: r.request.AmountCents, Currency: r.request.Currency}, "", nil
}

func (r *coordinatedRefundRepoStub) BeginUnusedRefundApproval(_ context.Context, command domain.BeginUnusedRefundApprovalCommand) (domain.RefundApprovalAttempt, error) {
	r.beginCalls++
	if r.attempt.AttemptID == uuid.Nil {
		r.attempt = domain.RefundApprovalAttempt{AttemptID: command.AttemptID, RequestID: command.RequestID, BookingID: r.request.BookingID,
			EventID: r.request.EventID, BuyerID: r.request.BuyerID, PaymentID: r.request.PaymentID, ApproverID: command.ApproverID,
			Generation: r.request.Generation, KeyDigest: command.KeyDigest, Fingerprint: command.Fingerprint, Reason: command.Reason,
			State: domain.RefundApprovalAttemptPending}
	}
	return r.attempt, nil
}

func (r *coordinatedRefundRepoStub) DecideUnusedRefund(_ context.Context, command domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	r.lastDecisionCommand = command
	if r.pendingAdmission && command.QualificationAttemptID == uuid.Nil {
		r.waitingCalls++
		r.lastWaitCommand = command
		request := r.request
		request.Status = domain.RefundRequestWaitingCheckInResolution
		return request, nil
	}
	r.finalizeCalls++
	if r.finalizeErr != nil {
		return domain.BookingRefundRequest{}, r.finalizeErr
	}
	r.attempt.State = domain.RefundApprovalAttemptApproved
	r.attempt.StartsAt = command.Event.StartsAt
	r.attempt.EventQualifiedAt = command.Event.EventQualifiedAt
	r.request.Status = domain.RefundRequestApproved
	r.request.DecisionID = command.DecisionID
	r.request.QualificationAttemptID = command.QualificationAttemptID
	r.request.EventQualifiedAt = command.Event.EventQualifiedAt
	return r.request, nil
}

func (r *coordinatedRefundRepoStub) FinalizeUnusedRefundApproval(ctx context.Context, command domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	return r.DecideUnusedRefund(ctx, command)
}

func (r *coordinatedRefundRepoStub) GetUnusedRefundApprovalAttempt(context.Context, uuid.UUID) (domain.RefundApprovalAttempt, error) {
	if r.attempt.AttemptID == uuid.Nil {
		return domain.RefundApprovalAttempt{}, repository.ErrRefundRequestNotFound
	}
	return r.attempt, nil
}

func (r *coordinatedRefundRepoStub) AbortUnusedRefundApprovalAttempt(_ context.Context, attemptID uuid.UUID, _ string) (domain.RefundApprovalAttempt, error) {
	r.abortCalls++
	if r.attempt.AttemptID != attemptID {
		return domain.RefundApprovalAttempt{}, repository.ErrRefundRequestNotFound
	}
	r.attempt.State = domain.RefundApprovalAttemptAborted
	return r.attempt, nil
}

func (r *coordinatedRefundRepoStub) ListUnusedRefundApprovalAttemptsForRecovery(context.Context, int) ([]domain.RefundApprovalAttempt, error) {
	r.listCalls++
	if r.attempt.AttemptID == uuid.Nil || r.attempt.ReleaseAcknowledged {
		return nil, nil
	}
	return []domain.RefundApprovalAttempt{r.attempt}, nil
}

func (r *coordinatedRefundRepoStub) AcknowledgeUnusedRefundAttemptRelease(context.Context, uuid.UUID, string) error {
	r.releaseAcks++
	r.attempt.ReleaseAcknowledged = true
	return nil
}

func (r *coordinatedRefundRepoStub) ListUnusedRefundsForRecovery(context.Context, int) ([]domain.BookingRefundRequest, error) {
	return nil, nil
}

type coordinatedRefundPaymentStub struct{ calls int }

func (p *coordinatedRefundPaymentStub) GetUnusedRefundPaymentSnapshot(_ context.Context, candidate domain.RefundApprovalCandidate) (domain.UnusedRefundPaymentSnapshot, error) {
	p.calls++
	return domain.UnusedRefundPaymentSnapshot{BookingID: candidate.BookingID, BuyerID: candidate.BuyerID, PaymentID: candidate.PaymentID,
		PaymentIntentID: "pi_test", ProviderAccount: "acct_test", Generation: candidate.Generation, AmountCents: candidate.AmountCents,
		Currency: candidate.Currency, RefundState: "not_required", Collected: true, EligibleForApproval: true}, nil
}

type coordinatedRefundEventStub struct {
	acquireBindings     []refundtime.Binding
	releaseOutcomes     []string
	acquireErr          error
	releaseErr          error
	permit              refundtime.Permit
	useConfiguredPermit bool
}

func (e *coordinatedRefundEventStub) Acquire(_ context.Context, binding refundtime.Binding) (refundtime.Permit, error) {
	e.acquireBindings = append(e.acquireBindings, binding)
	if e.acquireErr != nil {
		return refundtime.Permit{}, e.acquireErr
	}
	if e.useConfiguredPermit {
		if e.permit.Binding.AttemptID == uuid.Nil {
			e.permit.Binding = binding
		}
	} else if e.permit.Binding.AttemptID == uuid.Nil {
		e.permit = refundtime.Permit{Binding: binding, State: "QUALIFIED", StartsAt: time.Now().Add(time.Minute),
			QualifiedAt: time.Now(), Version: 1}
	}
	return e.permit, nil
}

func (e *coordinatedRefundEventStub) Release(_ context.Context, binding refundtime.Binding, outcome string) (refundtime.Permit, error) {
	e.releaseOutcomes = append(e.releaseOutcomes, outcome)
	if e.releaseErr != nil {
		return refundtime.Permit{}, e.releaseErr
	}
	return refundtime.Permit{Binding: binding, State: outcome, StartsAt: e.permit.StartsAt, QualifiedAt: e.permit.QualifiedAt, Version: 1}, nil
}

func newCoordinatedRefundService() (*UnusedTicketRefundService, *coordinatedRefundRepoStub, *coordinatedRefundEventStub, *coordinatedRefundPaymentStub) {
	request := domain.BookingRefundRequest{RequestID: uuid.New(), BookingID: uuid.New(), EventID: uuid.New(), BuyerID: uuid.New(), PaymentID: uuid.New(),
		Generation: 1, AmountCents: 725, Currency: "usd", Status: domain.RefundRequestRequested}
	repo := &coordinatedRefundRepoStub{request: request}
	event := &coordinatedRefundEventStub{}
	payment := &coordinatedRefundPaymentStub{}
	return NewUnusedTicketRefundService(repo, event, payment), repo, event, payment
}

func TestRefundTimeAcquireResponseLossReusesDurableAttempt(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	event.acquireErr = errors.New("Acquire response timed out")
	approver := uuid.New()
	_, firstErr := svc.Decide(context.Background(), approver, repo.request.EventID, repo.request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if firstErr == nil || repo.attempt.AttemptID == uuid.Nil {
		t.Fatalf("first decision error=%v attempt=%+v, want persisted attempt before Acquire error", firstErr, repo.attempt)
	}

	event.acquireErr = nil
	event.permit = refundtime.Permit{}
	result, err := svc.Decide(context.Background(), approver, repo.request.EventID, repo.request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if err != nil || result.Status != domain.RefundRequestApproved {
		t.Fatalf("retry result=%+v error=%v, want approved", result, err)
	}
	if len(event.acquireBindings) != 2 || event.acquireBindings[0].AttemptID != event.acquireBindings[1].AttemptID ||
		event.acquireBindings[1].AttemptID != repo.attempt.AttemptID {
		t.Fatalf("Acquire attempt bindings=%+v, want one stable persisted identity", event.acquireBindings)
	}
}

func TestRefundTimeQualificationAllowsLaterBookingCommit(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	startsAt := time.Now().Add(-time.Minute)
	event.permit = refundtime.Permit{State: "QUALIFIED", StartsAt: startsAt, QualifiedAt: startsAt.Add(-time.Second), Version: 4}
	event.useConfiguredPermit = true
	approver := uuid.New()
	result, err := svc.Decide(context.Background(), approver, repo.request.EventID, repo.request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if err != nil || result.Status != domain.RefundRequestApproved {
		t.Fatalf("post-start Booking commit result=%+v error=%v, want approved from pre-start Event qualification", result, err)
	}
	if result.QualificationAttemptID != repo.attempt.AttemptID || !result.EventQualifiedAt.Equal(startsAt.Add(-time.Second)) {
		t.Fatalf("approval qualification proof=%+v attempt=%+v, want persisted pre-start proof", result, repo.attempt)
	}
}

func TestRefundTimeQualificationAtStartAbortsAndReleases(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	startsAt := time.Now().Add(-time.Second)
	event.permit = refundtime.Permit{State: "QUALIFIED", StartsAt: startsAt, QualifiedAt: startsAt, Version: 2}
	event.useConfiguredPermit = true
	_, err := svc.Decide(context.Background(), uuid.New(), repo.request.EventID, repo.request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if !errors.Is(err, ErrUnusedRefundEventUnavailable) {
		t.Fatalf("qualification at exact start error=%v, want fail-closed Event qualification error", err)
	}
	if repo.attempt.State != domain.RefundApprovalAttemptAborted || repo.finalizeCalls != 0 || len(event.releaseOutcomes) != 1 || event.releaseOutcomes[0] != "ABORTED" {
		t.Fatalf("attempt=%+v finalizes=%d releases=%v, want aborted tombstone, no approval, and Event release", repo.attempt, repo.finalizeCalls, event.releaseOutcomes)
	}
}

func TestRefundTimeRecoveryNeverReacquiresAbort(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	repo.attempt = domain.RefundApprovalAttempt{AttemptID: uuid.New(), RequestID: repo.request.RequestID, BookingID: repo.request.BookingID,
		EventID: repo.request.EventID, BuyerID: repo.request.BuyerID, PaymentID: repo.request.PaymentID, ApproverID: uuid.New(), Generation: 1,
		State: domain.RefundApprovalAttemptAborted, Fingerprint: [32]byte{1}, KeyDigest: [32]byte{2}}
	if err := svc.RecoverBatch(context.Background(), 10); err != nil {
		t.Fatalf("recover aborted attempt: %v", err)
	}
	if len(event.acquireBindings) != 0 || len(event.releaseOutcomes) != 1 || event.releaseOutcomes[0] != "ABORTED" || repo.releaseAcks != 1 {
		t.Fatalf("acquires=%v releases=%v acks=%d, want release-only ABORTED recovery", event.acquireBindings, event.releaseOutcomes, repo.releaseAcks)
	}
}

func TestRefundTimeRecoveryPreservesUnknownBookingCommit(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	repo.attempt = domain.RefundApprovalAttempt{AttemptID: uuid.New(), RequestID: repo.request.RequestID, BookingID: repo.request.BookingID,
		EventID: repo.request.EventID, BuyerID: repo.request.BuyerID, PaymentID: repo.request.PaymentID, ApproverID: uuid.New(), Generation: 1,
		State: domain.RefundApprovalAttemptPending, Fingerprint: [32]byte{1}, KeyDigest: [32]byte{2}, Reason: ""}
	repo.finalizeErr = fmt.Errorf("commit result unknown")
	if err := svc.RecoverBatch(context.Background(), 10); err == nil {
		t.Fatal("unknown Booking commit returned success")
	}
	if len(event.acquireBindings) != 1 || len(event.releaseOutcomes) != 0 || repo.attempt.State != domain.RefundApprovalAttemptPending {
		t.Fatalf("acquires=%v releases=%v state=%s, want retained pending attempt without release", event.acquireBindings, event.releaseOutcomes, repo.attempt.State)
	}
}

func TestRefundTimeRecoveryPersistsAdmissionWaitUsingExistingAttempt(t *testing.T) {
	svc, repo, event, payment := newCoordinatedRefundService()
	repo.pendingAdmission = true
	repo.attempt = domain.RefundApprovalAttempt{AttemptID: uuid.New(), RequestID: repo.request.RequestID, BookingID: repo.request.BookingID,
		EventID: repo.request.EventID, BuyerID: repo.request.BuyerID, PaymentID: repo.request.PaymentID, ApproverID: uuid.New(), Generation: 1,
		State: domain.RefundApprovalAttemptPending, KeyDigest: [32]byte{2}, Fingerprint: [32]byte{1}, Reason: "approval reason"}
	if err := svc.RecoverBatch(context.Background(), 10); err != nil {
		t.Fatalf("persist recovery Admission wait: %v", err)
	}
	if repo.waitingCalls != 1 || repo.lastWaitCommand.DecisionID != repo.attempt.AttemptID || repo.lastWaitCommand.RequestID != repo.attempt.RequestID ||
		repo.attempt.State != domain.RefundApprovalAttemptPending || len(event.acquireBindings) != 0 || len(event.releaseOutcomes) != 0 || payment.calls != 0 {
		t.Fatalf("waits=%d command=%+v attempt=%+v acquires=%v releases=%v Payment calls=%d", repo.waitingCalls, repo.lastWaitCommand,
			repo.attempt, event.acquireBindings, event.releaseOutcomes, payment.calls)
	}
}

func TestRefundTimeInteractiveAdmissionWaitDoesNotAcquirePermit(t *testing.T) {
	svc, repo, event, payment := newCoordinatedRefundService()
	repo.pendingAdmission = true
	result, err := svc.Decide(context.Background(), uuid.New(), repo.request.EventID, repo.request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if err != nil || result.Status != domain.RefundRequestWaitingCheckInResolution {
		t.Fatalf("interactive pending result=%+v error=%v, want WAITING", result, err)
	}
	if repo.waitingCalls != 1 || repo.beginCalls != 0 || len(event.acquireBindings) != 0 || payment.calls != 0 {
		t.Fatalf("waits=%d begins=%d acquires=%v Payment calls=%d, want persisted WAITING without attempt qualification", repo.waitingCalls,
			repo.beginCalls, event.acquireBindings, payment.calls)
	}
}

func TestRefundTimeFinalDecisionRetryUsesStableDecisionID(t *testing.T) {
	svc, repo, event, payment := newCoordinatedRefundService()
	attemptID := uuid.New()
	repo.request.Status = domain.RefundRequestApproved
	repo.request.DecisionID = attemptID
	repo.request.QualificationAttemptID = attemptID
	repo.attempt = domain.RefundApprovalAttempt{AttemptID: attemptID, State: domain.RefundApprovalAttemptApproved}
	result, err := svc.Decide(context.Background(), uuid.New(), repo.request.EventID, repo.request.RequestID,
		domain.RefundDecisionApproved, "", "approval-key")
	if err != nil || result.Status != domain.RefundRequestApproved {
		t.Fatalf("same-key final retry result=%+v error=%v", result, err)
	}
	if repo.lastDecisionCommand.DecisionID != attemptID || repo.beginCalls != 0 || len(event.acquireBindings) != 0 || payment.calls != 0 {
		t.Fatalf("retry command=%+v begins=%d acquires=%v Payment calls=%d, want stable decision identity with no requalification",
			repo.lastDecisionCommand, repo.beginCalls, event.acquireBindings, payment.calls)
	}
}

func TestRefundTimeReleaseFailureRetriesFromDurableTerminalAttempt(t *testing.T) {
	svc, repo, event, _ := newCoordinatedRefundService()
	repo.attempt = domain.RefundApprovalAttempt{AttemptID: uuid.New(), RequestID: repo.request.RequestID, BookingID: repo.request.BookingID,
		EventID: repo.request.EventID, BuyerID: repo.request.BuyerID, PaymentID: repo.request.PaymentID, ApproverID: uuid.New(), Generation: 1,
		State: domain.RefundApprovalAttemptApproved, KeyDigest: [32]byte{2}, Fingerprint: [32]byte{1}, EventAuthorityVersion: "2",
		StartsAt: time.Now().Add(time.Hour), EventQualifiedAt: time.Now()}
	event.releaseErr = errors.New("release response unavailable")
	if err := svc.RecoverBatch(context.Background(), 10); err == nil {
		t.Fatal("release failure returned success")
	}
	if repo.attempt.ReleaseAcknowledged || repo.releaseAcks != 0 || len(event.releaseOutcomes) != 1 {
		t.Fatalf("attempt=%+v acks=%d releases=%v, want durable retry marker after failure", repo.attempt, repo.releaseAcks, event.releaseOutcomes)
	}
	event.releaseErr = nil
	if err := svc.RecoverBatch(context.Background(), 10); err != nil {
		t.Fatalf("retry Event release: %v", err)
	}
	if !repo.attempt.ReleaseAcknowledged || repo.releaseAcks != 1 || len(event.releaseOutcomes) != 2 {
		t.Fatalf("attempt=%+v acks=%d releases=%v, want retry and durable ack", repo.attempt, repo.releaseAcks, event.releaseOutcomes)
	}
}
