package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
)

type unusedRefundServiceRepoStub struct {
	repository.UnusedTicketRefundRepository
	request     domain.BookingRefundRequest
	pending     bool
	decideErr   error
	actorErr    error
	decideCalls int
	lastCommand domain.DecideUnusedRefundCommand
}

func (r *unusedRefundServiceRepoStub) GetEventUnusedRefundByID(context.Context, uuid.UUID, uuid.UUID) (domain.BookingRefundRequest, error) {
	return r.request, nil
}

func (r *unusedRefundServiceRepoStub) HasPendingBookingAdmission(context.Context, uuid.UUID) (bool, error) {
	return r.pending, nil
}

func (r *unusedRefundServiceRepoStub) CheckEventRefundDecisionActor(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return r.actorErr
}

func (r *unusedRefundServiceRepoStub) DecideUnusedRefund(_ context.Context, command domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error) {
	r.decideCalls++
	r.lastCommand = command
	if r.decideErr != nil {
		return domain.BookingRefundRequest{}, r.decideErr
	}
	request := r.request
	if r.pending {
		request.Status = domain.RefundRequestWaitingCheckInResolution
	}
	return request, nil
}

type unusedRefundPaymentStub struct{ calls int }

func (p *unusedRefundPaymentStub) GetUnusedRefundPaymentSnapshot(context.Context, domain.RefundApprovalCandidate) (domain.UnusedRefundPaymentSnapshot, error) {
	p.calls++
	return domain.UnusedRefundPaymentSnapshot{}, nil
}

func TestUnusedRefundPendingAdmissionReturnsWaitingWithoutPaymentAuthority(t *testing.T) {
	request := domain.BookingRefundRequest{RequestID: uuid.New(), BookingID: uuid.New(), EventID: uuid.New(), BuyerID: uuid.New(), Status: domain.RefundRequestRequested}
	repo := &unusedRefundServiceRepoStub{request: request, pending: true}
	payment := &unusedRefundPaymentStub{}
	svc := NewUnusedTicketRefundService(repo, nil, payment)
	result, err := svc.Decide(context.Background(), uuid.New(), request.EventID, request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if err != nil {
		t.Fatalf("pending refund approval error=%v, want WAITING result", err)
	}
	if result.Status != domain.RefundRequestWaitingCheckInResolution || repo.decideCalls != 1 || payment.calls != 0 {
		t.Fatalf("result=%+v decideCalls=%d paymentCalls=%d, want WAITING and no Payment call", result, repo.decideCalls, payment.calls)
	}
}

func TestUnusedRefundApprovalFailsClosedWithoutEventAuthority(t *testing.T) {
	request := domain.BookingRefundRequest{RequestID: uuid.New(), BookingID: uuid.New(), EventID: uuid.New(), BuyerID: uuid.New(), Status: domain.RefundRequestRequested}
	repo := &unusedRefundServiceRepoStub{request: request, decideErr: repository.ErrRefundRequestEventAuthorityUnavailable}
	payment := &unusedRefundPaymentStub{}
	svc := NewUnusedTicketRefundService(repo, nil, payment)
	_, err := svc.Decide(context.Background(), uuid.New(), request.EventID, request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if !errors.Is(err, ErrUnusedRefundApprovalDisabled) {
		t.Fatalf("approval error=%v, want fail-closed Event authority error", err)
	}
	// Payment must never be contacted; the repository is still called once so
	// it can atomically record WAITING if a pending Admission appeared.
	if payment.calls != 0 || repo.decideCalls != 1 || repo.lastCommand.Event.EventID != uuid.Nil {
		t.Fatalf("payment calls=%d decide calls=%d event=%+v, want no Payment and empty Event proof", payment.calls, repo.decideCalls, repo.lastCommand.Event)
	}
}

func TestUnusedRefundUnauthorizedApproverCannotTriggerPaymentLookup(t *testing.T) {
	request := domain.BookingRefundRequest{RequestID: uuid.New(), BookingID: uuid.New(), EventID: uuid.New(), BuyerID: uuid.New(), Status: domain.RefundRequestRequested}
	repo := &unusedRefundServiceRepoStub{request: request, actorErr: repository.ErrRefundRequestApproverDenied}
	payment := &unusedRefundPaymentStub{}
	svc := NewUnusedTicketRefundService(repo, nil, payment)
	_, err := svc.Decide(context.Background(), uuid.New(), request.EventID, request.RequestID, domain.RefundDecisionApproved, "", "approval-key")
	if !errors.Is(err, repository.ErrRefundRequestApproverDenied) {
		t.Fatalf("approval error=%v, want approver denial", err)
	}
	if payment.calls != 0 || repo.decideCalls != 0 {
		t.Fatalf("payment calls=%d repository decision calls=%d, want no external lookup or decision", payment.calls, repo.decideCalls)
	}
}

func TestUnusedRefundIdempotencyKeyValidation(t *testing.T) {
	for _, key := range []string{"", "bad key", "abc/def", "ok_123-XYZ"} {
		if got, want := validRefundIdempotencyKey(key), key == "ok_123-XYZ"; got != want {
			t.Errorf("validRefundIdempotencyKey(%q)=%t, want %t", key, got, want)
		}
	}
}
