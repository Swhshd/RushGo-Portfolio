package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

type checkoutRepo struct {
	repository.BookingRepository
	snapshot *domain.LifecycleSnapshot
	reads    int
	race     bool
}

func (r *checkoutRepo) GetLifecycle(context.Context, domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	r.reads++
	if r.race && r.reads > 1 {
		v := *r.snapshot
		v.Phase = "CONFIRMING"
		return &v, nil
	}
	return r.snapshot, nil
}

type checkoutPayment struct {
	paymentv1.PaymentServiceClient
	result  *paymentv1.CreatePaymentResponse
	err     error
	calls   int
	booking string
}

func (p *checkoutPayment) ResumePayment(_ context.Context, r *paymentv1.FinancialPaymentRequest, _ ...grpc.CallOption) (*paymentv1.CreatePaymentResponse, error) {
	p.calls++
	p.booking = r.BookingId
	return p.result, p.err
}

func TestResumeBookingOwnershipTerminalAndUnknown(t *testing.T) {
	for _, tc := range []struct {
		name, phase                   string
		expired, other, race, unknown bool
		state                         string
		call                          bool
	}{
		{name: "ready", phase: "OPEN", state: "ready", call: true},
		{name: "other buyer", phase: "OPEN", other: true},
		{name: "expired", phase: "OPEN", expired: true, state: "expired"},
		{name: "confirmed", phase: "TERMINAL", state: "completed"},
		{name: "confirmation in progress", phase: "CONFIRMING", state: "processing"},
		{name: "confirmation wins during read", phase: "OPEN", race: true, state: "processing", call: true},
		{name: "provider unknown", phase: "OPEN", unknown: true, call: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, buyer := uuid.New(), uuid.New()
			deadline := time.Now().Add(time.Minute)
			if tc.expired {
				deadline = time.Now().Add(-time.Minute)
			}
			repo := &checkoutRepo{snapshot: &domain.LifecycleSnapshot{Booking: &domain.Booking{ID: id, UserID: buyer, LifecycleGeneration: 1}, Generation: 1, Phase: tc.phase, ReservationDeadline: &deadline}, race: tc.race}
			payment := &checkoutPayment{result: &paymentv1.CreatePaymentResponse{CreateState: "ready", PaymentIntentClientSecret: "original-private-credential"}}
			if tc.unknown {
				payment.err = status.Error(codes.Unavailable, "synthetic unavailable")
			}
			s := NewBookingService(repo, nil, nil, nil)
			s.SetCheckoutPaymentClient(payment)
			caller := buyer
			if tc.other {
				caller = uuid.New()
			}
			result, err := s.ResumeBooking(context.Background(), id, caller)
			if tc.other {
				if err != repository.ErrNotFound {
					t.Fatal("ownership not enforced")
				}
				return
			}
			if tc.unknown {
				if status.Code(err) != codes.Unavailable {
					t.Fatal("unknown treated as unpaid")
				}
				return
			}
			if err != nil || result.CheckoutState != tc.state {
				t.Fatalf("state=%+v err=%v", result, err)
			}
			if (payment.calls > 0) != tc.call {
				t.Fatal("unexpected payment call")
			}
			if result.Booking.ID != id || (result.PaymentClientSecret != "") != (tc.state == "ready") || result.CanCancel != (tc.state == "ready") {
				t.Fatal("identity or eligibility mismatch")
			}
			if tc.call && payment.booking != id.String() {
				t.Fatal("original payment identity not reused")
			}
		})
	}
}
