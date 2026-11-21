package service

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	gateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

type resumePaymentRepo struct {
	repository.PaymentRepositoryInterface
	view         *domain.FinancialView
	observations int
}

func (r *resumePaymentRepo) GetByLogicalIdentity(context.Context, uuid.UUID, int64) (*domain.FinancialView, error) {
	return r.view, nil
}
func (r *resumePaymentRepo) RecordObservation(context.Context, domain.ProviderObservation) error {
	r.observations++
	return nil
}

type resumeProvider struct {
	gateway.FinancialProvider
	observation *domain.ProviderObservation
	err         error
	calls       int
}

func (p *resumeProvider) RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error) {
	p.calls++
	return p.observation, "same-intent-secret", p.err
}

func TestResumePaymentNeverCreatesAndRejectsUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name, state           string
		mismatch, unavailable bool
		ready                 bool
	}{
		{name: "same original unpaid intent", state: "requires_payment_method", ready: true},
		{name: "collected", state: "succeeded"},
		{name: "in flight", state: "processing"},
		{name: "cancelled", state: "canceled"},
		{name: "unknown", state: "unknown"},
		{name: "provider failure", unavailable: true},
		{name: "identity mismatch", state: "requires_payment_method", mismatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			booking, buyer, payment := uuid.New(), uuid.New(), uuid.New()
			live := false
			repo := &resumePaymentRepo{view: &domain.FinancialView{Managed: true, Generation: 1, ProviderAccount: "sim-only", LiveMode: &live, CreateState: "create_ready", RefundState: "not_required", Payment: &domain.Payment{ID: payment, UserId: buyer, BookingId: &booking, Price: 100, Currency: "usd", PaymentIntentId: "pi_existing"}}}
			provider := &resumeProvider{observation: &domain.ProviderObservation{PaymentID: payment, BookingID: booking, Account: "sim-only", IntentID: "pi_existing", Generation: 1, AmountCents: 100, Currency: "usd", Status: tc.state, Collected: tc.state == "succeeded"}}
			if tc.mismatch {
				provider.observation.BookingID = uuid.New()
			}
			if tc.unavailable {
				provider.err = errors.New("unknown")
			}
			reader := &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: buyer.String(), TotalAmountCents: 100}, Generation: 1, Phase: "OPEN", Currency: "usd", ReservationDeadline: timestamppb.New(time.Now().Add(time.Minute))}}
			s := &PaymentService{repo: repo, provider: provider, booking: reader, account: "sim-only"}
			for i := 0; i < 2; i++ {
				result, err := s.ResumePayment(context.Background(), booking, 1)
				if tc.mismatch || tc.unavailable {
					if err == nil {
						t.Fatal("uncertain response accepted")
					}
					continue
				}
				if err != nil || result.PaymentID != payment || (result.PaymentIntentClientSecret != "") != tc.ready {
					t.Fatalf("unsafe resume result %+v err=%v", result, err)
				}
			}
			// Embedded methods panic if CreateIntent/CreateOrGet/refund is invoked.
			if provider.calls != 2 {
				t.Fatal("resume did not retrieve original intent")
			}
		})
	}
}
