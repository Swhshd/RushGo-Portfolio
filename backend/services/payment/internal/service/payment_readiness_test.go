package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type readinessEvent struct {
	eventv1.EventServiceClient
	ready map[string]bool
	err   error
}

func (e *readinessEvent) GetTicketAvailability(_ context.Context, req *eventv1.GetTicketAvailabilityRequest, _ ...grpc.CallOption) (*eventv1.TicketAvailabilityResponse, error) {
	if e.err != nil {
		return nil, e.err
	}
	return &eventv1.TicketAvailabilityResponse{TierId: req.TierId, AccountingReady: e.ready[req.TierId]}, nil
}

func TestEventAccountingReadinessFailsClosed(t *testing.T) {
	tier := uuid.New()
	checker := NewEventAccountingReadiness(&readinessEvent{ready: map[string]bool{tier.String(): false}})
	if err := checker.Check(context.Background(), []uuid.UUID{tier}); !errors.Is(err, ErrAccountingNotReady) {
		t.Fatalf("unready tier: %v", err)
	}
	checker = NewEventAccountingReadiness(&readinessEvent{ready: map[string]bool{tier.String(): true}})
	if err := checker.Check(context.Background(), []uuid.UUID{tier}); err != nil {
		t.Fatalf("ready tier: %v", err)
	}
	checker = NewEventAccountingReadiness(&readinessEvent{err: status.Error(codes.Unavailable, "down")})
	if err := checker.Check(context.Background(), []uuid.UUID{tier}); !errors.Is(err, ErrAccountingUnavailable) {
		t.Fatalf("unavailable authority: %v", err)
	}
	checker = NewEventAccountingReadiness(&readinessEvent{err: status.Error(codes.NotFound, "missing")})
	if err := checker.Check(context.Background(), []uuid.UUID{tier}); !errors.Is(err, ErrAccountingNotReady) {
		t.Fatalf("missing tier: %v", err)
	}
	checker = NewEventAccountingReadiness(&readinessEvent{ready: map[string]bool{tier.String(): true}})
	if err := checker.Check(context.Background(), nil); !errors.Is(err, ErrAccountingUnavailable) {
		t.Fatalf("empty tier set: %v", err)
	}
}

type readinessBooking struct {
	bookingv1.BookingServiceClient
	view *bookingv1.LifecycleDisposition
}

func (b *readinessBooking) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	return b.view, nil
}

type denyAccounting struct{ err error }

func (d denyAccounting) Check(context.Context, []uuid.UUID) error { return d.err }

type noProvider struct{}

func (noProvider) CreateIntent(context.Context, domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("provider called")
}
func (noProvider) RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("provider called")
}
func (noProvider) FindIntent(context.Context, domain.CreateOperation) ([]domain.ProviderObservation, error) {
	return nil, errors.New("provider called")
}
func (noProvider) CancelIntent(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("provider called")
}
func (noProvider) CreateFullRefund(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("provider called")
}
func (noProvider) RetrieveRefund(context.Context, string, bool, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("provider called")
}
func (noProvider) InspectSucceededRefunds(context.Context, string, bool, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("provider called")
}
func (noProvider) VerifyWebhook(context.Context, []byte, string) (*domain.ProviderReceipt, error) {
	return nil, errors.New("provider called")
}

var _ paymentgateway.FinancialProvider = noProvider{}

func TestCreatePaymentRejectsUnreadyTierBeforeRepositoryOrProvider(t *testing.T) {
	booking, user, tier := uuid.New(), uuid.New(), uuid.New()
	view := &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id:               booking.String(),
			UserId:           user.String(),
			TotalAmountCents: 725,
			Items:            []*bookingv1.BookingItem{{TicketTierId: tier.String(), Quantity: 1}},
		},
		Generation:          1,
		Phase:               "OPEN",
		Currency:            "usd",
		ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour)),
	}
	s := &PaymentService{
		logger:     zap.NewNop(),
		booking:    &readinessBooking{view: view},
		accounting: denyAccounting{err: ErrAccountingNotReady},
		account:    "acct_fixture",
		provider:   noProvider{},
	}
	_, err := s.CreatePayment(context.Background(), &domain.CreatePaymentRequest{
		Payment:           domain.Payment{UserId: user, BookingId: &booking, Price: 725, Currency: "usd"},
		PaymentGeneration: 1,
	})
	if !errors.Is(err, ErrAccountingNotReady) {
		t.Fatalf("unready payment returned %v, want ErrAccountingNotReady", err)
	}
}
