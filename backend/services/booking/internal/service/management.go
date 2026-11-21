package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (s *BookingService) ListFeatureFlags(ctx context.Context) ([]domain.FeatureFlag, []domain.FeatureChange, error) {
	repo, ok := s.bookingRepo.(repository.ManagementRepository)
	if !ok {
		return nil, nil, ErrOperationsUnavailable
	}
	return repo.ListFeatureFlags(ctx)
}
func (s *BookingService) ChangeFeatureFlag(ctx context.Context, key string, enabled bool, version int64, actor uuid.UUID) (*domain.FeatureFlag, error) {
	repo, ok := s.bookingRepo.(repository.ManagementRepository)
	if !ok {
		return nil, ErrOperationsUnavailable
	}
	return repo.ChangeFeatureFlag(ctx, key, enabled, version, actor)
}
func (s *BookingService) OperationsMetrics(ctx context.Context, from, to time.Time) (*bookingv1.OperationsMetrics, error) {
	repo, ok := s.bookingRepo.(repository.ManagementRepository)
	if !ok {
		return nil, ErrOperationsUnavailable
	}
	v, err := repo.BookingMetrics(ctx, from, to)
	if err != nil {
		return nil, err
	}
	result := &bookingv1.OperationsMetrics{Orders: v.Orders, IssuedTickets: v.IssuedTickets, CheckedInTickets: v.CheckedInTickets, RefundRequests: v.RefundRequests, ConfirmedRefunds: v.ConfirmedRefunds, BookingObservedAt: timestamppb.New(v.ObservedAt), PaymentSourceState: "unavailable", PaymentUnavailableReason: "source_unavailable", Scope: "Current records of bookings and payments each created in [from,to); independently observed, at most 10000 records per service. Issued tickets include subsequently checked-in/revoked tickets. Not a settlement ledger."}
	if s.checkoutPayment == nil {
		return result, nil
	}
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := s.checkoutPayment.GetMoneyMetrics(call, &paymentv1.MoneyMetricsQuery{From: timestamppb.New(from), To: timestamppb.New(to)})
	if status.Code(err) == codes.ResourceExhausted {
		result.PaymentUnavailableReason = "narrow_date_range"
	}
	if err != nil || m == nil {
		return result, nil
	}
	result.PaymentSourceState = "available"
	result.PaymentUnavailableReason = ""
	result.PaymentObservedAt = m.ObservedAt
	for _, g := range m.Groups {
		result.Money = append(result.Money, &bookingv1.MoneyMetric{Currency: g.Currency, Mode: g.Mode, Payments: g.Payments, UnknownPayments: g.UnknownPayments, ConfirmedRefunds: g.ConfirmedRefunds, CollectedKnownMinor: g.CollectedKnownMinor, RefundedKnownMinor: g.RefundedKnownMinor})
	}
	return result, nil
}
