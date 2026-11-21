package grpc

import (
	"context"
	"errors"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (p *PaymentServer) GetMoneyMetrics(ctx context.Context, req *paymentv1.MoneyMetricsQuery) (*paymentv1.MoneyMetrics, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	if req == nil || req.From == nil || req.To == nil || !req.From.IsValid() || !req.To.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "bounded dates required")
	}
	from, to := req.From.AsTime(), req.To.AsTime()
	if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		return nil, status.Error(codes.InvalidArgument, "invalid metrics window")
	}
	v, err := p.service.MoneyMetrics(ctx, from, to)
	if errors.Is(err, domain.ErrMetricsBounds) {
		return nil, status.Error(codes.ResourceExhausted, "narrow the metrics date range")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "payment metrics unavailable")
	}
	result := &paymentv1.MoneyMetrics{ObservedAt: timestamppb.New(v.ObservedAt)}
	for _, g := range v.Groups {
		result.Groups = append(result.Groups, &paymentv1.MoneyMetric{Currency: g.Currency, Mode: g.Mode, Payments: g.Payments, UnknownPayments: g.UnknownPayments, ConfirmedRefunds: g.ConfirmedRefunds, CollectedKnownMinor: g.CollectedKnownMinor, RefundedKnownMinor: g.RefundedKnownMinor})
	}
	return result, nil
}
