package grpc

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (p *PaymentServer) GetOperationsPayment(ctx context.Context, req *paymentv1.FinancialPaymentRequest) (*paymentv1.OperationsPayment, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(req.GetBookingId())
	if err != nil || id == uuid.Nil || req.GetGeneration() != 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid booking")
	}
	view, stale, err := p.service.OperationsPayment(ctx, id, req.Generation)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return &paymentv1.OperationsPayment{Payment: financialPaymentEntry(view), RefundPreparedStale: stale, ObservedAt: timestamppb.New(time.Now().UTC())}, nil
}
