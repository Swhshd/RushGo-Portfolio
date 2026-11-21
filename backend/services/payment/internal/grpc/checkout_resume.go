package grpc

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *PaymentServer) ResumePayment(ctx context.Context, req *paymentv1.FinancialPaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	if req == nil || req.PaymentId != "" || req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid checkout identity")
	}
	id, err := uuid.Parse(req.BookingId)
	if err != nil || id == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid checkout identity")
	}
	result, err := p.service.ResumePayment(ctx, id, req.Generation)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return &paymentv1.CreatePaymentResponse{Id: result.PaymentID.String(), Status: result.Status, PaymentIntentId: result.PaymentItentId, PaymentIntentClientSecret: result.PaymentIntentClientSecret, CreateState: result.CreateState, RetryAfterSeconds: result.RetryAfterSeconds}, nil
}
