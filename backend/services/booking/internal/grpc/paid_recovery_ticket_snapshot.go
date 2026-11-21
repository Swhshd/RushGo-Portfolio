package grpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *BookingServer) GetPaidRecoveryTicketSnapshot(ctx context.Context, req *bookingv1.LifecycleRequest) (*bookingv1.PaidRecoveryTicketSnapshot, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentitySaga); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid paid recovery ticket snapshot request")
	}
	bookingID, err := uuid.Parse(req.GetBookingId())
	if err != nil || bookingID == uuid.Nil || req.GetGeneration() != 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid paid recovery Booking identity")
	}
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Unavailable, "paid recovery ticket authority unavailable")
	}
	snapshot, err := s.service.GetPaidRecoveryTicketSnapshot(ctx, bookingID, req.GetGeneration())
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			return nil, status.Error(codes.NotFound, "Booking not found")
		case errors.Is(err, repository.ErrInvalidLifecycleDecision):
			return nil, status.Error(codes.InvalidArgument, "invalid paid recovery Booking identity")
		case errors.Is(err, repository.ErrLifecycleConflict), errors.Is(err, repository.ErrLegacyLifecycleAttention):
			return nil, status.Error(codes.FailedPrecondition, "Booking is not eligible for ticket recovery")
		default:
			return nil, status.Error(codes.Unavailable, "paid recovery ticket facts unavailable")
		}
	}
	return &bookingv1.PaidRecoveryTicketSnapshot{
		BookingId:            snapshot.BookingID.String(),
		SagaId:               snapshot.SagaID.String(),
		Generation:           snapshot.Generation,
		TicketCount:          snapshot.TicketCount,
		IssuanceReceiptCount: snapshot.IssuanceReceiptCount,
		ObservedAt:           timestamppb.New(snapshot.ObservedAt),
	}, nil
}
