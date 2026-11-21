package grpc

import (
	"context"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *BookingServer) ResumeBooking(ctx context.Context, req *bookingv1.CancelBookingRequest) (*bookingv1.BookingDetail, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid checkout request")
	}
	principal, err := s.authenticateTicketPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	buyer, err := uuid.Parse(req.UserId)
	if err != nil || buyer == uuid.Nil || buyer != principal.UserID {
		return nil, status.Error(codes.PermissionDenied, "checkout owner denied")
	}
	booking, err := uuid.Parse(req.BookingId)
	if err != nil || booking == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking identity")
	}
	result, err := s.service.ResumeBooking(ctx, booking, buyer)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return nil, err
		}
		return nil, lifecycleRPCError(err)
	}
	return toBookingDetailV2(result), nil
}
