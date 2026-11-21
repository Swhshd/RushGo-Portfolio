package grpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *BookingServer) GetAdminBookingDetail(ctx context.Context, req *bookingv1.GetAdminBookingDetailRequest) (*bookingv1.AdminBookingDetail, error) {
	if s == nil || !s.adminBookingDetailSecure {
		return nil, status.Error(codes.Unavailable, "admin booking detail unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "admin booking detail authorization denied")
	}
	bookingID, err := uuid.Parse(req.GetBookingId())
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking ID")
	}
	detail, err := s.service.GetAdminBookingDetail(ctx, bookingID)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			return nil, status.Error(codes.NotFound, "booking not found")
		case errors.Is(err, service.ErrAdminBookingDetailUnavailable):
			return nil, status.Error(codes.Unavailable, "admin booking detail unavailable")
		default:
			return nil, status.Error(codes.Internal, "failed to read admin booking detail")
		}
	}
	if detail == nil || detail.Booking == nil || detail.Booking.ID != bookingID {
		return nil, status.Error(codes.Internal, "invalid admin booking detail")
	}
	return s.adminBookingDetailProjection(detail), nil
}

func (s *BookingServer) adminBookingDetailProjection(detail *domain.AdminBookingDetail) *bookingv1.AdminBookingDetail {
	result := &bookingv1.AdminBookingDetail{
		Booking:             toBookingDetail(detail.Booking),
		LifecycleGeneration: detail.Booking.LifecycleGeneration,
		LifecyclePhase:      detail.Booking.LifecyclePhase,
		Currency:            detail.Booking.Currency,
		BookingMode:         detail.Booking.BookingMode,
		ObservedAt:          timestamppb.New(detail.ObservedAt),
	}
	for _, ticket := range detail.Tickets {
		result.Tickets = append(result.Tickets, &bookingv1.AdminBookingDetailTicket{
			TicketId:     ticket.TicketID.String(),
			Status:       ticket.Status,
			SeatId:       ticket.SeatID.String(),
			TicketTierId: ticket.TicketTierID.String(),
			IssuedAt:     timestamppb.New(ticket.IssuedAt),
			CheckedInAt:  refundTimestamp(ticket.CheckedInAt),
			RevokedAt:    refundTimestamp(ticket.RevokedAt),
		})
	}
	if detail.Receipt != nil {
		result.ReceiptPresent = true
		result.Receipt = &bookingv1.AdminBookingDetailReceipt{
			Outcome:      detail.Receipt.Outcome,
			ReasonCode:   detail.Receipt.ReasonCode,
			TicketCount:  detail.Receipt.TicketCount,
			ReceiptCount: detail.Receipt.ReceiptCount,
			ObservedAt:   refundTimestamp(detail.Receipt.ObservedAt),
			CreatedAt:    refundTimestamp(detail.Receipt.CreatedAt),
		}
	}
	if detail.Refund != nil {
		result.RefundPresent = true
		result.Refund = s.refundProjection(*detail.Refund)
	}
	return result
}
