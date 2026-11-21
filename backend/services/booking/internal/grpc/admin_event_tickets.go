package grpc

import (
	"context"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *BookingServer) ListAdminEventTickets(ctx context.Context, req *bookingv1.ListAdminEventTicketsRequest) (*bookingv1.ListAdminEventTicketsResponse, error) {
	if s == nil || !s.adminEventTicketListSecure {
		return nil, status.Error(codes.Unavailable, "Admin event ticket list source unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "Admin event ticket list authorization denied")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid ticket list query")
	}
	eventID, err := uuid.Parse(req.GetEventId())
	if err != nil || eventID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	if s.adminEventTicketList == nil {
		return nil, status.Error(codes.Unavailable, "Admin event ticket list source unavailable")
	}
	projection, err := s.adminEventTicketList.List(ctx, eventID, req.GetStatusFilter(), req.GetPageSize(), req.GetCursor())
	if err != nil {
		switch {
		case err == service.ErrAdminEventTicketListInvalidRequest:
			return nil, status.Error(codes.InvalidArgument, "invalid ticket list query")
		case err == service.ErrAdminEventTicketListCapacityExceeded:
			return nil, status.Error(codes.ResourceExhausted, "Admin event ticket list source capacity exceeded")
		default:
			return nil, status.Error(codes.Unavailable, "Admin event ticket list source unavailable")
		}
	}
	response := &bookingv1.ListAdminEventTicketsResponse{
		EventId:           projection.EventID,
		ObservedAt:        timestamppb.New(projection.ObservedAt),
		StatusFilter:      projection.StatusFilter,
		PageSize:          projection.PageSize,
		HasMore:           projection.HasMore,
		NextCursor:        projection.NextCursor,
		SourceRecordLimit: projection.SourceRecordLimit,
		Tickets:           make([]*bookingv1.AdminEventTicketRecord, 0, len(projection.Tickets)),
	}
	for _, ticket := range projection.Tickets {
		record := &bookingv1.AdminEventTicketRecord{
			TicketId: ticket.TicketID,
			Status:   ticket.Status,
			IssuedAt: timestamppb.New(ticket.IssuedAt),
		}
		if ticket.CheckedInAt != nil {
			record.CheckedInAt = timestamppb.New(*ticket.CheckedInAt)
		}
		if ticket.RevokedAt != nil {
			record.RevokedAt = timestamppb.New(*ticket.RevokedAt)
		}
		response.Tickets = append(response.Tickets, record)
	}
	return response, nil
}
