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

func (s *BookingServer) GetAdminEventTicketOperations(ctx context.Context, req *bookingv1.GetAdminEventTicketOperationsRequest) (*bookingv1.GetAdminEventTicketOperationsResponse, error) {
	if s == nil || !s.adminEventTicketOperationsSecure {
		return nil, status.Error(codes.Unavailable, "Admin event ticket operations source unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "Admin event ticket operations authorization denied")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	eventID, err := uuid.Parse(req.GetEventId())
	if err != nil || eventID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	if s.adminEventTicketOperations == nil {
		return nil, status.Error(codes.Unavailable, "Admin event ticket operations source unavailable")
	}
	projection, err := s.adminEventTicketOperations.Get(ctx, eventID)
	if err != nil {
		switch {
		case err == service.ErrAdminEventTicketOperationsInvalidEventID:
			return nil, status.Error(codes.InvalidArgument, "invalid event identity")
		case err == service.ErrAdminEventTicketOperationsCapacityExceeded:
			return nil, status.Error(codes.ResourceExhausted, "Admin event ticket operations source capacity exceeded")
		default:
			return nil, status.Error(codes.Unavailable, "Admin event ticket operations source unavailable")
		}
	}
	response := &bookingv1.GetAdminEventTicketOperationsResponse{
		EventId:                projection.EventID,
		ObservedAt:             timestamppb.New(projection.ObservedAt),
		TotalRecordedCount:     projection.TotalRecordedCount,
		IssuedCount:            projection.IssuedCount,
		CheckedInCount:         projection.CheckedInCount,
		RevokedCount:           projection.RevokedCount,
		SuccessfulCheckinCount: projection.SuccessfulCheckinCount,
		SourceRecordLimit:      projection.SourceRecordLimit,
		RecentCheckins:         make([]*timestamppb.Timestamp, 0, len(projection.RecentCheckins)),
	}
	if projection.LatestCheckinAt != nil {
		response.LatestCheckinAt = timestamppb.New(*projection.LatestCheckinAt)
	}
	for _, checkedInAt := range projection.RecentCheckins {
		response.RecentCheckins = append(response.RecentCheckins, timestamppb.New(checkedInAt))
	}
	for _, exception := range projection.DeliveryExceptions {
		response.DeliveryExceptions = append(response.DeliveryExceptions, &bookingv1.AdminEventAfterSalesException{
			BookingId: exception.BookingID, State: exception.State, BookingStatus: exception.BookingStatus,
			ExpectedTicketCount: exception.ExpectedTicketCount, RecordedTicketCount: exception.RecordedTicketCount,
			IssuanceReceiptOutcome: exception.IssuanceReceiptOutcome, IssuanceReasonCode: exception.IssuanceReasonCode,
			BookingCreatedAt: timestamppb.New(exception.BookingCreatedAt),
		})
	}
	return response, nil
}
