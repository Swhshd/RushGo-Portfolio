package grpc

import (
	"context"
	"github.com/ticketbox/booking/internal/domain"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (s *BookingServer) ListOperationsBookings(ctx context.Context, req *bookingv1.OperationsQuery) (*bookingv1.OperationsBookings, error) {
	if s == nil || !s.adminBookingDetailSecure {
		return nil, status.Error(codes.Unavailable, "operations unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "admin required")
	}
	if req == nil || req.From == nil || req.To == nil || !req.From.IsValid() || !req.To.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "date range required")
	}
	q := domain.OperationsQuery{From: req.From.AsTime(), To: req.To.AsTime(), Limit: int(req.Limit), Offset: int(req.Offset)}
	if !q.From.Before(q.To) || q.To.Sub(q.From) > 31*24*time.Hour || q.Limit < 1 || q.Limit > 100 || q.Offset < 0 || q.Offset > 10000 {
		return nil, status.Error(codes.InvalidArgument, "invalid operations bounds")
	}
	rows, more, err := s.service.ListOperationsBookings(ctx, q)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "operations source unavailable")
	}
	result := &bookingv1.OperationsBookings{HasMore: more, ObservedAt: timestamppb.New(time.Now().UTC()), Scope: "Bookings created in the requested window; independently observed service records."}
	for _, r := range rows {
		v := &bookingv1.OperationsBooking{BookingId: r.BookingID.String(), EventId: r.EventID.String(), BookingStatus: r.Status, CreatedAt: timestamppb.New(r.CreatedAt), TicketCount: r.TicketCount, PaymentSourceState: r.PaymentSourceState, RefundStatus: r.RefundStatus}
		for _, issue := range r.Issues {
			v.Issues = append(v.Issues, &bookingv1.OperationIssue{Kind: issue.Kind, Source: issue.Source, State: issue.State, ObservedAt: timestamppb.New(issue.ObservedAt), NextAction: issue.NextAction})
		}
		result.Items = append(result.Items, v)
	}
	return result, nil
}
func (s *BookingServer) ListStaffWork(ctx context.Context, req *bookingv1.StaffWorkQuery) (*bookingv1.StaffWorkList, error) {
	if s == nil || !s.adminBookingDetailSecure {
		return nil, status.Error(codes.Unavailable, "worklist unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateTicketPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role == "admin" {
		principal, err = s.authenticateAdminOutboxPrincipal(ctx)
		if err != nil {
			return nil, err
		}
	}
	if req == nil || req.Limit < 1 || req.Limit > 100 || req.Offset < 0 || req.Offset > 10000 {
		return nil, status.Error(codes.InvalidArgument, "invalid worklist bounds")
	}
	result, err := s.service.ListStaffWork(ctx, principal.UserID, principal.Role == "admin", int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "worklist source unavailable")
	}
	return result, nil
}
