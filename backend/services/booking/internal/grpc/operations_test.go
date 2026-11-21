package grpc

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func TestOperationsRequiresGatewayCurrentAdminAndBounds(t *testing.T) {
	req := &bookingv1.OperationsQuery{From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.New(time.Now()), Limit: 50}
	s := &BookingServer{adminBookingDetailSecure: true, adminOutboxAuth: staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "user"}}}
	if _, err := s.ListOperationsBookings(context.Background(), req); err == nil {
		t.Fatal("missing gateway allowed")
	}
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer valid")
	if _, err := s.ListOperationsBookings(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonadmin %v", err)
	}
	s.adminOutboxAuth = staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "admin"}}
	req.From = timestamppb.New(time.Now().Add(-32 * 24 * time.Hour))
	if _, err := s.ListOperationsBookings(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unbounded %v", err)
	}
}
