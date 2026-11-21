package grpc

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"testing"
)

func TestManagementRejectsMissingPeerAndNonadmin(t *testing.T) {
	s := &BookingServer{adminBookingDetailSecure: true, adminOutboxAuth: staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "user"}}}
	if _, err := s.GetPublicFeatures(context.Background(), &emptypb.Empty{}); err == nil {
		t.Fatal("public flags bypassed gateway mTLS")
	}
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer valid")
	for _, call := range []func() error{
		func() error { _, e := s.ListFeatureFlags(ctx, &emptypb.Empty{}); return e },
		func() error {
			_, e := s.ChangeFeatureFlag(ctx, &bookingv1.ChangeFeatureFlagRequest{Key: "discovery_advanced_filters", Enabled: true})
			return e
		},
		func() error { _, e := s.GetOperationsMetrics(ctx, &bookingv1.OperationsQuery{}); return e },
	} {
		if err := call(); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("nonadmin accepted: %v", err)
		}
	}
}
