package grpc

import (
	"context"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"testing"
)

func TestScannerAssignmentCurrentRoleBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, role           string
		unavailable, revoked bool
		want                 codes.Code
	}{
		{name: "demoted", role: "user", want: codes.PermissionDenied},
		{name: "deleted", unavailable: true, want: codes.PermissionDenied},
		{name: "revoked", role: "admin", revoked: true, want: codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &scannerAssignmentListRepository{}
			server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
			users := &fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: tc.role, CurrentRoleVerified: true}}
			if tc.unavailable {
				users.currentRoleErr = status.Error(codes.NotFound, "deleted")
			}
			configureAdminOutboxAuthenticators(server, users, &fakeTicketRevocationChecker{revoked: tc.revoked})
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer old-admin"))
			_, err := server.SetEventScannerAssignment(ctx, &bookingv1.SetEventScannerAssignmentRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthOtherID, ActorUserId: ticketAuthUserID, Active: true})
			if status.Code(err) != tc.want {
				t.Fatalf("write status %v want %v", status.Code(err), tc.want)
			}
			_, err = server.ListEventScannerAssignments(ctx, &bookingv1.ListEventScannerAssignmentsRequest{EventId: ticketAuthEventID})
			if status.Code(err) != tc.want || store.calls != 0 {
				t.Fatal("denied principal reached scanner assignments")
			}
		})
	}
}
