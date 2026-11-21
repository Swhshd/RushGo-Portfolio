package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type adminEventTicketsListRPCReader struct {
	snapshot repository.AdminEventTicketListSnapshot
	calls    int
}

func (r *adminEventTicketsListRPCReader) ReadAdminEventTickets(_ context.Context, _ uuid.UUID, _ repository.AdminEventTicketListQuery) (repository.AdminEventTicketListSnapshot, error) {
	r.calls++
	return r.snapshot, nil
}

func TestListAdminEventTicketsRequiresVerifiedGatewayAndCurrentAdmin(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	observedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name           string
		ctx            context.Context
		currentRole    *userv1.ValidateTokenResponse
		currentRoleErr error
		revoked        bool
		wantCode       codes.Code
		wantReads      int
	}{
		{name: "missing verified peer", ctx: context.Background(), wantCode: codes.Unauthenticated},
		{name: "ordinary service identity", ctx: adminOutboxRPCContext("spiffe://rushgo.internal/event", true, "Bearer admin"), wantCode: codes.PermissionDenied},
		{name: "trusted Saga is not Gateway", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentitySaga, true, "Bearer admin"), wantCode: codes.PermissionDenied},
		{name: "unverified gateway peer", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, false, "Bearer admin"), wantCode: codes.Unauthenticated},
		{name: "admin demoted since token issue", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user", CurrentRoleVerified: true}, wantCode: codes.PermissionDenied},
		{name: "current account missing", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), currentRoleErr: status.Error(codes.NotFound, "account deleted"), wantCode: codes.PermissionDenied},
		{name: "revoked token", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer revoked"), revoked: true, wantCode: codes.Unauthenticated},
		{name: "verified current admin", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true}, wantCode: codes.OK, wantReads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminEventTicketsListRPCReader{snapshot: repository.AdminEventTicketListSnapshot{EventID: eventID, ObservedAt: observedAt}}
			server := NewBookingServer(nil, zap.NewNop())
			server.SetAdminEventTicketListSecure(true)
			server.SetAdminEventTicketListService(service.NewAdminEventTicketListService(reader))
			users := &fakeTicketTokenClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"},
				currentRole:    tc.currentRole,
				currentRoleErr: tc.currentRoleErr,
			}
			configureAdminOutboxAuthenticators(server, users, &fakeTicketRevocationChecker{revoked: tc.revoked})
			response, err := server.ListAdminEventTickets(tc.ctx, &bookingv1.ListAdminEventTicketsRequest{EventId: eventID.String(), StatusFilter: "ALL", PageSize: 20})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ListAdminEventTickets status = %s, want %s (err=%v)", got, tc.wantCode, err)
			}
			if reader.calls != tc.wantReads {
				t.Fatalf("repository reads = %d, want %d", reader.calls, tc.wantReads)
			}
			if tc.wantCode == codes.OK && (response == nil || response.GetEventId() != eventID.String() || response.GetObservedAt() == nil || response.GetSourceRecordLimit() != 5000) {
				t.Fatalf("authorized empty-page response = %+v", response)
			}
		})
	}
}

func TestListAdminEventTicketsFailsClosedWithoutSecureWiring(t *testing.T) {
	server := NewBookingServer(nil, nil)
	_, err := server.ListAdminEventTickets(context.Background(), &bookingv1.ListAdminEventTicketsRequest{EventId: "60000000-0000-4000-8000-000000000001"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ListAdminEventTickets status = %s, want Unavailable", status.Code(err))
	}
}
