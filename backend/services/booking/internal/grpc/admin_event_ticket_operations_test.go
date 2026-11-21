package grpc

import (
	"context"
	"errors"
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

type adminEventTicketOperationsRPCReader struct {
	snapshot repository.AdminEventTicketOperationsSnapshot
	err      error
	calls    int
}

func (r *adminEventTicketOperationsRPCReader) ReadAdminEventTicketOperations(context.Context, uuid.UUID) (repository.AdminEventTicketOperationsSnapshot, error) {
	r.calls++
	return r.snapshot, r.err
}

func TestGetAdminEventTicketOperationsFailsClosedWithoutSecureWiring(t *testing.T) {
	server := NewBookingServer(nil, nil)
	_, err := server.GetAdminEventTicketOperations(context.Background(), &bookingv1.GetAdminEventTicketOperationsRequest{
		EventId: "60000000-0000-4000-8000-000000000001",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetAdminEventTicketOperations status = %s, want Unavailable", status.Code(err))
	}
}

func TestGetAdminEventTicketOperationsRequiresVerifiedGatewayAndCurrentAdmin(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	observedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		ctx         context.Context
		currentRole *userv1.ValidateTokenResponse
		revoked     bool
		wantCode    codes.Code
		wantReads   int
	}{
		{name: "missing verified peer", ctx: context.Background(), wantCode: codes.Unauthenticated},
		{name: "ordinary service identity", ctx: adminOutboxRPCContext("spiffe://rushgo.internal/event", true, "Bearer admin"), wantCode: codes.PermissionDenied},
		{name: "unverified gateway peer", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, false, "Bearer admin"), wantCode: codes.Unauthenticated},
		{name: "admin demoted since token issue", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user", CurrentRoleVerified: true}, wantCode: codes.PermissionDenied},
		{name: "revoked token", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer revoked"), revoked: true, wantCode: codes.Unauthenticated},
		{name: "verified current admin", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true}, wantCode: codes.OK, wantReads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminEventTicketOperationsRPCReader{snapshot: repository.AdminEventTicketOperationsSnapshot{EventID: eventID, ObservedAt: observedAt}}
			server := NewBookingServer(nil, zap.NewNop())
			server.SetAdminEventTicketOperationsSecure(true)
			server.SetAdminEventTicketOperationsService(service.NewAdminEventTicketOperationsService(reader))
			users := &fakeTicketTokenClient{
				response:    &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"},
				currentRole: tc.currentRole,
			}
			revocations := &fakeTicketRevocationChecker{revoked: tc.revoked}
			configureAdminOutboxAuthenticators(server, users, revocations)
			response, err := server.GetAdminEventTicketOperations(tc.ctx, &bookingv1.GetAdminEventTicketOperationsRequest{EventId: eventID.String()})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("GetAdminEventTicketOperations status = %s, want %s (err=%v)", got, tc.wantCode, err)
			}
			if reader.calls != tc.wantReads {
				t.Fatalf("snapshot reads = %d, want %d", reader.calls, tc.wantReads)
			}
			if tc.wantCode == codes.OK {
				if response == nil || response.EventId != eventID.String() || response.ObservedAt == nil || response.ObservedAt.AsTime() != observedAt || response.TotalRecordedCount != 0 || response.SourceRecordLimit != 5000 {
					t.Fatalf("authorized empty snapshot response = %+v", response)
				}
				if users.calls != 2 || users.currentRoleCalls != 1 || revocations.calls != 1 {
					t.Fatalf("token/revocation/current-role calls = %d/%d/%d, want 2/1/1", users.calls, revocations.calls, users.currentRoleCalls)
				}
			}
		})
	}
}

func TestGetAdminEventTicketOperationsMapsCapacityWithoutPartialResponse(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	reader := &adminEventTicketOperationsRPCReader{err: errors.New("private read detail")}
	server := NewBookingServer(nil, zap.NewNop())
	server.SetAdminEventTicketOperationsSecure(true)
	server.SetAdminEventTicketOperationsService(service.NewAdminEventTicketOperationsService(reader))
	users := &fakeTicketTokenClient{
		response:    &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"},
		currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true},
	}
	configureAdminOutboxAuthenticators(server, users, &fakeTicketRevocationChecker{})
	_, err := server.GetAdminEventTicketOperations(adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), &bookingv1.GetAdminEventTicketOperationsRequest{EventId: eventID.String()})
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "Admin event ticket operations source unavailable" {
		t.Fatalf("read failure status/message = %s/%q, want fixed Unavailable message", status.Code(err), status.Convert(err).Message())
	}
	reader.err = repository.ErrAdminEventTicketOperationsCapacityExceeded
	response, err := server.GetAdminEventTicketOperations(adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), &bookingv1.GetAdminEventTicketOperationsRequest{EventId: eventID.String()})
	if status.Code(err) != codes.ResourceExhausted || response != nil || status.Convert(err).Message() != "Admin event ticket operations source capacity exceeded" {
		t.Fatalf("capacity response/status/message = %+v/%s/%q", response, status.Code(err), status.Convert(err).Message())
	}
}
