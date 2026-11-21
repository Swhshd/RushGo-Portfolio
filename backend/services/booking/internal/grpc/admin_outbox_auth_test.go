package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type adminOutboxRPCProjectionReader struct {
	rows  []repository.AdminOutboxProjection
	err   error
	calls int
}

func (r *adminOutboxRPCProjectionReader) ReadAdminOutboxProjection(context.Context) ([]repository.AdminOutboxProjection, error) {
	r.calls++
	return append([]repository.AdminOutboxProjection(nil), r.rows...), r.err
}

func adminOutboxRPCContext(identity string, verified bool, authorization ...string) context.Context {
	ctx := context.Background()
	if identity != "" {
		uri, _ := url.Parse(identity)
		cert := &x509.Certificate{URIs: []*url.URL{uri}}
		state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		if verified {
			state.VerifiedChains = [][]*x509.Certificate{{cert}}
		}
		ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	}
	if len(authorization) != 0 {
		pairs := make([]string, 0, len(authorization)*2)
		for _, value := range authorization {
			pairs = append(pairs, "authorization", value)
		}
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(pairs...))
	}
	return ctx
}

func adminOutboxRPCRequest() *bookingv1.ListAdminOutboxEventsRequest {
	return &bookingv1.ListAdminOutboxEventsRequest{
		Source: "booking", Status: "all", From: "2026-10-30T00:00:00Z", To: "2026-10-31T23:59:59.999999999Z", Limit: 20,
	}
}

func configureAdminOutboxAuthenticators(server *BookingServer, users userv1.UserServiceClient, revocations TicketTokenRevocationChecker) {
	ticketAuthenticator := NewTicketPrincipalAuthenticator(users, revocations)
	server.SetTicketPrincipalAuthenticator(ticketAuthenticator)
	server.SetAdminOutboxPrincipalAuthenticator(NewAdminOutboxPrincipalAuthenticator(ticketAuthenticator, users))
}

func TestListAdminOutboxEventsRequiresSecureWiringBeforeTokenValidation(t *testing.T) {
	reader := &adminOutboxRPCProjectionReader{}
	users := &fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}}
	revocations := &fakeTicketRevocationChecker{}
	server := NewBookingServer(nil, zap.NewNop())
	server.SetAdminOutboxService(service.NewAdminOutboxService(reader))
	server.SetAdminOutboxSecure(false)
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer must-not-reach-user")
	configureAdminOutboxAuthenticators(server, users, revocations)
	if _, err := server.ListAdminOutboxEvents(ctx, adminOutboxRPCRequest()); status.Code(err) != codes.Unavailable {
		t.Fatalf("insecure wiring status = %s, want Unavailable (err=%v)", status.Code(err), err)
	}
	if users.calls != 0 || revocations.calls != 0 || reader.calls != 0 {
		t.Fatalf("insecure wiring reached dependencies: user=%d revocation=%d reader=%d", users.calls, revocations.calls, reader.calls)
	}
}

func TestListAdminOutboxEventsRequiresDedicatedLiveRoleAuthenticator(t *testing.T) {
	reader := &adminOutboxRPCProjectionReader{}
	users := &fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}}
	revocations := &fakeTicketRevocationChecker{}
	server := NewBookingServer(nil, zap.NewNop())
	server.SetAdminOutboxService(service.NewAdminOutboxService(reader))
	server.SetAdminOutboxSecure(true)
	server.SetTicketPrincipalAuthenticator(NewTicketPrincipalAuthenticator(users, revocations))
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer valid-admin")
	if _, err := server.ListAdminOutboxEvents(ctx, adminOutboxRPCRequest()); status.Code(err) != codes.Unavailable {
		t.Fatalf("missing dedicated Outbox auth status = %s, want Unavailable (err=%v)", status.Code(err), err)
	}
	if users.calls != 0 || users.currentRoleCalls != 0 || revocations.calls != 0 || reader.calls != 0 {
		t.Fatalf("missing current-role auth reached dependencies: validate=%d current-role=%d revocation=%d reader=%d", users.calls, users.currentRoleCalls, revocations.calls, reader.calls)
	}
}

func TestListAdminOutboxEventsRequiresVerifiedGatewayAndCurrentAdmin(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name            string
		ctx             context.Context
		validation      *userv1.ValidateTokenResponse
		currentRole     *userv1.ValidateTokenResponse
		currentRoleErr  error
		currentRoleSet  bool
		revoked         bool
		wantCode        codes.Code
		wantUserCalls   int
		wantRevokeCalls int
		wantRoleCalls   int
		wantReadCalls   int
	}{
		{name: "current admin without role verification attestation", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin-unattested"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, wantCode: codes.Unavailable, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
		{name: "missing TLS peer", ctx: adminOutboxRPCContext("", false, "Bearer valid-token"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, wantCode: codes.Unauthenticated},
		{name: "unverified Gateway certificate", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, false, "Bearer valid-token"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, wantCode: codes.Unauthenticated},
		{name: "verified non-Gateway peer", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentitySaga, true, "Bearer valid-token"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, wantCode: codes.PermissionDenied},
		{name: "missing bearer", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, wantCode: codes.Unauthenticated},
		{name: "revoked token", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer revoked"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, revoked: true, wantCode: codes.Unauthenticated, wantRevokeCalls: 1},
		{name: "invalid token", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer expired"), validation: &userv1.ValidateTokenResponse{Valid: false}, wantCode: codes.Unauthenticated, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "ordinary user", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer user"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}, wantCode: codes.PermissionDenied, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "scanner role", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer scanner"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "scanner"}, wantCode: codes.PermissionDenied, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "forged role metadata", ctx: adminOutboxForgedRoleContext(), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}, wantCode: codes.PermissionDenied, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "verified admin with current role", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true}, wantCode: codes.OK, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1, wantReadCalls: 1},
		{name: "admin demoted since token issue", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer demoted-admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user", CurrentRoleVerified: true}, wantCode: codes.PermissionDenied, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
		{name: "current account missing", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer deleted-admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRoleErr: status.Error(codes.NotFound, "private deleted account detail"), wantCode: codes.PermissionDenied, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
		{name: "current identity mismatch", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer mismatched-admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthOtherID, Role: "admin", CurrentRoleVerified: true}, wantCode: codes.PermissionDenied, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
		{name: "current-role service unavailable", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer unavailable-admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRoleErr: status.Error(codes.Unavailable, "private current-role service detail"), wantCode: codes.Unavailable, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
		{name: "malformed current-role response", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer malformed-current-role-admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}, currentRoleSet: true, wantCode: codes.Unavailable, wantUserCalls: 2, wantRevokeCalls: 1, wantRoleCalls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminOutboxRPCProjectionReader{rows: []repository.AdminOutboxProjection{{
				ID: uuid.MustParse("80000000-0000-4000-8000-000000000001"), EventType: "InternalPrivateType", Published: false, CreatedAt: now,
			}}}
			users := &fakeTicketTokenClient{response: tc.validation, currentRole: tc.currentRole, currentRoleErr: tc.currentRoleErr, currentRoleSet: tc.currentRoleSet}
			revocations := &fakeTicketRevocationChecker{revoked: tc.revoked}
			server := NewBookingServer(nil, zap.NewNop())
			server.SetAdminOutboxService(service.NewAdminOutboxService(reader))
			server.SetAdminOutboxSecure(true)
			configureAdminOutboxAuthenticators(server, users, revocations)
			response, err := server.ListAdminOutboxEvents(tc.ctx, adminOutboxRPCRequest())
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ListAdminOutboxEvents status = %s, want %s (err=%v)", got, tc.wantCode, err)
			}
			if users.calls != tc.wantUserCalls || revocations.calls != tc.wantRevokeCalls || users.currentRoleCalls != tc.wantRoleCalls || reader.calls != tc.wantReadCalls {
				t.Fatalf("dependency calls validate/revocation/current-role/read = %d/%d/%d/%d, want %d/%d/%d/%d", users.calls, revocations.calls, users.currentRoleCalls, reader.calls, tc.wantUserCalls, tc.wantRevokeCalls, tc.wantRoleCalls, tc.wantReadCalls)
			}
			if tc.wantCode == codes.OK {
				if response == nil || len(response.GetRecords()) != 1 {
					t.Fatalf("authorized response = %+v", response)
				}
				if response.GetRecords()[0].GetEventType() != "Other" || response.GetRecords()[0].GetRecordedStatus() != "pending" || response.GetRecords()[0].GetConsumerOutcome() != "not_verified" {
					t.Fatalf("owner response reported unsupported facts: %+v", response.GetRecords()[0])
				}
			}
		})
	}
}

func adminOutboxForgedRoleContext() context.Context {
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer user")
	md, _ := metadata.FromIncomingContext(ctx)
	md.Append("role", "admin")
	return metadata.NewIncomingContext(ctx, md)
}

func TestListAdminOutboxEventsRejectsInvalidRequestAndDoesNotRead(t *testing.T) {
	reader := &adminOutboxRPCProjectionReader{}
	users := &fakeTicketTokenClient{
		response:    &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"},
		currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true},
	}
	revocations := &fakeTicketRevocationChecker{}
	server := NewBookingServer(nil, zap.NewNop())
	server.SetAdminOutboxService(service.NewAdminOutboxService(reader))
	server.SetAdminOutboxSecure(true)
	configureAdminOutboxAuthenticators(server, users, revocations)
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer valid-admin")
	request := adminOutboxRPCRequest()
	request.From = "2026-10-20T00:00:00Z"
	if _, err := server.ListAdminOutboxEvents(ctx, request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid time range status = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	if reader.calls != 0 {
		t.Fatalf("invalid request reached projection reader %d times", reader.calls)
	}
}

func TestListAdminOutboxEventsMapsCapacityAndReadFailuresToSafeCodes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     []repository.AdminOutboxProjection
		err      error
		wantCode codes.Code
	}{
		{name: "global cap", rows: make([]repository.AdminOutboxProjection, service.AdminOutboxRecordLimit+1), wantCode: codes.ResourceExhausted},
		{name: "reader failure", err: status.Error(codes.Internal, "database secret"), wantCode: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminOutboxRPCProjectionReader{rows: tc.rows, err: tc.err}
			users := &fakeTicketTokenClient{
				response:    &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"},
				currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin", CurrentRoleVerified: true},
			}
			server := NewBookingServer(nil, zap.NewNop())
			server.SetAdminOutboxService(service.NewAdminOutboxService(reader))
			server.SetAdminOutboxSecure(true)
			configureAdminOutboxAuthenticators(server, users, &fakeTicketRevocationChecker{})
			_, err := server.ListAdminOutboxEvents(adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer valid-admin"), adminOutboxRPCRequest())
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("RPC status = %s, want %s (err=%v)", got, tc.wantCode, err)
			}
			if err != nil && status.Convert(err).Message() == "database secret" {
				t.Fatal("owner error detail was returned to the caller")
			}
		})
	}
}
