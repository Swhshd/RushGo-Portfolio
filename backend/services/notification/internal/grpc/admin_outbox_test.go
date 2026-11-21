package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type adminOutboxReaderStub struct {
	rows  []repository.AdminOutboxProjection
	err   error
	calls int
}

func (r *adminOutboxReaderStub) ReadAdminOutboxProjection(context.Context) ([]repository.AdminOutboxProjection, error) {
	r.calls++
	return append([]repository.AdminOutboxProjection(nil), r.rows...), r.err
}

type adminOutboxUserClientStub struct {
	userv1.UserServiceClient
	initial    *userv1.ValidateTokenResponse
	current    *userv1.ValidateTokenResponse
	errInitial error
	errCurrent error
	requests   []*userv1.ValidateTokenRequest
}

func (c *adminOutboxUserClientStub) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	c.requests = append(c.requests, req)
	if req.GetRequireCurrentRole() {
		return c.current, c.errCurrent
	}
	return c.initial, c.errInitial
}

type adminOutboxRevocationStub struct {
	revoked bool
	err     error
	calls   int
}

func (r *adminOutboxRevocationStub) IsRevoked(context.Context, string) (bool, error) {
	r.calls++
	return r.revoked, r.err
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
	if len(authorization) > 0 {
		pairs := append([]string{"authorization"}, authorization...)
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(pairs...))
	}
	return ctx
}

func newAdminOutboxServerForTest(users userv1.UserServiceClient, revocations TokenRevocationChecker, secure bool) (*AdminOutboxServer, *adminOutboxReaderStub) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := &adminOutboxReaderStub{rows: []repository.AdminOutboxProjection{{
		ID: uuid.MustParse("90000000-0000-4000-8000-000000000001"), EventType: "booking_confirmation", Status: "SENT", Managed: true, CreatedAt: now.Add(-time.Minute),
	}}}
	outbox := service.NewAdminOutboxService(reader)
	// Keep this helper's response window deterministic without exposing internal
	// timing or identity from the owner service.
	return NewAdminOutboxServer(outbox, users, revocations, secure), reader
}

func adminOutboxRequest() *bookingv1.ListAdminOutboxEventsRequest {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	return &bookingv1.ListAdminOutboxEventsRequest{
		Source: "notification", Status: "all", From: now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		To: now.Format(time.RFC3339Nano), Limit: 20,
	}
}

func adminOutboxAdminUserClient(initialRole, currentRole, initialID, currentID string) *adminOutboxUserClientStub {
	return &adminOutboxUserClientStub{
		initial: &userv1.ValidateTokenResponse{Valid: true, UserId: initialID, Role: initialRole},
		current: &userv1.ValidateTokenResponse{Valid: true, UserId: currentID, Role: currentRole, CurrentRoleVerified: true},
	}
}

func TestNotificationAdminOutboxRequiresVerifiedGatewayAndCurrentAdmin(t *testing.T) {
	const userID = "90000000-0000-4000-8000-000000000009"
	cases := []struct {
		name           string
		identity       string
		verified       bool
		authorization  []string
		initial        *userv1.ValidateTokenResponse
		current        *userv1.ValidateTokenResponse
		revoked        bool
		revocationErr  error
		secure         bool
		wantCode       codes.Code
		wantUserCalls  int
		wantRevokeCall int
		wantReadCalls  int
	}{
		{name: "current admin without role verification attestation", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, secure: true, wantCode: codes.Unavailable, wantUserCalls: 2, wantRevokeCall: 1},
		{name: "verified gateway with current admin", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin", CurrentRoleVerified: true}, secure: true, wantCode: codes.OK, wantUserCalls: 2, wantRevokeCall: 1, wantReadCalls: 1},
		{name: "unverified certificate", identity: grpcsecurity.ServiceIdentityGateway, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, secure: true, wantCode: codes.Unauthenticated},
		{name: "wrong service identity", identity: "spiffe://rushgo.internal/booking", verified: true, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, secure: true, wantCode: codes.PermissionDenied},
		{name: "missing bearer", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, secure: true, wantCode: codes.Unauthenticated},
		{name: "revoked bearer", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, revoked: true, secure: true, wantCode: codes.Unauthenticated, wantRevokeCall: 1},
		{name: "revocation store unavailable", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, revocationErr: errors.New("redis unavailable"), secure: true, wantCode: codes.Unavailable, wantRevokeCall: 1},
		{name: "signed token is not admin", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-user"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "user"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "user"}, secure: true, wantCode: codes.PermissionDenied, wantUserCalls: 1, wantRevokeCall: 1},
		{name: "signed token invalid", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer invalid"}, initial: &userv1.ValidateTokenResponse{Valid: false}, secure: true, wantCode: codes.Unauthenticated, wantUserCalls: 1, wantRevokeCall: 1},
		{name: "current role demoted", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "user", CurrentRoleVerified: true}, secure: true, wantCode: codes.PermissionDenied, wantUserCalls: 2, wantRevokeCall: 1},
		{name: "current identity mismatch", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, current: &userv1.ValidateTokenResponse{Valid: true, UserId: "90000000-0000-4000-8000-000000000010", Role: "admin", CurrentRoleVerified: true}, secure: true, wantCode: codes.PermissionDenied, wantUserCalls: 2, wantRevokeCall: 1},
		{name: "insecure feature wiring", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: []string{"Bearer signed-admin"}, secure: false, wantCode: codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			users := &adminOutboxUserClientStub{initial: tc.initial, current: tc.current}
			revocations := &adminOutboxRevocationStub{revoked: tc.revoked, err: tc.revocationErr}
			server, reader := newAdminOutboxServerForTest(users, revocations, tc.secure)
			response, err := server.ListAdminOutboxEvents(adminOutboxRPCContext(tc.identity, tc.verified, tc.authorization...), adminOutboxRequest())
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("RPC status = %s, want %s (response=%+v, err=%v)", got, tc.wantCode, response, err)
			}
			if len(users.requests) != tc.wantUserCalls {
				t.Fatalf("ValidateToken calls = %d, want %d", len(users.requests), tc.wantUserCalls)
			}
			if revocations.calls != tc.wantRevokeCall {
				t.Fatalf("revocation calls = %d, want %d", revocations.calls, tc.wantRevokeCall)
			}
			if reader.calls != tc.wantReadCalls {
				t.Fatalf("projection reader calls = %d, want %d", reader.calls, tc.wantReadCalls)
			}
			for _, request := range users.requests {
				if request.GetAccessToken() != "signed-admin" && request.GetAccessToken() != "signed-user" && request.GetAccessToken() != "invalid" {
					t.Fatalf("unexpected token reached User: %q", request.GetAccessToken())
				}
			}
			if tc.wantCode == codes.OK {
				if response == nil || len(response.GetRecords()) != 1 || response.GetRecords()[0].GetRecordedStatus() != "sent" || response.GetRecords()[0].GetConsumerOutcome() != "not_verified" {
					t.Fatalf("safe response = %+v", response)
				}
				if !response.GetObservedAt().IsValid() || !response.GetFrom().IsValid() || !response.GetTo().IsValid() {
					t.Fatalf("response timestamps invalid: %+v", response)
				}
			}
		})
	}
}
