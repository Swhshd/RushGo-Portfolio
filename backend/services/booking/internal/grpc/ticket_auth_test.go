package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	ticketAuthUserID  = "11111111-1111-4111-8111-111111111111"
	ticketAuthOtherID = "22222222-2222-4222-8222-222222222222"
	ticketAuthEventID = "33333333-3333-4333-8333-333333333333"
)

type fakeTicketTokenClient struct {
	userv1.UserServiceClient
	response           *userv1.ValidateTokenResponse
	err                error
	currentRole        *userv1.ValidateTokenResponse
	currentRoleErr     error
	currentRoleSet     bool
	calls              int
	currentRoleCalls   int
	currentRoleFlagged int
	token              string
}

func (f *fakeTicketTokenClient) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	f.calls++
	f.token = req.GetAccessToken()
	if ticketAuthRequestRequiresCurrentRole(req) {
		f.currentRoleCalls++
		f.currentRoleFlagged++
		if f.currentRoleErr != nil {
			return nil, f.currentRoleErr
		}
		if f.currentRoleSet {
			return f.currentRole, nil
		}
		if f.currentRole != nil {
			return f.currentRole, nil
		}
	}
	return f.response, f.err
}

func ticketAuthRequestRequiresCurrentRole(req *userv1.ValidateTokenRequest) bool {
	if req == nil {
		return false
	}
	field := req.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("require_current_role"))
	return field != nil && req.ProtoReflect().Get(field).Bool()
}

type fakeTicketRevocationChecker struct {
	revoked bool
	err     error
	calls   int
	token   string
}

func (f *fakeTicketRevocationChecker) IsRevoked(_ context.Context, token string) (bool, error) {
	f.calls++
	f.token = token
	return f.revoked, f.err
}

type staticTicketPrincipalAuthenticator struct {
	principal TicketPrincipal
	err       error
}

func (a staticTicketPrincipalAuthenticator) Authenticate(context.Context) (TicketPrincipal, error) {
	return a.principal, a.err
}

type ownedTicketListRepository struct {
	repository.TicketRepository
	owner uuid.UUID
	calls int
}

func (r *ownedTicketListRepository) ListOwnedTickets(_ context.Context, userID uuid.UUID) ([]domain.Ticket, error) {
	r.calls++
	r.owner = userID
	return nil, nil
}

func TestTicketPrincipalAuthenticatorRequiresSingleBearerMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		md   metadata.MD
	}{
		{name: "missing"},
		{name: "wrong scheme", md: metadata.Pairs("authorization", "Basic token")},
		{name: "empty token", md: metadata.Pairs("authorization", "Bearer ")},
		{name: "duplicate", md: metadata.Pairs("authorization", "Bearer one", "authorization", "Bearer two")},
		{name: "whitespace in token", md: metadata.Pairs("authorization", "Bearer token with-space")},
	} {
		t.Run(test.name, func(t *testing.T) {
			users := &fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}}
			revocations := &fakeTicketRevocationChecker{}
			auth := NewTicketPrincipalAuthenticator(users, revocations)
			ctx := metadata.NewIncomingContext(context.Background(), test.md)
			if _, err := auth.Authenticate(ctx); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("malformed bearer status = %s, want %s (err=%v)", status.Code(err), codes.Unauthenticated, err)
			}
			if users.calls != 0 || revocations.calls != 0 {
				t.Fatalf("malformed metadata reached dependencies: user calls=%d revocation calls=%d", users.calls, revocations.calls)
			}
		})
	}
}

func TestTicketPrincipalAuthenticatorUsesBlacklistAndUserServiceAuthority(t *testing.T) {
	users := &fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "admin"}}
	revocations := &fakeTicketRevocationChecker{}
	auth := NewTicketPrincipalAuthenticator(users, revocations)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer verified-token"))
	principal, err := auth.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if principal.UserID.String() != ticketAuthUserID || principal.Role != "admin" {
		t.Fatalf("principal = %+v", principal)
	}
	if users.calls != 1 || users.token != "verified-token" || revocations.calls != 1 || revocations.token != "verified-token" {
		t.Fatalf("token verification path mismatch: users=%+v revocations=%+v", users, revocations)
	}
}

func TestTicketPrincipalAuthenticatorFailsClosedOnDependencyAndTokenFailures(t *testing.T) {
	for _, test := range []struct {
		name            string
		userResponse    *userv1.ValidateTokenResponse
		userErr         error
		revoked         bool
		revocationErr   error
		wantCode        codes.Code
		wantUserCalls   int
		wantRevokeCalls int
	}{
		{name: "revoked", revoked: true, userResponse: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}, wantCode: codes.Unauthenticated, wantRevokeCalls: 1},
		{name: "revocation store unavailable", revocationErr: errors.New("synthetic redis failure"), wantCode: codes.Unavailable, wantRevokeCalls: 1},
		{name: "user service unavailable", userErr: errors.New("synthetic user service failure"), wantCode: codes.Unavailable, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "invalid token", userResponse: &userv1.ValidateTokenResponse{Valid: false}, wantCode: codes.Unauthenticated, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "invalid subject", userResponse: &userv1.ValidateTokenResponse{Valid: true, UserId: "not-a-uuid", Role: "user"}, wantCode: codes.Unauthenticated, wantUserCalls: 1, wantRevokeCalls: 1},
		{name: "missing role", userResponse: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID}, wantCode: codes.Unauthenticated, wantUserCalls: 1, wantRevokeCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			users := &fakeTicketTokenClient{response: test.userResponse, err: test.userErr}
			revocations := &fakeTicketRevocationChecker{revoked: test.revoked, err: test.revocationErr}
			auth := NewTicketPrincipalAuthenticator(users, revocations)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer check-token"))
			if _, err := auth.Authenticate(ctx); status.Code(err) != test.wantCode {
				t.Fatalf("auth status = %s, want %s (err=%v)", status.Code(err), test.wantCode, err)
			}
			if users.calls != test.wantUserCalls || revocations.calls != test.wantRevokeCalls {
				t.Fatalf("dependency calls users=%d revocations=%d, want %d/%d", users.calls, revocations.calls, test.wantUserCalls, test.wantRevokeCalls)
			}
		})
	}
}

func TestTicketMutationRPCsBindCallerIdentityAndRequireAdminAssignment(t *testing.T) {
	adminID, _ := uuid.Parse(ticketAuthUserID)
	server := NewBookingServer(nil, zap.NewNop())
	server.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: adminID, Role: "admin"}})
	server.SetAdminOutboxPrincipalAuthenticator(server.ticketAuthenticator)

	if _, err := server.RotateTicketCredential(context.Background(), &bookingv1.RotateTicketCredentialRequest{UserId: ticketAuthOtherID, TicketId: ticketAuthOtherID}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed credential owner status = %s, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if _, err := server.CheckInTicket(context.Background(), &bookingv1.CheckInTicketRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthOtherID, Credential: "opaque", IdempotencyKey: "scan-1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed scanner status = %s, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	if _, err := server.SetEventScannerAssignment(context.Background(), &bookingv1.SetEventScannerAssignmentRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthOtherID, ActorUserId: ticketAuthOtherID, Active: true}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed admin actor status = %s, want PermissionDenied (err=%v)", status.Code(err), err)
	}

	server.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: adminID, Role: "user"}})
	server.SetAdminOutboxPrincipalAuthenticator(server.ticketAuthenticator)
	if _, err := server.SetEventScannerAssignment(context.Background(), &bookingv1.SetEventScannerAssignmentRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthOtherID, ActorUserId: ticketAuthUserID, Active: true}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-admin assignment status = %s, want PermissionDenied (err=%v)", status.Code(err), err)
	}
}

func TestTicketMutationRPCsRejectMissingPrincipal(t *testing.T) {
	server := NewBookingServer(nil, zap.NewNop())
	server.SetTicketPrincipalAuthenticator(NewTicketPrincipalAuthenticator(
		&fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}},
		&fakeTicketRevocationChecker{},
	))
	server.SetAdminOutboxPrincipalAuthenticator(NewAdminOutboxPrincipalAuthenticator(server.ticketAuthenticator, &fakeTicketTokenClient{}))
	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "rotate credential",
			call: func(ctx context.Context) error {
				_, err := server.RotateTicketCredential(ctx, &bookingv1.RotateTicketCredentialRequest{UserId: ticketAuthUserID, TicketId: ticketAuthOtherID})
				return err
			},
		},
		{
			name: "scanner assignment",
			call: func(ctx context.Context) error {
				_, err := server.SetEventScannerAssignment(ctx, &bookingv1.SetEventScannerAssignmentRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthUserID, ActorUserId: ticketAuthOtherID, Active: true})
				return err
			},
		},
		{
			name: "check in",
			call: func(ctx context.Context) error {
				_, err := server.CheckInTicket(ctx, &bookingv1.CheckInTicketRequest{EventId: ticketAuthEventID, StaffUserId: ticketAuthUserID, Credential: "opaque", IdempotencyKey: "scan-1"})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(context.Background()); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("missing principal status = %s, want %s (err=%v)", status.Code(err), codes.Unauthenticated, err)
			}
		})
	}
}

func TestTicketMutationRPCsFailClosedWhenPrincipalVerifierIsUnconfigured(t *testing.T) {
	server := NewBookingServer(nil, zap.NewNop())
	_, err := server.RotateTicketCredential(context.Background(), &bookingv1.RotateTicketCredentialRequest{UserId: ticketAuthUserID, TicketId: ticketAuthOtherID})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unconfigured principal verifier status = %s, want Unavailable (err=%v)", status.Code(err), err)
	}
}

func TestListOwnedTicketsBindsRequestedOwnerToAuthenticatedPrincipal(t *testing.T) {
	principalID, _ := uuid.Parse(ticketAuthUserID)
	tests := []struct {
		name          string
		ctx           context.Context
		principalID   string
		requestUserID string
		wantCode      codes.Code
		wantCalls     int
	}{
		{
			name:          "missing bearer principal",
			ctx:           context.Background(),
			principalID:   ticketAuthUserID,
			requestUserID: ticketAuthUserID,
			wantCode:      codes.Unauthenticated,
		},
		{
			name:          "spoofed requested owner",
			ctx:           metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-token")),
			principalID:   ticketAuthUserID,
			requestUserID: ticketAuthOtherID,
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "authenticated owner",
			ctx:           metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-token")),
			principalID:   ticketAuthUserID,
			requestUserID: ticketAuthUserID,
			wantCalls:     1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &ownedTicketListRepository{}
			server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
			server.SetTicketPrincipalAuthenticator(NewTicketPrincipalAuthenticator(
				&fakeTicketTokenClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: test.principalID, Role: "user"}},
				&fakeTicketRevocationChecker{},
			))
			_, err := server.ListOwnedTickets(test.ctx, &bookingv1.ListOwnedTicketsRequest{UserId: test.requestUserID})
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("ListOwnedTickets status = %s, want %s (err=%v)", got, test.wantCode, err)
			}
			if store.calls != test.wantCalls {
				t.Fatalf("ticket repository calls = %d, want %d", store.calls, test.wantCalls)
			}
			if test.wantCalls != 0 && store.owner != principalID {
				t.Fatalf("repository owner = %s, want authenticated owner %s", store.owner, principalID)
			}
		})
	}
}
