package grpc

import (
	"context"
	"strings"

	"github.com/google/uuid"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const maxTicketBearerTokenBytes = 8192

type TicketPrincipal struct {
	UserID uuid.UUID
	Role   string
}

type TicketPrincipalAuthenticator interface {
	Authenticate(context.Context) (TicketPrincipal, error)
}

type TicketTokenRevocationChecker interface {
	IsRevoked(context.Context, string) (bool, error)
}

type ticketPrincipalAuthenticator struct {
	users       userv1.UserServiceClient
	revocations TicketTokenRevocationChecker
}

type adminOutboxPrincipalAuthenticator struct {
	ticketAuthenticator TicketPrincipalAuthenticator
	users               userv1.UserServiceClient
}

func NewTicketPrincipalAuthenticator(users userv1.UserServiceClient, revocations TicketTokenRevocationChecker) TicketPrincipalAuthenticator {
	return &ticketPrincipalAuthenticator{users: users, revocations: revocations}
}

// NewAdminOutboxPrincipalAuthenticator adds a current-role check to the
// existing token and revocation authority for the read-only Admin outbox RPC.
// Ticket routes continue to use NewTicketPrincipalAuthenticator directly.
func NewAdminOutboxPrincipalAuthenticator(ticketAuthenticator TicketPrincipalAuthenticator, users userv1.UserServiceClient) TicketPrincipalAuthenticator {
	return &adminOutboxPrincipalAuthenticator{ticketAuthenticator: ticketAuthenticator, users: users}
}

func (a *ticketPrincipalAuthenticator) Authenticate(ctx context.Context) (TicketPrincipal, error) {
	if a == nil || a.users == nil || a.revocations == nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "ticket authentication unavailable")
	}
	token, err := ticketBearerTokenFromContext(ctx)
	if err != nil {
		return TicketPrincipal{}, err
	}
	revoked, err := a.revocations.IsRevoked(ctx, token)
	if err != nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "ticket authentication unavailable")
	}
	if revoked {
		return TicketPrincipal{}, status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	validated, err := a.users.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token})
	if err != nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "ticket authentication unavailable")
	}
	if validated == nil || !validated.GetValid() {
		return TicketPrincipal{}, status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	userID, err := uuid.Parse(validated.GetUserId())
	if err != nil || userID == uuid.Nil || strings.TrimSpace(validated.GetRole()) == "" {
		return TicketPrincipal{}, status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	return TicketPrincipal{UserID: userID, Role: validated.GetRole()}, nil
}

func (a *adminOutboxPrincipalAuthenticator) Authenticate(ctx context.Context) (TicketPrincipal, error) {
	if a == nil || a.ticketAuthenticator == nil || a.users == nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
	}
	principal, err := a.ticketAuthenticator.Authenticate(ctx)
	if err != nil {
		return TicketPrincipal{}, err
	}
	if principal.UserID == uuid.Nil || principal.Role != "admin" {
		return TicketPrincipal{}, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
	}
	token, err := ticketBearerTokenFromContext(ctx)
	if err != nil {
		return TicketPrincipal{}, err
	}
	current, err := a.users.ValidateToken(ctx, &userv1.ValidateTokenRequest{
		AccessToken: token, RequireCurrentRole: true,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return TicketPrincipal{}, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
		case codes.Unauthenticated:
			return TicketPrincipal{}, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
		default:
			return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
		}
	}
	if current == nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
	}
	if !current.GetValid() {
		return TicketPrincipal{}, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
	}
	if !current.GetCurrentRoleVerified() {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
	}
	currentUserID, err := uuid.Parse(current.GetUserId())
	if err != nil || currentUserID == uuid.Nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
	}
	if currentUserID != principal.UserID || current.GetRole() != "admin" {
		return TicketPrincipal{}, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
	}
	return TicketPrincipal{UserID: currentUserID, Role: current.GetRole()}, nil
}

func ticketBearerTokenFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	authorization := md.Get("authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") {
		return "", status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	token := strings.TrimPrefix(authorization[0], "Bearer ")
	if token == "" || len(token) > maxTicketBearerTokenBytes || token != strings.TrimSpace(token) || strings.ContainsAny(token, " \t\r\n") {
		return "", status.Error(codes.Unauthenticated, "ticket authentication required")
	}
	return token, nil
}
