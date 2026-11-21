package grpc

import (
	"context"
	"github.com/google/uuid"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"testing"
)

func TestAccountMutationsBindCallerAndCredentialVersion(t *testing.T) {
	manager := service.NewJWTManager("unit-test-account-signing-secret", 15)
	id := uuid.New()
	users := &currentRoleUserRepositoryStub{user: &domain.User{ID: id, Role: "user", CredentialVersion: 1}}
	server := NewUserServer(service.NewAuthService(users, nil, manager, 7, zap.NewNop(), service.AuthDependencies{}), nil, zap.NewNop())
	for _, tc := range []struct {
		name, owner string
		epoch       int64
		want        codes.Code
	}{{"wrong owner", uuid.NewString(), 1, codes.Unauthenticated}, {"old session", id.String(), 0, codes.Unauthenticated}, {"current owner reaches validation", id.String(), 1, codes.InvalidArgument}} {
		t.Run(tc.name, func(t *testing.T) {
			token, err := manager.GenerateAccessToken(id, "buyer@rushgo.test", "user", tc.epoch)
			if err != nil {
				t.Fatal(err)
			}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
			_, err = server.ChangePassword(ctx, &userv1.ChangePasswordRequest{UserId: tc.owner})
			if status.Code(err) != tc.want {
				t.Fatalf("status %v", err)
			}
		})
	}
}
