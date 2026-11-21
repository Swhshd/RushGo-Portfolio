package grpc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"github.com/ticketbox/user/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestProtoAuthContracts(t *testing.T) {
	file := userv1.File_proto_user_v1_user_proto
	svc := file.Services().ByName("UserService")
	for _, name := range []protoreflect.Name{"RequestRegistrationCode", "RequestPasswordReset", "ResetPassword"} {
		if svc.Methods().ByName(name) == nil {
			t.Fatal("required recovery RPC absent")
		}
	}
	if svc.Methods().ByName("Register").Output().Name() != "RegistrationResponse" {
		t.Fatal("registration still returns session")
	}
	expected := map[protoreflect.Name]map[protoreflect.Name]protoreflect.FieldNumber{
		"RegisterRequest": {"email": 1, "password": 2, "name": 3, "verification_id": 4, "verification_code": 5}, "RegistrationResponse": {"account_created": 1, "message": 2},
		"RegistrationCodeRequest": {"email": 1}, "RegistrationCodeResponse": {"verification_id": 1, "expires_at": 2, "next_resend_at": 3, "message": 4}, "PasswordResetRequest": {"email": 1}, "PasswordResetResponse": {"message": 1}, "ResetPasswordRequest": {"token": 1, "new_password": 2}, "ResetPasswordResponse": {"message": 1}, "LogoutRequest": {"access_token": 1, "refresh_token": 2},
		"ValidateTokenRequest":  {"access_token": 1, "require_current_role": 2},
		"ValidateTokenResponse": {"valid": 1, "user_id": 2, "email": 3, "role": 4, "current_role_verified": 5},
	}
	for name, fields := range expected {
		m := file.Messages().ByName(name)
		if m == nil {
			t.Fatal("message absent")
		}
		if m.Fields().Len() != len(fields) {
			t.Fatal("unexpected field exposure")
		}
		for field, number := range fields {
			f := m.Fields().ByName(field)
			if f == nil || f.Number() != number {
				t.Fatal("wire field changed")
			}
		}
	}
}
func TestRegisterContractRefusesUnverified(t *testing.T) {
	s := NewUserServer(nil, nil, zap.NewNop())
	_, err := s.Register(context.Background(), &userv1.RegisterRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("unverified contract not refused")
	}
}

func TestValidateTokenKeepsJWTClaimsAuthoritative(t *testing.T) {
	manager := service.NewJWTManager("unit-test-user-jwt-secret", 15)
	userID := uuid.New()
	validToken, err := manager.GenerateAccessToken(userID, "buyer@rushgo.test", "user")
	if err != nil {
		t.Fatal(err)
	}
	server := NewUserServer(service.NewAuthService(&currentRoleUserRepositoryStub{user: &domain.User{ID: userID, Role: "user"}}, nil, manager, 7, zap.NewNop(), service.AuthDependencies{}), nil, zap.NewNop())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("role", "admin", "user_id", uuid.NewString()))
	response, err := server.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: validToken})
	if err != nil || !response.GetValid() || response.GetUserId() != userID.String() || response.GetRole() != "user" || response.GetCurrentRoleVerified() {
		t.Fatalf("signed User JWT response = (%+v, %v), want its signed identity and role", response, err)
	}

	wrongSigner := service.NewJWTManager("different-unit-test-secret", 15)
	badSignature, err := wrongSigner.GenerateAccessToken(uuid.New(), "attacker@rushgo.test", "admin")
	if err != nil {
		t.Fatal(err)
	}
	expiredManager := service.NewJWTManager("unit-test-user-jwt-secret", -1)
	expiredToken, err := expiredManager.GenerateAccessToken(uuid.New(), "buyer@rushgo.test", "user")
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []struct {
		name  string
		value string
	}{{"empty", ""}, {"malformed", "Bearer broken"}, {"wrong signature", badSignature}, {"expired", expiredToken}} {
		t.Run(token.name, func(t *testing.T) {
			response, err := server.ValidateToken(context.Background(), &userv1.ValidateTokenRequest{AccessToken: token.value})
			if err != nil || response.GetValid() {
				t.Fatalf("invalid token response = (%+v, %v), want valid=false", response, err)
			}
		})
	}
}

func TestValidateTokenCurrentRoleFlagDoesNotTrustExternalIdentity(t *testing.T) {
	manager := service.NewJWTManager("current-role-user-jwt-secret", 15)
	userID := uuid.New()
	validToken, err := manager.GenerateAccessToken(userID, "admin@rushgo.test", "admin")
	if err != nil {
		t.Fatal(err)
	}
	users := &currentRoleUserRepositoryStub{user: &domain.User{ID: userID, Email: "admin@rushgo.test", Role: "user"}}
	server := NewUserServer(service.NewAuthService(users, nil, manager, 7, zap.NewNop(), service.AuthDependencies{}), nil, zap.NewNop())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("user_id", uuid.NewString(), "role", "admin"))

	legacy, err := server.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: validToken})
	if err != nil || !legacy.GetValid() || legacy.GetUserId() != userID.String() || legacy.GetRole() != "admin" || legacy.GetCurrentRoleVerified() {
		t.Fatalf("legacy ValidateToken response = (%+v, %v), want signed claims unchanged", legacy, err)
	}
	if users.calls != 1 {
		t.Fatalf("legacy ValidateToken read the current profile %d times", users.calls)
	}

	current, err := server.ValidateToken(ctx, currentRoleValidateTokenRequest(validToken))
	if err != nil || !current.GetValid() || current.GetUserId() != userID.String() || current.GetEmail() != "admin@rushgo.test" || current.GetRole() != "user" || !current.GetCurrentRoleVerified() {
		t.Fatalf("current-role ValidateToken response = (%+v, %v), want signed identity and current role", current, err)
	}
	if users.calls != 2 || users.userID != userID {
		t.Fatalf("current-role profile lookup calls/id = %d/%s, want 2/%s", users.calls, users.userID, userID)
	}
}

func TestValidateTokenCurrentRoleUnavailableAndMissingAccountFailClosed(t *testing.T) {
	manager := service.NewJWTManager("current-role-failure-user-jwt-secret", 15)
	userID := uuid.New()
	validToken, err := manager.GenerateAccessToken(userID, "admin@rushgo.test", "admin")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		repositoryErr    error
		wantCode         codes.Code
		identityMismatch bool
	}{
		{name: "missing account", repositoryErr: repository.ErrNotFound, wantCode: codes.NotFound},
		{name: "database unavailable", repositoryErr: status.Error(codes.Internal, "private database error"), wantCode: codes.Unavailable},
		{name: "missing repository response", wantCode: codes.Unavailable},
		{name: "repository identity mismatch", wantCode: codes.Unavailable, identityMismatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			users := &currentRoleUserRepositoryStub{err: tc.repositoryErr}
			if tc.identityMismatch {
				users.user = &domain.User{ID: uuid.New(), Role: "admin"}
			}
			server := NewUserServer(service.NewAuthService(users, nil, manager, 7, zap.NewNop(), service.AuthDependencies{}), nil, zap.NewNop())
			response, err := server.ValidateToken(context.Background(), currentRoleValidateTokenRequest(validToken))
			if status.Code(err) != tc.wantCode {
				t.Fatalf("current-role error code = %s, want %s (response=%+v err=%v)", status.Code(err), tc.wantCode, response, err)
			}
			if response != nil {
				t.Fatalf("current-role failure response = %+v, want no claim-bearing response", response)
			}
			if users.calls != 1 || users.userID != userID {
				t.Fatalf("current-role lookup calls/id = %d/%s, want 1/%s", users.calls, users.userID, userID)
			}
		})
	}
}

func currentRoleValidateTokenRequest(token string) *userv1.ValidateTokenRequest {
	request := &userv1.ValidateTokenRequest{AccessToken: token}
	field := request.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("require_current_role"))
	if field != nil {
		request.ProtoReflect().Set(field, protoreflect.ValueOfBool(true))
	}
	return request
}

type currentRoleUserRepositoryStub struct {
	repository.UserRepository
	user   *domain.User
	err    error
	calls  int
	userID uuid.UUID
}

func (r *currentRoleUserRepositoryStub) GetByID(_ context.Context, userID uuid.UUID) (*domain.User, error) {
	r.calls++
	r.userID = userID
	return r.user, r.err
}

func TestValidateTokenCredentialVersionRevokesExistingJWT(t *testing.T) {
	manager := service.NewJWTManager("epoch-test-signing-secret", 15)
	id := uuid.New()
	users := &currentRoleUserRepositoryStub{user: &domain.User{ID: id, Role: "user", CredentialVersion: 1}}
	server := NewUserServer(service.NewAuthService(users, nil, manager, 7, zap.NewNop(), service.AuthDependencies{}), nil, zap.NewNop())
	for _, epoch := range []int64{0, 1} {
		token, err := manager.GenerateAccessToken(id, "test@rushgo.test", "user", epoch)
		if err != nil {
			t.Fatal(err)
		}
		result, err := server.ValidateToken(context.Background(), &userv1.ValidateTokenRequest{AccessToken: token})
		if err != nil || result.GetValid() != (epoch == 1) {
			t.Fatalf("epoch %d result=%v err=%v", epoch, result, err)
		}
	}
}
