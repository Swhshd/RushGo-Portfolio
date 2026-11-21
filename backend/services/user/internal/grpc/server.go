package grpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"github.com/ticketbox/user/internal/service"
)

type UserServer struct {
	userv1.UnimplementedUserServiceServer
	authService *service.AuthService
	producer    EventProducer
	logger      *zap.Logger
}

type EventProducer interface {
	PublishUserRegistered(ctx context.Context, user *domain.User) error
}

func NewUserServer(authService *service.AuthService, producer EventProducer, logger *zap.Logger) *UserServer {
	return &UserServer{authService: authService, producer: producer, logger: logger}
}

func (s *UserServer) Register(ctx context.Context, req *userv1.RegisterRequest) (*userv1.RegistrationResponse, error) {
	id, err := uuid.Parse(req.VerificationId)
	if err != nil || id == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "VALIDATION_ERROR")
	}
	user, err := s.authService.RegisterVerified(ctx, domain.RegisterInput{Email: req.Email, Password: req.Password, Name: req.Name, VerificationID: id, VerificationCode: req.VerificationCode})
	if err != nil {
		return nil, authStatus(err)
	}
	if s.producer != nil {
		if err = s.producer.PublishUserRegistered(ctx, user); err != nil {
			s.logger.Warn("UserRegistered publication unavailable")
		}
	}
	return &userv1.RegistrationResponse{AccountCreated: true, Message: "Account created. Please sign in."}, nil
}

func (s *UserServer) RequestRegistrationCode(ctx context.Context, req *userv1.RegistrationCodeRequest) (*userv1.RegistrationCodeResponse, error) {
	v, err := s.authService.RequestRegistrationCode(ctx, req.Email)
	if err != nil {
		return nil, authStatus(err)
	}
	return &userv1.RegistrationCodeResponse{VerificationId: v.VerificationID.String(), ExpiresAt: timestamppb.New(v.ExpiresAt), NextResendAt: timestamppb.New(v.NextResendAt), Message: "Verification message accepted for delivery."}, nil
}
func authStatus(err error) error {
	for _, entry := range []struct {
		err  error
		code codes.Code
	}{
		{domain.ErrValidation, codes.InvalidArgument}, {domain.ErrInvalidCode, codes.InvalidArgument}, {domain.ErrCodeExpired, codes.InvalidArgument}, {domain.ErrCodeAttemptsExhausted, codes.InvalidArgument}, {domain.ErrRateLimited, codes.ResourceExhausted}, {domain.ErrDeliveryUnavailable, codes.Unavailable}, {domain.ErrEmailExists, codes.AlreadyExists}, {domain.ErrInvalidResetToken, codes.InvalidArgument}, {domain.ErrResetTokenExpired, codes.InvalidArgument}, {domain.ErrResetTokenUsed, codes.InvalidArgument}, {domain.ErrEmailChangeNotSupported, codes.InvalidArgument}, {domain.ErrInvalidSession, codes.InvalidArgument}, {domain.ErrInvalidCredentials, codes.Unauthenticated}, {domain.ErrTokenExpired, codes.Unauthenticated},
	} {
		if errors.Is(err, entry.err) {
			return status.Error(entry.code, entry.err.Error())
		}
	}
	return status.Error(codes.Unavailable, "TEMPORARY_UNAVAILABLE")
}

func (s *UserServer) RequestPasswordReset(ctx context.Context, req *userv1.PasswordResetRequest) (*userv1.PasswordResetResponse, error) {
	if err := s.authService.RequestPasswordReset(ctx, req.Email); err != nil {
		return nil, authStatus(err)
	}
	return &userv1.PasswordResetResponse{Message: "Request received. If this email is eligible for password recovery, a reset message will be sent."}, nil
}
func (s *UserServer) ResetPassword(ctx context.Context, req *userv1.ResetPasswordRequest) (*userv1.ResetPasswordResponse, error) {
	if err := s.authService.ResetPassword(ctx, req.Token, req.NewPassword); err != nil {
		return nil, authStatus(err)
	}
	return &userv1.ResetPasswordResponse{Message: "Password updated. Please sign in."}, nil
}

func (s *UserServer) Login(ctx context.Context, req *userv1.LoginRequest) (*userv1.AuthResponse, error) {
	user, accessToken, refreshToken, err := s.authService.Login(ctx, req.Email, req.Password)
	if err != nil {
		return nil, authStatus(err)
	}

	return &userv1.AuthResponse{
		AccessToken: accessToken, RefreshToken: refreshToken, User: toUserProfile(user),
	}, nil
}

func (s *UserServer) RefreshToken(ctx context.Context, req *userv1.RefreshTokenRequest) (*userv1.AuthResponse, error) {
	user, accessToken, refreshToken, err := s.authService.RefreshToken(ctx, req.RefreshToken)
	if err != nil {
		return nil, authStatus(err)
	}

	return &userv1.AuthResponse{
		AccessToken: accessToken, RefreshToken: refreshToken, User: toUserProfile(user),
	}, nil
}

func (s *UserServer) Logout(ctx context.Context, req *userv1.LogoutRequest) (*userv1.LogoutResponse, error) {
	if err := s.authService.Logout(ctx, req.AccessToken, req.RefreshToken); err != nil {
		return nil, authStatus(err)
	}
	return &userv1.LogoutResponse{}, nil
}

func (s *UserServer) GetProfile(ctx context.Context, req *userv1.GetProfileRequest) (*userv1.UserProfile, error) {
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user ID")
	}

	user, err := s.authService.GetProfile(ctx, userID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	return toUserProfile(user), nil
}

func (s *UserServer) UpdateProfile(ctx context.Context, req *userv1.UpdateProfileRequest) (*userv1.UserProfile, error) {
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user ID")
	}

	user, err := s.authService.UpdateProfile(ctx, userID, req.Name, req.Email, req.Locale)
	if err != nil {
		return nil, authStatus(err)
	}

	return toUserProfile(user), nil
}

func (s *UserServer) ValidateToken(ctx context.Context, req *userv1.ValidateTokenRequest) (*userv1.ValidateTokenResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid token validation request")
	}
	claims, err := s.authService.ValidateToken(req.AccessToken)
	if err != nil {
		return &userv1.ValidateTokenResponse{Valid: false}, nil
	}

	response := &userv1.ValidateTokenResponse{
		Valid: true, UserId: claims.UserID, Email: claims.Email, Role: claims.Role,
	}
	userID, err := uuid.Parse(claims.UserID)
	if err != nil || userID == uuid.Nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	user, err := s.authService.GetProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "current User identity is unavailable")
		}
		return nil, status.Error(codes.Unavailable, "current User role is unavailable")
	}
	if user == nil || user.ID != userID || user.Role == "" {
		return nil, status.Error(codes.Unavailable, "current User role is unavailable")
	}
	if claims.CredentialVersion != user.CredentialVersion {
		return &userv1.ValidateTokenResponse{Valid: false}, nil
	}
	if !req.GetRequireCurrentRole() {
		return response, nil
	}
	response.Role = user.Role
	response.CurrentRoleVerified = true
	return response, nil
}

func toUserProfile(user *domain.User) *userv1.UserProfile {
	return &userv1.UserProfile{
		Id: user.ID.String(), Email: user.Email, Name: user.Name,
		Role: user.Role, CreatedAt: timestamppb.New(user.CreatedAt),
		Locale: user.Locale,
	}
}
