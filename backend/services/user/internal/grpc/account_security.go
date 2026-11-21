package grpc

import (
	"context"
	"github.com/google/uuid"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
)

func (s *UserServer) accountOwner(ctx context.Context, id string) (uuid.UUID, error) {
	owner, err := uuid.Parse(id)
	if err != nil || owner == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid account")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	headers := md.Get("authorization")
	if len(headers) != 1 || !strings.HasPrefix(headers[0], "Bearer ") {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	token := strings.TrimPrefix(headers[0], "Bearer ")
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	result, err := s.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token})
	if err != nil {
		return uuid.Nil, err
	}
	if !result.GetValid() || result.UserId != owner.String() {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	return owner, nil
}

func (s *UserServer) ChangePassword(ctx context.Context, req *userv1.ChangePasswordRequest) (*userv1.ResetPasswordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	owner, err := s.accountOwner(ctx, req.UserId)
	if err != nil {
		return nil, err
	}
	if err = s.authService.ChangePassword(ctx, owner, req.CurrentPassword, req.NewPassword); err != nil {
		return nil, authStatus(err)
	}
	return &userv1.ResetPasswordResponse{Message: "Password changed. All sessions ended; please sign in."}, nil
}
func (s *UserServer) RequestEmailChange(ctx context.Context, req *userv1.EmailChangeRequest) (*userv1.RegistrationCodeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	owner, err := s.accountOwner(ctx, req.UserId)
	if err != nil {
		return nil, err
	}
	result, err := s.authService.RequestEmailChange(ctx, owner, req.CurrentPassword, req.Email)
	if err != nil {
		return nil, authStatus(err)
	}
	return &userv1.RegistrationCodeResponse{VerificationId: result.VerificationID.String(), ExpiresAt: timestamppb.New(result.ExpiresAt), NextResendAt: timestamppb.New(result.NextResendAt), Message: "Verification message accepted for delivery."}, nil
}
func (s *UserServer) CompleteEmailChange(ctx context.Context, req *userv1.CompleteEmailChangeRequest) (*userv1.ResetPasswordResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	owner, err := s.accountOwner(ctx, req.UserId)
	if err != nil {
		return nil, err
	}
	change, err := uuid.Parse(req.VerificationId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid verification")
	}
	if err = s.authService.CompleteEmailChange(ctx, owner, change, req.CurrentPassword, req.VerificationCode); err != nil {
		return nil, authStatus(err)
	}
	return &userv1.ResetPasswordResponse{Message: "Email changed. All sessions ended; sign in with your new email."}, nil
}
func (s *UserServer) GetNotificationRecipient(ctx context.Context, req *userv1.GetProfileRequest) (*userv1.NotificationRecipient, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	id, err := uuid.Parse(req.UserId)
	if err != nil || id == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid account")
	}
	user, err := s.authService.GetProfile(ctx, id)
	if err != nil || user == nil {
		return nil, status.Error(codes.Unavailable, "recipient unavailable")
	}
	locale := user.Locale
	if locale != "zh" {
		locale = "en"
	}
	return &userv1.NotificationRecipient{UserId: id.String(), Email: user.EmailCanonical, Locale: locale, EmailVerified: user.EmailVerifiedAt != nil}, nil
}
