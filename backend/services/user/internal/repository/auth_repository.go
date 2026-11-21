package repository

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/user/internal/domain"
	"time"
)

type RegistrationPreparation struct {
	ID                             uuid.UUID
	CanonicalEmail, CredentialHash string
}
type PasswordResetPreparation struct {
	ID, UserID                uuid.UUID
	CanonicalEmail, TokenHash string
}
type ResetCompletion struct{ TokenHash, PasswordHash string }
type IssueSession func(*domain.User, time.Time) (*domain.SessionPair, error)
type AuthRepository interface {
	PrepareRegistrationVerification(context.Context, RegistrationPreparation) (*domain.RegistrationVerification, error)
	FinalizeRegistrationDelivery(context.Context, uuid.UUID, domain.DeliveryOutcome) error
	RegisterVerifiedUser(context.Context, domain.RegisterCompletion, func(string) bool) (*domain.User, error)
	PreparePasswordReset(context.Context, PasswordResetPreparation) (*domain.PasswordResetRequest, error)
	FinalizePasswordResetDelivery(context.Context, uuid.UUID, domain.DeliveryOutcome) error
	ResetPasswordAndRevokeSessions(context.Context, ResetCompletion) error
	LoginAndIssueRefresh(context.Context, string, func(string) bool, IssueSession) (*domain.User, *domain.SessionPair, error)
	RotateRefreshToken(context.Context, string, IssueSession) (*domain.User, *domain.SessionPair, error)
	RevokeCurrentRefresh(context.Context, uuid.UUID, string) error
}
