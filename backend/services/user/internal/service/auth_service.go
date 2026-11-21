package service

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/ticketbox/pkg/authpolicy"
	"go.opentelemetry.io/otel/baggage"
	"google.golang.org/grpc/metadata"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
)

var (
	ErrInvalidCredentials = domain.ErrInvalidCredentials
	ErrEmailExists        = domain.ErrEmailExists
	ErrTokenExpired       = domain.ErrTokenExpired
)

type AuthService struct {
	recoveryQueue   *RecoveryQueue
	authRepo        repository.AuthRepository
	deps            AuthDependencies
	userRepo        repository.UserRepository
	jwtManager      *JWTManager
	refreshTokenTTL time.Duration
	logger          *zap.Logger
}

func (s *AuthService) SetRecoveryQueue(q *RecoveryQueue) { s.recoveryQueue = q }
func (s *AuthService) RequestPasswordReset(ctx context.Context, email string) (outcomeErr error) {
	defer func() { s.recordAuth("request", outcomeErr) }()
	canonical, err := authpolicy.CanonicalEmail(email)
	if err != nil {
		return domain.ErrValidation
	}
	if s.recoveryQueue == nil {
		return domain.ErrTemporaryUnavailable
	}
	return s.recoveryQueue.Enqueue(RecoveryJob{CanonicalEmail: canonical, EnqueuedAt: time.Now().UTC()})
}
func (s *AuthService) ResetPassword(ctx context.Context, token, password string) (outcomeErr error) {
	defer func() { s.recordAuth("complete", outcomeErr) }()
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != token || ValidateNewPassword(password) != nil {
		return domain.ErrValidation
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return s.authRepo.ResetPasswordAndRevokeSessions(ctx, repository.ResetCompletion{TokenHash: HashToken(token), PasswordHash: string(hash)})
}
func (s *AuthService) processRecoveryJob(ctx context.Context, job RecoveryJob) error {
	user, err := s.userRepo.GetByEmail(ctx, job.CanonicalEmail)
	if errors.Is(err, repository.ErrNotFound) {
		return nil
	}
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if s.deps.FrontendOrigin == nil {
		return domain.ErrDeliveryUnavailable
	}
	token, hash, err := GenerateResetToken(s.deps.RNG)
	if err != nil {
		return err
	}
	v, err := s.authRepo.PreparePasswordReset(ctx, repository.PasswordResetPreparation{ID: uuid.New(), UserID: user.ID, CanonicalEmail: job.CanonicalEmail, TokenHash: hash})
	if err != nil {
		return err
	}
	link := *s.deps.FrontendOrigin
	link.Path = "/reset-password"
	link.Fragment = "token=" + token
	result := s.deps.MailSender.Send(ctx, AuthMail{Recipient: job.CanonicalEmail, ResetURL: link.String(), Purpose: ResetMail, ExpiresAt: v.ExpiresAt})
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err = s.authRepo.FinalizePasswordResetDelivery(finalizeCtx, v.ID, result.Class); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if result.Class != domain.Accepted {
		return domain.ErrDeliveryUnavailable
	}
	return nil
}

// ProcessRecoveryJob is the service-lifecycle worker entry point; HTTP admission never calls it.
func (s *AuthService) ProcessRecoveryJob(ctx context.Context, job RecoveryJob) error {
	return s.processRecoveryJob(ctx, job)
}

type AuthDependencies struct {
	MailSender     MailSender
	RNG            io.Reader
	FrontendOrigin *url.URL
	Metrics        *observability.Metrics
}

func NewAuthService(
	userRepo repository.UserRepository,
	authRepo repository.AuthRepository,
	jwtManager *JWTManager,
	refreshTokenTTLDays int,
	logger *zap.Logger,
	deps AuthDependencies,
) *AuthService {
	if deps.RNG == nil {
		deps.RNG = rand.Reader
	}
	if deps.MailSender == nil {
		deps.MailSender = unavailableMailSender{}
	}
	return &AuthService{
		userRepo: userRepo,
		authRepo: authRepo, deps: deps,
		jwtManager:      jwtManager,
		refreshTokenTTL: time.Duration(refreshTokenTTLDays) * 24 * time.Hour,
		logger:          logger,
	}
}
func (s *AuthService) RequestRegistrationCode(ctx context.Context, email string) (_ *domain.RegistrationCodeResult, outcomeErr error) {
	ctx = metadata.NewOutgoingContext(baggage.ContextWithBaggage(ctx, baggage.Baggage{}), metadata.MD{})
	defer func() { s.recordAuth("send", outcomeErr) }()
	canonical, err := authpolicy.CanonicalEmail(email)
	if err != nil {
		return nil, domain.ErrValidation
	}
	code, err := GenerateOTP(s.deps.RNG)
	if err != nil {
		return nil, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(code), 10)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	v, err := s.authRepo.PrepareRegistrationVerification(ctx, repository.RegistrationPreparation{ID: uuid.New(), CanonicalEmail: canonical, CredentialHash: string(hash)})
	if err != nil {
		return nil, err
	}
	result := s.deps.MailSender.Send(ctx, AuthMail{Recipient: canonical, Code: code, Purpose: RegistrationMail, ExpiresAt: v.ExpiresAt})
	// Finalization has its own bounded context when sending consumed the request deadline.
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	err = s.authRepo.FinalizeRegistrationDelivery(finalizeCtx, v.ID, result.Class)
	if result.Class != domain.Accepted {
		return nil, domain.ErrDeliveryUnavailable
	}
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	return &domain.RegistrationCodeResult{VerificationID: v.ID, ExpiresAt: v.ExpiresAt, NextResendAt: v.NextResendAt}, nil
}
func (s *AuthService) RegisterVerified(ctx context.Context, input domain.RegisterInput) (_ *domain.User, outcomeErr error) {
	defer func() { s.recordAuth("verify", outcomeErr) }()
	canonical, err := authpolicy.CanonicalEmail(input.Email)
	if err != nil || ValidateNewPassword(input.Password) != nil || input.VerificationID == uuid.Nil || !validOTP(input.VerificationCode) || strings.TrimSpace(input.Name) == "" || !utf8.ValidString(input.Name) || utf8.RuneCountInString(input.Name) > 255 {
		return nil, domain.ErrValidation
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(input.Password), 10)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	return s.authRepo.RegisterVerifiedUser(ctx, domain.RegisterCompletion{UserID: uuid.New(), VerificationID: input.VerificationID, CanonicalEmail: canonical, Name: input.Name, PasswordHash: string(hash)}, func(stored string) bool {
		return bcrypt.CompareHashAndPassword([]byte(stored), []byte(input.VerificationCode)) == nil
	})
}
func validOTP(code string) bool {
	if len(code) != 6 {
		return false
	}
	for _, r := range code {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
func (s *AuthService) issueSession(user *domain.User, now time.Time) (*domain.SessionPair, error) {
	access, err := s.jwtManager.GenerateAccessToken(user.ID, user.Email, user.Role, user.CredentialVersion)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	refresh := GenerateRefreshToken()
	return &domain.SessionPair{AccessToken: access, RefreshToken: refresh, StoredRefresh: domain.RefreshToken{ID: uuid.New(), UserID: user.ID, TokenHash: HashToken(refresh), ExpiresAt: now.Add(s.refreshTokenTTL), CreatedAt: now}}, nil
}

func (s *AuthService) Login(ctx context.Context, email, password string) (*domain.User, string, string, error) {
	user, pair, err := s.authRepo.LoginAndIssueRefresh(ctx, authpolicy.NormalizeEmail(email), func(hash string) bool { return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil }, s.issueSession)
	if err != nil {
		return nil, "", "", err
	}
	return user, pair.AccessToken, pair.RefreshToken, nil
}

func (s *AuthService) RefreshToken(ctx context.Context, refreshTokenStr string) (*domain.User, string, string, error) {
	user, pair, err := s.authRepo.RotateRefreshToken(ctx, HashToken(refreshTokenStr), s.issueSession)
	if err != nil {
		return nil, "", "", err
	}
	return user, pair.AccessToken, pair.RefreshToken, nil
}

func (s *AuthService) ValidateToken(tokenStr string) (*TokenClaims, error) {
	return s.jwtManager.ValidateToken(tokenStr)
}
func (s *AuthService) Logout(ctx context.Context, access, refresh string) error {
	if _, err := uuid.Parse(refresh); err != nil {
		return domain.ErrValidation
	}
	claims, err := s.jwtManager.ValidateToken(access)
	if err != nil {
		return domain.ErrInvalidCredentials
	}
	owner, err := uuid.Parse(claims.UserID)
	if err != nil || owner == uuid.Nil {
		return domain.ErrInvalidCredentials
	}
	return s.authRepo.RevokeCurrentRefresh(ctx, owner, HashToken(refresh))
}

func (s *AuthService) GetProfile(ctx context.Context, userID uuid.UUID) (*domain.User, error) {
	if s.userRepo == nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	return s.userRepo.GetByID(ctx, userID)
}

func (s *AuthService) UpdateProfile(ctx context.Context, userID uuid.UUID, name, email string, locales ...string) (*domain.User, error) {
	if email != "" {
		return nil, domain.ErrEmailChangeNotSupported
	}
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > 255 {
		return nil, domain.ErrValidation
	}
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, err
	}

	if name != "" {
		user.Name = name
	}
	if len(locales) > 0 && locales[0] != "" {
		if locales[0] != "en" && locales[0] != "zh" {
			return nil, domain.ErrValidation
		}
		user.Locale = locales[0]
	}

	if err := s.userRepo.Update(ctx, user); err != nil {
		return nil, err
	}

	return user, nil
}
