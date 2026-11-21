package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"golang.org/x/crypto/bcrypt"
	"time"
)

func passwordVerifier(password string) func(string) bool {
	return func(hash string) bool {
		return len(password) > 0 && len(password) <= 72 && bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
}

func (s *AuthService) ChangePassword(ctx context.Context, id uuid.UUID, current, next string) error {
	if id == uuid.Nil || len(current) > 72 || ValidateNewPassword(next) != nil || current == next {
		return domain.ErrValidation
	}
	repo, ok := s.authRepo.(repository.AccountSecurityRepository)
	if !ok {
		return domain.ErrTemporaryUnavailable
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(next), 10)
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return repo.ChangePassword(ctx, id, string(hash), passwordVerifier(current))
}

func (s *AuthService) RequestEmailChange(ctx context.Context, id uuid.UUID, current, email string) (*domain.RegistrationCodeResult, error) {
	canonical, err := authpolicy.CanonicalEmail(email)
	if err != nil || id == uuid.Nil || len(current) > 72 {
		return nil, domain.ErrValidation
	}
	repo, ok := s.authRepo.(repository.AccountSecurityRepository)
	if !ok {
		return nil, domain.ErrTemporaryUnavailable
	}
	code, err := GenerateOTP(s.deps.RNG)
	if err != nil {
		return nil, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(code), 10)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	change, expiry, err := repo.PrepareEmailChange(ctx, id, canonical, string(hash), passwordVerifier(current))
	if err != nil {
		return nil, err
	}
	outcome := s.deps.MailSender.Send(ctx, AuthMail{Recipient: canonical, Code: code, Purpose: EmailChangeMail, ExpiresAt: expiry})
	final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	err = repo.FinalizeEmailChangeDelivery(final, change, outcome.Class)
	if outcome.Class != domain.Accepted {
		return nil, domain.ErrDeliveryUnavailable
	}
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	return &domain.RegistrationCodeResult{VerificationID: change, ExpiresAt: expiry, NextResendAt: expiry.Add(-9 * time.Minute)}, nil
}

func (s *AuthService) CompleteEmailChange(ctx context.Context, id, change uuid.UUID, current, code string) error {
	if id == uuid.Nil || change == uuid.Nil || len(current) > 72 || !validOTP(code) {
		return domain.ErrValidation
	}
	repo, ok := s.authRepo.(repository.AccountSecurityRepository)
	if !ok {
		return domain.ErrTemporaryUnavailable
	}
	return repo.CompleteEmailChange(ctx, id, change, passwordVerifier(current), passwordVerifier(code))
}
