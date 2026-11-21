package service

import (
	"bytes"
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"testing"
	"time"
)

type registrationRepo struct {
	repository.AuthRepository
	prepared *domain.RegistrationVerification
	final    domain.DeliveryOutcome
	finalErr error
	created  int
	exists   bool
}

func (r *registrationRepo) PrepareRegistrationVerification(_ context.Context, p repository.RegistrationPreparation) (*domain.RegistrationVerification, error) {
	r.prepared = &domain.RegistrationVerification{ID: p.ID, CanonicalEmail: p.CanonicalEmail, CredentialHash: p.CredentialHash, ExpiresAt: time.Now().Add(10 * time.Minute), NextResendAt: time.Now().Add(time.Minute)}
	return r.prepared, nil
}
func (r *registrationRepo) FinalizeRegistrationDelivery(_ context.Context, id uuid.UUID, outcome domain.DeliveryOutcome) error {
	if r.prepared == nil || id != r.prepared.ID {
		return domain.ErrInvalidCode
	}
	r.final = outcome
	return r.finalErr
}
func (r *registrationRepo) RegisterVerifiedUser(_ context.Context, p domain.RegisterCompletion, compare func(string) bool) (*domain.User, error) {
	if r.prepared == nil || p.VerificationID != r.prepared.ID || r.final != domain.Accepted || !compare(r.prepared.CredentialHash) {
		return nil, domain.ErrInvalidCode
	}
	if r.exists {
		return nil, domain.ErrEmailExists
	}
	r.created++
	now := time.Now()
	return &domain.User{ID: p.UserID, Email: p.CanonicalEmail, Name: p.Name, PasswordHash: p.PasswordHash, EmailVerifiedAt: &now}, nil
}

type capturingMail struct {
	result DeliveryResult
	input  AuthMail
	calls  int
}

func (m *capturingMail) Send(_ context.Context, input AuthMail) DeliveryResult {
	m.input = input
	m.calls++
	return m.result
}
func TestRegistrationProofAndNoSession(t *testing.T) {
	r := &registrationRepo{}
	m := &capturingMail{result: DeliveryResult{domain.Accepted, "accepted"}}
	s := NewAuthService(nil, r, nil, 7, zap.NewNop(), AuthDependencies{MailSender: m, RNG: bytes.NewReader(make([]byte, 100))})
	if _, err := s.RegisterVerified(context.Background(), domain.RegisterInput{Email: "new@example.com", Password: "synthetic long input", Name: "M6"}); !errors.Is(err, domain.ErrValidation) {
		t.Fatal("missing proof accepted")
	}
	v, err := s.RequestRegistrationCode(context.Background(), " New@Example.com ")
	if err != nil {
		t.Fatal("send failed")
	}
	if m.input.Code != "000000" || m.input.Recipient != "new@example.com" || v.VerificationID == uuid.Nil {
		t.Fatal("mail/proof shape invalid")
	}
	if bcrypt.CompareHashAndPassword([]byte(r.prepared.CredentialHash), []byte(m.input.Code)) != nil {
		t.Fatal("code stored without correct hash")
	}
	u, err := s.RegisterVerified(context.Background(), domain.RegisterInput{Email: "NEW@example.com", Password: "synthetic long input", Name: "M6", VerificationID: v.VerificationID, VerificationCode: m.input.Code})
	if err != nil || u.EmailVerifiedAt == nil || r.created != 1 {
		t.Fatal("verified creation failed")
	}
}
func TestRegistrationDeliveryFailureAndFinalization(t *testing.T) {
	for _, outcome := range []domain.DeliveryOutcome{domain.ExplicitFailure, domain.Unknown, domain.ConfigurationFailure} {
		r := &registrationRepo{}
		m := &capturingMail{result: DeliveryResult{outcome, "synthetic"}}
		s := NewAuthService(nil, r, nil, 7, zap.NewNop(), AuthDependencies{MailSender: m, RNG: bytes.NewReader(make([]byte, 100))})
		if _, err := s.RequestRegistrationCode(context.Background(), "new@example.com"); !errors.Is(err, domain.ErrDeliveryUnavailable) {
			t.Fatal("failed mail reported success")
		}
		if r.final != outcome {
			t.Fatal("failed delivery state not finalized")
		}
	}
	r := &registrationRepo{finalErr: domain.ErrTemporaryUnavailable}
	m := &capturingMail{result: DeliveryResult{domain.Accepted, "accepted"}}
	s := NewAuthService(nil, r, nil, 7, zap.NewNop(), AuthDependencies{MailSender: m, RNG: bytes.NewReader(make([]byte, 100))})
	if _, err := s.RequestRegistrationCode(context.Background(), "new@example.com"); !errors.Is(err, domain.ErrTemporaryUnavailable) {
		t.Fatal("failed DB activation reported sent")
	}
}
