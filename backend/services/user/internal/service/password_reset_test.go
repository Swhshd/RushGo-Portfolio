package service

import (
	"bytes"
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"go.uber.org/zap"
	"strings"
	"testing"
	"time"
)

type resetRepo struct {
	repository.AuthRepository
	input    repository.ResetCompletion
	prepared repository.PasswordResetPreparation
	final    domain.DeliveryOutcome
}

func (r *resetRepo) ResetPasswordAndRevokeSessions(_ context.Context, p repository.ResetCompletion) error {
	r.input = p
	return nil
}
func (r *resetRepo) PreparePasswordReset(_ context.Context, p repository.PasswordResetPreparation) (*domain.PasswordResetRequest, error) {
	r.prepared = p
	return &domain.PasswordResetRequest{ID: p.ID, UserID: p.UserID, ExpiresAt: time.Now().Add(20 * time.Minute)}, nil
}
func (r *resetRepo) FinalizePasswordResetDelivery(_ context.Context, _ uuid.UUID, o domain.DeliveryOutcome) error {
	r.final = o
	return nil
}

type recoveryUsers struct {
	repository.UserRepository
	lookup  chan struct{}
	release chan struct{}
	user    *domain.User
}

func (r *recoveryUsers) GetByEmail(context.Context, string) (*domain.User, error) {
	if r.lookup != nil {
		r.lookup <- struct{}{}
		<-r.release
	}
	if r.user == nil {
		return nil, repository.ErrNotFound
	}
	return r.user, nil
}
func TestForgotPublicAdmissionDecoupled(t *testing.T) {
	u := &recoveryUsers{lookup: make(chan struct{}, 2), release: make(chan struct{})}
	r := &resetRepo{}
	s := NewAuthService(u, r, nil, 7, zap.NewNop(), AuthDependencies{})
	q := NewRecoveryQueue(s.processRecoveryJob)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)
	s.SetRecoveryQueue(q)
	defer func() { close(u.release); _ = q.Shutdown(context.Background()) }()
	for _, email := range []string{"known@example.com", "unknown@example.com"} {
		done := make(chan error, 1)
		go func() { done <- s.RequestPasswordReset(context.Background(), email) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("generic admission failed")
			}
		case <-time.After(time.Second):
			t.Fatal("public forgot waits for account lookup")
		}
	}
}
func TestPasswordResetHashesOnlyAndPolicy(t *testing.T) {
	r := &resetRepo{}
	s := NewAuthService(nil, r, nil, 7, zap.NewNop(), AuthDependencies{})
	token, _, _ := GenerateResetToken(bytes.NewReader(make([]byte, 32)))
	if err := s.ResetPassword(context.Background(), token, "synthetic long input"); err != nil {
		t.Fatal("reset submission failed")
	}
	if r.input.TokenHash == token || len(r.input.TokenHash) != 64 || r.input.PasswordHash == "synthetic long input" {
		t.Fatal("raw reset/password repository input")
	}
	if err := s.ResetPassword(context.Background(), token, "short"); !errors.Is(err, domain.ErrValidation) {
		t.Fatal("reset password policy missing")
	}
	if err := s.ResetPassword(context.Background(), "bad", strings.Repeat("x", 15)); !errors.Is(err, domain.ErrValidation) {
		t.Fatal("malformed reset accepted")
	}
}
func TestRecoveryConfiguredOriginAndHashOnly(t *testing.T) {
	r := &resetRepo{}
	u := &recoveryUsers{user: &domain.User{ID: uuid.New(), EmailCanonical: "recover@example.com"}}
	m := &capturingMail{result: DeliveryResult{domain.Accepted, "accepted"}}
	origin, _ := ValidateFrontendOrigin("http://localhost:3002")
	s := NewAuthService(u, r, nil, 7, zap.NewNop(), AuthDependencies{MailSender: m, FrontendOrigin: origin, RNG: bytes.NewReader(make([]byte, 32))})
	if err := s.processRecoveryJob(context.Background(), RecoveryJob{CanonicalEmail: "recover@example.com"}); err != nil {
		t.Fatal("recovery worker failed")
	}
	if !strings.HasPrefix(m.input.ResetURL, "http://localhost:3002/reset-password#token=") || strings.Contains(m.input.ResetURL, r.prepared.TokenHash) || len(r.prepared.TokenHash) != 64 || r.final != domain.Accepted {
		t.Fatal("trusted origin or hash boundary violated")
	}
}
