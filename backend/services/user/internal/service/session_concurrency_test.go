package service

import (
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

type sessionRepo struct {
	repository.AuthRepository
	owner  uuid.UUID
	hash   string
	calls  int
	stored string
}

func (r *sessionRepo) RevokeCurrentRefresh(_ context.Context, owner uuid.UUID, hash string) error {
	r.owner = owner
	r.hash = hash
	r.calls++
	return nil
}
func (r *sessionRepo) LoginAndIssueRefresh(_ context.Context, email string, compare func(string) bool, issue repository.IssueSession) (*domain.User, *domain.SessionPair, error) {
	if email != "legacy@example.com" || !compare(r.stored) {
		return nil, nil, domain.ErrInvalidCredentials
	}
	u := &domain.User{ID: uuid.New(), Email: email, Role: "user"}
	p, err := issue(u, time.Now())
	return u, p, err
}
func TestLogoutAccessDerivedCurrentRefresh(t *testing.T) {
	r := &sessionRepo{}
	jwt := NewJWTManager(uuid.NewString(), 15)
	s := NewAuthService(nil, r, jwt, 7, zap.NewNop(), AuthDependencies{})
	id := uuid.New()
	access, _ := jwt.GenerateAccessToken(id, "legacy@example.com", "user")
	raw := uuid.NewString()
	if err := s.Logout(context.Background(), access, raw); err != nil || r.owner != id || r.hash != HashToken(raw) || r.calls != 1 {
		t.Fatal("logout did not derive access owner/revoke current hash")
	}
	if err := s.Logout(context.Background(), "invalid", raw); !errors.Is(err, domain.ErrInvalidCredentials) || r.calls != 1 {
		t.Fatal("invalid access revoked refresh")
	}
	if err := s.Logout(context.Background(), access, ""); !errors.Is(err, domain.ErrValidation) {
		t.Fatal("missing refresh accepted")
	}
}
func TestLegacyShortPasswordLogin(t *testing.T) {
	raw := string([]byte{'s', 'h', 'o', 'r', 't'})
	hash, _ := bcrypt.GenerateFromPassword([]byte(raw), 10)
	r := &sessionRepo{stored: string(hash)}
	s := NewAuthService(nil, r, NewJWTManager(uuid.NewString(), 15), 7, zap.NewNop(), AuthDependencies{})
	u, access, refresh, err := s.Login(context.Background(), " Legacy@Example.com ", raw)
	if err != nil || u.EmailVerifiedAt != nil || access == "" || refresh == "" {
		t.Fatal("legacy NULL/short-password login blocked")
	}
}
