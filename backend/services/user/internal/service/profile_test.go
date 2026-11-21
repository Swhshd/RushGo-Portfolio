package service

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/user/internal/domain"
	"github.com/ticketbox/user/internal/repository"
	"go.uber.org/zap"
	"testing"
)

type profileRepo struct {
	repository.UserRepository
	reads, writes int
	user          domain.User
}

func (r *profileRepo) GetByID(context.Context, uuid.UUID) (*domain.User, error) {
	r.reads++
	return &r.user, nil
}
func (r *profileRepo) Update(context.Context, *domain.User) error { r.writes++; return nil }
func TestProfileEmailRejectedBeforeRepository(t *testing.T) {
	r := &profileRepo{user: domain.User{Email: "original@example.com"}}
	s := NewAuthService(r, nil, nil, 7, zap.NewNop(), AuthDependencies{})
	if _, err := s.UpdateProfile(context.Background(), uuid.New(), "name", "changed@example.com"); !errors.Is(err, domain.ErrEmailChangeNotSupported) || r.reads != 0 || r.writes != 0 {
		t.Fatal("ordinary email mutation reached repository")
	}
	if _, err := s.UpdateProfile(context.Background(), uuid.New(), "Renamed", ""); err != nil || r.user.Name != "Renamed" || r.user.Email != "original@example.com" {
		t.Fatal("name update failed or changed email")
	}
}
