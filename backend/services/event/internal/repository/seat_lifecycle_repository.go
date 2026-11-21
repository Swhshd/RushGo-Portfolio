package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
)

var (
	ErrLifecycleClosed    = errors.New("seat lifecycle closed")
	ErrLifecycleConflict  = errors.New("seat lifecycle conflict")
	ErrManagedSeat        = errors.Join(ErrSeatNotAvailable, errors.New("managed seat requires lifecycle authority"))
	ErrAccountingBaseline = errors.New("sale accounting baseline unknown or inconsistent")
)

type SeatLifecycleRepository interface {
	ApplyLifecycle(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error)
	ApplySaleAccounting(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error)
	GetSeatAuthorities(context.Context, []uuid.UUID) ([]domain.SeatAuthority, error)
}
