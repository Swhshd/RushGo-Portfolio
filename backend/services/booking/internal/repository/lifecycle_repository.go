package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"time"
)

var (
	ErrLifecycleConflict        = errors.New("booking lifecycle decision conflict")
	ErrInvalidLifecycleDecision = errors.New("invalid booking lifecycle decision")
	ErrLegacyLifecycleAttention = errors.New("legacy booking lifecycle requires evidence classification")
	ErrRequestConflict          = errors.New("booking request identity conflict")
	ErrInvalidBookingRequest    = errors.New("invalid booking request")
)

type LifecycleRepository interface {
	GetLifecycle(context.Context, domain.LifecycleRef) (*domain.LifecycleSnapshot, error)
	DecideLifecycle(context.Context, domain.LifecycleDecision, *OutboxEvent) (*domain.LifecycleSnapshot, bool, error)
	ListExpiryCandidates(context.Context, int, uuid.UUID) ([]domain.LifecycleSnapshot, error)
	GetByRequest(context.Context, domain.BookingRequestIdentity) (*domain.LifecycleSnapshot, error)
	CreateOrGetRequest(context.Context, domain.CreateBookingCommand) (*domain.LifecycleSnapshot, bool, error)
	ClaimDispatch(context.Context, int, time.Duration) ([]domain.DispatchClaim, error)
	FinishDispatch(context.Context, domain.DispatchClaim, uuid.UUID, bool, string, time.Time) (bool, error)
}
