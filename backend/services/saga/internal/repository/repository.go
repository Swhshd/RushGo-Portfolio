package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

var ErrNotFound = errors.New("record not found")
var ErrRecoveryConflict = errors.New("saga recovery identity conflict")
var ErrRecoveryAttention = errors.New("saga recovery attention required")
var ErrInvalidRecoveryInput = errors.New("invalid saga recovery input")
var ErrPaidUnissuedBindingMissing = errors.New("paid-unissued Saga is missing its authoritative payment binding")

type RecoveryRepository interface {
	CreateOrGetSaga(context.Context, domain.SagaInput, []domain.SagaStep) (*domain.Saga, bool, error)
	ClaimDueSagas(context.Context, int, time.Duration) ([]domain.SagaClaim, error)
	RenewClaim(context.Context, domain.SagaClaim, time.Duration) (bool, error)
	PersistStepIntent(context.Context, domain.SagaClaim, uuid.UUID, string) (bool, error)
	PersistStepResult(context.Context, domain.SagaClaim, domain.StepResult) (bool, error)
	WakeSaga(context.Context, uuid.UUID, int64, *uuid.UUID, string) error
}

// ReplayRepository adds targeted fast-path claims and authoritative terminal
// settlement to the original recovery storage contract.
type ReplayRepository interface {
	RecoveryRepository
	ClaimSaga(context.Context, uuid.UUID, time.Duration) (*domain.SagaClaim, error)
	SwitchToCompensation(context.Context, domain.SagaClaim) (*domain.SagaClaim, error)
	PersistClaimFailure(context.Context, domain.SagaClaim, string, time.Time, bool) (bool, error)
	SettleSaga(context.Context, uuid.UUID, int64, *uuid.UUID, *uuid.UUID, bool, string) error
}

type SagaRepositoryInterface interface {
	Create(ctx context.Context, saga *domain.Saga) error
	GetSagaById(ctx context.Context, id uuid.UUID) (*domain.Saga, error)
	UpsertBatchSagaSteps(ctx context.Context, steps []domain.SagaStep) error
	GetSagaByBookingId(ctx context.Context, id uuid.UUID) (*domain.Saga, error)
	UpdateSaga(ctx context.Context, saga *domain.Saga, sagaStepId *uuid.UUID) error
	TransitionWaitingSaga(ctx context.Context, bookingID uuid.UUID, status domain.SagaStatus) (bool, error)
	ResetProcessingSaga(ctx context.Context, bookingID uuid.UUID) (bool, error)
}

type TicketIssuanceOutboxRepository interface {
	ClaimTicketIssuanceOutbox(context.Context, int, time.Duration) ([]domain.TicketIssuanceOutboxEvent, error)
	CompleteTicketIssuanceOutbox(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	RetryTicketIssuanceOutbox(context.Context, uuid.UUID, uuid.UUID, time.Time) (bool, error)
}
