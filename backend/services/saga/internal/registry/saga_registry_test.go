package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
)

type sagaHandlerTestRepository struct{}

func (sagaHandlerTestRepository) Create(context.Context, *domain.Saga) error { return nil }
func (sagaHandlerTestRepository) GetSagaById(context.Context, uuid.UUID) (*domain.Saga, error) {
	return nil, repository.ErrNotFound
}
func (sagaHandlerTestRepository) UpsertBatchSagaSteps(context.Context, []domain.SagaStep) error {
	return nil
}
func (sagaHandlerTestRepository) GetSagaByBookingId(context.Context, uuid.UUID) (*domain.Saga, error) {
	return nil, repository.ErrNotFound
}
func (sagaHandlerTestRepository) UpdateSaga(context.Context, *domain.Saga, *uuid.UUID) error {
	return nil
}
func (sagaHandlerTestRepository) TransitionWaitingSaga(context.Context, uuid.UUID, domain.SagaStatus) (bool, error) {
	return false, nil
}
func (sagaHandlerTestRepository) ResetProcessingSaga(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}

func TestSagaHandlerReturnsReservationFailureAfterCompensation(t *testing.T) {
	reserveErr := errors.New("seat reservation conflict")
	compensated := false
	saga := &domain.Saga{
		ID:     uuid.New(),
		Status: domain.SAGA_PROCESSING,
		Steps: []domain.SagaStep{
			{
				ID:     uuid.New(),
				Name:   "RESERVED_SEAT_STEP",
				Status: domain.SAGA_STEP_PENDING,
				Execute: func(context.Context) error {
					return reserveErr
				},
				Compensate: func(context.Context) error {
					compensated = true
					return nil
				},
			},
			{
				ID:                    uuid.New(),
				Name:                  "CREATE_PAYMENT_INTENT_STEP",
				Status:                domain.SAGA_STEP_PENDING,
				ShouldPauseForPayment: true,
				Execute: func(context.Context) error {
					t.Fatal("payment step executed after seat reservation failed")
					return nil
				},
			},
		},
	}
	handler := NewSagaHandler(saga, sagaHandlerTestRepository{}, zap.NewNop())

	err := handler.Execute(context.Background(), 0)
	if !errors.Is(err, reserveErr) {
		t.Fatalf("Execute error=%v, want reservation conflict", err)
	}
	if compensated {
		t.Fatal("failed reservation step must not be compensated after atomic acquisition failure")
	}
	if saga.Status != domain.SAGA_ROLLED_BACK {
		t.Fatalf("Saga status=%s, want ROLLED_BACK", saga.Status)
	}
}
