package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
)

type ticketIssuanceSagaRepository struct {
	repository.SagaRepositoryInterface
	saga *domain.Saga
	err  error
}

func (r ticketIssuanceSagaRepository) GetSagaById(context.Context, uuid.UUID) (*domain.Saga, error) {
	return r.saga, r.err
}

func TestVerifyTicketIssuanceSagaRequiresCompletedManagedForwardGraph(t *testing.T) {
	sagaID, bookingID, paymentID, eventID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	saga := &domain.Saga{
		ID: sagaID, BookingID: bookingID, PaymentID: paymentID, Generation: 1,
		RecoveryManaged: true, Status: domain.SAGA_COMPLETED, Direction: "forward", CurrentStepIndex: 5,
		Input: &domain.SagaInput{BookingID: bookingID, EventID: eventID, Generation: 1},
		Steps: make([]domain.SagaStep, 5),
	}
	for i := range saga.Steps {
		saga.Steps[i].ExecuteState, saga.Steps[i].CompensateState = "completed", "pending"
	}
	svc := NewSagaService(zap.NewNop(), ticketIssuanceSagaRepository{saga: saga}, nil, nil, nil, nil)
	eligible, reason, err := svc.VerifyTicketIssuanceSaga(context.Background(), sagaID, bookingID, paymentID, eventID, 1)
	if err != nil || !eligible || reason != "managed_saga_completed" {
		t.Fatalf("completed managed graph rejected: eligible=%v reason=%q err=%v", eligible, reason, err)
	}
	saga.Steps[3].CompensateState = "completed"
	eligible, reason, err = svc.VerifyTicketIssuanceSaga(context.Background(), sagaID, bookingID, paymentID, eventID, 1)
	if err != nil || eligible || reason != "saga_graph_not_final" {
		t.Fatalf("compensated saga graph accepted: eligible=%v reason=%q err=%v", eligible, reason, err)
	}
	_, _, err = svc.VerifyTicketIssuanceSaga(context.Background(), sagaID, bookingID, paymentID, eventID, 2)
	if !errors.Is(err, repository.ErrInvalidRecoveryInput) {
		t.Fatalf("invalid generation returned %v", err)
	}
}
