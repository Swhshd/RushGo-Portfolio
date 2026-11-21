package service

import (
	"testing"
	"time"

	"github.com/google/uuid"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
)

func TestPaymentClientSecretOrEmptyHandlesMissingPaymentResponse(t *testing.T) {
	if got := paymentClientSecretOrEmpty(nil); got != "" {
		t.Fatalf("expected an empty client secret for a missing payment response, got %q", got)
	}

	response := &paymentv1.CreatePaymentResponse{PaymentIntentClientSecret: "pi_secret"}
	if got := paymentClientSecretOrEmpty(response); got != "pi_secret" {
		t.Fatalf("expected the payment client secret, got %q", got)
	}
}

func TestMarkSagaPaymentFailureCompensatesExecutedStepsOnce(t *testing.T) {
	saga := &domain.Saga{ID: uuid.New(), Status: domain.SAGA_WAIT_FOR_PAYMENT, CurrentStepIndex: 1, Steps: []domain.SagaStep{{ID: uuid.New(), Name: "RESERVED_SEAT_STEP", Status: domain.SAGA_STEP_COMPLETED}, {ID: uuid.New(), Name: "CREATE_PAYMENT_INTENT_STEP", Status: domain.SAGA_STEP_COMPLETED}}}
	if !markSagaPaymentFailure(saga) || saga.Status != domain.SAGA_ROLLED_BACK || saga.CurrentStepIndex != 0 {
		t.Fatal("first failure did not roll back")
	}
	if saga.Steps[0].Status != domain.SAGA_STEP_COMPENSATED || saga.Steps[0].CompenstatedAt == nil || saga.Steps[1].Status != domain.SAGA_STEP_FAILED {
		t.Fatal("executed steps were not transitioned once")
	}
	compensatedAt := *saga.Steps[0].CompenstatedAt
	if markSagaPaymentFailure(saga) || !saga.Steps[0].CompenstatedAt.Equal(compensatedAt) || saga.Steps[0].CompenstatedAt.After(time.Now()) {
		t.Fatal("duplicate failure changed durable result")
	}
}
