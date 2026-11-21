package service

import (
	"github.com/ticketbox/saga/internal/domain"
	"time"
)

// Kept for the compatibility regression and old adapters. Managed execution
// uses durable step intents/results and does not call this helper.
func markSagaPaymentFailure(saga *domain.Saga) bool {
	if saga == nil || saga.Status == domain.SAGA_ROLLED_BACK || saga.Status == domain.SAGA_COMPLETED || saga.Status == domain.SAGA_FAIL {
		return false
	}
	if len(saga.Steps) > 1 && saga.Steps[1].Status == domain.SAGA_STEP_COMPLETED {
		saga.Steps[1].Status = domain.SAGA_STEP_FAILED
	}
	if len(saga.Steps) > 0 && saga.Steps[0].Status == domain.SAGA_STEP_COMPLETED {
		saga.Steps[0].Status = domain.SAGA_STEP_COMPENSATED
		now := time.Now()
		saga.Steps[0].CompenstatedAt = &now
		saga.CurrentStepIndex = 0
	}
	saga.Status = domain.SAGA_ROLLED_BACK
	return true
}
