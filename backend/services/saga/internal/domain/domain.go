package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type SagaStatus string

var (
	SAGA_PENDING          SagaStatus = "PENDING"
	SAGA_WAIT_FOR_PAYMENT SagaStatus = "WAIT_FOR_PAYMENT"
	SAGA_PROCESSING       SagaStatus = "PROCESSING"
	SAGA_COMPLETED        SagaStatus = "COMPLETED"
	SAGA_ROLLING_BACK     SagaStatus = "ROLLING_BACK"
	SAGA_ROLLED_BACK      SagaStatus = "ROLLED_BACK"
	SAGA_FAIL             SagaStatus = "FAIL"
)

type Saga struct {
	ID                       uuid.UUID
	BookingID                uuid.UUID
	Name                     string
	Steps                    []SagaStep
	Status                   SagaStatus
	CurrentStepIndex         int
	PaymentIntentId          string
	PaymentID                uuid.UUID
	LastObligationID         *uuid.UUID
	Generation               int64
	RecoveryManaged          bool
	Input                    *SagaInput
	Direction                string
	RecoveryState            string
	ErrorCategory            string
	NextAttemptAt            *time.Time
	WakeVersion              int64
	ConsumedWakeVersion      int64
	Attempts                 int
	FailureAttempts          int
	ClaimToken               *uuid.UUID
	ClaimFence               int64
	LeaseUntil               *time.Time
	NullableStatus           *SagaStatus
	NullableCurrentStepIndex *int
	NullableFields           bool `json:"-"`
}

type SagaStepStatus string

var (
	SAGA_STEP_PENDING      SagaStepStatus = "PENDING"
	SAGA_STEP_EXECUTING    SagaStepStatus = "EXECUTING"
	SAGA_STEP_COMPLETED    SagaStepStatus = "COMPLETED"
	SAGA_STEP_COMPENSATING SagaStepStatus = "COMPENSATING"
	SAGA_STEP_COMPENSATED  SagaStepStatus = "COMPENSATED"
	SAGA_STEP_FAILED       SagaStepStatus = "FAILED"
)

type SagaStepFunc func(ctx context.Context, req SagaStepRequest) (SagaStepResponse, error)

type SagaStepRequest interface {
	String() string
}

type SagaStepResponse interface {
	String() string
}

type SagaStep struct {
	ID                    uuid.UUID
	SagaID                uuid.UUID
	Name                  string
	ExecuteReq            SagaStepRequest                 `json:"-"`
	CompensateReq         SagaStepRequest                 `json:"-"`
	Execute               func(ctx context.Context) error `json:"-"`
	Compensate            func(ctx context.Context) error `json:"-"`
	ExecutedAt            *time.Time
	CompenstatedAt        *time.Time
	Status                SagaStepStatus
	Order                 int16
	ShouldPauseForPayment bool
	NullableStatus        *SagaStepStatus
	NullableOrder         *int32
	NullablePause         *bool
	NullableFields        bool `json:"-"`
	ExecuteState          string
	CompensateState       string
	ExecuteEvidence       []byte
	CompensateEvidence    []byte
	// SagaPaymentRes        *paymentv1.CreatePaymentResponse
}
