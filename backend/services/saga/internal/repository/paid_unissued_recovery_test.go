package repository

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

func TestValidatePaidUnissuedAttentionSagaOnlyAcceptsIntendedFinalForwardStep(t *testing.T) {
	state, command := paidUnissuedAttentionFixture()
	if err := validatePaidUnissuedAttentionSaga(state, command); err != nil {
		t.Fatalf("exact final-step binding failure was not recoverable: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*PaidUnissuedSagaState)
	}{
		{name: "wrong error category", mutate: func(s *PaidUnissuedSagaState) { s.Saga.ErrorCategory = "invalid_authority" }},
		{name: "earlier step", mutate: func(s *PaidUnissuedSagaState) { s.Saga.CurrentStepIndex = 3 }},
		{name: "compensation direction", mutate: func(s *PaidUnissuedSagaState) { s.Saga.Direction = "compensate" }},
		{name: "outbox exists", mutate: func(s *PaidUnissuedSagaState) { s.OutboxCount = 1 }},
		{name: "payment already bound", mutate: func(s *PaidUnissuedSagaState) { s.Saga.PaymentID = uuid.New() }},
		{name: "live claim", mutate: func(s *PaidUnissuedSagaState) {
			lease := time.Now().Add(time.Minute)
			token := uuid.New()
			s.Saga.ClaimToken, s.Saga.LeaseUntil = &token, &lease
		}},
		{name: "completed final step", mutate: func(s *PaidUnissuedSagaState) { s.Saga.Steps[4].ExecuteState = "completed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, candidateCommand := paidUnissuedAttentionFixture()
			test.mutate(candidate)
			if err := validatePaidUnissuedAttentionSaga(candidate, candidateCommand); err == nil {
				t.Fatal("ineligible Saga state passed the narrow recovery gate")
			}
		})
	}
}

func TestPaidUnissuedRequestFingerprintBindsOperationIdentityAndOperatorReason(t *testing.T) {
	command := PaidUnissuedRecoveryCommand{
		SagaID: uuid.New(), BookingID: uuid.New(), OperationKey: uuid.New(),
		Operator: "operator-1", Reason: "restore final ticket step", ExpectedProviderAccount: "acct_test",
		Mode: "dry_run",
	}
	first := paidUnissuedRequestFingerprint(command)
	command.Mode = "execute"
	if next := paidUnissuedRequestFingerprint(command); first != next {
		t.Fatal("dry-run plan and its execute replay need the same request fingerprint")
	}
	command.Reason = "different operation"
	if next := paidUnissuedRequestFingerprint(command); first == next {
		t.Fatal("changed audit reason reused the same operation fingerprint")
	}
}

func paidUnissuedAttentionFixture() (*PaidUnissuedSagaState, PaidUnissuedRecoveryCommand) {
	sagaID, bookingID := uuid.New(), uuid.New()
	input := &domain.SagaInput{BookingID: bookingID, UserID: uuid.New(), EventID: uuid.New(), Generation: 1, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().Add(time.Hour), SeatIDs: []uuid.UUID{uuid.New()}}
	status, index := domain.SAGA_PROCESSING, 4
	saga := &domain.Saga{
		ID: sagaID, BookingID: bookingID, Status: status, NullableStatus: &status, CurrentStepIndex: index,
		NullableCurrentStepIndex: &index, RecoveryManaged: true, Input: input, Generation: 1,
		Direction: "forward", RecoveryState: "attention", ErrorCategory: "paid_unissued_binding_missing", FailureAttempts: 10,
	}
	for i, name := range orderNames {
		stepStatus := domain.SAGA_STEP_COMPLETED
		executeState := "completed"
		var executedAt *time.Time
		if i < 4 {
			completed := time.Now()
			executedAt = &completed
		} else {
			stepStatus, executeState = domain.SAGA_STEP_EXECUTING, "intended"
		}
		order, pause := int32(i), i == 1
		saga.Steps = append(saga.Steps, domain.SagaStep{
			ID: uuid.New(), SagaID: sagaID, Name: string(name), Order: int16(i), NullableOrder: &order,
			ShouldPauseForPayment: pause, NullablePause: &pause, Status: stepStatus, NullableStatus: &stepStatus,
			ExecuteState: executeState, CompensateState: "pending", ExecutedAt: executedAt,
		})
	}
	paymentID := uuid.New()
	return &PaidUnissuedSagaState{Saga: saga}, PaidUnissuedRecoveryCommand{
		SagaID: sagaID, BookingID: bookingID, OperationKey: uuid.New(), Mode: "execute", PaymentID: &paymentID,
		PaymentIntentID: "pi_test_recovery", ExpectedProviderAccount: "acct_test",
	}
}
