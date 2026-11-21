package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/saga/internal/domain"
)

func TestSagaReliabilityBarrierIsIdentityBoundAndAfterDurableClaim(t *testing.T) {
	dir := t.TempDir()
	const token = "private-t23-saga-hook-token"
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	service := &SagaService{reliabilityHook: hook}
	bookingID, sagaID, stepID := uuid.New(), uuid.New(), uuid.New()
	claim := domain.SagaClaim{SagaID: sagaID, Token: uuid.New(), Fence: 7, Direction: "forward",
		Saga: &domain.Saga{ID: sagaID, BookingID: bookingID, Steps: []domain.SagaStep{{
			ID: stepID, Name: "RESERVED_SEAT_STEP", Order: 0, Status: domain.SAGA_STEP_EXECUTING,
		}}}}
	command, err := json.Marshal(reliabilityhook.Command{
		WindowID: "saga.forward.S0.before_effect", OperationID: bookingID.String(),
		Checkpoint: "saga.forward.S0.before_effect", Action: reliabilityhook.ActionPause,
		Token: token, Nonce: "nonce-saga-before-effect",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "command.json"), command, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.awaitReliabilityStepBoundary(ctx, claim, 0, "before_effect") }()
	deadline := time.Now().Add(time.Second)
	var barrier reliabilityhook.Barrier
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(dir, "barrier-*.json"))
		if len(matches) == 1 {
			raw, err := os.ReadFile(matches[0])
			if err != nil || json.Unmarshal(raw, &barrier) != nil {
				t.Fatal("Saga barrier evidence is unreadable")
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if barrier.WindowID != "saga.forward.S0.before_effect" ||
		barrier.OperationID != bookingID.String() || barrier.Facts["saga_id"] != sagaID.String() ||
		barrier.Facts["step_id"] != stepID.String() || barrier.Facts["claim_fence"] != "7" {
		t.Fatalf("Saga barrier does not identify the durable claim and logical step: %+v", barrier)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("barrier did not stop at process-control boundary: %v", err)
	}
}

func TestSagaReliabilityBarrierIsNoOpByDefault(t *testing.T) {
	service := &SagaService{}
	err := service.awaitReliabilityStepBoundary(context.Background(), domain.SagaClaim{}, 0, "before_effect")
	if err != nil {
		t.Fatalf("disabled hook changed Saga execution: %v", err)
	}
}

func TestSagaEffectBeforeResultReportsDurableIntentWithoutResultPhase(t *testing.T) {
	service := &SagaService{}
	bookingID, sagaID, stepID := uuid.New(), uuid.New(), uuid.New()
	claim := domain.SagaClaim{SagaID: sagaID, Token: uuid.New(), Fence: 8, Direction: "forward",
		Saga: &domain.Saga{ID: sagaID, BookingID: bookingID, Steps: []domain.SagaStep{{
			ID: stepID, Name: "RESERVED_SEAT_STEP", Order: 0, Status: domain.SAGA_STEP_PENDING,
		}}}}
	boundary, err := service.reliabilityStepBoundary(claim, 0, "effect_before_result")
	if err != nil {
		t.Fatal(err)
	}
	if boundary.WindowID != "saga.forward.S0.effect_before_result" ||
		boundary.Facts["phase"] != "effect_before_result" || boundary.Facts["step_status"] != "EXECUTING" ||
		boundary.Facts["claim_fence"] != "8" || boundary.Facts["step_id"] != stepID.String() {
		t.Fatalf("effect/result boundary did not carry durable intent identity: %+v", boundary)
	}
	if _, err := service.reliabilityStepBoundary(claim, 0, "result_before_next"); err == nil {
		t.Fatal("result-before-next barrier was enabled without a claim-hold contract")
	}
}
