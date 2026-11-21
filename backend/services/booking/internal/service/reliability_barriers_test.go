package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
)

func TestDispatchReliabilityBoundaryCarriesClaimIdentityAndWaits(t *testing.T) {
	const token = "private-recovery-booking-test-token"
	controlDir := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: controlDir})
	if err != nil {
		t.Fatal(err)
	}
	bookingID := uuid.New()
	claim := domain.DispatchClaim{Kind: "start", Token: uuid.New(), Fence: 7,
		LeaseUntil: time.Now().Add(time.Minute), Snapshot: domain.LifecycleSnapshot{
			Booking: &domain.Booking{ID: bookingID, Managed: true, RequestID: "booking-request-42"},
		}}
	service := NewBookingService(nil, nil, nil, zap.NewNop())
	if err := service.ConfigureReliabilityHook(hook); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(controlDir, "command.json"), mustMarshalJSON(t, reliabilityhook.Command{
		WindowID: "booking.commit_before_saga_dispatch", OperationID: "booking-request-42",
		Checkpoint: "booking_dispatch_claimed_before_saga_start", Action: reliabilityhook.ActionPause,
		Token: token, Nonce: "nonce-booking-42",
	}), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := &DispatchWorker{service: service}
	done := make(chan error, 1)
	go func() { done <- worker.awaitReliabilityDispatchBoundary(context.Background(), claim) }()
	barrierPath := waitForReliabilityBarrier(t, controlDir)
	var barrier reliabilityhook.Barrier
	if err := json.Unmarshal(mustReadFile(t, barrierPath), &barrier); err != nil {
		t.Fatal(err)
	}
	if barrier.WindowID != "booking.commit_before_saga_dispatch" || barrier.OperationID != "booking-request-42" ||
		barrier.Checkpoint != "booking_dispatch_claimed_before_saga_start" ||
		barrier.Facts["booking_id"] != bookingID.String() || barrier.Facts["request_id"] != "booking-request-42" ||
		barrier.Facts["dispatch_state"] != "claimed" || barrier.Facts["dispatch_fence"] != "7" {
		t.Fatalf("Booking barrier did not preserve the durable dispatch claim identity: %+v", barrier)
	}
	if err := writeReliabilityDecision(t, controlDir, barrierPath, token, "nonce-booking-42", reliabilityhook.DecisionContinue); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("dispatch barrier returned error after exact continue decision: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch barrier did not resume after the exact control decision")
	}
}

func TestDispatchReliabilityBoundaryIsDisabledWithoutConfiguredHook(t *testing.T) {
	worker := &DispatchWorker{service: NewBookingService(nil, nil, nil, zap.NewNop())}
	claim := domain.DispatchClaim{Kind: "start", Fence: 7, Snapshot: domain.LifecycleSnapshot{
		Booking: &domain.Booking{ID: uuid.New(), Managed: true, RequestID: "booking-request-42"},
	}}
	if err := worker.awaitReliabilityDispatchBoundary(context.Background(), claim); err != nil {
		t.Fatalf("default disabled Booking path must remain a pass-through: %v", err)
	}
}

func TestEnabledDispatchBarrierRejectsMissingManagedRequestIdentity(t *testing.T) {
	hook, err := reliabilityhook.New(reliabilityhook.Config{
		Enabled: true, Token: "private-recovery-booking-test-token", Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	service := NewBookingService(nil, nil, nil, zap.NewNop())
	if err := service.ConfigureReliabilityHook(hook); err != nil {
		t.Fatal(err)
	}
	worker := &DispatchWorker{service: service}
	claim := domain.DispatchClaim{Kind: "start", Token: uuid.New(), Fence: 2,
		LeaseUntil: time.Now().Add(time.Minute), Snapshot: domain.LifecycleSnapshot{
			Booking: &domain.Booking{ID: uuid.New(), Managed: true},
		}}
	if err := worker.awaitReliabilityDispatchBoundary(context.Background(), claim); err == nil {
		t.Fatal("enabled Booking hook silently skipped a managed dispatch without request identity")
	}
}

func TestDisabledDispatchBarrierKeepsMissingRequestIdentityInert(t *testing.T) {
	worker := &DispatchWorker{service: NewBookingService(nil, nil, nil, zap.NewNop())}
	claim := domain.DispatchClaim{Kind: "start", Snapshot: domain.LifecycleSnapshot{
		Booking: &domain.Booking{Managed: true},
	}}
	if err := worker.awaitReliabilityDispatchBoundary(context.Background(), claim); err != nil {
		t.Fatalf("default Booking path changed for missing optional request identity: %v", err)
	}
}

func waitForReliabilityBarrier(t *testing.T, dir string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		paths, err := filepath.Glob(filepath.Join(dir, "barrier-*.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) == 1 {
			return paths[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("service did not publish the expected reliability barrier")
	return ""
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeReliabilityDecision(t *testing.T, dir, barrierPath, token, nonce string, decision reliabilityhook.Decision) error {
	t.Helper()
	key := filepath.Base(barrierPath)
	key = key[len("barrier-") : len(key)-len(".json")]
	data := mustMarshalJSON(t, reliabilityhook.ControlDecision{Token: token, Nonce: nonce, Decision: decision})
	return os.WriteFile(filepath.Join(dir, "decision-"+key+".json"), data, 0o600)
}
