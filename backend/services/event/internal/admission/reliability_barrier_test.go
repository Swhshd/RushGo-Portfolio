package admission

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/pkg/reliabilityhook"
)

type recoveryAdmissionStore struct {
	capture     CapturedAdmission
	deleteCalls atomic.Int32
}

func (s *recoveryAdmissionStore) ScanAdmissions(context.Context, uint64, int64) ([]CapturedAdmission, uint64, error) {
	return []CapturedAdmission{s.capture}, 0, nil
}

func (s *recoveryAdmissionStore) CompareDeleteAdmission(context.Context, CapturedAdmission) (bool, error) {
	s.deleteCalls.Add(1)
	return true, nil
}

type recoverySeatAuthority struct{ value domain.SeatAuthority }

func (a recoverySeatAuthority) GetSeatAuthorities(context.Context, []uuid.UUID) ([]domain.SeatAuthority, error) {
	return []domain.SeatAuthority{a.value}, nil
}

func TestReconcilerWaitsAtExactStaleAdmissionRepairBoundary(t *testing.T) {
	const token = "private-recovery-event-test-token"
	dir := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	eventID, seatID, ownerID := uuid.New(), uuid.New(), uuid.New()
	key := Key(eventID, seatID)
	store := &recoveryAdmissionStore{capture: CapturedAdmission{
		Key: key, Value: ownerID.String(), EventID: eventID, SeatID: seatID,
	}}
	worker := NewReconciler(store, recoverySeatAuthority{domain.SeatAuthority{
		SeatID: seatID, EventID: eventID, Status: domain.SeatStatusAvailable, Coherent: true,
	}})
	if err := worker.SetAdmissionRepairBarrier(hook, "sweeper-1"); err != nil {
		t.Fatal(err)
	}
	writeRecoveryJSON(t, filepath.Join(dir, "repair-command-sweeper-1.json"), reliabilityhook.Command{
		WindowID: "redis.stale_owner_exact_repair", OperationID: ownerID.String() + ".sweeper-1",
		Checkpoint: "redis_before_compare_delete", Action: reliabilityhook.ActionPause,
		Token: token, Nonce: "nonce-event-repair-1",
	})
	done := make(chan error, 1)
	go func() { _, runErr := worker.RunOnce(context.Background()); done <- runErr }()
	barrierPath := waitForEventBarrier(t, dir)
	var barrier reliabilityhook.Barrier
	if err := json.Unmarshal(readRecoveryFile(t, barrierPath), &barrier); err != nil {
		t.Fatal(err)
	}
	if barrier.WindowID != "redis.stale_owner_exact_repair" || barrier.OperationID != ownerID.String()+".sweeper-1" ||
		barrier.Checkpoint != "redis_before_compare_delete" || barrier.Facts["owner_id"] != ownerID.String() ||
		barrier.Facts["event_id"] != eventID.String() || barrier.Facts["seat_id"] != seatID.String() ||
		barrier.Facts["redis_key"] != key || barrier.Facts["sweeper_id"] != "sweeper-1" {
		t.Fatalf("Event barrier did not preserve the exact stale owner repair identity: %+v", barrier)
	}
	if store.deleteCalls.Load() != 0 {
		t.Fatal("Redis compare/delete ran before the exact barrier decision")
	}
	writeEventDecision(t, dir, barrierPath, token, "nonce-event-repair-1")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunOnce failed after the exact repair decision: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Event reconciler did not resume after the exact repair decision")
	}
	if store.deleteCalls.Load() != 1 {
		t.Fatalf("expected exactly one compare/delete after release; got %d", store.deleteCalls.Load())
	}
	paths, err := filepath.Glob(filepath.Join(dir, "repair-result-*.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one outcome record for the observed repair barrier, got %v (%v)", paths, err)
	}
	var result reliabilityhook.AdmissionRepairResult
	if err := json.Unmarshal(readRecoveryFile(t, paths[0]), &result); err != nil || !result.Deleted || result.Error {
		t.Fatalf("repair outcome did not report the actual compare/delete result: %+v (%v)", result, err)
	}
}

func TestReconcilerWithoutReliabilityBarrierKeepsDefaultRepairPath(t *testing.T) {
	eventID, seatID, ownerID := uuid.New(), uuid.New(), uuid.New()
	store := &recoveryAdmissionStore{capture: CapturedAdmission{
		Key: Key(eventID, seatID), Value: ownerID.String(), EventID: eventID, SeatID: seatID,
	}}
	worker := NewReconciler(store, recoverySeatAuthority{domain.SeatAuthority{
		SeatID: seatID, EventID: eventID, Status: domain.SeatStatusAvailable, Coherent: true,
	}})
	deleted, err := worker.RunOnce(context.Background())
	if err != nil || deleted != 1 || store.deleteCalls.Load() != 1 {
		t.Fatalf("default disabled path did not preserve repair behavior: deleted=%d calls=%d err=%v", deleted, store.deleteCalls.Load(), err)
	}
}

func waitForEventBarrier(t *testing.T, dir string) string {
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
	t.Fatal("Event reconciler did not publish the expected repair barrier")
	return ""
}

func writeRecoveryJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readRecoveryFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeEventDecision(t *testing.T, dir, barrierPath, token, nonce string) {
	t.Helper()
	name := filepath.Base(barrierPath)
	key := name[len("barrier-") : len(name)-len(".json")]
	writeRecoveryJSON(t, filepath.Join(dir, "decision-"+key+".json"), reliabilityhook.ControlDecision{
		Token: token, Nonce: nonce, Decision: reliabilityhook.DecisionContinue,
	})
}
