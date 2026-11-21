package service_scheduler

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
	"github.com/ticketbox/scheduler/internal/domain"
)

func TestSchedulerUOWBarrierIdentifiesStagedConfigAndOutboundOperation(t *testing.T) {
	dir := t.TempDir()
	const token = "private-t23-scheduler-hook-token"
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	svc := &SchedulerService{reliabilityHook: hook}
	configID := uuid.New()
	eventID := uuid.New()
	command, err := json.Marshal(reliabilityhook.Command{
		WindowID: "scheduler.uow_second_operation_rollback", OperationID: eventID.String(),
		Checkpoint: "scheduler_uow_first_mutation_before_outbound_insert",
		Action:     reliabilityhook.ActionPause, Token: token, Nonce: "nonce-scheduler-uow",
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
	go func() {
		done <- svc.awaitUnitOfWorkBoundary(ctx,
			&domain.SchedulerConfig{Id: configID, Version: 2}, 1, eventID)
	}()

	deadline := time.Now().Add(time.Second)
	var barrier reliabilityhook.Barrier
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(dir, "barrier-*.json"))
		if len(matches) == 1 {
			raw, err := os.ReadFile(matches[0])
			if err != nil || json.Unmarshal(raw, &barrier) != nil {
				t.Fatal("Scheduler barrier evidence is unreadable")
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if barrier.WindowID != "scheduler.uow_second_operation_rollback" ||
		barrier.OperationID != eventID.String() ||
		barrier.Facts["scheduler_config_id"] != configID.String() ||
		barrier.Facts["expected_version"] != "1" ||
		barrier.Facts["staged_version"] != "2" ||
		barrier.Facts["outbound_event_id"] != eventID.String() {
		t.Fatalf("Scheduler barrier does not identify the staged UOW operation: %+v", barrier)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("barrier did not stop at the UOW boundary: %v", err)
	}
}

func TestSchedulerUOWBarrierIsNoopByDefault(t *testing.T) {
	svc := &SchedulerService{}
	err := svc.awaitUnitOfWorkBoundary(context.Background(),
		&domain.SchedulerConfig{Id: uuid.New(), Version: 2}, 1, uuid.New())
	if err != nil {
		t.Fatalf("disabled hook changed Scheduler execution: %v", err)
	}
}
