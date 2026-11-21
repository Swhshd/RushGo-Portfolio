package cronjob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/scheduler/internal/domain"
	"go.uber.org/zap"
)

const reloadTestToken = "t23-private-token-for-unit-tests-only"

func TestReplacementBarrierRetainsOldLoadedVersionUntilContinue(t *testing.T) {
	dir := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true,
		Token: reloadTestToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	m := NewCronjobManager(zap.NewNop(), nil, WithSecond)
	if err := m.ConfigureReliabilityHook(hook); err != nil {
		t.Fatal(err)
	}
	old := domain.SchedulerConfig{Id: uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
		Name: "recovery-fixture", Version: 1, Timeout: time.Second,
		IntervalExpression: "*/5 * * * * *", Valid: true}
	m.jobConfig[old.Name] = old
	m.jobs[old.Name] = recoveryJob{}
	newConfig := old
	newConfig.Version = 2
	newConfig.IntervalExpression = "*/10 * * * * *"
	eventID := uuid.NewSHA1(uuid.NameSpaceURL,
		[]byte("ticketbox:scheduler:"+old.Id.String()+":2")).String()
	checkpoint := "scheduler_config_before_apply"
	key := reloadBoundaryKey("scheduler.config_replacement_reload", eventID, checkpoint)
	writeReloadJSON(t, filepath.Join(dir, "command.json"), map[string]string{
		"window_id": "scheduler.config_replacement_reload", "operation_id": eventID,
		"checkpoint": checkpoint, "action": "pause", "token": reloadTestToken,
		"nonce": "replacement",
	})
	done := make(chan error, 1)
	go func() { done <- m.ReRegisterJob(context.Background(), newConfig) }()
	waitReloadFile(t, filepath.Join(dir, "barrier-"+key+".json"))
	if m.jobConfig[old.Name].Version != old.Version {
		t.Fatal("replacement changed loaded version before the reload barrier continued")
	}
	writeReloadJSON(t, filepath.Join(dir, "decision-"+key+".json"), map[string]string{
		"token": reloadTestToken, "nonce": "replacement", "decision": "continue",
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := m.jobConfig[old.Name]; got.Version != newConfig.Version ||
		got.IntervalExpression != newConfig.IntervalExpression {
		t.Fatalf("replacement was not applied after continue: %+v", got)
	}
	// A delayed old event is a no-op and cannot roll the loaded version back.
	if err := m.ReRegisterJob(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if m.jobConfig[old.Name].Version != newConfig.Version {
		t.Fatal("stale version replaced the current schedule")
	}
}

func reloadBoundaryKey(windowID, operationID, checkpoint string) string {
	digest := sha256.Sum256([]byte(windowID + "\x00" + operationID + "\x00" + checkpoint))
	return hex.EncodeToString(digest[:16])
}

func writeReloadJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitReloadFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Scheduler replacement barrier was not observed: %s", path)
}
