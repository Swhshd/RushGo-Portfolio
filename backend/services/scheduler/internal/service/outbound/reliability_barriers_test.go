package service_outbound

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

const relayTestToken = "t23-private-token-for-unit-tests-only"

type relayBoundaryRepository struct {
	claim       repository.OutboundClaim
	finished    atomic.Bool
	finishCount atomic.Int32
}

func (r *relayBoundaryRepository) GetByConfigVersion(context.Context, uuid.UUID, int) (*outbound.OutboundEvent, error) {
	return nil, nil
}
func (r *relayBoundaryRepository) ClaimOutbound(context.Context, int, time.Duration) ([]repository.OutboundClaim, error) {
	return []repository.OutboundClaim{r.claim}, nil
}
func (r *relayBoundaryRepository) FinishOutbound(context.Context, repository.OutboundClaim, bool, string, time.Time) (bool, error) {
	r.finishCount.Add(1)
	r.finished.Store(true)
	return true, nil
}

type relayBoundaryPublisher struct{ published atomic.Int32 }

func (p *relayBoundaryPublisher) Publish(context.Context, string, string, pkgkafka.Event) error {
	p.published.Add(1)
	return nil
}

func TestRelayWaitsForDurableClaimDecisionBeforePublish(t *testing.T) {
	for _, windowID := range []string{
		"scheduler.outbound_claim_before_effect",
		"scheduler.multiple_instances_restart",
	} {
		t.Run(windowID, func(t *testing.T) {
			relay, repo, publisher, dir, operationID := newBoundaryRelay(t)
			checkpoint := "scheduler_claimed_before_publish"
			installRelayCommand(t, dir, windowID, operationID, checkpoint, "claim")
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _, err := relay.RunOnce(ctx, 1); done <- err }()
			key := relayBoundaryKey(windowID, operationID, checkpoint)
			waitForFile(t, filepath.Join(dir, "barrier-"+key+".json"))
			if publisher.published.Load() != 0 || repo.finished.Load() {
				t.Fatal("publish or marker advanced while the durable claim barrier was held")
			}
			installRelayDecision(t, dir, key, "claim", "continue")
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if publisher.published.Load() != 1 || repo.finishCount.Load() != 1 {
				t.Fatal("continued claim did not publish and mark exactly once")
			}
		})
	}
}

func TestRelayRestartCommandForAnotherClaimDoesNotPause(t *testing.T) {
	relay, repo, publisher, dir, _ := newBoundaryRelay(t)
	windowID := "scheduler.multiple_instances_restart"
	checkpoint := "scheduler_claimed_before_publish"
	otherOperationID := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	installRelayCommand(t, dir, windowID, otherOperationID, checkpoint, "other")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := relay.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Load() != 1 || repo.finishCount.Load() != 1 {
		t.Fatal("a command for another operation interfered with the current claim")
	}
	if _, err := os.Stat(filepath.Join(dir, "command.json")); err != nil {
		t.Fatal("the relay consumed another operation's command:", err)
	}
}

func TestRelayEffectBeforeMarkerBarrierLeavesMarkerPending(t *testing.T) {
	relay, repo, publisher, dir, operationID := newBoundaryRelay(t)
	checkpoint := "scheduler_effect_before_marker"
	windowID := "scheduler.effect_before_marker"
	installRelayCommand(t, dir, windowID, operationID, checkpoint, "effect")
	done := make(chan error, 1)
	go func() { _, err := relay.RunOnce(context.Background(), 1); done <- err }()
	key := relayBoundaryKey(windowID, operationID, checkpoint)
	waitForFile(t, filepath.Join(dir, "barrier-"+key+".json"))
	if publisher.published.Load() != 1 || repo.finished.Load() {
		t.Fatal("barrier did not prove downstream effect before local marker")
	}
	installRelayDecision(t, dir, key, "effect", "continue")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if repo.finishCount.Load() != 1 {
		t.Fatal("marker was not persisted after the barrier continued")
	}
}

func newBoundaryRelay(t *testing.T) (*Relay, *relayBoundaryRepository,
	*relayBoundaryPublisher, string, string) {
	t.Helper()
	dir := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true,
		Token: relayTestToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	configID := uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
	operationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(
		"ticketbox:scheduler:"+configID.String()+":2")).String()
	payload, err := json.Marshal(map[string]any{"Id": configID.String(), "Version": 2})
	if err != nil {
		t.Fatal(err)
	}
	repo := &relayBoundaryRepository{claim: repository.OutboundClaim{
		Event: outbound.OutboundEvent{Id: uuid.MustParse(operationID), Topic: "scheduler.config.events.t23-aaaaaaaaaaaaaaaaaaaaaaaa",
			EventType: "SchedulerConfigChanged", Payload: payload},
		Token: uuid.New(), Fence: 4, LeaseUntil: time.Now().UTC().Add(time.Minute),
		CreatedAt: time.Now().UTC(),
	}}
	publisher := &relayBoundaryPublisher{}
	relay := NewRelay(repo, publisher, zap.NewNop())
	if err := relay.ConfigureReliabilityHook(hook); err != nil {
		t.Fatal(err)
	}
	return relay, repo, publisher, dir, operationID
}

func installRelayCommand(t *testing.T, dir, windowID, operationID, checkpoint, nonce string) {
	t.Helper()
	value := map[string]string{"window_id": windowID, "operation_id": operationID,
		"checkpoint": checkpoint, "action": "pause", "token": relayTestToken, "nonce": nonce}
	writeRelayJSON(t, filepath.Join(dir, "command.json"), value)
}

func installRelayDecision(t *testing.T, dir, key, nonce, decision string) {
	t.Helper()
	writeRelayJSON(t, filepath.Join(dir, "decision-"+key+".json"),
		map[string]string{"token": relayTestToken, "nonce": nonce, "decision": decision})
}

func writeRelayJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func relayBoundaryKey(windowID, operationID, checkpoint string) string {
	digest := sha256.Sum256([]byte(windowID + "\x00" + operationID + "\x00" + checkpoint))
	return hex.EncodeToString(digest[:16])
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("barrier file was not written: %s", path)
}
