//go:build reliability_component || reliability_e2e

package recoverycomponent

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPostCommitSendsVersionedEventAndWaitsForExplicitAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR", listener.Addr().String())
	t.Setenv("RUSHGO_SAGA_COMPONENT_RUN_ID", "run-123")
	t.Setenv("RUSHGO_SAGA_COMPONENT_WORKER_ID", "target-456")
	t.Setenv("RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT", "2s")

	input := PostCommitEvent{
		ContractVersion:  "rushgo.saga.post-commit.v1",
		SagaID:           "saga-1",
		StepID:           "step-2",
		Direction:        "forward",
		ResultState:      "completed",
		PriorIndex:       2,
		NextIndex:        3,
		ClaimAction:      "retained",
		ClaimFence:       7,
		ClaimTokenSHA256: "test-token-digest",
	}
	finished := make(chan struct{})
	go func() {
		PostCommit(input)
		close(finished)
	}()

	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var got PostCommitEvent
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ContractVersion != input.ContractVersion || got.SagaID != input.SagaID || got.StepID != input.StepID ||
		got.Direction != input.Direction || got.ResultState != input.ResultState || got.PriorIndex != input.PriorIndex ||
		got.NextIndex != input.NextIndex || got.ClaimAction != input.ClaimAction || got.ClaimFence != input.ClaimFence ||
		got.ClaimTokenSHA256 != input.ClaimTokenSHA256 {
		t.Fatalf("post-commit event mismatch: %+v", got)
	}
	if got.RunID != "run-123" || got.WorkerID != "target-456" || got.PID != os.Getpid() {
		t.Fatalf("process identity mismatch: %+v", got)
	}
	if got.EmittedAt.IsZero() {
		t.Fatal("missing post-commit emission time")
	}
	select {
	case <-finished:
		t.Fatal("post-commit observer returned before acknowledgement")
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := conn.Write([]byte("1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("post-commit observer did not return after acknowledgement")
	}
}

func TestPostCommitDoesNothingWithoutObserverAddress(t *testing.T) {
	for _, name := range []string{"RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR", "RUSHGO_SAGA_COMPONENT_RUN_ID", "RUSHGO_SAGA_COMPONENT_WORKER_ID", "RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	done := make(chan struct{})
	go func() {
		PostCommit(PostCommitEvent{ContractVersion: "rushgo.saga.post-commit.v1", SagaID: strconv.Itoa(os.Getpid())})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("observer without an explicit address must be inert")
	}
}
