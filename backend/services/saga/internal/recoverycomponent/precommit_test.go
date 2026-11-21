//go:build reliability_component

package recoverycomponent

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"
)

func TestPreCommitControlEmitsDistinctUncommittedBoundaryAndWaitsForAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR", listener.Addr().String())
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_RUN_ID", "run-precommit")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_WORKER_ID", "precommit-target")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_START_ID", "start-precommit")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT", "2s")

	input := PreCommitEvent{
		ContractVersion:  PreCommitContractVersion,
		TransactionState: "OPEN_UNCOMMITTED",
		SagaID:           "saga-before-commit",
		StepID:           "step-result-pending",
		Direction:        "forward",
		ResultState:      "completed",
		PriorIndex:       0,
		NextIndex:        1,
		ClaimFence:       17,
		ClaimTokenSHA256: "token-digest",
	}
	done := make(chan struct{})
	go func() {
		PreCommitControl(input)
		close(done)
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
	var got PreCommitEvent
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ContractVersion != PreCommitContractVersion || got.TransactionState != "OPEN_UNCOMMITTED" ||
		got.SagaID != input.SagaID || got.StepID != input.StepID || got.Direction != input.Direction ||
		got.ResultState != input.ResultState || got.PriorIndex != input.PriorIndex || got.NextIndex != input.NextIndex ||
		got.ClaimFence != input.ClaimFence || got.ClaimTokenSHA256 != input.ClaimTokenSHA256 ||
		got.RunID != "run-precommit" || got.WorkerID != "precommit-target" || got.StartID != "start-precommit" || got.PID != os.Getpid() || got.EmittedAt.IsZero() {
		t.Fatalf("pre-COMMIT event mismatch: %+v", got)
	}
	select {
	case <-done:
		t.Fatal("pre-COMMIT control returned before the observer acknowledged it")
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := conn.Write([]byte("1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pre-COMMIT control did not return after acknowledgement")
	}
}

func TestPreCommitControlDoesNothingWithoutAddress(t *testing.T) {
	for _, key := range []string{
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR",
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_RUN_ID",
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_WORKER_ID",
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_START_ID",
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT",
	} {
		t.Setenv(key, "")
	}
	done := make(chan struct{})
	go func() {
		PreCommitControl(PreCommitEvent{ContractVersion: PreCommitContractVersion, SagaID: "no-address"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("pre-COMMIT control without an explicit address must be inert")
	}
}
