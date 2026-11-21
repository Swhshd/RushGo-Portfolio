//go:build !reliability_component

package recoverycomponent

import (
	"net"
	"testing"
	"time"
)

func TestDefaultBuildIgnoresPreCommitControlEnvironment(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR", listener.Addr().String())
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_RUN_ID", "default-build-must-ignore")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_WORKER_ID", "default-build-must-ignore")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_START_ID", "default-build-must-ignore")
	t.Setenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT", "2s")
	PreCommitControl(PreCommitEvent{ContractVersion: "rushgo.saga.pre-commit.v1", SagaID: "default-build"})
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.Accept(); err == nil {
		t.Fatal("environment unexpectedly enabled the default-build pre-COMMIT controller")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("unexpected listener result: %v", err)
	}
}
