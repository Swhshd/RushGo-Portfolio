//go:build reliability_component

package recoverycomponent

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"time"
)

// PreCommitControl is enabled only by the explicit component build tag and a
// separate pre-COMMIT observer endpoint. It pauses the real result transaction
// before Commit so tests can independently prove that the pending result is
// invisible and terminate the owning process to force PostgreSQL rollback.
func PreCommitControl(event PreCommitEvent) {
	address := os.Getenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR")
	runID := os.Getenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_RUN_ID")
	workerID := os.Getenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_WORKER_ID")
	startID := os.Getenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_START_ID")
	rawTimeout := os.Getenv("RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT")
	if address == "" || runID == "" || workerID == "" || startID == "" || rawTimeout == "" {
		return
	}
	timeout, err := time.ParseDuration(rawTimeout)
	if err != nil || timeout <= 0 || timeout > time.Minute {
		return
	}
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	event.ContractVersion = PreCommitContractVersion
	event.TransactionState = "OPEN_UNCOMMITTED"
	event.RunID = runID
	event.WorkerID = workerID
	event.StartID = startID
	event.PID = os.Getpid()
	event.EmittedAt = time.Now().UTC()
	if json.NewEncoder(conn).Encode(event) != nil {
		return
	}
	ack, err := bufio.NewReader(conn).ReadByte()
	if err != nil || ack != '1' {
		return
	}
}
