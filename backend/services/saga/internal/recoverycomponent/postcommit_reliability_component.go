//go:build reliability_component || reliability_e2e

package recoverycomponent

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"time"
)

// PostCommit emits a bounded local observation only in an explicit reliability
// build. The ACK keeps the target worker at the committed-result boundary until
// an independent observer has inspected PostgreSQL. It never changes claim or
// lease state. The reliability_e2e build is the formal T23 topology; component
// builds retain the same wire contract and dial/ACK behavior.
func PostCommit(event PostCommitEvent) {
	address := os.Getenv("RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR")
	runID := os.Getenv("RUSHGO_SAGA_COMPONENT_RUN_ID")
	workerID := os.Getenv("RUSHGO_SAGA_COMPONENT_WORKER_ID")
	rawTimeout := os.Getenv("RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT")
	if address == "" || runID == "" || workerID == "" || rawTimeout == "" {
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
	event.RunID = runID
	event.WorkerID = workerID
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
