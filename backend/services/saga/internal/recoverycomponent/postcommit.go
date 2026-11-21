package recoverycomponent

import "time"

const PostCommitContractVersion = "rushgo.saga.post-commit.v1"

// PostCommitEvent describes one repository result transaction after Commit
// returned successfully. Ownership is an intended transaction action here;
// callers must verify the live token/fence/lease using a separate DB connection.
type PostCommitEvent struct {
	ContractVersion      string    `json:"contract_version"`
	CommitOutcome        string    `json:"commit_outcome"`
	SagaID               string    `json:"saga_id"`
	StepID               string    `json:"step_id"`
	Direction            string    `json:"direction"`
	ResultState          string    `json:"result_state"`
	PriorIndex           int       `json:"prior_index"`
	NextIndex            int       `json:"next_index"`
	NextStatus           string    `json:"next_status"`
	RecoveryState        string    `json:"recovery_state"`
	ClaimAction          string    `json:"claim_action"`
	ClaimFence           int64     `json:"claim_fence"`
	ClaimTokenSHA256     string    `json:"claim_token_sha256"`
	TicketOutboxExpected bool      `json:"ticket_outbox_expected"`
	RunID                string    `json:"run_id"`
	WorkerID             string    `json:"worker_id"`
	PID                  int       `json:"pid"`
	EmittedAt            time.Time `json:"emitted_at"`
}
