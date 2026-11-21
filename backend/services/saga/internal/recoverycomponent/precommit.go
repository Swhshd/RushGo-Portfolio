package recoverycomponent

import "time"

const PreCommitContractVersion = "rushgo.saga.pre-commit.v1"

// PreCommitEvent identifies a result write that is still inside an open
// transaction. It is a separate contract from PostCommitEvent and never
// establishes that any result has committed or is independently visible.
type PreCommitEvent struct {
	ContractVersion  string    `json:"contract_version"`
	TransactionState string    `json:"transaction_state"`
	SagaID           string    `json:"saga_id"`
	StepID           string    `json:"step_id"`
	Direction        string    `json:"direction"`
	ResultState      string    `json:"result_state"`
	PriorIndex       int       `json:"prior_index"`
	NextIndex        int       `json:"next_index"`
	ClaimFence       int64     `json:"claim_fence"`
	ClaimTokenSHA256 string    `json:"claim_token_sha256"`
	RunID            string    `json:"run_id"`
	WorkerID         string    `json:"worker_id"`
	StartID          string    `json:"start_id"`
	PID              int       `json:"pid"`
	EmittedAt        time.Time `json:"emitted_at"`
}
