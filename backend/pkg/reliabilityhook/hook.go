// Package reliabilityhook contains private, opt-in T23 fault barriers.
// Nothing in this package is active unless the isolated acceptance runtime
// explicitly enables it and supplies its private control volume and token.
package reliabilityhook

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	commandFile                  = "command.json"
	maxControlFileBytes          = 32 * 1024
	ActionPause                  = "pause"
	ActionAckGate                = "ack_gate"
	DecisionContinue    Decision = "continue"
	DecisionWithhold    Decision = "withhold"
	DecisionFailOnce    Decision = "fail_once"
)

// ErrUnresolvedClaim means this exact boundary was durably claimed, but no
// explicit terminal decision has released it. Callers must not run the effect.
var ErrUnresolvedClaim = errors.New("reliability boundary has an unresolved claimed command")

// ErrKafkaBrokerRevocationProofMissing marks the old-generation rebalance
// window as unsupported when only work-context cancellation is observable.
var ErrKafkaBrokerRevocationProofMissing = errors.New("HUMAN_GATE_PROOF_GAP: BROKER_REVOCATION_PROOF_MISSING")

var controlIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,160}$`)
var paymentEventIDPattern = regexp.MustCompile(`^evt_[A-Za-z0-9_]{1,180}$`)
var containerIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var sagaStepPattern = regexp.MustCompile(`^S[0-4]$`)

type Decision string

type Config struct {
	Enabled   bool
	Token     string
	Directory string
}

type Controller struct {
	config   Config
	activeMu sync.Mutex
	active   map[string]Command
}

type Command struct {
	WindowID    string `json:"window_id"`
	OperationID string `json:"operation_id"`
	Checkpoint  string `json:"checkpoint"`
	Action      string `json:"action"`
	Token       string `json:"token"`
	Nonce       string `json:"nonce"`
}

type Boundary struct {
	WindowID    string
	OperationID string
	Checkpoint  string
	Action      string
	Facts       map[string]string
}

type Barrier struct {
	WindowID    string            `json:"window_id"`
	OperationID string            `json:"operation_id"`
	Checkpoint  string            `json:"checkpoint"`
	Action      string            `json:"action"`
	Nonce       string            `json:"nonce"`
	ObservedAt  string            `json:"observed_at"`
	Facts       map[string]string `json:"facts,omitempty"`
}

type AdmissionRepairResult struct {
	WindowID    string            `json:"window_id"`
	OperationID string            `json:"operation_id"`
	Checkpoint  string            `json:"checkpoint"`
	Nonce       string            `json:"nonce"`
	ObservedAt  string            `json:"observed_at"`
	Facts       map[string]string `json:"facts"`
	Deleted     bool              `json:"deleted"`
	Error       bool              `json:"error"`
}

// KafkaInjectedFailure records that the exact A-frontier fail-once decision
// reached the consumer immediately before it returned a retryable handler error.
type KafkaInjectedFailure struct {
	WindowID   string `json:"window_id"`
	GroupID    string `json:"group_id"`
	Generation int32  `json:"generation"`
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     int64  `json:"offset"`
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	ObservedAt string `json:"observed_at"`
}

// KafkaRebalanceCommitAttempt is recorded only after an old handler is
// cancelled by a real consumer-group generation change and its stale offset
// commit is rejected by the broker.
type KafkaRebalanceCommitAttempt struct {
	WindowID     string `json:"window_id"`
	GroupID      string `json:"group_id"`
	Generation   int32  `json:"generation"`
	Topic        string `json:"topic"`
	Partition    int    `json:"partition"`
	RecordOffset int64  `json:"record_offset"`
	CommitOffset int64  `json:"commit_offset"`
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	ErrorCode    string `json:"error_code"`
	ObservedAt   string `json:"observed_at"`
}

type RequestReceipt struct {
	WindowID    string `json:"window_id"`
	OperationID string `json:"operation_id"`
	Checkpoint  string `json:"checkpoint"`
	Nonce       string `json:"nonce"`
	ReceivedAt  string `json:"request_received_at"`
}

type ControlDecision struct {
	Token             string                  `json:"token"`
	Nonce             string                  `json:"nonce"`
	Decision          Decision                `json:"decision"`
	Proof             *AckProof               `json:"proof,omitempty"`
	PaymentProof      *PaymentRelayProof      `json:"payment_proof,omitempty"`
	NotificationProof *NotificationFaultProof `json:"notification_proof,omitempty"`
	SagaRecoveryProof *SagaRecoveryProof      `json:"saga_recovery_proof,omitempty"`
}

// SagaRecoveryProof grants one retry of an exact Saga boundary after the host
// observed SIGKILL of the original process incarnation.
type SagaRecoveryProof struct {
	Kind           string `json:"kind"`
	WindowID       string `json:"window_id"`
	OperationID    string `json:"operation_id"`
	BookingID      string `json:"booking_id"`
	SagaID         string `json:"saga_id"`
	StepID         string `json:"step_id"`
	StepName       string `json:"step_name"`
	Direction      string `json:"direction"`
	Phase          string `json:"phase"`
	ClaimFence     string `json:"claim_fence"`
	FaultKind      string `json:"fault_kind"`
	EffectObserved bool   `json:"effect_observed,omitempty"`
	ContainerID    string `json:"container_id"`
	PID            int    `json:"pid"`
	StartedAt      string `json:"started_at"`
	ProcessStopped bool   `json:"process_stopped"`
}

// NotificationFaultProof binds the one-shot UNKNOWN outcome to the exact
// semantic notification whose sender attempt and durable state were observed.
type NotificationFaultProof struct {
	Kind                  string `json:"kind"`
	OperationID           string `json:"operation_id"`
	Observed              bool   `json:"observed"`
	SenderAttemptObserved bool   `json:"sender_attempt_observed"`
	ResultUnknown         bool   `json:"result_unknown"`
	UnknownStatePersisted bool   `json:"unknown_state_persisted"`
}

// PaymentRelayProof binds a withheld Kafka acknowledgement to the exact
// durable receipt, Payment operation, and broker record observed by T23.
type PaymentRelayProof struct {
	Kind              string `json:"kind"`
	PaymentID         string `json:"payment_id"`
	BookingID         string `json:"booking_id"`
	ReceiptEventID    string `json:"receipt_event_id"`
	EnvelopeID        string `json:"envelope_id"`
	ClaimToken        string `json:"claim_token"`
	ClaimFence        int64  `json:"claim_fence"`
	LeaseUntil        string `json:"lease_until"`
	Topic             string `json:"topic"`
	Partition         int32  `json:"partition"`
	Offset            int64  `json:"offset"`
	RecordAtBroker    bool   `json:"record_at_broker"`
	MarkerUncommitted bool   `json:"marker_uncommitted"`
	ObservedAt        string `json:"observed_at"`
}

type AckProof struct {
	Kind          string `json:"kind"`
	BookingID     string `json:"booking_id"`
	SagaID        string `json:"saga_id,omitempty"`
	DecisionID    string `json:"decision_id,omitempty"`
	OutboxEventID string `json:"outbox_event_id,omitempty"`
	Durable       bool   `json:"durable"`
	ObservedAt    string `json:"observed_at"`
}

func New(config Config) (*Controller, error) {
	return internalNew(config)
}

// internalNew validates a fully explicit hook configuration. Tests use it to
// exercise the control protocol without requiring a product entry point to
// enable acceptance mode.
func internalNew(config Config) (*Controller, error) {
	if !config.Enabled {
		return &Controller{}, nil
	}
	if len(config.Token) < 24 || strings.ContainsAny(config.Token, "\r\n\x00") {
		return nil, errors.New("reliability hook token is invalid")
	}
	if !filepath.IsAbs(config.Directory) || filepath.Clean(config.Directory) != config.Directory {
		return nil, errors.New("reliability hook directory is invalid")
	}
	info, err := os.Lstat(config.Directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("reliability hook directory is unavailable or unsafe")
	}
	return &Controller{config: config, active: make(map[string]Command)}, nil
}

// FromEnv is disabled by default. Enabling it also requires the explicit
// reliability_e2e build tag, a private token, and a pre-mounted control directory.
func FromEnv() (*Controller, error) {
	raw := strings.TrimSpace(os.Getenv("RELIABILITY_ACCEPTANCE_MODE"))
	token := os.Getenv("RELIABILITY_FAULT_TOKEN")
	directory := os.Getenv("RELIABILITY_CONTROL_DIR")
	if raw == "" || strings.EqualFold(raw, "false") || raw == "0" {
		if token != "" || directory != "" {
			return nil, errors.New("reliability hook credentials are set while acceptance mode is disabled")
		}
		return internalNew(Config{})
	}
	if !strings.EqualFold(raw, "true") && raw != "1" {
		return nil, errors.New("invalid reliability acceptance mode")
	}
	if !acceptanceBuildAllowed {
		return nil, errors.New("reliability acceptance mode requires the reliability_e2e build tag")
	}
	return internalNew(Config{Enabled: true, Token: token, Directory: directory})
}

func (c *Controller) Enabled() bool { return c != nil && c.config.Enabled }

// HasMatchingCommand reports whether the exact authenticated command for this
// boundary is pending or already durably claimed. It does not claim commands,
// publish barriers, or wait for a decision.
func (c *Controller) HasMatchingCommand(boundary Boundary) bool {
	if !c.Enabled() || !validBoundary(boundary) {
		return false
	}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	c.activeMu.Lock()
	active, activeOK := c.active[key]
	c.activeMu.Unlock()
	if activeOK {
		return c.matchesActiveBoundary(key, active, boundary)
	}
	claimedPath := filepath.Join(c.config.Directory, ".claimed-"+key+".json")
	if _, err := os.Lstat(claimedPath); err == nil {
		command, readErr := readJSONFile[Command](claimedPath)
		return readErr == nil && c.matchesActiveBoundary(key, command, boundary)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false
	}
	command, err := readJSONFile[Command](filepath.Join(c.config.Directory, commandFile))
	return err == nil && c.matchesBoundary(command, boundary)
}

// Await publishes an identity-bound barrier and waits for one private control
// decision. A nonmatching or wrongly tokened command is a normal pass-through.
func (c *Controller) Await(ctx context.Context, boundary Boundary) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	return c.await(ctx, boundary, filepath.Join(c.config.Directory, commandFile))
}

// AwaitW28Refund keeps the W28 refund observation independent of the
// concurrent initial-receipt observation. It uses the same claim and decision checks.
func (c *Controller) AwaitW28Refund(ctx context.Context, boundary Boundary) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	if boundary.WindowID != "saga.late_collection.compensation_verified_full_refund" ||
		boundary.Checkpoint != "payment_w28_verified_refund_before_marker" || boundary.Action != ActionPause {
		return DecisionContinue, false, errors.New("invalid W28 refund observation boundary")
	}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	return c.await(ctx, boundary, filepath.Join(c.config.Directory, "w28-refund-command-"+key+".json"))
}

func (c *Controller) await(ctx context.Context, boundary Boundary, commandPath string) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	if !validBoundary(boundary) {
		return DecisionContinue, false, errors.New("invalid reliability boundary")
	}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	claimedPath := filepath.Join(c.config.Directory, ".claimed-"+key+".json")
	c.activeMu.Lock()
	active, activeOK := c.active[key]
	c.activeMu.Unlock()
	if activeOK {
		if c.matchesActiveBoundary(key, active, boundary) {
			return c.waitDecision(ctx, boundary, key, active.Nonce)
		}
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	if _, statErr := os.Lstat(claimedPath); statErr == nil {
		// A newly queued command must never replace an unresolved durable claim
		// for this exact boundary. Resolve the old claim first; on cancellation,
		// malformed state, or changed identity this returns ErrUnresolvedClaim.
		return c.resumeClaimedBoundary(boundary, key)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	command, err := readJSONFile[Command](commandPath)
	if errors.Is(err, os.ErrNotExist) {
		return c.joinActive(ctx, boundary, key)
	}
	if err != nil {
		return DecisionContinue, false, errors.New("reliability control command is invalid")
	}
	if !c.matchesBoundary(command, boundary) {
		return c.joinActive(ctx, boundary, key)
	}
	c.activeMu.Lock()
	if active, ok := c.active[key]; ok {
		c.activeMu.Unlock()
		if c.matchesActiveBoundary(key, active, boundary) {
			return c.waitDecision(ctx, boundary, key, active.Nonce)
		}
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	if err := os.Rename(commandPath, claimedPath); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrExist) {
			active, ok := c.active[key]
			c.activeMu.Unlock()
			if ok && c.matchesActiveBoundary(key, active, boundary) {
				return c.waitDecision(ctx, boundary, key, active.Nonce)
			}
			return c.resumeClaimedBoundary(boundary, key)
		}
		c.activeMu.Unlock()
		return DecisionContinue, false, errors.New("reliability control command could not be claimed")
	}
	c.active[key] = command
	c.activeMu.Unlock()
	barrier := Barrier{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: boundary.Action, Nonce: command.Nonce,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Facts: cloneFacts(boundary.Facts)}
	if err := writeJSONAtomic(filepath.Join(c.config.Directory, barrierFile(key)), barrier); err != nil {
		c.clearActive(key, command.Nonce)
		return DecisionContinue, true, errors.New("reliability barrier could not be recorded")
	}
	defer c.clearActive(key, command.Nonce)
	return c.waitDecision(ctx, boundary, key, command.Nonce)
}

// AwaitKafkaRecord observes a private T23 command only when it names the exact
// Kafka group, candidate topic, and record identity currently being handled.
// The caller supplies broker/consumer metadata captured from the fetched record.
func (c *Controller) AwaitKafkaRecord(ctx context.Context, groupID, topic string,
	generation int32, partition int, offset int64, eventID, eventType string) (Decision, bool, error) {
	return c.AwaitKafkaRecordAtCheckpoint(ctx, groupID, topic, generation, partition,
		offset, eventID, eventType, "kafka_before_handler")
}

// AwaitKafkaRecordAtCheckpoint permits one second A-frontier pause after the
// injected failure, so the driver can independently read broker offsets while
// A remains unresolved and before the same record is retried.
func (c *Controller) AwaitKafkaRecordAtCheckpoint(ctx context.Context, groupID, topic string,
	generation int32, partition int, offset int64, eventID, eventType, checkpoint string) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	var windowIDs []string
	switch checkpoint {
	case "kafka_before_handler":
		windowIDs = []string{
			"kafka.partition.A_failure_offset_frontier",
			"kafka.partition.restart_replay_A_then_B",
			"kafka.partition.rebalance_old_generation_fenced",
			"kafka.partition.commit_ack_uncertainty",
			"kafka.partition.poison_stalls_later_records",
			"kafka.broker.outage_restart_recovery",
			"kafka.group.notification-booking-group.restart_fence",
			"kafka.group.notification-user-group.restart_fence",
			"kafka.group.scheduler-config-event-group.restart_fence",
			"kafka.group.saga-payment-group.restart_fence",
		}
	case "kafka_retry_before_handler":
		windowIDs = []string{"kafka.partition.A_failure_offset_frontier"}
	case "kafka_rebalance_new_generation_before_handler":
		windowIDs = []string{"kafka.partition.rebalance_old_generation_fenced"}
	default:
		return DecisionContinue, false, nil
	}
	facts := map[string]string{
		"group_id": groupID, "generation": strconv.FormatInt(int64(generation), 10),
		"topic": topic, "partition": strconv.Itoa(partition),
		"offset":   strconv.FormatInt(offset, 10),
		"event_id": eventID, "event_type": eventType,
	}
	checkUnresolvedClaim := func() (Decision, bool, error) {
		for _, windowID := range windowIDs {
			decision, matched, err := c.rejectInvalidBoundaryIfClaimed(Boundary{
				WindowID: windowID, OperationID: eventID, Checkpoint: checkpoint,
				Action: ActionPause, Facts: facts,
			})
			if matched || err != nil {
				return decision, matched, err
			}
		}
		return DecisionContinue, false, nil
	}
	if generation <= 0 || partition < 0 || offset < 0 ||
		!controlIdentityPattern.MatchString(eventID) ||
		!controlIdentityPattern.MatchString(eventType) || strings.ToLower(eventID) != eventID {
		return checkUnresolvedClaim()
	}
	if _, err := parseUUID(eventID); err != nil {
		return checkUnresolvedClaim()
	}
	for _, windowID := range windowIDs {
		if !validKafkaWindowGroupTopic(windowID, groupID, topic) {
			decision, matched, err := c.rejectInvalidBoundaryIfClaimed(Boundary{
				WindowID: windowID, OperationID: eventID, Checkpoint: checkpoint,
				Action: ActionPause, Facts: facts,
			})
			if matched || err != nil {
				return decision, matched, err
			}
			continue
		}
		decision, matched, err := c.Await(ctx, Boundary{WindowID: windowID,
			OperationID: eventID, Checkpoint: checkpoint, Action: ActionPause, Facts: facts})
		if matched || err != nil {
			return decision, matched, err
		}
	}
	return DecisionContinue, false, nil
}

// AwaitNotificationDelivery exposes only the semantic notification identity and
// the exact durable delivery claim. Recipient and payload data are never
// accepted into a T23 barrier. The command must name one of the four approved
// Notification windows and its exact checkpoint.
func (c *Controller) AwaitNotificationDelivery(ctx context.Context, windowID,
	notificationID, checkpoint string, facts map[string]string) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	if !validNotificationBoundary(windowID, notificationID, checkpoint, facts) {
		return c.rejectInvalidBoundaryIfClaimed(Boundary{
			WindowID: windowID, OperationID: notificationID, Checkpoint: checkpoint,
			Action: ActionPause, Facts: facts,
		})
	}
	return c.Await(ctx, Boundary{WindowID: windowID, OperationID: notificationID,
		Checkpoint: checkpoint, Action: ActionPause, Facts: facts})
}

// rejectInvalidBoundaryIfClaimed preserves pass-through behavior for malformed
// calls with no matching reliability claim, while preventing changed metadata
// from hiding an already claimed exact operation boundary.
func (c *Controller) rejectInvalidBoundaryIfClaimed(boundary Boundary) (Decision, bool, error) {
	if !c.Enabled() || !validBoundary(boundary) {
		return DecisionContinue, false, nil
	}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	c.activeMu.Lock()
	_, active := c.active[key]
	c.activeMu.Unlock()
	if active {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	claimedPath := filepath.Join(c.config.Directory, ".claimed-"+key+".json")
	if _, err := os.Lstat(claimedPath); err == nil {
		return DecisionContinue, true, ErrUnresolvedClaim
	} else if !errors.Is(err, os.ErrNotExist) {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	return DecisionContinue, false, nil
}

// AwaitSchedulerBoundary pauses only the approved T23 Scheduler delivery
// boundaries. Facts contain stable config/outbound identity and, for relay
// claims, the exact durable fencing lease; no payload or credentials are sent.
func (c *Controller) AwaitSchedulerBoundary(ctx context.Context, windowID,
	operationID, checkpoint string, facts map[string]string) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	if !validSchedulerBoundary(windowID, operationID, checkpoint, facts) {
		return DecisionContinue, false, errors.New("invalid Scheduler reliability boundary")
	}
	command, err := readJSONFile[Command](filepath.Join(c.config.Directory, commandFile))
	if errors.Is(err, os.ErrNotExist) {
		return c.Await(ctx, Boundary{WindowID: windowID, OperationID: operationID,
			Checkpoint: checkpoint, Action: ActionPause, Facts: facts})
	}
	if err != nil {
		return DecisionContinue, false, errors.New("Scheduler reliability command is invalid")
	}
	if command.WindowID != windowID || command.OperationID != operationID ||
		command.Checkpoint != checkpoint || command.Action != ActionPause ||
		!tokenMatches(c.config.Token, command.Token) {
		return DecisionContinue, false, nil
	}
	return c.Await(ctx, Boundary{WindowID: windowID, OperationID: operationID,
		Checkpoint: checkpoint, Action: ActionPause, Facts: facts})
}

func validSchedulerBoundary(windowID, operationID, checkpoint string,
	facts map[string]string) bool {
	checkpoints := map[string]string{
		"scheduler.outbound_claim_before_effect": "scheduler_claimed_before_publish",
		"scheduler.effect_before_marker":         "scheduler_effect_before_marker",
		"scheduler.config_replacement_reload":    "scheduler_config_before_apply",
		"scheduler.multiple_instances_restart":   "scheduler_claimed_before_publish",
	}
	if expected, ok := checkpoints[windowID]; !ok || expected != checkpoint ||
		!validControlIdentity(operationID) || len(facts) < 4 || len(facts) > 8 ||
		facts["outbound_event_id"] == "" || facts["outbound_event_id"] != operationID ||
		!validControlIdentity(facts["scheduler_config_id"]) {
		return false
	}
	if _, err := parseUUID(operationID); err != nil {
		return false
	}
	if _, err := parseUUID(facts["scheduler_config_id"]); err != nil {
		return false
	}
	version, err := strconv.Atoi(facts["version"])
	if err != nil || version < 2 || strconv.Itoa(version) != facts["version"] {
		return false
	}
	if windowID == "scheduler.config_replacement_reload" {
		prior, err := strconv.Atoi(facts["prior_version"])
		return err == nil && prior > 0 && prior+1 == version &&
			strconv.Itoa(prior) == facts["prior_version"] && len(facts) == 4
	}
	if len(facts) != 8 || !validControlIdentity(facts["claim_token"]) {
		return false
	}
	if _, err := parseUUID(facts["claim_token"]); err != nil {
		return false
	}
	fence, err := strconv.ParseInt(facts["claim_fence"], 10, 64)
	if err != nil || fence < 1 || strconv.FormatInt(fence, 10) != facts["claim_fence"] {
		return false
	}
	lease, err := time.Parse(time.RFC3339Nano, facts["lease_until"])
	return err == nil && lease.Location() == time.UTC &&
		lease.UTC().Format(time.RFC3339Nano) == facts["lease_until"] &&
		validControlIdentity(facts["topic"]) && facts["event_type"] == "SchedulerConfigChanged"
}

// AwaitAdmissionRepair pauses the Event Redis reconciler only after PostgreSQL
// has proven the captured Redis owner stale and immediately before the atomic
// compare/delete. Separate operation identities make concurrent sweepers
// independently observable while the facts retain the same business owner.
func (c *Controller) AwaitAdmissionRepair(ctx context.Context, ownerID,
	eventID, seatID, redisKey, sweeperID string) (Decision, bool, error) {
	if !c.Enabled() {
		return DecisionContinue, false, nil
	}
	if !validAdmissionRepairBoundary("redis.stale_owner_exact_repair", ownerID, eventID, seatID, redisKey, sweeperID) &&
		!validAdmissionRepairBoundary("redis.two_sweepers_fenced", ownerID, eventID, seatID, redisKey, sweeperID) {
		return DecisionContinue, false, errors.New("invalid Event admission repair boundary")
	}
	checkpoint := "redis_before_compare_delete"
	commandPath := filepath.Join(c.config.Directory, admissionRepairCommandFile(sweeperID))
	operationID := ownerID + "." + sweeperID
	facts := map[string]string{"owner_id": ownerID, "event_id": eventID,
		"seat_id": seatID, "redis_key": redisKey, "sweeper_id": sweeperID}
	for _, windowID := range []string{
		"redis.stale_owner_exact_repair", "redis.two_sweepers_fenced",
	} {
		if !validAdmissionRepairBoundary(windowID, ownerID, eventID, seatID, redisKey, sweeperID) {
			continue
		}
		decision, matched, err := c.await(ctx, Boundary{WindowID: windowID,
			OperationID: operationID, Checkpoint: checkpoint, Action: ActionPause,
			Facts: facts}, commandPath)
		if matched || err != nil {
			return decision, matched, err
		}
	}
	return DecisionContinue, false, nil
}

// RecordAdmissionRepairOutcome records the result of the exact atomic Lua
// compare/delete that followed an observed barrier; error text is never saved.
func (c *Controller) RecordAdmissionRepairOutcome(ownerID, eventID, seatID,
	redisKey, sweeperID string, deleted bool, repairErr error) error {
	if !c.Enabled() {
		return nil
	}
	if !validAdmissionRepairBoundary("redis.two_sweepers_fenced", ownerID, eventID,
		seatID, redisKey, sweeperID) {
		return errors.New("invalid Event admission repair outcome identity")
	}
	operationID := ownerID + "." + sweeperID
	checkpoint := "redis_before_compare_delete"
	for _, windowID := range []string{"redis.stale_owner_exact_repair", "redis.two_sweepers_fenced"} {
		key := boundaryKey(windowID, operationID, checkpoint)
		barrier, err := readJSONFile[Barrier](filepath.Join(c.config.Directory, barrierFile(key)))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || barrier.WindowID != windowID || barrier.OperationID != operationID ||
			barrier.Checkpoint != checkpoint || barrier.Action != ActionPause ||
			!validControlIdentity(barrier.Nonce) || barrier.Facts["owner_id"] != ownerID ||
			barrier.Facts["event_id"] != eventID || barrier.Facts["seat_id"] != seatID ||
			barrier.Facts["redis_key"] != redisKey || barrier.Facts["sweeper_id"] != sweeperID {
			return errors.New("Event admission repair result does not match its observed barrier")
		}
		result := AdmissionRepairResult{WindowID: windowID, OperationID: operationID,
			Checkpoint: checkpoint, Nonce: barrier.Nonce,
			ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Facts:      cloneFacts(barrier.Facts), Deleted: deleted, Error: repairErr != nil}
		return writeJSONAtomic(filepath.Join(c.config.Directory,
			repairResultFile(key, barrier.Nonce)), result)
	}
	return nil
}

func validAdmissionRepairBoundary(windowID, ownerID, eventID, seatID, redisKey,
	sweeperID string) bool {
	if windowID != "redis.stale_owner_exact_repair" && windowID != "redis.two_sweepers_fenced" {
		return false
	}
	for _, value := range []string{ownerID, eventID, seatID} {
		if _, err := parseUUID(value); err != nil || strings.ToLower(value) != value {
			return false
		}
	}
	if redisKey != "rushgo:seat-admission:"+eventID+":"+seatID ||
		(sweeperID != "sweeper-1" && sweeperID != "sweeper-2") {
		return false
	}
	return true
}

func validNotificationBoundary(windowID, notificationID, checkpoint string,
	facts map[string]string) bool {
	checkpoints := map[string]string{
		"notification.intent_before_sender":         "notification_before_sender",
		"notification.sender_effect_before_marker":  "notification_sender_effect_before_marker",
		"notification.unknown_retry_attention":      "notification_unknown_before_retry",
		"notification.stale_fence_multiple_workers": "notification_stale_before_sender",
	}
	if windowID == "notification.unknown_retry_attention" &&
		checkpoint == "notification_unknown_seed_before_sender" {
		checkpoints[windowID] = checkpoint
	}
	if expected, ok := checkpoints[windowID]; !ok || expected != checkpoint ||
		!validControlIdentity(notificationID) || len(facts) < 9 || len(facts) > 10 {
		return false
	}
	if _, err := parseUUID(notificationID); err != nil || strings.ToLower(notificationID) != notificationID ||
		facts["notification_id"] != notificationID || facts["delivery_id"] != notificationID {
		return false
	}
	allowed := map[string]bool{
		"notification_id": true, "delivery_id": true, "notification_type": true,
		"entity_id": true, "prior_status": true, "delivery_token": true,
		"delivery_fence": true, "delivery_attempts": true, "delivery_lease_until": true,
	}
	if windowID == "notification.sender_effect_before_marker" {
		allowed["sender_result"] = true
		if facts["sender_result"] != "sent" {
			return false
		}
	}
	for key := range facts {
		if !allowed[key] {
			return false
		}
	}
	for key := range allowed {
		_, present := facts[key]
		if key == "sender_result" {
			if present != (windowID == "notification.sender_effect_before_marker") {
				return false
			}
		} else if !present {
			return false
		}
	}
	for _, name := range []string{"entity_id", "delivery_token"} {
		if _, err := parseUUID(facts[name]); err != nil {
			return false
		}
	}
	switch facts["notification_type"] {
	case "booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email":
	default:
		return false
	}
	switch windowID {
	case "notification.intent_before_sender":
		if facts["prior_status"] != "PENDING" {
			return false
		}
	case "notification.unknown_retry_attention":
		if checkpoint == "notification_unknown_seed_before_sender" && facts["prior_status"] != "PENDING" ||
			checkpoint == "notification_unknown_before_retry" && facts["prior_status"] != "UNKNOWN" {
			return false
		}
	case "notification.stale_fence_multiple_workers":
		if facts["prior_status"] != "PENDING" && facts["prior_status"] != "RETRYABLE" && facts["prior_status"] != "UNKNOWN" {
			return false
		}
	case "notification.sender_effect_before_marker":
		if facts["prior_status"] != "PENDING" && facts["prior_status"] != "RETRYABLE" && facts["prior_status"] != "UNKNOWN" {
			return false
		}
	}
	for _, field := range []string{"delivery_fence", "delivery_attempts"} {
		value, err := strconv.ParseInt(facts[field], 10, 64)
		if err != nil || value < 1 || strconv.FormatInt(value, 10) != facts[field] {
			return false
		}
	}
	lease, err := time.Parse(time.RFC3339Nano, facts["delivery_lease_until"])
	return err == nil && lease.Location() == time.UTC &&
		lease.UTC().Format(time.RFC3339Nano) == facts["delivery_lease_until"]
}

// KafkaRebalanceBoundaryClaimed confirms that the exact seven Kafka identity
// facts and authenticated command nonce still match the stored old-generation
// barrier. The probe has no effect when reliability mode is disabled.
func (c *Controller) KafkaRebalanceBoundaryClaimed(groupID, topic string,
	generation int32, partition int, offset int64, eventID, eventType string) bool {
	if !c.Enabled() || generation <= 0 || partition < 0 || offset < 0 ||
		!controlIdentityPattern.MatchString(eventType) || groupID != "notification-booking-group" ||
		!validKafkaWindowGroupTopic("kafka.partition.rebalance_old_generation_fenced", groupID, topic) {
		return false
	}
	if _, err := parseUUID(eventID); err != nil {
		return false
	}
	key := boundaryKey("kafka.partition.rebalance_old_generation_fenced", eventID,
		"kafka_before_handler")
	boundary := Boundary{WindowID: "kafka.partition.rebalance_old_generation_fenced",
		OperationID: eventID, Checkpoint: "kafka_before_handler", Action: ActionPause}
	command, err := readJSONFile[Command](filepath.Join(c.config.Directory, ".claimed-"+key+".json"))
	if err != nil || !c.matchesBoundary(command, boundary) {
		return false
	}
	barrier, err := readJSONFile[Barrier](filepath.Join(c.config.Directory, barrierFile(key)))
	if err != nil || barrier.WindowID != boundary.WindowID ||
		barrier.OperationID != boundary.OperationID || barrier.Checkpoint != boundary.Checkpoint ||
		barrier.Action != boundary.Action || barrier.Nonce != command.Nonce ||
		barrier.Nonce == "" || barrier.ObservedAt == "" {
		return false
	}
	facts := map[string]string{
		"group_id": groupID, "generation": strconv.FormatInt(int64(generation), 10),
		"topic": topic, "partition": strconv.Itoa(partition),
		"offset": strconv.FormatInt(offset, 10), "event_id": eventID,
		"event_type": eventType,
	}
	return len(barrier.Facts) == len(facts) && sameFacts(barrier.Facts, facts)
}

// RecordKafkaRebalanceCommitAttempt persists the redacted result from an exact
// old-generation OffsetCommit request. Only an IllegalGeneration response is
// considered a proof of broker fencing; transport errors fail closed later.
func (c *Controller) RecordKafkaRebalanceCommitAttempt(attempt KafkaRebalanceCommitAttempt) error {
	if !c.KafkaRebalanceBoundaryClaimed(attempt.GroupID, attempt.Topic,
		attempt.Generation, attempt.Partition, attempt.RecordOffset,
		attempt.EventID, attempt.EventType) ||
		attempt.WindowID != "kafka.partition.rebalance_old_generation_fenced" ||
		attempt.CommitOffset != attempt.RecordOffset+1 ||
		(attempt.ErrorCode != "ILLEGAL_GENERATION" && attempt.ErrorCode != "ACCEPTED" &&
			attempt.ErrorCode != "OTHER") || attempt.ObservedAt != "" {
		return errors.New("invalid Kafka rebalance commit attempt")
	}
	attempt.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	key := boundaryKey(attempt.WindowID, attempt.EventID, "kafka_before_handler")
	return writeJSONExclusive(filepath.Join(c.config.Directory, kafkaRebalanceFile(key)), attempt)
}

// RecordKafkaInjectedFailure persists evidence only after the one-shot
// A-frontier hook has instructed the consumer to fail this record.
func (c *Controller) RecordKafkaInjectedFailure(failure KafkaInjectedFailure) error {
	if !c.Enabled() || failure.WindowID != "kafka.partition.A_failure_offset_frontier" ||
		failure.Generation <= 0 || failure.Partition < 0 || failure.Offset < 0 ||
		failure.ObservedAt != "" || !controlIdentityPattern.MatchString(failure.EventType) ||
		failure.GroupID != "notification-booking-group" ||
		!validKafkaWindowGroupTopic(failure.WindowID, failure.GroupID, failure.Topic) {
		return errors.New("invalid Kafka injected-failure evidence")
	}
	if _, err := parseUUID(failure.EventID); err != nil {
		return errors.New("invalid Kafka injected-failure identity")
	}
	failure.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	key := boundaryKey(failure.WindowID, failure.EventID, "kafka_before_handler")
	return writeJSONExclusive(filepath.Join(c.config.Directory, kafkaFailureFile(key)), failure)
}

func validKafkaWindowGroupTopic(windowID, groupID, topic string) bool {
	baseTopic := ""
	switch windowID {
	case "kafka.partition.A_failure_offset_frontier",
		"kafka.partition.restart_replay_A_then_B",
		"kafka.partition.rebalance_old_generation_fenced",
		"kafka.partition.commit_ack_uncertainty",
		"kafka.partition.poison_stalls_later_records",
		"kafka.broker.outage_restart_recovery":
		if groupID != "notification-booking-group" {
			return false
		}
		baseTopic = "booking.events"
	case "kafka.group.notification-booking-group.restart_fence":
		if groupID != "notification-booking-group" {
			return false
		}
		baseTopic = "booking.events"
	case "kafka.group.notification-user-group.restart_fence":
		if groupID != "notification-user-group" {
			return false
		}
		baseTopic = "user.events"
	case "kafka.group.scheduler-config-event-group.restart_fence":
		if groupID != "scheduler-config-event-group" {
			return false
		}
		baseTopic = "scheduler.config.events"
	case "kafka.group.saga-payment-group.restart_fence":
		if groupID != "saga-payment-group" {
			return false
		}
		baseTopic = "payment.events"
	default:
		return false
	}
	prefix := baseTopic + ".t23-"
	if !strings.HasPrefix(topic, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(topic, prefix)
	if len(suffix) != 24 {
		return false
	}
	for _, r := range suffix {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

// Concurrent dispatch paths for the same durable request join the one claimed
// barrier only within this controller process. A restarted process treats an
// old claimed command as consumed, preserving one-shot recovery semantics.
func (c *Controller) joinActive(ctx context.Context, boundary Boundary, key string) (Decision, bool, error) {
	c.activeMu.Lock()
	command, ok := c.active[key]
	c.activeMu.Unlock()
	if ok && c.matchesActiveBoundary(key, command, boundary) {
		return c.waitDecision(ctx, boundary, key, command.Nonce)
	}
	return c.resumeClaimedBoundary(boundary, key)
}

// resumeClaimedBoundary distinguishes an unresolved durable claim from an
// absent command. It is used after a caller retry or process restart, when the
// in-memory active map no longer owns the original wait.
func (c *Controller) resumeClaimedBoundary(boundary Boundary, key string) (Decision, bool, error) {
	command, err := readJSONFile[Command](filepath.Join(c.config.Directory, ".claimed-"+key+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return DecisionContinue, false, nil
	}
	if err != nil || !c.matchesBoundary(command, boundary) {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	barrier, err := readJSONFile[Barrier](filepath.Join(c.config.Directory, barrierFile(key)))
	if err != nil || barrier.WindowID != boundary.WindowID ||
		barrier.OperationID != boundary.OperationID || barrier.Checkpoint != boundary.Checkpoint ||
		barrier.Action != boundary.Action || barrier.Nonce != command.Nonce {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	if !sameFacts(barrier.Facts, boundary.Facts) {
		if !sameSagaFactsForRecoveryAdvance(barrier.Facts, boundary.Facts) {
			return DecisionContinue, true, ErrUnresolvedClaim
		}
		return c.resumeSagaRecoveryBoundary(boundary, key, command, barrier)
	}
	decision, found, err := c.readDecision(boundary, key, command.Nonce)
	if err != nil {
		return DecisionContinue, true, err
	}
	if !found {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	if decision == DecisionFailOnce && validKafkaFailOnceBoundary(boundary) {
		failure, err := readJSONFile[KafkaInjectedFailure](
			filepath.Join(c.config.Directory, kafkaFailureFile(key)))
		if err != nil || !kafkaFailureMatchesBoundary(failure, boundary, barrier) {
			return DecisionContinue, true, ErrUnresolvedClaim
		}
		return DecisionContinue, false, nil
	}
	return decision, true, nil
}

func (c *Controller) resumeSagaRecoveryBoundary(boundary Boundary, key string,
	command Command, barrier Barrier) (Decision, bool, error) {
	decisionPath := filepath.Join(c.config.Directory, decisionFile(key))
	consumedPath := filepath.Join(c.config.Directory,
		".consumed-"+key+"-"+command.Nonce+".json")
	if err := os.Rename(decisionPath, consumedPath); err != nil {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	defer os.Remove(consumedPath)
	decision, err := readJSONFile[ControlDecision](consumedPath)
	if err != nil || decision.Decision != DecisionContinue ||
		decision.Nonce != command.Nonce || !tokenMatches(c.config.Token, decision.Token) ||
		decision.Proof != nil || decision.PaymentProof != nil ||
		decision.NotificationProof != nil ||
		!validSagaRecoveryProof(boundary, barrier, decision.SagaRecoveryProof) {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	claimedPath := filepath.Join(c.config.Directory, ".claimed-"+key+".json")
	claimed, err := readJSONFile[Command](claimedPath)
	if err != nil || claimed.Nonce != command.Nonce || !c.matchesBoundary(claimed, boundary) {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	// The permit resolves this one-shot fault boundary. Keep its barrier receipt,
	// but remove the claimed command so later retries of the same Saga step pass
	// through instead of being mistaken for another unresolved SIGKILL.
	if err := os.Remove(claimedPath); err != nil {
		return DecisionContinue, true, ErrUnresolvedClaim
	}
	return DecisionContinue, true, nil
}

func kafkaFailureMatchesBoundary(failure KafkaInjectedFailure, boundary Boundary,
	barrier Barrier) bool {
	facts := boundary.Facts
	if len(facts) != 7 || failure.WindowID != boundary.WindowID ||
		failure.GroupID != facts["group_id"] || failure.Topic != facts["topic"] ||
		failure.EventID != facts["event_id"] || failure.EventID != boundary.OperationID ||
		failure.EventType != facts["event_type"] || failure.ObservedAt == "" {
		return false
	}
	generation, generationErr := strconv.ParseInt(facts["generation"], 10, 32)
	partition, partitionErr := strconv.Atoi(facts["partition"])
	offset, offsetErr := strconv.ParseInt(facts["offset"], 10, 64)
	if generationErr != nil || partitionErr != nil || offsetErr != nil || generation <= 0 ||
		partition < 0 || offset < 0 || failure.Generation != int32(generation) ||
		failure.Partition != partition || failure.Offset != offset {
		return false
	}
	observedAt, observedErr := time.Parse(time.RFC3339Nano, failure.ObservedAt)
	barrierAt, barrierErr := time.Parse(time.RFC3339Nano, barrier.ObservedAt)
	return observedErr == nil && barrierErr == nil &&
		!observedAt.Before(barrierAt) && !observedAt.After(time.Now().Add(5*time.Second))
}

func sameFacts(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, exists := right[key]; !exists || other != value {
			return false
		}
	}
	return true
}

// sameSagaFactsForRecoveryAdvance permits only a claim-fence advance at a
// before-effect or effect-before-result boundary. A pending step may become
// executing before its effect; an effect-before-result step must keep its
// executing/compensating state. Every other fact must remain identical.
func sameSagaFactsForRecoveryAdvance(oldFacts, newFacts map[string]string) bool {
	if len(oldFacts) != 8 || len(newFacts) != 8 ||
		oldFacts["claim_fence"] == "" || newFacts["claim_fence"] == "" ||
		oldFacts["phase"] != newFacts["phase"] ||
		(oldFacts["phase"] != "before_effect" && oldFacts["phase"] != "effect_before_result") {
		return false
	}
	if oldFacts["phase"] == "effect_before_result" &&
		oldFacts["step_status"] != newFacts["step_status"] {
		return false
	}
	for key, value := range oldFacts {
		if key == "claim_fence" {
			continue
		}
		other, exists := newFacts[key]
		if !exists {
			return false
		}
		if key == "step_status" && value == "PENDING" && other == "EXECUTING" {
			continue
		}
		if other != value {
			return false
		}
	}
	oldFence, oldErr := strconv.ParseInt(oldFacts["claim_fence"], 10, 64)
	newFence, newErr := strconv.ParseInt(newFacts["claim_fence"], 10, 64)
	return oldErr == nil && newErr == nil && oldFence > 0 && newFence > oldFence &&
		strconv.FormatInt(oldFence, 10) == oldFacts["claim_fence"] &&
		strconv.FormatInt(newFence, 10) == newFacts["claim_fence"]
}

func validSagaRecoveryProof(boundary Boundary, barrier Barrier, proof *SagaRecoveryProof) bool {
	phaseAllowed := proof != nil &&
		(proof.Phase == "before_effect" || proof.Phase == "effect_before_result")
	effectProofMatchesPhase := proof != nil &&
		(proof.Phase == "before_effect" && !proof.EffectObserved ||
			proof.Phase == "effect_before_result" && proof.EffectObserved)
	if proof == nil || boundary.Action != ActionPause ||
		boundary.Checkpoint != boundary.WindowID || proof.Kind != "T23_SAGA_SIGKILL_RECOVERY" ||
		proof.WindowID != boundary.WindowID || proof.OperationID != boundary.OperationID ||
		proof.BookingID != boundary.Facts["booking_id"] || proof.BookingID != boundary.OperationID ||
		proof.SagaID != boundary.Facts["saga_id"] || proof.StepID != boundary.Facts["step_id"] ||
		proof.StepName != boundary.Facts["step_name"] || proof.Direction != boundary.Facts["direction"] ||
		!phaseAllowed || !effectProofMatchesPhase || boundary.Facts["phase"] != proof.Phase ||
		proof.ClaimFence != barrier.Facts["claim_fence"] || proof.FaultKind != "SIGKILL" ||
		!containerIDPattern.MatchString(proof.ContainerID) || proof.PID <= 0 ||
		!proof.ProcessStopped || proof.StartedAt == "" {
		return false
	}
	startedAt, err := time.Parse(time.RFC3339Nano, proof.StartedAt)
	if err != nil || startedAt.IsZero() {
		return false
	}
	parts := strings.Split(boundary.WindowID, ".")
	if len(parts) != 4 || parts[0] != "saga" ||
		(parts[1] != "forward" && parts[1] != "compensation") ||
		!sagaStepPattern.MatchString(parts[2]) ||
		parts[3] != proof.Phase || parts[1] != proof.Direction {
		return false
	}
	stepIndex, _ := strconv.Atoi(strings.TrimPrefix(parts[2], "S"))
	if proof.Direction == "compensation" && stepIndex > 3 {
		return false
	}
	stepNames := [...]string{"RESERVED_SEAT_STEP", "CREATE_PAYMENT_INTENT_STEP",
		"UPDATE_SEAT_BOOKED", "UPDATE_BOOKING_STATUS_CONFIRMED", "UPDATE_AVAILABLE_SEAT_NUMBER"}
	if proof.StepName != stepNames[stepIndex] ||
		proof.Direction == "forward" && boundary.Facts["step_status"] != "EXECUTING" ||
		proof.Direction == "compensation" && boundary.Facts["step_status"] != "COMPENSATING" {
		return false
	}
	if proof.Phase == "effect_before_result" &&
		barrier.Facts["step_status"] != boundary.Facts["step_status"] {
		return false
	}
	for _, id := range []string{proof.BookingID, proof.SagaID, proof.StepID} {
		parsed, err := parseUUID(id)
		if err != nil || parsed != id {
			return false
		}
	}
	return barrier.Facts["booking_id"] == proof.BookingID &&
		barrier.Facts["saga_id"] == proof.SagaID &&
		barrier.Facts["step_id"] == proof.StepID &&
		barrier.Facts["step_name"] == proof.StepName &&
		barrier.Facts["direction"] == proof.Direction &&
		barrier.Facts["phase"] == proof.Phase
}

func (c *Controller) clearActive(key, nonce string) {
	c.activeMu.Lock()
	defer c.activeMu.Unlock()
	if active, ok := c.active[key]; ok && active.Nonce == nonce {
		delete(c.active, key)
	}
}

func (c *Controller) matchesBoundary(command Command, boundary Boundary) bool {
	return command.WindowID == boundary.WindowID && command.OperationID == boundary.OperationID &&
		command.Checkpoint == boundary.Checkpoint && command.Action == boundary.Action &&
		validControlIdentity(command.Nonce) && tokenMatches(c.config.Token, command.Token)
}

func (c *Controller) matchesActiveBoundary(key string, command Command, boundary Boundary) bool {
	if !c.matchesBoundary(command, boundary) {
		return false
	}
	barrier, err := readJSONFile[Barrier](filepath.Join(c.config.Directory, barrierFile(key)))
	return err == nil && barrier.WindowID == boundary.WindowID &&
		barrier.OperationID == boundary.OperationID && barrier.Checkpoint == boundary.Checkpoint &&
		barrier.Action == boundary.Action && barrier.Nonce == command.Nonce &&
		sameFacts(barrier.Facts, boundary.Facts)
}

func (c *Controller) waitDecision(ctx context.Context, boundary Boundary, key, nonce string) (Decision, bool, error) {
	for {
		decision, found, err := c.readDecision(boundary, key, nonce)
		if err != nil {
			return DecisionContinue, true, err
		}
		if found {
			return decision, true, nil
		}
		select {
		case <-ctx.Done():
			return DecisionContinue, true, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *Controller) readDecision(boundary Boundary, key, nonce string) (Decision, bool, error) {
	decision, err := readJSONFile[ControlDecision](filepath.Join(c.config.Directory, decisionFile(key)))
	if errors.Is(err, os.ErrNotExist) || err == nil &&
		(decision.Nonce != nonce || !tokenMatches(c.config.Token, decision.Token)) {
		return DecisionContinue, false, nil
	}
	if err != nil {
		return DecisionContinue, false, errors.New("reliability control decision is invalid")
	}
	if decision.Decision == DecisionContinue && decision.SagaRecoveryProof == nil {
		return DecisionContinue, true, nil
	}
	if decision.Decision == DecisionFailOnce && validKafkaFailOnceBoundary(boundary) {
		return DecisionFailOnce, true, nil
	}
	if decision.Decision == DecisionFailOnce && validNotificationFailOnceBoundary(boundary) &&
		(boundary.Checkpoint == "notification_unknown_seed_before_sender" ||
			validNotificationUnknownProof(boundary, decision.NotificationProof)) {
		return DecisionFailOnce, true, nil
	}
	if decision.Decision == DecisionWithhold && boundary.Action == ActionAckGate {
		if validAckProof(boundary.OperationID, decision.Proof) ||
			validPaymentRelayProof(boundary, decision.PaymentProof) {
			return DecisionWithhold, true, nil
		}
	}
	return DecisionContinue, false, errors.New("reliability control decision failed validation")
}

// A single injected transient handler error is only allowed at the exact
// canonical A-frontier Kafka boundary. The command is one-shot because Await
// atomically consumes its control file before returning this decision.
func validKafkaFailOnceBoundary(boundary Boundary) bool {
	return boundary.WindowID == "kafka.partition.A_failure_offset_frontier" &&
		boundary.Checkpoint == "kafka_before_handler" && boundary.Action == ActionPause
}

func validNotificationFailOnceBoundary(boundary Boundary) bool {
	return boundary.WindowID == "notification.unknown_retry_attention" &&
		(boundary.Checkpoint == "notification_unknown_before_retry" ||
			boundary.Checkpoint == "notification_unknown_seed_before_sender") && boundary.Action == ActionPause &&
		validNotificationBoundary(boundary.WindowID, boundary.OperationID,
			boundary.Checkpoint, boundary.Facts)
}

func validNotificationUnknownProof(boundary Boundary, proof *NotificationFaultProof) bool {
	return proof != nil && boundary.Checkpoint == "notification_unknown_before_retry" &&
		proof.Kind == "UNKNOWN_EXTERNAL_OUTCOME" && proof.OperationID == boundary.OperationID &&
		proof.Observed && proof.SenderAttemptObserved && proof.ResultUnknown &&
		proof.UnknownStatePersisted
}

func validPaymentRelayProof(boundary Boundary, proof *PaymentRelayProof) bool {
	if boundary.WindowID != "payment.relay_before_marker" ||
		boundary.Checkpoint != "payment_broker_ack_dropped_before_relay_marker" ||
		proof == nil || proof.Kind != "payment_receipt_relay" ||
		proof.PaymentID != boundary.OperationID || proof.Topic != "payment.events" ||
		proof.Partition < 0 || proof.Offset < 0 || !proof.RecordAtBroker ||
		!proof.MarkerUncommitted {
		return false
	}
	if _, err := parseUUID(proof.PaymentID); err != nil {
		return false
	}
	if _, err := parseUUID(proof.BookingID); err != nil {
		return false
	}
	if _, err := parseUUID(proof.EnvelopeID); err != nil {
		return false
	}
	if !paymentEventIDPattern.MatchString(proof.ReceiptEventID) {
		return false
	}
	facts := boundary.Facts
	if len(facts) != 8 || facts["payment_id"] == "" || facts["booking_id"] == "" ||
		facts["receipt_event_id"] == "" || facts["envelope_id"] == "" ||
		facts["topic"] != "payment.events" {
		return false
	}
	if _, err := parseUUID(facts["claim_token"]); err != nil {
		return false
	}
	claimFence, err := strconv.ParseInt(facts["claim_fence"], 10, 64)
	if err != nil || claimFence < 1 {
		return false
	}
	leaseUntil, err := time.Parse(time.RFC3339Nano, facts["lease_until"])
	if err != nil || !leaseUntil.After(time.Now()) {
		return false
	}
	if facts["payment_id"] != proof.PaymentID || facts["booking_id"] != proof.BookingID ||
		facts["receipt_event_id"] != proof.ReceiptEventID || facts["envelope_id"] != proof.EnvelopeID ||
		facts["topic"] != proof.Topic || facts["claim_token"] != proof.ClaimToken ||
		claimFence != proof.ClaimFence || facts["lease_until"] != proof.LeaseUntil {
		return false
	}
	observedAt, err := time.Parse(time.RFC3339Nano, proof.ObservedAt)
	return err == nil && !observedAt.After(time.Now().Add(5*time.Second)) &&
		time.Since(observedAt) <= 2*time.Minute
}

// UnaryClientInterceptor creates a deterministic post-server-result ACK gate
// for only StartOrderSaga and SettleOrderSaga. The external acceptance driver
// must supply a fresh authoritative durability proof before it can withhold.
func (c *Controller) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var boundary Boundary
		switch typed := req.(type) {
		case *sagav1.StartOrderSagaRequest:
			if method != sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName || typed == nil {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			boundary = Boundary{WindowID: "booking.saga_ack_lost", OperationID: typed.BookingId,
				Checkpoint: method, Action: ActionAckGate}
		default:
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
		if response, ok := reply.(*sagav1.OrderSagaResponse); ok && response != nil {
			boundary.Facts = map[string]string{"upstream_success": "true", "saga_id": response.SagaId,
				"saga_status": response.Status}
		}
		decision, matched, err := c.Await(ctx, boundary)
		if err != nil {
			return err
		}
		if matched && decision == DecisionWithhold {
			return status.Error(codes.Unavailable, "reliability acknowledgement withheld")
		}
		return nil
	}
}

// SagaStartRequestObserver records receipt inside the Saga gRPC server before
// the application handler runs. The receipt is separate from the post-result
// ACK gate and is emitted only for its exact tokened operation.
func (c *Controller) SagaStartRequestObserver() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil || info.FullMethod != sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName {
			return handler(ctx, req)
		}
		start, ok := req.(*sagav1.StartOrderSagaRequest)
		if !ok || start == nil {
			return handler(ctx, req)
		}
		if err := c.ObserveRequest("booking.saga_ack_lost", start.BookingId, info.FullMethod); err != nil {
			return nil, status.Error(codes.Internal, "reliability request receipt unavailable")
		}
		return handler(ctx, req)
	}
}

// ObserveRequest records the precise in-application receive boundary without
// consuming the command that the caller-side ACK interceptor will claim later.
func (c *Controller) ObserveRequest(windowID, operationID, checkpoint string) error {
	if !c.Enabled() {
		return nil
	}
	if !validBoundary(Boundary{WindowID: windowID, OperationID: operationID,
		Checkpoint: checkpoint, Action: ActionAckGate}) {
		return errors.New("invalid reliability request observation")
	}
	command, err := readJSONFile[Command](filepath.Join(c.config.Directory, commandFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("reliability request command is invalid")
	}
	if command.WindowID != windowID || command.OperationID != operationID || command.Checkpoint != checkpoint ||
		command.Action != ActionAckGate || !validControlIdentity(command.Nonce) ||
		!tokenMatches(c.config.Token, command.Token) {
		return nil
	}
	key := boundaryKey(windowID, operationID, checkpoint)
	path := filepath.Join(c.config.Directory, requestReceiptFile(key))
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("reliability request receipt path is invalid")
	}
	receipt := RequestReceipt{WindowID: windowID, OperationID: operationID,
		Checkpoint: checkpoint, Nonce: command.Nonce,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writeJSONAtomic(path, receipt); err != nil {
		return errors.New("reliability request receipt could not be written")
	}
	return nil
}

// UnaryServerInterceptor gates only the public Booking CancelBooking response
// after the handler has durably committed its terminal decision and outbox.
func (c *Controller) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil || info.FullMethod != bookingv1.BookingService_CancelBooking_FullMethodName {
			return handler(ctx, req)
		}
		cancel, ok := req.(*bookingv1.CancelBookingRequest)
		if !ok || cancel == nil {
			return handler(ctx, req)
		}
		receivedAt := time.Now().UTC().Format(time.RFC3339Nano)
		response, err := handler(ctx, req)
		if err != nil {
			return response, err
		}
		decision, matched, err := c.Await(ctx, Boundary{
			WindowID: "booking.terminal_ack_lost", OperationID: cancel.BookingId,
			Checkpoint: info.FullMethod, Action: ActionAckGate,
			Facts: map[string]string{"booking_id": cancel.BookingId,
				"request_received_at": receivedAt},
		})
		if err != nil {
			return nil, err
		}
		if matched && decision == DecisionWithhold {
			return nil, status.Error(codes.Unavailable, "reliability acknowledgement withheld")
		}
		return response, nil
	}
}

func validBoundary(boundary Boundary) bool {
	return validControlIdentity(boundary.WindowID) && validControlIdentity(boundary.OperationID) &&
		validControlIdentity(boundary.Checkpoint) &&
		(boundary.Action == ActionPause || boundary.Action == ActionAckGate)
}

func validControlIdentity(value string) bool { return controlIdentityPattern.MatchString(value) }

func tokenMatches(expected, provided string) bool {
	if len(expected) != len(provided) || expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) == 1
}

func validAckProof(operationID string, proof *AckProof) bool {
	if proof == nil || !proof.Durable || proof.BookingID != operationID {
		return false
	}
	booking, err := parseUUID(proof.BookingID)
	if err != nil || booking == "" {
		return false
	}
	observedAt, err := time.Parse(time.RFC3339Nano, proof.ObservedAt)
	if err != nil || observedAt.After(time.Now().Add(5*time.Second)) || time.Since(observedAt) > 2*time.Minute {
		return false
	}
	switch proof.Kind {
	case "saga_start":
		sagaID, err := parseUUID(proof.SagaID)
		return err == nil && sagaID != "" && proof.DecisionID == "" && proof.OutboxEventID == ""
	case "saga_terminal":
		decisionID, decisionErr := parseUUID(proof.DecisionID)
		outboxID, outboxErr := parseUUID(proof.OutboxEventID)
		return decisionErr == nil && outboxErr == nil && decisionID != "" && outboxID != "" && proof.SagaID == ""
	default:
		return false
	}
}

func parseUUID(value string) (string, error) {
	if len(value) != 36 || strings.Count(value, "-") != 4 {
		return "", fmt.Errorf("invalid UUID")
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return "", fmt.Errorf("invalid UUID")
			}
			continue
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return "", fmt.Errorf("invalid UUID")
		}
	}
	return value, nil
}

func boundaryKey(windowID, operationID, checkpoint string) string {
	if uuid, err := parseUUID(operationID); err == nil {
		operationID = strings.ToLower(uuid)
	}
	digest := sha256.Sum256([]byte(windowID + "\x00" + operationID + "\x00" + checkpoint))
	return hex.EncodeToString(digest[:16])
}

func barrierFile(key string) string { return "barrier-" + key + ".json" }
func admissionRepairCommandFile(sweeperID string) string {
	return "repair-command-" + sweeperID + ".json"
}
func repairResultFile(key, nonce string) string {
	return "repair-result-" + key + "-" + nonce + ".json"
}
func decisionFile(key string) string       { return "decision-" + key + ".json" }
func requestReceiptFile(key string) string { return "request-" + key + ".json" }
func kafkaFailureFile(key string) string   { return "kafka-failure-" + key + ".json" }
func kafkaRebalanceFile(key string) string { return "kafka-rebalance-" + key + ".json" }

func readJSONFile[T any](path string) (T, error) {
	var result T
	info, err := os.Lstat(path)
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxControlFileBytes {
		return result, errors.New("unsafe reliability control file")
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxControlFileBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return result, errors.New("reliability control file has trailing data")
	}
	return result, nil
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxControlFileBytes {
		return errors.New("reliability barrier payload is invalid")
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".t23-control-*.tmp")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func writeJSONExclusive(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxControlFileBytes {
		return errors.New("reliability evidence payload is invalid")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(path)
		return errors.New("reliability evidence could not be persisted")
	}
	return nil
}

func cloneFacts(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]string, len(source))
	for key, value := range source {
		if validControlIdentity(key) && len(value) <= 256 {
			copy[key] = value
		}
	}
	return copy
}
