package kafkacollector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

const (
	envAllowIsolatedBroker = "RECOVERY_KAFKA_ALLOW_ISOLATED_BROKER"
	envBrokers             = "RECOVERY_KAFKA_BROKERS"
	envRunID               = "RECOVERY_KAFKA_RUN_ID"
	envSourceRevision      = "RECOVERY_KAFKA_SOURCE_REVISION"
	envEvidenceDir         = "RECOVERY_KAFKA_EVIDENCE_DIR"
	envAttempt             = "RECOVERY_KAFKA_ATTEMPT"
)

type memberAcquisitionEvidence struct {
	Role              string           `json:"role"`
	GroupID           string           `json:"groupId"`
	MemberID          string           `json:"memberId"`
	GenerationID      int32            `json:"generationId"`
	GroupInstanceID   string           `json:"groupInstanceId"`
	ReturnedByNext    bool             `json:"returnedByNext"`
	AcquiredAt        time.Time        `json:"acquiredAt"`
	ProtocolType      string           `json:"protocolType,omitempty"`
	ProtocolName      string           `json:"protocolName,omitempty"`
	AssignmentVersion int16            `json:"assignmentVersion,omitempty"`
	Assignment        []wireAssignment `json:"assignment,omitempty"`
	AssignmentError   string           `json:"assignmentError,omitempty"`
	JoinGroupExchange Exchange         `json:"joinGroupExchange"`
	SyncGroupExchange Exchange         `json:"syncGroupExchange"`
}

type offsetPartitionEvidence struct {
	Partition       int    `json:"partition"`
	CommittedOffset int64  `json:"committedOffset"`
	ErrorType       string `json:"errorType,omitempty"`
}

type processingEffectEvidence struct {
	Phase           string    `json:"phase"`
	HandlerType     string    `json:"handlerType"`
	WorkerRole      string    `json:"workerRole"`
	GroupID         string    `json:"groupId"`
	MemberID        string    `json:"memberId"`
	GenerationID    int32     `json:"generationId"`
	Topic           string    `json:"topic"`
	Partition       int       `json:"partition"`
	Offset          int64     `json:"offset"`
	KeySHA256       string    `json:"keySha256"`
	ValueSHA256     string    `json:"valueSha256"`
	ObservedAt      time.Time `json:"observedAt"`
	OffsetCommitted bool      `json:"offsetCommitted"`
}

type generationRetryEvidence struct {
	Role       string    `json:"role"`
	Attempt    int       `json:"attempt"`
	ErrorCode  int16     `json:"errorCode"`
	ObservedAt time.Time `json:"observedAt"`
}

type realBrokerRunEvidence struct {
	Status                         string                     `json:"status"`
	RunID                          string                     `json:"runId"`
	Attempt                        int                        `json:"attempt"`
	SourceRevision                 string                     `json:"sourceRevision"`
	ComponentSourceSHA256          string                     `json:"componentSourceSha256"`
	Candidate                      string                     `json:"candidate"`
	ClusterID                      string                     `json:"clusterId"`
	BootstrapBrokers               []string                   `json:"bootstrapBrokers"`
	Topic                          string                     `json:"topic"`
	GroupID                        string                     `json:"groupId"`
	ExperimentalParameters         map[string]string          `json:"experimentalParameters"`
	OldMember                      *memberAcquisitionEvidence `json:"oldMember,omitempty"`
	InterimMember                  *memberAcquisitionEvidence `json:"interimMember,omitempty"`
	NewMember                      *memberAcquisitionEvidence `json:"newMember,omitempty"`
	OldMemberLeave                 *Exchange                  `json:"oldMemberLeave,omitempty"`
	RealRebalanceObserved          bool                       `json:"realRebalanceObserved"`
	ProducerWrites                 int                        `json:"producerWrites"`
	ProcessingEffects              []processingEffectEvidence `json:"processingEffects"`
	NormalProcessedRecordCommits   int                        `json:"normalProcessedRecordCommits"`
	HandlerEffectsFile             string                     `json:"handlerEffectsFile"`
	GenerationRetryEvents          []generationRetryEvidence  `json:"generationRetryEvents,omitempty"`
	HeartbeatProbe                 *Exchange                  `json:"heartbeatProbe,omitempty"`
	HeartbeatBrokerClassification  string                     `json:"heartbeatBrokerClassification,omitempty"`
	HeartbeatCodeAgreement         string                     `json:"heartbeatCodeAgreement,omitempty"`
	HeartbeatClientBrokerErrorCode *int16                     `json:"heartbeatClientBrokerErrorCode,omitempty"`
	HeartbeatCallOutcome           string                     `json:"heartbeatCallOutcome,omitempty"`
	HeartbeatCallErrorType         string                     `json:"heartbeatCallErrorType,omitempty"`
	ExplicitHeartbeatProbeCount    int                        `json:"explicitHeartbeatProbeCount"`
	MemberLifecycleHeartbeatCount  int                        `json:"memberLifecycleHeartbeatCount"`
	HeartbeatMayRefreshSession     bool                       `json:"heartbeatMayRefreshSession"`
	OffsetFetchBefore              []offsetPartitionEvidence  `json:"offsetFetchBefore,omitempty"`
	OffsetFetchAfter               []offsetPartitionEvidence  `json:"offsetFetchAfter,omitempty"`
	OffsetCommitRequests           uint64                     `json:"offsetCommitRequests"`
	ProcessingFrontierStatus       string                     `json:"processingFrontierStatus"`
	OldWorkerSideEffectsStatus     string                     `json:"oldWorkerSideEffectsStatus"`
	NewWorkerRecoveryStatus        string                     `json:"newWorkerRecoveryStatus"`
	Boundary                       string                     `json:"boundary"`
	FailureStage                   string                     `json:"failureStage,omitempty"`
	FailureType                    string                     `json:"failureType,omitempty"`
	CollectorStats                 CollectorStats             `json:"collectorStats"`
	Exchanges                      []Exchange                 `json:"exchanges"`
}

func TestRealBrokerRebalanceCapturesResponseForPreviouslyValidHeartbeatTuple(t *testing.T) {
	brokers := splitNonEmpty(os.Getenv(envBrokers))
	runID := strings.TrimSpace(os.Getenv(envRunID))
	sourceRevision := strings.TrimSpace(os.Getenv(envSourceRevision))
	evidenceDir := strings.TrimSpace(os.Getenv(envEvidenceDir))
	if os.Getenv(envAllowIsolatedBroker) != "1" || len(brokers) == 0 || runID == "" ||
		sourceRevision == "" || evidenceDir == "" {
		t.Skip("requires explicit Root-supplied isolated Kafka 7.6 brokers, run ID, source revision, evidence directory, and RECOVERY_KAFKA_ALLOW_ISOLATED_BROKER=1")
	}
	if len(brokers) == 0 {
		t.Fatal("no bootstrap brokers supplied")
	}
	attempt := 1
	if rawAttempt := strings.TrimSpace(os.Getenv(envAttempt)); rawAttempt != "" {
		parsedAttempt, parseErr := strconv.Atoi(rawAttempt)
		if parseErr != nil || parsedAttempt < 1 {
			t.Fatalf("%s must be a positive integer attempt number", envAttempt)
		}
		attempt = parsedAttempt
	}

	componentHash, err := componentSourceSHA256()
	if err != nil {
		t.Fatalf("compute component source hash: %v", err)
	}
	candidate := "kafka-worker-sha256-" + componentHash
	identity := Identity{RunID: runID, SourceRevision: sourceRevision, Candidate: candidate}
	collector := NewCollector(identity)

	runHash := sha256.Sum256([]byte(runID))
	attemptKey := fmt.Sprintf("%s-attempt-%d", runID, attempt)
	attemptHash := sha256.Sum256([]byte(attemptKey))
	namespace := "rc-k-" + hex.EncodeToString(attemptHash[:6])
	topic := namespace + "-topic"
	groupID := namespace + "-group"

	evidence := realBrokerRunEvidence{
		Status:                "RUNNING",
		RunID:                 runID,
		Attempt:               attempt,
		SourceRevision:        sourceRevision,
		ComponentSourceSHA256: componentHash,
		Candidate:             candidate,
		BootstrapBrokers:      append([]string(nil), brokers...),
		Topic:                 topic,
		GroupID:               groupID,
		ExperimentalParameters: map[string]string{
			"overallDeadline":    "90s",
			"sessionTimeout":     "10s",
			"heartbeatInterval":  "1s",
			"rebalanceTimeout":   "15s",
			"explicitProbeCount": "1",
		},
		HeartbeatMayRefreshSession: true,
		ProcessingFrontierStatus:   "NOT_OBSERVED_RUN_INCOMPLETE",
		OldWorkerSideEffectsStatus: "NOT_OBSERVED_RUN_INCOMPLETE",
		NewWorkerRecoveryStatus:    "NOT_OBSERVED_RUN_INCOMPLETE",
		Boundary:                   "The controlled record handler records test-local effects and offset progression only. It does not run the product handler or verify an application database fence.",
	}
	fileStem := "kafka-worker-" + hex.EncodeToString(runHash[:8]) + "-attempt-" + strconv.Itoa(attempt)
	evidencePath := filepath.Join(evidenceDir, fileStem+".json")
	effectsPath := filepath.Join(evidenceDir, fileStem+"-handler-effects.jsonl")
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatalf("create evidence directory: %v", err)
	}
	if _, err := os.Stat(evidencePath); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("evidence target already exists or cannot be inspected; require a new run ID")
	}
	if _, err := os.Stat(effectsPath); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("handler effects target already exists or cannot be inspected; require a new run ID")
	}
	if effectsFile, err := os.OpenFile(effectsPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
		t.Fatalf("create run-scoped component test handler effects file: %v", err)
	} else if err := effectsFile.Close(); err != nil {
		t.Fatalf("close run-scoped component test handler effects file: %v", err)
	}
	evidence.HandlerEffectsFile = effectsPath
	defer func() {
		if evidence.Status == "RUNNING" {
			evidence.Status = "INCOMPLETE"
		}
		evidence.CollectorStats = collector.Stats()
		evidence.OffsetCommitRequests = evidence.CollectorStats.OffsetCommitRequests
		evidence.Exchanges = collector.Snapshot()
		payload, marshalErr := json.MarshalIndent(evidence, "", "  ")
		if marshalErr != nil {
			t.Errorf("marshal evidence: %v", marshalErr)
			return
		}
		file, openErr := os.OpenFile(evidencePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if openErr != nil {
			t.Errorf("create evidence file %s exclusively: %v", evidencePath, openErr)
			return
		}
		if _, writeErr := file.Write(append(payload, '\n')); writeErr != nil {
			_ = file.Close()
			t.Errorf("write evidence file %s: %v", evidencePath, writeErr)
			return
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("close evidence file %s: %v", evidencePath, closeErr)
			return
		}
		t.Logf("KAFKA_COMPONENT_EVIDENCE=%s", evidencePath)
	}()

	runCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	netDialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &kafkago.Transport{
		Dial:        collector.WrapDial("probe-client", netDialer.DialContext),
		DialTimeout: 5 * time.Second,
	}
	client := &kafkago.Client{
		Addr:      kafkago.TCP(brokers[0]),
		Transport: transport,
		Timeout:   5 * time.Second,
	}
	if transport.TLS != nil || transport.SASL != nil {
		t.Fatalf("frame capture requires the fresh test broker's plaintext protocol path")
	}

	metadata, err := client.Metadata(runCtx, &kafkago.MetadataRequest{})
	if err != nil {
		evidence.FailureStage = "initial_metadata"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("initial Metadata call failed: %T", err)
	}
	if metadata.ClusterID == "" {
		evidence.FailureStage = "initial_metadata_cluster_id"
		t.Fatalf("Metadata response did not contain a cluster ID")
	}
	evidence.ClusterID = metadata.ClusterID

	createTopics, err := client.CreateTopics(runCtx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		evidence.FailureStage = "create_unique_topic"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("create run-scoped topic failed: %T", err)
	}
	if err := createTopics.Errors[topic]; err != nil {
		evidence.FailureStage = "create_unique_topic_result"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("create run-scoped topic returned Broker error: %T", err)
	}

	producer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers[0]),
		Topic:        topic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireAll,
		Transport:    &kafkago.Transport{Dial: collector.WrapDial("synthetic-producer", netDialer.DialContext), DialTimeout: 5 * time.Second},
	}
	messageKey := []byte(runID)
	messageValue := []byte("recovery-component-test-record:" + runID)
	if err := producer.WriteMessages(runCtx, kafkago.Message{Key: messageKey, Value: messageValue, Time: time.Now()}); err != nil {
		evidence.FailureStage = "produce_synthetic_record"
		evidence.FailureType = fmt.Sprintf("%T", err)
		_ = producer.Close()
		t.Fatalf("produce run-scoped synthetic record: %T", err)
	}
	if err := producer.Close(); err != nil {
		evidence.FailureStage = "close_synthetic_producer"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("close synthetic producer: %T", err)
	}
	evidence.ProducerWrites = 1

	newMemberGroup := func(role string) (*kafkago.ConsumerGroup, error) {
		return kafkago.NewConsumerGroup(kafkago.ConsumerGroupConfig{
			ID:                groupID,
			Brokers:           brokers,
			Dialer:            &kafkago.Dialer{Timeout: 5 * time.Second, DialFunc: collector.WrapDial(role, netDialer.DialContext)},
			Topics:            []string{topic},
			HeartbeatInterval: time.Second,
			SessionTimeout:    10 * time.Second,
			RebalanceTimeout:  15 * time.Second,
			JoinGroupBackoff:  100 * time.Millisecond,
		})
	}
	groupA, err := newMemberGroup("previous-member")
	if err != nil {
		evidence.FailureStage = "create_previous_member"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("create prior member: %T", err)
	}
	defer func() { _ = groupA.Close() }()

	oldGeneration, oldRetries, err := nextGenerationWithTransientBrokerRetry(runCtx, groupA, "previous-member")
	evidence.GenerationRetryEvents = append(evidence.GenerationRetryEvents, oldRetries...)
	if err != nil {
		evidence.FailureStage = "acquire_previous_member_generation"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("acquire prior generation: %T", err)
	}
	oldAcquisition, err := verifyMemberAcquisition(collector.Snapshot(), "previous-member", oldGeneration)
	if err != nil {
		evidence.FailureStage = "verify_previous_member_join_sync"
		evidence.FailureType = "wire_provenance_missing"
		t.Fatalf("prior member acquisition lacks correlated JoinGroup/SyncGroup success: %v", err)
	}
	evidence.OldMember = &oldAcquisition

	groupB, err := newMemberGroup("rebalance-member")
	if err != nil {
		evidence.FailureStage = "create_rebalance_member"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("create rebalance member: %T", err)
	}
	defer func() { _ = groupB.Close() }()

	offsetBefore, err := fetchOffsetSnapshot(runCtx, client, groupID, topic, []int{0})
	if err != nil {
		evidence.FailureStage = "offset_fetch_before_handler"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("read group offset before handler effects: %T", err)
	}
	if len(offsetBefore) != 1 || offsetBefore[0].CommittedOffset != -1 {
		evidence.FailureStage = "verify_fresh_group_offset_baseline"
		t.Fatalf("fresh run-scoped group offset baseline is not unset: %#v", offsetBefore)
	}
	evidence.OffsetFetchBefore = offsetBefore
	if _, ok := findPartitionAssignment(oldGeneration, topic, 0); !ok {
		evidence.FailureStage = "verify_old_member_partition_assignment"
		t.Fatalf("previous valid member has no assignment for the synthetic record partition")
	}
	oldEffect, err := fetchSyntheticRecordForHandler(runCtx, collector, netDialer, brokers, topic, 0, "PRE_REBALANCE", "old-worker", oldGeneration)
	if err != nil {
		evidence.FailureStage = "old_worker_test_handler"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("old component test handler could not fetch the synthetic record: %T", err)
	}
	if err := appendHandlerEffect(effectsPath, oldEffect); err != nil {
		evidence.FailureStage = "persist_old_test_handler_effect"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("persist old component test handler effect: %T", err)
	}
	expectedKeyHash := sha256.Sum256(messageKey)
	expectedValueHash := sha256.Sum256(messageValue)
	if oldEffect.KeySHA256 != hex.EncodeToString(expectedKeyHash[:]) ||
		oldEffect.ValueSHA256 != hex.EncodeToString(expectedValueHash[:]) {
		evidence.FailureStage = "verify_old_worker_received_produced_record"
		t.Fatalf("old test handler received bytes that do not match the run-scoped synthetic record")
	}
	evidence.ProcessingEffects = append(evidence.ProcessingEffects, oldEffect)
	if commits := collector.Stats().OffsetCommitRequests; commits != 0 {
		evidence.FailureStage = "verify_old_handler_did_not_checkpoint"
		t.Fatalf("old test handler unexpectedly sent %d OffsetCommit requests before its checkpoint", commits)
	}

	interimGeneration, interimRetries, err := nextGenerationWithTransientBrokerRetry(runCtx, groupB, "rebalance-member-interim")
	evidence.GenerationRetryEvents = append(evidence.GenerationRetryEvents, interimRetries...)
	if err != nil {
		evidence.FailureStage = "acquire_rebalanced_generation"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("acquire generation after second member joined: %T", err)
	}
	interimAcquisition, err := verifyMemberAcquisition(collector.Snapshot(), "rebalance-member", interimGeneration)
	if err != nil {
		evidence.FailureStage = "verify_rebalance_member_join_sync"
		evidence.FailureType = "wire_provenance_missing"
		t.Fatalf("rebalance member acquisition lacks correlated JoinGroup/SyncGroup success: %v", err)
	}
	evidence.InterimMember = &interimAcquisition
	if oldGeneration.GroupID != groupID || interimGeneration.GroupID != groupID ||
		oldGeneration.ID == interimGeneration.ID {
		evidence.FailureStage = "verify_generation_change"
		t.Fatalf("real group generation did not change after the second member joined: old=%d interim=%d", oldGeneration.ID, interimGeneration.ID)
	}
	evidence.RealRebalanceObserved = true

	if err := groupA.Close(); err != nil {
		evidence.FailureStage = "close_previous_member"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("close previous worker group after its local effect: %T", err)
	}
	leaveExchange, err := verifyMemberLeave(collector.Snapshot(), "previous-member", oldGeneration)
	if err != nil {
		evidence.FailureStage = "verify_previous_member_leave_response"
		evidence.FailureType = "wire_provenance_missing"
		t.Fatalf("previous member close lacks a matched successful LeaveGroup response: %v", err)
	}
	evidence.OldMemberLeave = &leaveExchange
	newGeneration, activeRetries, err := nextGenerationWithTransientBrokerRetry(runCtx, groupB, "rebalance-member-active")
	evidence.GenerationRetryEvents = append(evidence.GenerationRetryEvents, activeRetries...)
	if err != nil {
		evidence.FailureStage = "acquire_generation_after_previous_member_left"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("acquire active generation after previous member left: %T", err)
	}
	newAcquisition, err := verifyMemberAcquisition(collector.Snapshot(), "rebalance-member", newGeneration)
	if err != nil {
		evidence.FailureStage = "verify_active_member_join_sync"
		evidence.FailureType = "wire_provenance_missing"
		t.Fatalf("active member generation lacks correlated JoinGroup/SyncGroup success: %v", err)
	}
	evidence.NewMember = &newAcquisition
	if newGeneration.GroupID != groupID || newGeneration.ID == interimGeneration.ID ||
		newGeneration.ID == oldGeneration.ID {
		evidence.FailureStage = "verify_previous_member_departure_generation"
		t.Fatalf("generation did not change after previous member left: old=%d interim=%d active=%d", oldGeneration.ID, interimGeneration.ID, newGeneration.ID)
	}
	if _, ok := findPartitionAssignment(newGeneration, topic, 0); !ok {
		evidence.FailureStage = "verify_new_worker_partition_assignment"
		t.Fatalf("active new member has no assignment for the uncommitted record partition")
	}

	newEffect, err := fetchSyntheticRecordForHandler(runCtx, collector, netDialer, brokers, topic, 0, "POST_REBALANCE", "new-worker", newGeneration)
	if err != nil {
		evidence.FailureStage = "new_worker_recovery_handler"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("new component test handler could not refetch the uncommitted synthetic record: %T", err)
	}
	if oldEffect.Offset != newEffect.Offset || oldEffect.KeySHA256 != newEffect.KeySHA256 ||
		oldEffect.ValueSHA256 != newEffect.ValueSHA256 {
		evidence.FailureStage = "verify_same_uncommitted_record_replayed"
		t.Fatalf("new worker did not refetch the exact old worker record: old=%#v new=%#v", oldEffect, newEffect)
	}
	evidence.ProcessingEffects = append(evidence.ProcessingEffects, newEffect)
	if err := newGeneration.CommitOffsets(map[string]map[int]int64{topic: {0: newEffect.Offset + 1}}); err != nil {
		evidence.FailureStage = "commit_after_new_worker_handler"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("commit offset after the new test handler completed: %T", err)
	}
	evidence.ProcessingEffects[len(evidence.ProcessingEffects)-1].OffsetCommitted = true
	evidence.NormalProcessedRecordCommits = 1
	newEffect.OffsetCommitted = true
	if err := appendHandlerEffect(effectsPath, newEffect); err != nil {
		evidence.FailureStage = "persist_new_test_handler_effect"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("persist new component test handler effect: %T", err)
	}
	evidence.ProcessingFrontierStatus = "OBSERVED_OLD_HANDLER_OFFSET_0_WITHOUT_CHECKPOINT; NEW_HANDLER_REPLAYED_OFFSET_0_AND_COMMITTED_FRONTIER_1"
	evidence.OldWorkerSideEffectsStatus = "COMPONENT_TEST_HANDLER_EFFECT_RECORDED_BEFORE_REBALANCE; PRODUCT_DATABASE_FENCING_NOT_VERIFIED"
	evidence.NewWorkerRecoveryStatus = "COMPONENT_TEST_HANDLER_REREAD_IDENTICAL_UNCOMMITTED_OFFSET_0_AFTER_REBALANCE; NORMAL_RECORD_COMMIT_ADVANCED_TO_1"

	freshMetadata, err := client.Metadata(runCtx, &kafkago.MetadataRequest{})
	if err != nil {
		evidence.FailureStage = "refresh_metadata_before_probe"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("refresh Metadata before probe: %T", err)
	}
	if freshMetadata.ClusterID == "" || freshMetadata.ClusterID != metadata.ClusterID {
		evidence.FailureStage = "verify_cluster_identity"
		t.Fatalf("cluster ID changed or was absent across component run")
	}
	evidence.ClusterID = freshMetadata.ClusterID

	coordinatorResponse, err := client.FindCoordinator(runCtx, &kafkago.FindCoordinatorRequest{
		Key:     groupID,
		KeyType: kafkago.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		evidence.FailureStage = "find_group_coordinator"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("FindCoordinator failed: %T", err)
	}
	if coordinatorResponse.Error != nil || coordinatorResponse.Coordinator == nil {
		evidence.FailureStage = "verify_group_coordinator"
		if coordinatorResponse.Error != nil {
			evidence.FailureType = fmt.Sprintf("%T", coordinatorResponse.Error)
		}
		t.Fatalf("FindCoordinator response did not identify the group coordinator")
	}
	coordinatorAddress := net.JoinHostPort(coordinatorResponse.Coordinator.Host, strconv.Itoa(coordinatorResponse.Coordinator.Port))

	probeCtx, probeCancel := context.WithTimeout(runCtx, 5*time.Second)
	heartbeatResponse, heartbeatErr := client.Heartbeat(probeCtx, &kafkago.HeartbeatRequest{
		Addr:            kafkago.TCP(coordinatorAddress),
		GroupID:         oldGeneration.GroupID,
		MemberID:        oldGeneration.MemberID,
		GenerationID:    oldGeneration.ID,
		GroupInstanceID: "",
	})
	callCtxErr := probeCtx.Err()
	probeCancel()
	evidence.HeartbeatCallOutcome = classifyCallOutcome(callCtxErr, heartbeatErr)
	if heartbeatErr != nil {
		evidence.HeartbeatCallErrorType = fmt.Sprintf("%T", heartbeatErr)
	}
	if heartbeatResponse != nil {
		clientCode := int16(0)
		if heartbeatResponse.Error != nil {
			brokerError, ok := heartbeatResponse.Error.(kafkago.Error)
			if !ok {
				evidence.FailureStage = "verify_public_heartbeat_error_type"
				t.Fatalf("public Heartbeat returned an unexpected Broker error type: %T", heartbeatResponse.Error)
			}
			clientCode = int16(brokerError)
		}
		evidence.HeartbeatClientBrokerErrorCode = &clientCode
	}

	var probe *Exchange
	explicitProbeCount := 0
	memberLifecycleHeartbeatCount := 0
	for _, exchange := range collector.Snapshot() {
		if exchange.APIKey != int16(protocol.Heartbeat) {
			continue
		}
		if exchange.Role == "previous-member" || exchange.Role == "rebalance-member" {
			memberLifecycleHeartbeatCount++
		}
		if exchange.Role == "probe-client" {
			explicitProbeCount++
			copy := exchange
			probe = &copy
		}
	}
	evidence.ExplicitHeartbeatProbeCount = explicitProbeCount
	evidence.MemberLifecycleHeartbeatCount = memberLifecycleHeartbeatCount
	if probe != nil {
		collector.recordCallOutcome(probe.ConnectionID, probe.CorrelationID, callCtxErr, heartbeatErr)
		probe.CallOutcome = evidence.HeartbeatCallOutcome
		evidence.HeartbeatProbe = probe
	}
	if explicitProbeCount != 1 || probe == nil {
		evidence.FailureStage = "capture_exactly_one_probe"
		t.Fatalf("captured explicit Heartbeat probe count = %d, want exactly 1", explicitProbeCount)
	}
	if !probe.Matched || probe.MatchStatus != "MATCHED" || probe.ResponseAt == nil ||
		probe.APIKey != int16(protocol.Heartbeat) || probe.GroupID != oldGeneration.GroupID ||
		probe.MemberID != oldGeneration.MemberID || probe.GenerationID != oldGeneration.ID {
		evidence.FailureStage = "verify_correlated_old_tuple_response"
		t.Fatalf("no exact correlated Heartbeat response for the previously valid tuple: %#v", probe)
	}
	if probe.ResponseErrorCode == nil {
		evidence.FailureStage = "verify_stale_tuple_broker_code"
		t.Fatalf("matched Heartbeat exchange has no Broker response code")
	}
	evidence.HeartbeatBrokerClassification = probe.ResponseClassification
	evidence.HeartbeatCodeAgreement = probe.ResponseCodeAgreement
	if probe.ClusterID == "" || probe.ClusterID != metadata.ClusterID ||
		probe.ClusterSourceCorrelationID == nil ||
		probe.CoordinatorNodeID == nil ||
		probe.CoordinatorLookupCorrelationID == nil ||
		probe.CoordinatorHost != coordinatorResponse.Coordinator.Host ||
		probe.CoordinatorPort != int32(coordinatorResponse.Coordinator.Port) ||
		probe.DialTarget != coordinatorAddress {
		evidence.FailureStage = "verify_cluster_and_coordinator_binding"
		t.Fatalf("Heartbeat exchange lacks matching cluster/coordinator provenance: %#v", probe)
	}
	if heartbeatErr == nil {
		if heartbeatResponse == nil {
			evidence.FailureStage = "verify_high_level_heartbeat_result"
			t.Fatalf("Heartbeat returned neither a response nor an error")
		}
		if probe.ResponseCodecErrorCode == nil || probe.ResponseWireSchemaErrorCode == nil {
			evidence.FailureStage = "verify_wire_heartbeat_error_code"
			t.Fatalf("matched Heartbeat exchange did not retain both local-codec and typed response codes")
		}
		if probe.ResponseErrorCode == nil || *probe.ResponseErrorCode != *probe.ResponseWireSchemaErrorCode ||
			probe.ResponseWireSchemaAgreement != "RAW_WIRE_SCHEMA_AGREE" ||
			probe.ResponseWireSchemaHeaderTags != 0 || probe.ResponseWireSchemaBodyTags != 0 {
			evidence.FailureStage = "compare_raw_and_local_codec"
			t.Fatalf("raw and local wire-schema Heartbeat response codes disagree")
		}
		// The locked kafka-go v0.4.50 typed Heartbeat response is known to put
		// ErrorCode before ThrottleTimeMs. Keep the typed fact separate from the
		// corrected collector-local wire decoder and compare the public result
		// to the typed value, not to the corrected one.
		if heartbeatResponse.Error == nil && *probe.ResponseCodecErrorCode != 0 {
			evidence.FailureStage = "compare_wire_and_public_response"
			t.Fatalf("high-level Heartbeat result and typed response code disagree")
		}
		if heartbeatResponse.Error != nil {
			brokerError, ok := heartbeatResponse.Error.(kafkago.Error)
			if !ok || int16(brokerError) != *probe.ResponseCodecErrorCode {
				evidence.FailureStage = "compare_wire_and_public_response"
				t.Fatalf("high-level Heartbeat result and typed response code disagree")
			}
		}
	}

	offsetAfter, err := fetchOffsetSnapshot(runCtx, client, groupID, topic, []int{0})
	if err != nil {
		evidence.FailureStage = "offset_fetch_after_normal_handler_commit"
		evidence.FailureType = fmt.Sprintf("%T", err)
		t.Fatalf("read committed offset after the new handler and normal checkpoint: %T", err)
	}
	evidence.OffsetFetchAfter = offsetAfter
	if len(offsetAfter) != 1 || offsetAfter[0].CommittedOffset != newEffect.Offset+1 {
		evidence.FailureStage = "verify_processing_frontier_after_normal_commit"
		t.Fatalf("committed frontier does not match the handled record: got=%#v want=%d", offsetAfter, newEffect.Offset+1)
	}

	evidence.OffsetCommitRequests = collector.Stats().OffsetCommitRequests
	if evidence.NormalProcessedRecordCommits != 1 || evidence.OffsetCommitRequests != uint64(evidence.NormalProcessedRecordCommits) {
		evidence.FailureStage = "verify_only_normal_processed_record_commit"
		t.Fatalf("observed %d OffsetCommit requests and %d normal processed-record commits", evidence.OffsetCommitRequests, evidence.NormalProcessedRecordCommits)
	}
	if stats := collector.Stats(); stats.UnmatchedResponses != 0 ||
		stats.FrameDecodeFailures != 0 || stats.OversizedFrames != 0 {
		evidence.FailureStage = "verify_collector_integrity"
		t.Fatalf("collector integrity counters are non-zero: %#v", stats)
	}
	evidence.Status = "REAL_BROKER_REBALANCE_RECORD_RECOVERY_CAPTURED"
}

func verifyMemberAcquisition(exchanges []Exchange, role string, generation *kafkago.Generation) (memberAcquisitionEvidence, error) {
	var join, sync Exchange
	joinFound, syncFound := false, false
	for _, exchange := range exchanges {
		if exchange.Role != role || exchange.GroupID != generation.GroupID || !exchange.Matched {
			continue
		}
		switch exchange.APIKey {
		case int16(protocol.JoinGroup):
			if exchange.ResponseErrorCode != nil && *exchange.ResponseErrorCode == 0 &&
				exchange.ResponseGenerationID == generation.ID && exchange.ResponseMemberID == generation.MemberID {
				join, joinFound = exchange, true
			}
		case int16(protocol.SyncGroup):
			if exchange.MemberID == generation.MemberID && exchange.GenerationID == generation.ID &&
				exchange.ResponseErrorCode != nil && *exchange.ResponseErrorCode == 0 {
				sync, syncFound = exchange, true
			}
		}
	}
	if !joinFound || !syncFound {
		return memberAcquisitionEvidence{}, fmt.Errorf("JoinGroup success=%t SyncGroup success=%t", joinFound, syncFound)
	}
	return memberAcquisitionEvidence{
		Role:              role,
		GroupID:           generation.GroupID,
		MemberID:          generation.MemberID,
		GenerationID:      generation.ID,
		GroupInstanceID:   "",
		ReturnedByNext:    true,
		AcquiredAt:        time.Now(),
		ProtocolType:      sync.ResponseProtocolType,
		ProtocolName:      sync.ResponseProtocolName,
		AssignmentVersion: sync.ResponseAssignmentVersion,
		Assignment:        sync.ResponseAssignments,
		AssignmentError:   sync.ResponseAssignmentError,
		JoinGroupExchange: join,
		SyncGroupExchange: sync,
	}, nil
}

func verifyMemberLeave(exchanges []Exchange, role string, generation *kafkago.Generation) (Exchange, error) {
	for _, exchange := range exchanges {
		if exchange.Role != role || exchange.APIKey != int16(protocol.LeaveGroup) ||
			exchange.GroupID != generation.GroupID || exchange.MemberID != generation.MemberID ||
			!exchange.Matched || exchange.ResponseErrorCode == nil || *exchange.ResponseErrorCode != 0 {
			continue
		}
		if exchange.APIVersion >= 3 && (exchange.ResponseMemberErrorCode == nil || *exchange.ResponseMemberErrorCode != 0) {
			continue
		}
		if exchange.ResponseMemberID != "" && exchange.ResponseMemberID != generation.MemberID {
			continue
		}
		return exchange, nil
	}
	return Exchange{}, errors.New("no matched successful LeaveGroup exchange for the exact old member")
}

func nextGenerationWithTransientBrokerRetry(
	ctx context.Context,
	group *kafkago.ConsumerGroup,
	role string,
) (*kafkago.Generation, []generationRetryEvidence, error) {
	const maxRetries = 8
	var retries []generationRetryEvidence
	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		generation, err := group.Next(ctx)
		if err == nil {
			return generation, retries, nil
		}
		var brokerError kafkago.Error
		if !errors.As(err, &brokerError) || !isTransientCoordinatorError(brokerError) || attempt == maxRetries+1 {
			return nil, retries, err
		}
		retries = append(retries, generationRetryEvidence{
			Role:       role,
			Attempt:    attempt,
			ErrorCode:  int16(brokerError),
			ObservedAt: time.Now(),
		})
	}
	return nil, retries, errors.New("unreachable consumer group retry state")
}

func isTransientCoordinatorError(err kafkago.Error) bool {
	switch err {
	case kafkago.GroupCoordinatorNotAvailable, kafkago.GroupLoadInProgress,
		kafkago.NotCoordinatorForGroup, kafkago.RebalanceInProgress:
		return true
	default:
		return false
	}
}

func fetchOffsetSnapshot(ctx context.Context, client *kafkago.Client, groupID, topic string, partitions []int) ([]offsetPartitionEvidence, error) {
	response, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  map[string][]int{topic: append([]int(nil), partitions...)},
	})
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}
	byPartition := make(map[int]kafkago.OffsetFetchPartition)
	for _, partition := range response.Topics[topic] {
		if partition.Error != nil {
			return nil, partition.Error
		}
		byPartition[partition.Partition] = partition
	}
	snapshot := make([]offsetPartitionEvidence, 0, len(partitions))
	for _, partitionID := range partitions {
		partition, ok := byPartition[partitionID]
		if !ok {
			return nil, fmt.Errorf("OffsetFetch did not return partition %d", partitionID)
		}
		snapshot = append(snapshot, offsetPartitionEvidence{
			Partition:       partition.Partition,
			CommittedOffset: partition.CommittedOffset,
		})
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].Partition < snapshot[j].Partition })
	return snapshot, nil
}

func appendHandlerEffect(path string, effect processingEffectEvidence) error {
	payload, err := json.Marshal(effect)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(payload, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func findPartitionAssignment(generation *kafkago.Generation, topic string, partition int) (kafkago.PartitionAssignment, bool) {
	for _, assignment := range generation.Assignments[topic] {
		if assignment.ID == partition {
			return assignment, true
		}
	}
	return kafkago.PartitionAssignment{}, false
}

func fetchSyntheticRecordForHandler(
	ctx context.Context,
	collector *Collector,
	dialer *net.Dialer,
	brokers []string,
	topic string,
	partition int,
	phase string,
	workerRole string,
	generation *kafkago.Generation,
) (processingEffectEvidence, error) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		Dialer:    &kafkago.Dialer{Timeout: 5 * time.Second, DialFunc: collector.WrapDial(phase, dialer.DialContext)},
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   time.Second,
	})
	defer reader.Close()

	message, err := reader.FetchMessage(ctx)
	if err != nil {
		return processingEffectEvidence{}, err
	}
	keyHash := sha256.Sum256(message.Key)
	valueHash := sha256.Sum256(message.Value)
	return processingEffectEvidence{
		Phase:        phase,
		HandlerType:  "COMPONENT_TEST_HANDLER",
		WorkerRole:   workerRole,
		GroupID:      generation.GroupID,
		MemberID:     generation.MemberID,
		GenerationID: generation.ID,
		Topic:        message.Topic,
		Partition:    message.Partition,
		Offset:       message.Offset,
		KeySHA256:    hex.EncodeToString(keyHash[:]),
		ValueSHA256:  hex.EncodeToString(valueHash[:]),
		ObservedAt:   time.Now(),
	}, nil
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func componentSourceSHA256() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("test source path unavailable")
	}
	entries, err := os.ReadDir(filepath.Dir(file))
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(file), entry.Name()))
		if err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(entry.Name()))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
