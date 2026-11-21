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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

const envKafkaResourceManifest = "RECOVERY_KAFKA_RESOURCE_MANIFEST"

const (
	probeSessionTimeoutMS           = int32(30_000)
	probeRebalanceTimeoutMS         = int32(30_000)
	maxCoordinatorDiscoveryAttempts = 8
	coordinatorDiscoveryRetryDelay  = 100 * time.Millisecond
)

type generationTupleEvidence struct {
	GroupID      string `json:"groupId"`
	MemberID     string `json:"memberId"`
	GenerationID int    `json:"generationId"`
}

type noLeaveOffsetEvidence struct {
	ObservedAt time.Time                            `json:"observedAt"`
	Topics     map[string][]offsetPartitionEvidence `json:"topics"`
	Error      string                               `json:"error,omitempty"`
}

type coordinatorDiscoveryAttempt struct {
	Attempt           int       `json:"attempt"`
	Classification    string    `json:"classification"`
	ResponseErrorCode *int16    `json:"responseErrorCode,omitempty"`
	CallError         string    `json:"callError,omitempty"`
	ExchangeCaptured  bool      `json:"exchangeCaptured"`
	Exchange          *Exchange `json:"exchange,omitempty"`
}

type noLeaveGenerationEvidence struct {
	Status                            string                        `json:"status"`
	RunID                             string                        `json:"runId"`
	Attempt                           int                           `json:"attempt"`
	SourceRevision                    string                        `json:"sourceRevision"`
	ResourceManifestSHA256            string                        `json:"resourceManifestSha256"`
	ComponentSourceSHA256             string                        `json:"componentSourceSha256"`
	Candidate                         string                        `json:"candidate"`
	ClusterID                         string                        `json:"clusterId,omitempty"`
	BootstrapBrokers                  []string                      `json:"bootstrapBrokers"`
	Coordinator                       string                        `json:"coordinator,omitempty"`
	CoordinatorDiscoveryAttempts      []coordinatorDiscoveryAttempt `json:"coordinatorDiscoveryAttempts"`
	GroupID                           string                        `json:"groupId"`
	MemberBRetryCapturedBeforeARejoin bool                          `json:"memberBRetryCapturedBeforeARejoin"`
	JoinGroupLeaderID                 string                        `json:"joinGroupLeaderId,omitempty"`
	JoinGroupLeaderMemberIDs          []string                      `json:"joinGroupLeaderMemberIds"`
	APIVersionBasis                   string                        `json:"apiVersionBasis"`
	ApiVersionsObservation            string                        `json:"apiVersionsObservation"`
	CapabilityNegotiationClaim        bool                          `json:"capabilityNegotiationClaim"`
	SessionTimeoutMS                  int32                         `json:"sessionTimeoutMs"`
	RebalanceTimeoutMS                int32                         `json:"rebalanceTimeoutMs"`
	SessionExpiryObservation          string                        `json:"sessionExpiryObservation"`
	OldMember                         generationTupleEvidence       `json:"oldMember"`
	RejoinedMember                    generationTupleEvidence       `json:"rejoinedMember"`
	SecondMember                      generationTupleEvidence       `json:"secondMember"`
	GenerationAdvanced                bool                          `json:"generationAdvanced"`
	HeartbeatProbe                    *Exchange                     `json:"heartbeatProbe,omitempty"`
	HeartbeatProbeOutcome             string                        `json:"heartbeatProbeOutcome"`
	HeartbeatClientError              string                        `json:"heartbeatClientError,omitempty"`
	HeartbeatProbeDeadlineMS          int                           `json:"heartbeatProbeDeadlineMs"`
	ExplicitHeartbeatProbeCount       int                           `json:"explicitHeartbeatProbeCount"`
	MemberLifecycleHeartbeatCount     int                           `json:"memberLifecycleHeartbeatCount"`
	HeartbeatMayRefreshSession        bool                          `json:"heartbeatMayRefreshSession"`
	LeaveGroupRequestCount            int                           `json:"leaveGroupRequestCount"`
	OffsetCommitRequestCount          uint64                        `json:"offsetCommitRequestCount"`
	OffsetFetchBefore                 *noLeaveOffsetEvidence        `json:"offsetFetchBefore,omitempty"`
	OffsetFetchAfter                  *noLeaveOffsetEvidence        `json:"offsetFetchAfter,omitempty"`
	TopicRecordWrites                 int                           `json:"topicRecordWrites"`
	ProducerWrites                    int                           `json:"producerWrites"`
	TestHandlerInvocations            int                           `json:"testHandlerInvocations"`
	ApplicationEffects                string                        `json:"applicationEffects"`
	ProcessedOffsetStatus             string                        `json:"processedOffsetStatus"`
	BusinessFenceStatus               string                        `json:"businessFenceStatus"`
	LeaveControlBoundary              string                        `json:"leaveControlBoundary"`
	FailureStage                      string                        `json:"failureStage,omitempty"`
	FailureType                       string                        `json:"failureType,omitempty"`
	FailureMessage                    string                        `json:"failureMessage,omitempty"`
	CollectorStats                    CollectorStats                `json:"collectorStats"`
	Exchanges                         []Exchange                    `json:"exchanges"`
}

type joinCall struct {
	response *kafkago.JoinGroupResponse
	err      error
}

type generationJoinFunc func(context.Context, string) (*kafkago.JoinGroupResponse, error)

type syncCall struct {
	response *kafkago.SyncGroupResponse
	err      error
}

type coordinatorLookupFunc func(context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error)

func joinGenerationUpdateMembers(
	ctx context.Context,
	collector *Collector,
	groupID string,
	memberAID string,
	joinA generationJoinFunc,
	joinB generationJoinFunc,
) (*kafkago.JoinGroupResponse, *kafkago.JoinGroupResponse, bool, string, error) {
	bFirst, err := joinB(ctx, "")
	if err != nil {
		return nil, nil, false, "initial_member_b_join", err
	}
	if bFirst == nil {
		return nil, nil, false, "initial_member_b_join_result", errors.New("JoinGroup returned no response for member B")
	}
	if !errors.Is(bFirst.Error, kafkago.MemberIDRequired) {
		return nil, nil, false, "member_b_member_id_required", fmt.Errorf("initial member B JoinGroup error = %v, want MemberIDRequired", bFirst.Error)
	}
	if bFirst.MemberID == "" {
		return nil, nil, false, "member_b_id_required_without_id", errors.New("MemberIDRequired response omitted assigned member ID")
	}

	bRetryCh := make(chan joinCall, 1)
	bRetryDone := make(chan struct{})
	go func() {
		defer close(bRetryDone)
		response, callErr := joinB(ctx, bFirst.MemberID)
		bRetryCh <- joinCall{response: response, err: callErr}
	}()
	if _, err := waitForCapturedJoinGroupRequest(ctx, collector, "member-b", groupID, bFirst.MemberID, bRetryDone); err != nil {
		return nil, nil, false, "member_b_join_retry_dispatch", err
	}

	aRejoinCh := make(chan joinCall, 1)
	go func() {
		response, callErr := joinA(ctx, memberAID)
		aRejoinCh <- joinCall{response: response, err: callErr}
	}()
	var aRejoin, bRetry joinCall
	for received := 0; received < 2; {
		select {
		case aRejoin = <-aRejoinCh:
			received++
			if aRejoin.err != nil {
				return nil, nil, true, "member_a_rejoin", aRejoin.err
			}
			if aRejoin.response == nil {
				return nil, nil, true, "member_a_rejoin_result", errors.New("JoinGroup returned no response for member A")
			}
		case bRetry = <-bRetryCh:
			received++
			if bRetry.err != nil {
				return nil, nil, true, "member_b_join_retry", bRetry.err
			}
			if bRetry.response == nil {
				return nil, nil, true, "member_b_join_retry_result", errors.New("JoinGroup returned no response for member B retry")
			}
		case <-ctx.Done():
			return nil, nil, true, "generation_update_join_wait", ctx.Err()
		}
	}
	return aRejoin.response, bRetry.response, true, "", nil
}

func waitForCapturedJoinGroupRequest(
	ctx context.Context,
	collector *Collector,
	role string,
	groupID string,
	memberID string,
	callDone <-chan struct{},
) (Exchange, error) {
	findRequest := func() (Exchange, bool) {
		exchanges := collector.Snapshot()
		for index := len(exchanges) - 1; index >= 0; index-- {
			exchange := exchanges[index]
			if exchange.Role == role && exchange.GroupID == groupID && exchange.MemberID == memberID &&
				exchange.APIKey == int16(protocol.JoinGroup) && exchange.RequestFrameLength > 0 &&
				exchange.RequestFrameHex != "" && !exchange.RequestFrameTruncated &&
				(exchange.MatchStatus == "REQUEST_ONLY" || exchange.MatchStatus == "MATCHED" || exchange.MatchStatus == "RESPONSE_DECODE_ERROR") {
				return exchange, true
			}
		}
		return Exchange{}, false
	}

	if exchange, ok := findRequest(); ok {
		return exchange, nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-callDone:
			if exchange, ok := findRequest(); ok {
				return exchange, nil
			}
			return Exchange{}, errors.New("member B retry returned before a complete JoinGroup request was captured")
		case <-ctx.Done():
			return Exchange{}, fmt.Errorf("wait for member B retry request capture: %w", ctx.Err())
		case <-ticker.C:
			if exchange, ok := findRequest(); ok {
				return exchange, nil
			}
		}
	}
}

// This test sends only Kafka group protocol requests. It does not create topics,
// produce records, run ConsumerGroup lifecycle heartbeats, or issue LeaveGroup.
func TestRealBrokerGenerationUpdateWithoutLeaveCapturesOldHeartbeatOutcome(t *testing.T) {
	brokers := splitNonEmpty(os.Getenv(envBrokers))
	runID := strings.TrimSpace(os.Getenv(envRunID))
	sourceRevision := strings.TrimSpace(os.Getenv(envSourceRevision))
	evidenceDir := strings.TrimSpace(os.Getenv(envEvidenceDir))
	manifestPath := strings.TrimSpace(os.Getenv(envKafkaResourceManifest))
	if os.Getenv(envAllowIsolatedBroker) != "1" || len(brokers) == 0 || runID == "" ||
		sourceRevision == "" || evidenceDir == "" || manifestPath == "" {
		t.Skip("requires Root-supplied fresh Kafka manifest, run identity, evidence directory, brokers, and RECOVERY_KAFKA_ALLOW_ISOLATED_BROKER=1")
	}

	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read Root-supplied resource manifest: %v", err)
	}
	manifestSum := sha256.Sum256(manifest)
	componentHash, err := componentSourceSHA256()
	if err != nil {
		t.Fatalf("compute component source hash: %v", err)
	}

	attempt := 1
	if value := strings.TrimSpace(os.Getenv(envAttempt)); value != "" {
		parsed, parseErr := strconv.Atoi(value)
		if parseErr != nil || parsed < 1 {
			t.Fatalf("%s must be a positive integer", envAttempt)
		}
		attempt = parsed
	}
	runSum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", runID, attempt)))
	runKey := hex.EncodeToString(runSum[:8])
	groupID := "rc-kgen-" + runKey
	evidencePath := filepath.Join(evidenceDir, "kafka-generation-update-"+runKey+".json")
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatalf("create evidence directory: %v", err)
	}
	if _, err := os.Stat(evidencePath); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("evidence target already exists or cannot be inspected; use a new run ID or attempt")
	}

	identity := Identity{
		RunID:          runID,
		SourceRevision: sourceRevision,
		Candidate:      "kafka-generation-update-sha256-" + componentHash,
	}
	collector := NewCollector(identity)
	evidence := noLeaveGenerationEvidence{
		Status:                       "RUNNING",
		RunID:                        runID,
		Attempt:                      attempt,
		SourceRevision:               sourceRevision,
		ResourceManifestSHA256:       hex.EncodeToString(manifestSum[:]),
		ComponentSourceSHA256:        componentHash,
		Candidate:                    identity.Candidate,
		BootstrapBrokers:             append([]string(nil), brokers...),
		CoordinatorDiscoveryAttempts: []coordinatorDiscoveryAttempt{},
		GroupID:                      groupID,
		JoinGroupLeaderMemberIDs:     []string{},
		APIVersionBasis:              "Request API versions are observed from captured wire headers. Locked kafka-go Transport source performs ApiVersions followed by SelectVersion/SetVersions; this collector does not retain that exchange or selected map, so whether negotiation completed and which capabilities were selected remain UNKNOWN.",
		ApiVersionsObservation:       "NOT_CAPTURED_BY_KAFKACOLLECTOR; negotiated capability set UNKNOWN",
		CapabilityNegotiationClaim:   false,
		SessionTimeoutMS:             probeSessionTimeoutMS,
		RebalanceTimeoutMS:           probeRebalanceTimeoutMS,
		SessionExpiryObservation:     "NOT_WAITED; this bounded attempt observes a broker-issued generation update and then probes the old tuple.",
		HeartbeatProbeOutcome:        "NOT_ATTEMPTED",
		HeartbeatProbeDeadlineMS:     5_000,
		HeartbeatMayRefreshSession:   true,
		TopicRecordWrites:            0,
		ProducerWrites:               0,
		TestHandlerInvocations:       0,
		ApplicationEffects:           "NOT_RUN",
		ProcessedOffsetStatus:        "NOT_EXERCISED",
		BusinessFenceStatus:          "NOT_TESTED",
		LeaveControlBoundary:         "This raw Client-API attempt sends no explicit or automatic LeaveGroup. The historical explicit LeaveGroup control is separate and is not substituted here.",
		Exchanges:                    []Exchange{},
	}
	defer func() {
		if evidence.Status == "RUNNING" {
			evidence.Status = "INCOMPLETE"
		}
		evidence.CollectorStats = collector.Stats()
		evidence.OffsetCommitRequestCount = evidence.CollectorStats.OffsetCommitRequests
		evidence.Exchanges = collector.Snapshot()
		evidence.LeaveGroupRequestCount = 0
		evidence.ExplicitHeartbeatProbeCount = 0
		for _, exchange := range evidence.Exchanges {
			if exchange.APIKey == int16(protocol.LeaveGroup) {
				evidence.LeaveGroupRequestCount++
			}
			if exchange.APIKey == int16(protocol.Heartbeat) {
				evidence.ExplicitHeartbeatProbeCount++
				probe := exchange
				evidence.HeartbeatProbe = &probe
			}
		}
		payload, marshalErr := json.MarshalIndent(evidence, "", "  ")
		if marshalErr != nil {
			t.Errorf("marshal generation evidence: %v", marshalErr)
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
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			t.Errorf("sync evidence file %s: %v", evidencePath, syncErr)
			return
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("close evidence file %s: %v", evidencePath, closeErr)
			return
		}
		t.Logf("KAFKA_GENERATION_UPDATE_EVIDENCE=%s", evidencePath)
	}()
	fail := func(stage string, failure error) {
		evidence.FailureStage = stage
		evidence.FailureType = fmt.Sprintf("%T", failure)
		evidence.FailureMessage = failure.Error()
		t.Fatalf("%s: %v", stage, failure)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	bootstrap := newGenerationClient(collector, dialer, "bootstrap", brokers[0])
	metadata, err := bootstrap.Metadata(ctx, &kafkago.MetadataRequest{Addr: kafkago.TCP(brokers[0])})
	if err != nil {
		fail("initial_metadata", err)
	}
	if metadata.ClusterID == "" {
		fail("initial_metadata_cluster_id", errors.New("Metadata response did not contain a cluster ID"))
	}
	evidence.ClusterID = metadata.ClusterID
	coordResponse, coordinatorAttempts, err := findCoordinatorWithReadinessRetry(ctx, func(callCtx context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error) {
		beforeCount := len(collector.Snapshot())
		response, callErr := bootstrap.FindCoordinator(callCtx, &kafkago.FindCoordinatorRequest{
			Addr: kafkago.TCP(brokers[0]), Key: groupID, KeyType: kafkago.CoordinatorKeyTypeConsumer,
		})
		exchanges := collector.Snapshot()
		for index := len(exchanges) - 1; index >= beforeCount; index-- {
			exchange := exchanges[index]
			if exchange.APIKey == int16(protocol.FindCoordinator) && exchange.GroupID == groupID &&
				exchange.CoordinatorKeyType == int8(kafkago.CoordinatorKeyTypeConsumer) {
				return response, exchange, true, callErr
			}
		}
		return response, Exchange{}, false, callErr
	})
	evidence.CoordinatorDiscoveryAttempts = coordinatorAttempts
	if err != nil {
		fail("find_group_coordinator", err)
	}
	if coordResponse == nil {
		fail("find_group_coordinator_empty_response", errors.New("FindCoordinator returned no response"))
	}
	if coordResponse.Error != nil {
		fail("find_group_coordinator_response", coordResponse.Error)
	}
	if coordResponse.Coordinator == nil {
		fail("find_group_coordinator_missing_coordinator", errors.New("successful FindCoordinator response omitted coordinator"))
	}
	coordAddr := net.JoinHostPort(coordResponse.Coordinator.Host, strconv.Itoa(coordResponse.Coordinator.Port))
	coordinator := kafkago.TCP(coordAddr)
	evidence.Coordinator = coordAddr
	before, err := fetchGenerationOffsetState(ctx, bootstrap, coordinator, groupID)
	if err != nil {
		fail("offset_fetch_before_membership", err)
	}
	evidence.OffsetFetchBefore = &before

	memberAClient := newGenerationClient(collector, dialer, "member-a", coordAddr)
	memberBClient := newGenerationClient(collector, dialer, "member-b", coordAddr)
	joinA, err := memberAClient.JoinGroup(ctx, generationJoinRequest(coordinator, groupID, ""))
	if err != nil {
		fail("initial_member_a_join", err)
	}
	memberAID := joinA.MemberID
	if errors.Is(joinA.Error, kafkago.MemberIDRequired) {
		if memberAID == "" {
			fail("member_a_id_required_without_id", errors.New("MemberIDRequired response omitted assigned member ID"))
		}
		joinA, err = memberAClient.JoinGroup(ctx, generationJoinRequest(coordinator, groupID, memberAID))
		if err != nil {
			fail("initial_member_a_join_retry", err)
		}
	}
	if joinA.Error != nil || joinA.GenerationID < 1 || joinA.MemberID == "" {
		fail("initial_member_a_join_result", fmt.Errorf("error=%v memberId=%q generation=%d", joinA.Error, joinA.MemberID, joinA.GenerationID))
	}
	memberAID = joinA.MemberID
	oldSync, err := memberAClient.SyncGroup(ctx, generationSyncRequest(coordinator, groupID, joinA.GenerationID, memberAID,
		joinA.ProtocolName, []kafkago.SyncGroupRequestAssignment{{MemberID: memberAID, Assignment: emptyGenerationAssignment()}}))
	if err != nil {
		fail("initial_member_a_sync", err)
	}
	if oldSync.Error != nil {
		fail("initial_member_a_sync_result", oldSync.Error)
	}
	oldTuple := generationTupleEvidence{GroupID: groupID, MemberID: memberAID, GenerationID: joinA.GenerationID}
	evidence.OldMember = oldTuple

	joinAUpdated, joinBUpdated, bRetryCaptured, joinStage, joinErr := joinGenerationUpdateMembers(
		ctx, collector, groupID, memberAID,
		func(callCtx context.Context, memberID string) (*kafkago.JoinGroupResponse, error) {
			return memberAClient.JoinGroup(callCtx, generationJoinRequest(coordinator, groupID, memberID))
		},
		func(callCtx context.Context, memberID string) (*kafkago.JoinGroupResponse, error) {
			return memberBClient.JoinGroup(callCtx, generationJoinRequest(coordinator, groupID, memberID))
		},
	)
	evidence.MemberBRetryCapturedBeforeARejoin = bRetryCaptured
	if joinErr != nil {
		fail(joinStage, joinErr)
	}
	if joinAUpdated.Error != nil || joinBUpdated.Error != nil {
		fail("generation_update_join_errors", fmt.Errorf("A=%v B=%v", joinAUpdated.Error, joinBUpdated.Error))
	}
	if joinAUpdated.MemberID != oldTuple.MemberID || joinAUpdated.GenerationID <= oldTuple.GenerationID ||
		joinBUpdated.GenerationID != joinAUpdated.GenerationID || joinBUpdated.MemberID == "" || joinBUpdated.MemberID == oldTuple.MemberID {
		fail("generation_update_identity", fmt.Errorf("old=%+v rejoined={member:%s generation:%d} second={member:%s generation:%d}",
			oldTuple, joinAUpdated.MemberID, joinAUpdated.GenerationID, joinBUpdated.MemberID, joinBUpdated.GenerationID))
	}
	leaderID, leaderMemberIDs, leaderErr := verifyJoinGroupLeaderMembers(joinAUpdated, joinBUpdated)
	evidence.JoinGroupLeaderID = leaderID
	evidence.JoinGroupLeaderMemberIDs = leaderMemberIDs
	if leaderErr != nil {
		fail("generation_update_member_list", leaderErr)
	}
	newGeneration := joinAUpdated.GenerationID
	evidence.RejoinedMember = generationTupleEvidence{GroupID: groupID, MemberID: joinAUpdated.MemberID, GenerationID: newGeneration}
	evidence.SecondMember = generationTupleEvidence{GroupID: groupID, MemberID: joinBUpdated.MemberID, GenerationID: joinBUpdated.GenerationID}
	evidence.GenerationAdvanced = newGeneration > oldTuple.GenerationID

	aAssignments, bAssignments, assignmentsErr := generationSyncAssignments(
		joinAUpdated.LeaderID, joinAUpdated.MemberID, joinBUpdated.MemberID,
	)
	if assignmentsErr != nil {
		fail("generation_update_unknown_leader", assignmentsErr)
	}
	aSyncCh := make(chan syncCall, 1)
	go func() {
		response, callErr := memberAClient.SyncGroup(ctx, generationSyncRequest(coordinator, groupID, newGeneration,
			joinAUpdated.MemberID, joinAUpdated.ProtocolName, aAssignments))
		aSyncCh <- syncCall{response: response, err: callErr}
	}()
	bSyncCh := make(chan syncCall, 1)
	go func() {
		response, callErr := memberBClient.SyncGroup(ctx, generationSyncRequest(coordinator, groupID, newGeneration,
			joinBUpdated.MemberID, joinBUpdated.ProtocolName, bAssignments))
		bSyncCh <- syncCall{response: response, err: callErr}
	}()
	aSync, bSync := <-aSyncCh, <-bSyncCh
	if aSync.err != nil || bSync.err != nil {
		fail("generation_update_sync_transport", errors.Join(aSync.err, bSync.err))
	}
	if aSync.response == nil || bSync.response == nil || aSync.response.Error != nil || bSync.response.Error != nil {
		fail("generation_update_sync_results", fmt.Errorf("SyncGroup A=%v B=%v", responseError(aSync.response), responseError(bSync.response)))
	}
	if err := verifyGenerationTupleCaptures(collector.Snapshot(), oldTuple, evidence.RejoinedMember, evidence.SecondMember); err != nil {
		fail("verify_join_sync", err)
	}

	probeCtx, cancelProbe := context.WithTimeout(ctx, time.Duration(evidence.HeartbeatProbeDeadlineMS)*time.Millisecond)
	probeResponse, probeErr := memberAClient.Heartbeat(probeCtx, &kafkago.HeartbeatRequest{
		Addr: coordinator, GroupID: groupID, MemberID: oldTuple.MemberID, GenerationID: int32(oldTuple.GenerationID),
	})
	probeContextError := probeCtx.Err()
	cancelProbe()
	if probeErr != nil {
		evidence.HeartbeatClientError = fmt.Sprintf("%T: %v", probeErr, probeErr)
		evidence.HeartbeatProbeOutcome = "NO_DECODED_RESPONSE"
	} else if probeResponse == nil {
		evidence.HeartbeatClientError = "Heartbeat returned no response"
		evidence.HeartbeatProbeOutcome = "NO_DECODED_RESPONSE"
	} else if probeResponse.Error != nil {
		evidence.HeartbeatClientError = fmt.Sprintf("%T: %v", probeResponse.Error, probeResponse.Error)
		evidence.HeartbeatProbeOutcome = "CLIENT_DECODED_BROKER_ERROR"
	} else {
		evidence.HeartbeatProbeOutcome = "CLIENT_RETURNED_NO_ERROR"
	}
	probe, foundProbe := findGenerationHeartbeat(collector.Snapshot(), groupID, oldTuple.MemberID, int32(oldTuple.GenerationID))
	if foundProbe {
		collector.recordCallOutcome(probe.ConnectionID, probe.CorrelationID, probeContextError, probeErr)
		probe, _ = findGenerationHeartbeat(collector.Snapshot(), groupID, oldTuple.MemberID, int32(oldTuple.GenerationID))
		evidence.HeartbeatProbe = &probe
		switch {
		case probe.ResponseClassification != "":
			evidence.HeartbeatProbeOutcome = probe.ResponseClassification
		case probe.ResponseDecoderError != "":
			evidence.HeartbeatProbeOutcome = "RESPONSE_DECODE_ERROR"
		case !probe.Matched:
			evidence.HeartbeatProbeOutcome = "UNKNOWN_NO_MATCHED_RESPONSE"
		default:
			evidence.HeartbeatProbeOutcome = "UNKNOWN_RAW_CLASSIFICATION"
		}
		if probe.RequestFrameHex == "" || probe.ResponseFrameHex == "" || probe.RequestFrameTruncated || probe.ResponseFrameTruncated {
			fail("heartbeat_probe_frame_capture", errors.New("Heartbeat does not contain complete request and response frames"))
		}
	} else {
		evidence.HeartbeatProbeOutcome = "UNKNOWN_NO_CAPTURED_EXCHANGE"
	}

	after, err := fetchGenerationOffsetState(ctx, bootstrap, coordinator, groupID)
	if err != nil {
		fail("offset_fetch_after_probe", err)
	}
	evidence.OffsetFetchAfter = &after
	evidence.CollectorStats = collector.Stats()
	exchanges := collector.Snapshot()
	evidence.OffsetCommitRequestCount = evidence.CollectorStats.OffsetCommitRequests
	evidence.LeaveGroupRequestCount = countCapturedAPI(exchanges, int16(protocol.LeaveGroup))
	evidence.ExplicitHeartbeatProbeCount = countCapturedAPI(exchanges, int16(protocol.Heartbeat))
	if evidence.LeaveGroupRequestCount != 0 || evidence.OffsetCommitRequestCount != 0 || evidence.ExplicitHeartbeatProbeCount != 1 {
		fail("no_leave_no_offsetcommit_boundary", fmt.Errorf("LeaveGroup=%d OffsetCommit=%d Heartbeat=%d",
			evidence.LeaveGroupRequestCount, evidence.OffsetCommitRequestCount, evidence.ExplicitHeartbeatProbeCount))
	}
	for _, exchange := range exchanges {
		if exchange.APIKey != int16(protocol.JoinGroup) && exchange.APIKey != int16(protocol.SyncGroup) && exchange.APIKey != int16(protocol.Heartbeat) {
			continue
		}
		if exchange.RequestFrameLength == 0 || exchange.RequestFrameHex == "" || exchange.RequestFrameTruncated ||
			exchange.ResponseFrameLength == 0 || exchange.ResponseFrameHex == "" || exchange.ResponseFrameTruncated {
			fail("group_protocol_frame_capture", fmt.Errorf("API %d correlation %d did not retain complete request/response frames", exchange.APIKey, exchange.CorrelationID))
		}
	}
	evidence.Status = "CAPTURED_MEMBERSHIP_GENERATION_UPDATE_AND_OLD_HEARTBEAT_OUTCOME"
}

func newGenerationClient(collector *Collector, dialer *net.Dialer, role, address string) *kafkago.Client {
	return &kafkago.Client{
		Addr: kafkago.TCP(address),
		Transport: &kafkago.Transport{
			Dial:        collector.WrapDial(role, dialer.DialContext),
			DialTimeout: 5 * time.Second,
		},
		Timeout: 5 * time.Second,
	}
}

func generationJoinRequest(coordinator net.Addr, groupID, memberID string) *kafkago.JoinGroupRequest {
	return &kafkago.JoinGroupRequest{
		Addr: coordinator, GroupID: groupID,
		SessionTimeout:   time.Duration(probeSessionTimeoutMS) * time.Millisecond,
		RebalanceTimeout: time.Duration(probeRebalanceTimeoutMS) * time.Millisecond,
		MemberID:         memberID, ProtocolType: "consumer",
		Protocols: []kafkago.GroupProtocol{{Name: "range", Metadata: kafkago.GroupProtocolSubscription{Topics: []string{}, UserData: []byte("membership-only")}}},
	}
}

func generationSyncRequest(coordinator net.Addr, groupID string, generationID int, memberID, protocolName string, assignments []kafkago.SyncGroupRequestAssignment) *kafkago.SyncGroupRequest {
	return &kafkago.SyncGroupRequest{
		Addr: coordinator, GroupID: groupID, GenerationID: generationID, MemberID: memberID,
		ProtocolType: "consumer", ProtocolName: protocolName, Assignments: assignments,
	}
}

func emptyGenerationAssignment() kafkago.GroupProtocolAssignment {
	return kafkago.GroupProtocolAssignment{AssignedPartitions: map[string][]int{}}
}

func generationSyncAssignments(leaderID, memberAID, memberBID string) (assignmentsA, assignmentsB []kafkago.SyncGroupRequestAssignment, err error) {
	leaderAssignments := []kafkago.SyncGroupRequestAssignment{
		{MemberID: memberAID, Assignment: emptyGenerationAssignment()},
		{MemberID: memberBID, Assignment: emptyGenerationAssignment()},
	}
	if leaderID == memberAID {
		return leaderAssignments, nil, nil
	}
	if leaderID != memberBID {
		return nil, nil, fmt.Errorf("leader %q is neither member A nor B", leaderID)
	}
	return nil, leaderAssignments, nil
}

func verifyJoinGroupLeaderMembers(joinA, joinB *kafkago.JoinGroupResponse) (string, []string, error) {
	if joinA == nil || joinB == nil {
		return "", nil, errors.New("JoinGroup member response is missing")
	}
	if joinA.MemberID == "" || joinB.MemberID == "" {
		return "", nil, fmt.Errorf("JoinGroup member IDs are empty: A=%q B=%q", joinA.MemberID, joinB.MemberID)
	}
	if joinA.LeaderID == "" || joinA.LeaderID != joinB.LeaderID {
		return "", nil, fmt.Errorf("JoinGroup leaders differ or are empty: A=%q B=%q", joinA.LeaderID, joinB.LeaderID)
	}

	leaderResponse := (*kafkago.JoinGroupResponse)(nil)
	switch joinA.LeaderID {
	case joinA.MemberID:
		leaderResponse = joinA
	case joinB.MemberID:
		leaderResponse = joinB
	default:
		return "", nil, fmt.Errorf("JoinGroup leader %q is neither member A %q nor member B %q", joinA.LeaderID, joinA.MemberID, joinB.MemberID)
	}

	memberIDs := make([]string, 0, len(leaderResponse.Members))
	seen := make(map[string]struct{}, len(leaderResponse.Members))
	for _, member := range leaderResponse.Members {
		if member.ID == "" {
			return "", nil, errors.New("leader JoinGroup member list contains an empty member ID")
		}
		if _, exists := seen[member.ID]; exists {
			return "", nil, fmt.Errorf("leader JoinGroup member list repeats member ID %q", member.ID)
		}
		seen[member.ID] = struct{}{}
		memberIDs = append(memberIDs, member.ID)
	}
	sort.Strings(memberIDs)
	for _, memberID := range []string{joinA.MemberID, joinB.MemberID} {
		if _, exists := seen[memberID]; !exists {
			return joinA.LeaderID, memberIDs, fmt.Errorf("leader JoinGroup member list %v omits acquired member ID %q", memberIDs, memberID)
		}
	}
	return joinA.LeaderID, memberIDs, nil
}

func findCoordinatorWithReadinessRetry(ctx context.Context, lookup coordinatorLookupFunc) (*kafkago.FindCoordinatorResponse, []coordinatorDiscoveryAttempt, error) {
	attempts := make([]coordinatorDiscoveryAttempt, 0, maxCoordinatorDiscoveryAttempts)
	var lastResponse *kafkago.FindCoordinatorResponse
	for number := 1; number <= maxCoordinatorDiscoveryAttempts; number++ {
		if number > 1 {
			if err := ctx.Err(); err != nil {
				return lastResponse, attempts, err
			}
		}
		response, exchange, captured, callErr := lookup(ctx)
		attempt := coordinatorDiscoveryAttempt{Attempt: number, ExchangeCaptured: captured}
		if captured {
			copy := exchange
			attempt.Exchange = &copy
		}
		if callErr != nil {
			attempt.Classification = "TRANSPORT_ERROR"
			attempt.CallError = fmt.Sprintf("%T: %v", callErr, callErr)
			attempts = append(attempts, attempt)
			return response, attempts, callErr
		}
		if response == nil {
			attempt.Classification = "NO_RESPONSE"
			attempts = append(attempts, attempt)
			return nil, attempts, errors.New("FindCoordinator returned no response")
		}
		if response.Error == nil {
			if response.Coordinator == nil {
				attempt.Classification = "SUCCESS_WITHOUT_COORDINATOR"
				attempts = append(attempts, attempt)
				return response, attempts, errors.New("successful FindCoordinator response omitted coordinator")
			}
			attempt.Classification = "COORDINATOR_RESOLVED"
			attempts = append(attempts, attempt)
			return response, attempts, nil
		}
		var brokerError kafkago.Error
		if errors.As(response.Error, &brokerError) {
			code := int16(brokerError)
			attempt.ResponseErrorCode = &code
		}
		if errors.As(response.Error, &brokerError) && brokerError == kafkago.GroupCoordinatorNotAvailable &&
			isTransientCoordinatorError(brokerError) {
			attempt.Classification = "COORDINATOR_NOT_READY"
			attempts = append(attempts, attempt)
			lastResponse = response
			if number == maxCoordinatorDiscoveryAttempts {
				return lastResponse, attempts, nil
			}
			timer := time.NewTimer(coordinatorDiscoveryRetryDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return lastResponse, attempts, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		attempt.Classification = "NONRETRYABLE_BROKER_ERROR"
		attempts = append(attempts, attempt)
		return response, attempts, nil
	}
	return lastResponse, attempts, nil
}

func TestFindCoordinatorReadinessRetriesCode15ThenSucceeds(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, attempts, err := findCoordinatorWithReadinessRetry(ctx, func(context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error) {
		calls++
		if calls == 1 {
			return &kafkago.FindCoordinatorResponse{Error: kafkago.GroupCoordinatorNotAvailable}, coordinatorTestExchange(calls, int16(kafkago.GroupCoordinatorNotAvailable)), true, nil
		}
		return &kafkago.FindCoordinatorResponse{
			Coordinator: &kafkago.FindCoordinatorResponseCoordinator{NodeID: 1, Host: "broker", Port: 9092},
		}, coordinatorTestExchange(calls, 0), true, nil
	})
	if err != nil {
		t.Fatalf("findCoordinatorWithReadinessRetry: %v", err)
	}
	if response == nil || response.Error != nil || response.Coordinator == nil || response.Coordinator.NodeID != 1 {
		t.Fatalf("response = %+v, want resolved coordinator node 1", response)
	}
	if calls != 2 || len(attempts) != 2 {
		t.Fatalf("calls=%d attempts=%d, want 2 each", calls, len(attempts))
	}
	if attempts[0].Classification != "COORDINATOR_NOT_READY" || attempts[0].ResponseErrorCode == nil || *attempts[0].ResponseErrorCode != 15 {
		t.Fatalf("first attempt = %+v, want code 15 classified COORDINATOR_NOT_READY", attempts[0])
	}
	if attempts[0].Exchange == nil || attempts[0].Exchange.RequestFrameHex != "request-frame-1" ||
		attempts[0].Exchange.ResponseFrameHex != "response-frame-1" || attempts[0].Exchange.RequestFrameTruncated ||
		attempts[0].Exchange.ResponseFrameTruncated {
		t.Fatalf("first failed exchange not preserved: %+v", attempts[0].Exchange)
	}
	if attempts[1].Classification != "COORDINATOR_RESOLVED" || attempts[1].Exchange == nil ||
		attempts[1].Exchange.RequestFrameHex != "request-frame-2" || attempts[1].Exchange.ResponseFrameHex != "response-frame-2" {
		t.Fatalf("second attempt = %+v, want preserved coordinator success", attempts[1])
	}
}

func TestFindCoordinatorReadinessDoesNotRetryNonretryableBrokerError(t *testing.T) {
	for _, brokerError := range []kafkago.Error{kafkago.GroupAuthorizationFailed, kafkago.GroupLoadInProgress} {
		t.Run(brokerError.Error(), func(t *testing.T) {
			calls := 0
			response, attempts, err := findCoordinatorWithReadinessRetry(context.Background(), func(context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error) {
				calls++
				return &kafkago.FindCoordinatorResponse{Error: brokerError}, coordinatorTestExchange(calls, int16(brokerError)), true, nil
			})
			if err != nil {
				t.Fatalf("findCoordinatorWithReadinessRetry: %v", err)
			}
			if response == nil || response.Error != brokerError {
				t.Fatalf("response = %+v, want original broker error %v", response, brokerError)
			}
			if calls != 1 || len(attempts) != 1 || attempts[0].Classification != "NONRETRYABLE_BROKER_ERROR" {
				t.Fatalf("calls=%d attempts=%+v, want one nonretryable attempt", calls, attempts)
			}
		})
	}
}

func TestFindCoordinatorReadinessRetryStopsAtContextDeadline(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	response, attempts, err := findCoordinatorWithReadinessRetry(ctx, func(context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error) {
		calls++
		return &kafkago.FindCoordinatorResponse{Error: kafkago.GroupCoordinatorNotAvailable}, coordinatorTestExchange(calls, int16(kafkago.GroupCoordinatorNotAvailable)), true, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
	if response == nil || response.Error != kafkago.GroupCoordinatorNotAvailable {
		t.Fatalf("last response = %+v, want preserved code 15", response)
	}
	if calls != 1 || len(attempts) != 1 || attempts[0].Classification != "COORDINATOR_NOT_READY" {
		t.Fatalf("calls=%d attempts=%+v, want one preserved coordinator-not-ready attempt", calls, attempts)
	}
}

func TestFindCoordinatorReadinessRetryStopsAtMaximumAttempts(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, attempts, err := findCoordinatorWithReadinessRetry(ctx, func(context.Context) (*kafkago.FindCoordinatorResponse, Exchange, bool, error) {
		calls++
		return &kafkago.FindCoordinatorResponse{Error: kafkago.GroupCoordinatorNotAvailable}, coordinatorTestExchange(calls, int16(kafkago.GroupCoordinatorNotAvailable)), true, nil
	})
	if err != nil {
		t.Fatalf("findCoordinatorWithReadinessRetry: %v", err)
	}
	if response == nil || response.Error != kafkago.GroupCoordinatorNotAvailable {
		t.Fatalf("last response = %+v, want preserved code 15", response)
	}
	if calls != maxCoordinatorDiscoveryAttempts || len(attempts) != maxCoordinatorDiscoveryAttempts {
		t.Fatalf("calls=%d attempts=%d, want maximum %d", calls, len(attempts), maxCoordinatorDiscoveryAttempts)
	}
	for index, attempt := range attempts {
		if attempt.Attempt != index+1 || attempt.Classification != "COORDINATOR_NOT_READY" || attempt.Exchange == nil {
			t.Fatalf("attempt[%d] = %+v, want consecutive captured coordinator-not-ready result", index, attempt)
		}
	}
}

func coordinatorTestExchange(correlationID int, responseErrorCode int16) Exchange {
	code := responseErrorCode
	requestFrame := fmt.Sprintf("request-frame-%d", correlationID)
	responseFrame := fmt.Sprintf("response-frame-%d", correlationID)
	return Exchange{
		APIKey: int16(protocol.FindCoordinator), CorrelationID: int32(correlationID), Matched: true,
		RequestFrameLength: len(requestFrame), RequestFrameHex: requestFrame,
		ResponseErrorCode: &code, ResponseFrameLength: len(responseFrame), ResponseFrameHex: responseFrame,
	}
}

func verifyGenerationTupleCaptures(exchanges []Exchange, old, rejoined, second generationTupleEvidence) error {
	for _, target := range []struct {
		role  string
		tuple generationTupleEvidence
	}{
		{role: "member-a", tuple: old},
		{role: "member-a", tuple: rejoined},
		{role: "member-b", tuple: second},
	} {
		if err := verifyCapturedJoinSync(exchanges, target.role, target.tuple.GroupID, target.tuple.MemberID, target.tuple.GenerationID); err != nil {
			return fmt.Errorf("%s tuple %+v: %w", target.role, target.tuple, err)
		}
	}
	return nil
}

func TestGenerationSyncAssignmentsFollowJoinGroupLeader(t *testing.T) {
	for _, tc := range []struct {
		name     string
		leader   string
		wantA    int
		wantB    int
		follower string
	}{
		{name: "member A leads", leader: "member-a", wantA: 2, follower: "member-b"},
		{name: "member B leads", leader: "member-b", wantB: 2, follower: "member-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b, err := generationSyncAssignments(tc.leader, "member-a", "member-b")
			if err != nil {
				t.Fatalf("generationSyncAssignments: %v", err)
			}
			if len(a) != tc.wantA || len(b) != tc.wantB {
				t.Fatalf("assignment counts A=%d B=%d, want A=%d B=%d", len(a), len(b), tc.wantA, tc.wantB)
			}
			leaderAssignments := a
			if tc.leader == "member-b" {
				leaderAssignments = b
			}
			if len(leaderAssignments) != 2 || leaderAssignments[0].MemberID != "member-a" || leaderAssignments[1].MemberID != "member-b" {
				t.Fatalf("leader assignments = %+v, want both members in A/B order", leaderAssignments)
			}
			if tc.follower == "member-a" && len(a) != 0 || tc.follower == "member-b" && len(b) != 0 {
				t.Fatalf("follower %q received assignments: A=%+v B=%+v", tc.follower, a, b)
			}
		})
	}
}

func TestJoinGenerationUpdateMembersCapturesBRegistrationRetryBeforeARejoin(t *testing.T) {
	const (
		groupID   = "group"
		memberAID = "member-a"
		memberBID = "member-b"
	)

	collector := NewCollector(Identity{})
	aRejoinEntered := make(chan struct{})
	joinA := func(_ context.Context, memberID string) (*kafkago.JoinGroupResponse, error) {
		defer close(aRejoinEntered)
		if memberID != memberAID {
			return nil, fmt.Errorf("member A join used ID %q", memberID)
		}
		requestCaptured := false
		for _, exchange := range collector.Snapshot() {
			if exchange.Role == "member-b" && exchange.GroupID == groupID && exchange.MemberID == memberBID &&
				exchange.APIKey == int16(protocol.JoinGroup) && exchange.RequestFrameLength > 0 &&
				exchange.RequestFrameHex != "" && !exchange.RequestFrameTruncated &&
				exchange.MatchStatus == "REQUEST_ONLY" {
				requestCaptured = true
				break
			}
		}
		if !requestCaptured {
			return nil, errors.New("member A rejoin started before member B retry request capture")
		}
		return &kafkago.JoinGroupResponse{
			GenerationID: 8, MemberID: memberAID, LeaderID: memberAID,
			Members: []kafkago.JoinGroupResponseMember{{ID: memberAID}, {ID: memberBID}},
		}, nil
	}
	joinB := func(_ context.Context, memberID string) (*kafkago.JoinGroupResponse, error) {
		if memberID == "" {
			return &kafkago.JoinGroupResponse{Error: kafkago.MemberIDRequired, MemberID: memberBID}, nil
		}
		if memberID != memberBID {
			return nil, fmt.Errorf("member B retry used ID %q", memberID)
		}
		collector.appendRequest(Exchange{
			Role: "member-b", GroupID: groupID, MemberID: memberID,
			APIKey: int16(protocol.JoinGroup), MatchStatus: "REQUEST_ONLY",
			RequestFrameLength: 1, RequestFrameHex: "00",
		})
		<-aRejoinEntered
		return &kafkago.JoinGroupResponse{
			GenerationID: 8, MemberID: memberBID, LeaderID: memberAID,
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	joinAUpdated, joinBUpdated, retryCapturedBeforeA, stage, err := joinGenerationUpdateMembers(
		ctx, collector, groupID, memberAID, joinA, joinB,
	)
	if !retryCapturedBeforeA {
		t.Fatal("member B retry was not captured before member A rejoin")
	}
	if err != nil {
		t.Fatalf("joinGenerationUpdateMembers failed at %q: %v", stage, err)
	}
	if joinAUpdated == nil || joinAUpdated.GenerationID != 8 || joinBUpdated == nil || joinBUpdated.GenerationID != 8 {
		t.Fatalf("updated joins A=%+v B=%+v, want both members in generation 8", joinAUpdated, joinBUpdated)
	}
}

func TestVerifyJoinGroupLeaderMembersRequiresBothJoinedMembers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		joinA      *kafkago.JoinGroupResponse
		joinB      *kafkago.JoinGroupResponse
		wantLeader string
		wantIDs    string
		wantError  bool
	}{
		{
			name:       "member A leader roster contains both",
			joinA:      &kafkago.JoinGroupResponse{LeaderID: "member-a", MemberID: "member-a", Members: []kafkago.JoinGroupResponseMember{{ID: "member-a"}, {ID: "member-b"}}},
			joinB:      &kafkago.JoinGroupResponse{LeaderID: "member-a", MemberID: "member-b"},
			wantLeader: "member-a", wantIDs: "member-a,member-b",
		},
		{
			name:       "member B leader roster contains both",
			joinA:      &kafkago.JoinGroupResponse{LeaderID: "member-b", MemberID: "member-a"},
			joinB:      &kafkago.JoinGroupResponse{LeaderID: "member-b", MemberID: "member-b", Members: []kafkago.JoinGroupResponseMember{{ID: "member-b"}, {ID: "member-a"}}},
			wantLeader: "member-b", wantIDs: "member-a,member-b",
		},
		{
			name:      "leader roster omits member B",
			joinA:     &kafkago.JoinGroupResponse{LeaderID: "member-a", MemberID: "member-a", Members: []kafkago.JoinGroupResponseMember{{ID: "member-a"}}},
			joinB:     &kafkago.JoinGroupResponse{LeaderID: "member-a", MemberID: "member-b", Members: []kafkago.JoinGroupResponseMember{{ID: "member-a"}, {ID: "member-b"}}},
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaderID, memberIDs, err := verifyJoinGroupLeaderMembers(tc.joinA, tc.joinB)
			if tc.wantError {
				if err == nil {
					t.Fatalf("verifyJoinGroupLeaderMembers unexpectedly accepted roster %v", memberIDs)
				}
				if leaderID != "member-a" || strings.Join(memberIDs, ",") != "member-a" {
					t.Fatalf("failed membership verification did not preserve observed roster: leader=%q memberIDs=%v", leaderID, memberIDs)
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyJoinGroupLeaderMembers: %v", err)
			}
			if leaderID != tc.wantLeader || strings.Join(memberIDs, ",") != tc.wantIDs {
				t.Fatalf("leader=%q memberIDs=%v, want leader=%q members=%s", leaderID, memberIDs, tc.wantLeader, tc.wantIDs)
			}
		})
	}
}

func TestVerifyGenerationTupleCapturesRequiresRejoinedGenerationForSameMemberID(t *testing.T) {
	old := generationTupleEvidence{GroupID: "group", MemberID: "member-a", GenerationID: 7}
	rejoined := generationTupleEvidence{GroupID: "group", MemberID: "member-a", GenerationID: 8}
	second := generationTupleEvidence{GroupID: "group", MemberID: "member-b", GenerationID: 8}
	exchanges := append(capturedJoinSyncPair("member-a", old), capturedJoinSyncPair("member-b", second)...)

	if err := verifyGenerationTupleCaptures(exchanges, old, rejoined, second); err == nil {
		t.Fatal("verification passed without JoinGroup and SyncGroup captures for rejoined member A generation 8")
	}
}

func TestVerifyGenerationTupleCapturesAcceptsAllDistinctFullTuples(t *testing.T) {
	old := generationTupleEvidence{GroupID: "group", MemberID: "member-a", GenerationID: 7}
	rejoined := generationTupleEvidence{GroupID: "group", MemberID: "member-a", GenerationID: 8}
	second := generationTupleEvidence{GroupID: "group", MemberID: "member-b", GenerationID: 8}
	exchanges := append(capturedJoinSyncPair("member-a", old), capturedJoinSyncPair("member-a", rejoined)...)
	exchanges = append(exchanges, capturedJoinSyncPair("member-b", second)...)

	if err := verifyGenerationTupleCaptures(exchanges, old, rejoined, second); err != nil {
		t.Fatalf("verifyGenerationTupleCaptures: %v", err)
	}
}

func capturedJoinSyncPair(role string, tuple generationTupleEvidence) []Exchange {
	zero := int16(0)
	return []Exchange{
		{
			Role: role, GroupID: tuple.GroupID, APIKey: int16(protocol.JoinGroup), Matched: true,
			ResponseErrorCode: &zero, ResponseGenerationID: int32(tuple.GenerationID), ResponseMemberID: tuple.MemberID,
		},
		{
			Role: role, GroupID: tuple.GroupID, APIKey: int16(protocol.SyncGroup), Matched: true,
			MemberID: tuple.MemberID, GenerationID: int32(tuple.GenerationID), ResponseErrorCode: &zero,
		},
	}
}

func fetchGenerationOffsetState(ctx context.Context, client *kafkago.Client, coordinator net.Addr, groupID string) (noLeaveOffsetEvidence, error) {
	state := noLeaveOffsetEvidence{ObservedAt: time.Now(), Topics: map[string][]offsetPartitionEvidence{}}
	response, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{Addr: coordinator, GroupID: groupID})
	if err != nil {
		state.Error = fmt.Sprintf("%T: %v", err, err)
		return state, err
	}
	if response.Error != nil {
		state.Error = fmt.Sprintf("%T: %v", response.Error, response.Error)
		return state, response.Error
	}
	for topic, partitions := range response.Topics {
		facts := make([]offsetPartitionEvidence, 0, len(partitions))
		for _, partition := range partitions {
			fact := offsetPartitionEvidence{Partition: partition.Partition, CommittedOffset: partition.CommittedOffset}
			if partition.Error != nil {
				fact.ErrorType = fmt.Sprintf("%T", partition.Error)
			}
			facts = append(facts, fact)
		}
		sort.Slice(facts, func(i, j int) bool { return facts[i].Partition < facts[j].Partition })
		state.Topics[topic] = facts
	}
	return state, nil
}

func verifyCapturedJoinSync(exchanges []Exchange, role, groupID, memberID string, generationID int) error {
	joinFound, syncFound := false, false
	for _, exchange := range exchanges {
		if exchange.Role != role || exchange.GroupID != groupID || !exchange.Matched {
			continue
		}
		switch exchange.APIKey {
		case int16(protocol.JoinGroup):
			if exchange.ResponseErrorCode != nil && *exchange.ResponseErrorCode == 0 &&
				exchange.ResponseGenerationID == int32(generationID) && exchange.ResponseMemberID == memberID {
				joinFound = true
			}
		case int16(protocol.SyncGroup):
			if exchange.MemberID == memberID && exchange.GenerationID == int32(generationID) &&
				exchange.ResponseErrorCode != nil && *exchange.ResponseErrorCode == 0 {
				syncFound = true
			}
		}
	}
	if !joinFound || !syncFound {
		return fmt.Errorf("captured JoinGroup success=%t SyncGroup success=%t for %s generation %d", joinFound, syncFound, memberID, generationID)
	}
	return nil
}

func findGenerationHeartbeat(exchanges []Exchange, groupID, memberID string, generationID int32) (Exchange, bool) {
	for index := len(exchanges) - 1; index >= 0; index-- {
		exchange := exchanges[index]
		if exchange.APIKey == int16(protocol.Heartbeat) && exchange.GroupID == groupID &&
			exchange.MemberID == memberID && exchange.GenerationID == generationID {
			return exchange, true
		}
	}
	return Exchange{}, false
}

func countCapturedAPI(exchanges []Exchange, apiKey int16) int {
	count := 0
	for _, exchange := range exchanges {
		if exchange.APIKey == apiKey {
			count++
		}
	}
	return count
}

func responseError(response *kafkago.SyncGroupResponse) error {
	if response == nil {
		return errors.New("no response")
	}
	return response.Error
}
