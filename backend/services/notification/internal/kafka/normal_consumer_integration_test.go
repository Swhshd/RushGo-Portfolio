//go:build reliability_component && reliability_normal_consumer

package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/recoverycomponent/kafkacollector"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type normalCommitFact struct {
	Role            string `json:"role"`
	Generation      int32  `json:"generation"`
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committedOffset"`
	ErrorCode       int16  `json:"errorCode"`
	CallOutcome     string `json:"callOutcome"`
}

type normalConsumerEvidence struct {
	Status                  string             `json:"status"`
	Phase                   string             `json:"phase"`
	RunID                   string             `json:"runId"`
	SourceSHA256            string             `json:"sourceSha256"`
	SourceCommit            string             `json:"sourceCommit"`
	SourceTree              string             `json:"sourceTree"`
	EvaluatorCommit         string             `json:"evaluatorCommit"`
	ExecutableSha256        string             `json:"executableSha256"`
	Topic                   string             `json:"topic"`
	GroupID                 string             `json:"groupId"`
	Partition               int                `json:"partition"`
	Booking1ID              string             `json:"booking1Id"`
	Booking2ID              string             `json:"booking2Id"`
	Event1ID                string             `json:"event1Id"`
	Event1Offset            int64              `json:"event1Offset"`
	Event2ID                string             `json:"event2Id"`
	Event2Offset            int64              `json:"event2Offset"`
	RowsAfterEvent1         int                `json:"notificationRowsAfterEvent1"`
	RowsAfterEvent2Phase1   int                `json:"notificationRowsAfterEvent2Phase1"`
	CommittedAfterEvent1    int64              `json:"committedOffsetAfterEvent1"`
	CommittedAtPhase1Exit   int64              `json:"committedOffsetAtPhase1Exit"`
	Phase1BoundaryObserved  bool               `json:"phase1BeforeCommitBoundaryObserved"`
	Phase1ExitCode          int                `json:"phase1ExitCode"`
	Phase1HandlerCalls      int64              `json:"phase1HandlerCalls"`
	Phase1CommitFacts       []normalCommitFact `json:"phase1OffsetCommitFacts"`
	Phase2HandlerCalls      int64              `json:"phase2HandlerCalls"`
	RowsAfterEvent2Phase2   int                `json:"notificationRowsAfterEvent2Phase2"`
	Phase1Booking1RecordID  string             `json:"phase1Booking1RecordId,omitempty"`
	Phase1Booking2RecordID  string             `json:"phase1Booking2RecordId,omitempty"`
	Phase1Booking2CreatedAt string             `json:"phase1Booking2CreatedAt,omitempty"`
	Phase1RecordReadBefore  bool               `json:"phase1RecordReadBeforeReplay"`
	RestartRecordPreserved  bool               `json:"restartRecordIdentityPreserved"`
	CommittedAfterPhase2    int64              `json:"committedOffsetAfterPhase2"`
	Phase2CommitFacts       []normalCommitFact `json:"phase2OffsetCommitFacts"`
	DuplicateSuppressed     bool               `json:"duplicateSuppressedByProductSemanticKey"`
	ConsumerPath            string             `json:"consumerPath"`
	Composition             string             `json:"composition"`
	Sender                  string             `json:"sender"`
	RecipientLookup         string             `json:"recipientLookup"`
	Phase2HandlerError      string             `json:"phase2HandlerError,omitempty"`
	Phase2HandlerAccepted   bool               `json:"phase2HandlerAccepted"`
	EffectSource            string             `json:"effectSource"`
	Phase1EvidencePath      string             `json:"phase1EvidencePath,omitempty"`
	Phase1EvidenceSha256    string             `json:"phase1EvidenceSha256,omitempty"`
	FormalEvidence          bool               `json:"formalEvidence"`
	Boundary                string             `json:"boundary"`
}

type normalConsumerRun struct {
	collector *kafkacollector.Collector
	client    *kafkago.Client
	producer  *kafkago.Writer
	topic     string
	groupID   string
	brokers   []string
	runID     string
	evidence  string
}

func normalConsumerEnv(t *testing.T) (brokers []string, dialer *net.Dialer) {
	t.Helper()
	brokers = splitBrokers(os.Getenv("NORMAL_CONSUMER_KAFKA_BROKERS"))
	if len(brokers) == 0 || os.Getenv("NORMAL_CONSUMER_ALLOW_ISOLATED_BROKER") != "1" {
		t.Skip("requires isolated broker and NORMAL_CONSUMER_ALLOW_ISOLATED_BROKER=1")
	}
	return brokers, &net.Dialer{Timeout: 5 * time.Second}
}

func normalConsumerSetup(t *testing.T, role string) *normalConsumerRun {
	t.Helper()
	brokers, dialer := normalConsumerEnv(t)
	runID := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_RUN_ID"))
	evidenceDir := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_EVIDENCE_DIR"))
	if runID == "" || evidenceDir == "" {
		t.Fatal("NORMAL_CONSUMER_RUN_ID and NORMAL_CONSUMER_EVIDENCE_DIR are required")
	}
	if strings.TrimSpace(os.Getenv("NOTIFICATION_COMPONENT_SCHEMA")) == "" {
		t.Fatal("NOTIFICATION_COMPONENT_SCHEMA is required for the restart phases")
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	collector := kafkacollector.NewCollector(kafkacollector.Identity{
		RunID: runID, SourceRevision: os.Getenv("NORMAL_CONSUMER_SOURCE_COMMIT"),
		Candidate: "notification-normal-consumer-" + os.Getenv("NORMAL_CONSUMER_SOURCE_SHA256"),
	})
	client := &kafkago.Client{Addr: kafkago.TCP(brokers[0]), Timeout: 5 * time.Second,
		Transport: &kafkago.Transport{Dial: collector.WrapDial("admin", dialer.DialContext)}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Metadata(ctx, &kafkago.MetadataRequest{}); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	suffix := hex.EncodeToString(sha256Sum([]byte(runID))[:6])
	topic := topics.TopicBookingEvents + ".rc-normal-" + suffix
	groupID := "notification-booking-group.rc-normal-" + suffix
	created, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if topicErr := created.Errors[topic]; topicErr != nil &&
		!errors.Is(topicErr, kafkago.TopicAlreadyExists) {
		t.Fatalf("create topic: %v", topicErr)
	}
	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers[0]), Topic: topic,
		RequiredAcks: kafkago.RequireAll,
		Transport:    &kafkago.Transport{Dial: collector.WrapDial("synthetic-booking-producer", dialer.DialContext)}}
	t.Cleanup(func() { _ = producer.Close() })
	return &normalConsumerRun{collector: collector, client: client, producer: producer,
		topic: topic, groupID: groupID, brokers: brokers, runID: runID, evidence: evidenceDir}
}

func produceBookingConfirmedAt(t *testing.T, run *normalConsumerRun, bookingID uuid.UUID, expectedOffset int64) (string, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offset := topicNextOffset(t, run.collector, &net.Dialer{Timeout: 5 * time.Second},
		run.brokers[0], run.topic, 0)
	if offset != expectedOffset {
		t.Fatalf("topic offset advanced before produce: got %d want %d", offset, expectedOffset)
	}
	eventID := uuid.NewString()
	payload := map[string]any{
		"booking_id": bookingID.String(), "user_id": uuid.NewString(),
		"event_id": uuid.NewString(), "total": 725, "currency": "USD",
		"email": "normal-consumer@example.test",
	}
	envelope, _ := json.Marshal(map[string]any{
		"id": eventID, "type": topics.TypeBookingConfirmed,
		"timestamp": time.Now().UTC(), "data": payload,
	})
	if err := run.producer.WriteMessages(ctx, kafkago.Message{
		Key: []byte(bookingID.String()), Value: envelope, Time: time.Now(),
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	return eventID, offset
}

func waitRows(t *testing.T, reader *pgxpool.Pool, bookingID uuid.UUID, want int, budget time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		var count int
		if err := reader.QueryRow(context.Background(),
			`SELECT count(*)::int FROM notifications WHERE payload->>'booking_id'=$1`,
			bookingID.String()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count >= want {
			return count
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification rows for %s = %d, want %d", bookingID, count, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitCommitted(t *testing.T, run *normalConsumerRun, want int64, budget time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		facts := offsetFacts(t, run.client, run.groupID, run.topic, 0)
		if len(facts) == 1 && facts[0].CommittedOffset >= want {
			return facts[0].CommittedOffset
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed offset = %#v, want %d", facts, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func normalCommitFacts(exchanges []kafkacollector.Exchange, role string) []normalCommitFact {
	var facts []normalCommitFact
	for _, exchange := range exchanges {
		if exchange.APIKey != int16(protocol.OffsetCommit) || exchange.Role != role {
			continue
		}
		for _, request := range exchange.OffsetCommitRequest {
			errorCode := int16(-1)
			for _, result := range exchange.ResponseOffsetCommitResults {
				if result.Topic == request.Topic && result.Partition == request.Partition {
					errorCode = result.ErrorCode
					break
				}
			}
			facts = append(facts, normalCommitFact{
				Role: exchange.Role, Generation: exchange.GenerationID,
				Topic: request.Topic, Partition: request.Partition,
				CommittedOffset: request.CommittedOffset, ErrorCode: errorCode,
				CallOutcome: exchange.CallOutcome,
			})
		}
	}
	return facts
}

func TestNormalNotificationConsumerCommitAndRestart(t *testing.T) {
	phase := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_PHASE"))
	if phase != "1" && phase != "2" {
		t.Fatal("NORMAL_CONSUMER_PHASE must be 1 or 2")
	}
	sourceSHA := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_SOURCE_SHA256"))
	sourceCommit := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_SOURCE_COMMIT"))
	sourceTree := strings.TrimSpace(os.Getenv("NORMAL_CONSUMER_SOURCE_TREE"))
	if len(sourceSHA) != 64 || len(sourceCommit) != 40 || len(sourceTree) != 40 {
		t.Fatal("source identity is required")
	}
	writer, reader, _, _ := notificationComponentPools(t)
	repo := repository.NewPostgresNotificationRepository(writer)
	svc := service.NewNotificationService(repo, nil, zap.NewNop())
	nc := &NotificationConsumer{service: svc, logger: zap.NewNop()}
	run := normalConsumerSetup(t, "normal-consumer")
	evidence := normalConsumerEvidence{
		Status: "RUNNING", Phase: phase, RunID: run.runID,
		SourceSHA256: sourceSHA, SourceCommit: sourceCommit, SourceTree: sourceTree,
		EvaluatorCommit:  os.Getenv("NORMAL_CONSUMER_EVALUATOR_COMMIT"),
		ExecutableSha256: executableSHA256(t),
		Topic:            run.topic, GroupID: run.groupID, Partition: 0,
		ConsumerPath:    "PRODUCTION_PKG_KAFKA_CONSUMER_START",
		Composition:     "TEST_COMPOSITION_PARTIAL_NOTIFICATION_SERVICE",
		Sender:          "NIL_SENDER",
		RecipientLookup: "UNSET",
		EffectSource:    "REAL_NOTIFICATION_PRODUCT_HANDLER_AND_PG16_PARTIAL_COMPOSITION",
		FormalEvidence:  false,
		Boundary: "The production pkg/kafka consumer group loop reads a synthetic BookingConfirmed record, " +
			"dispatches the real Notification product handler and PG16 repository, and commits offsets on the " +
			"normal path. The Notification service is assembled without the production sender/recipient lookup; " +
			"the broker and database are isolated local components; no email or external request occurs.",
	}
	phase1Path := normalConsumerEvidencePath(run.evidence, run.runID, "1")
	evidence.Phase1EvidencePath = phase1Path

	if phase == "1" {
		booking1, booking2 := uuid.New(), uuid.New()
		evidence.Booking1ID, evidence.Booking2ID = booking1.String(), booking2.String()
		var handlerCalls atomic.Int64
		boundary := make(chan struct{})
		var boundaryOffset atomic.Int64
		boundaryOffset.Store(-1)
		observation := &pkgkafka.ConsumerObservation{
			DialFunc: run.collector.WrapDial("normal-consumer",
				(&net.Dialer{Timeout: 5 * time.Second}).DialContext),
			BeforeCommit: func(ctx context.Context, generation int32, topic string,
				partition int, next int64) error {
				if next == boundaryOffset.Load()+1 {
					select {
					case <-boundary:
					default:
						close(boundary)
					}
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
		}
		consumer := pkgkafka.NewConsumerWithObservation(run.brokers, run.topic, run.groupID,
			func(ctx context.Context, event pkgkafka.Event) error {
				handlerCalls.Add(1)
				return nc.handleBookingEvent(ctx, event)
			}, zap.NewNop(), observation)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = consumer.Start(ctx) }()

		offset1 := topicNextOffset(t, run.collector, &net.Dialer{Timeout: 5 * time.Second},
			run.brokers[0], run.topic, 0)
		event1, offset1 := produceBookingConfirmedAt(t, run, booking1, offset1)
		evidence.Event1ID, evidence.Event1Offset = event1, offset1
		evidence.RowsAfterEvent1 = waitRows(t, reader, booking1, 1, 30*time.Second)
		evidence.CommittedAfterEvent1 = waitCommitted(t, run, offset1+1, 30*time.Second)
		evidence.Phase1CommitFacts = normalCommitFacts(run.collector.Snapshot(), "normal-consumer")

		offset2 := topicNextOffset(t, run.collector, &net.Dialer{Timeout: 5 * time.Second},
			run.brokers[0], run.topic, 0)
		// Arm the exact boundary before the record can be consumed.
		boundaryOffset.Store(offset2)
		event2, offset2 := produceBookingConfirmedAt(t, run, booking2, offset2)
		evidence.Event2ID, evidence.Event2Offset = event2, offset2
		evidence.RowsAfterEvent2Phase1 = waitRows(t, reader, booking2, 1, 30*time.Second)
		record1 := readNotificationRecord(t, reader, booking1)
		record2 := readNotificationRecord(t, reader, booking2)
		evidence.Phase1Booking1RecordID = record1.ID
		evidence.Phase1Booking2RecordID = record2.ID
		evidence.Phase1Booking2CreatedAt = record2.CreatedAt
		select {
		case <-boundary:
		case <-time.After(30 * time.Second):
			t.Fatal("product consumer did not reach the before-commit boundary")
		}
		evidence.Phase1BoundaryObserved = true
		evidence.Phase1HandlerCalls = handlerCalls.Load()
		evidence.CommittedAtPhase1Exit = waitCommitted(t, run, offset1+1, 5*time.Second)
		evidence.Phase1ExitCode = 137
		evidence.Status = "NORMAL_CONSUMER_PHASE1_EXITED_BEFORE_COMMIT"
		writeNormalConsumerEvidence(t, phase1Path, evidence)
		os.Exit(137)
	}

	phase1Raw, err := os.ReadFile(phase1Path)
	if err != nil {
		t.Fatalf("phase 1 evidence unavailable: %v", err)
	}
	var phase1 normalConsumerEvidence
	if err := json.Unmarshal(phase1Raw, &phase1); err != nil {
		t.Fatal(err)
	}
	evidence.Phase1EvidenceSha256 = hex.EncodeToString(sha256Sum(phase1Raw))
	if phase1.Status != "NORMAL_CONSUMER_PHASE1_EXITED_BEFORE_COMMIT" ||
		phase1.Event2Offset != phase1.CommittedAtPhase1Exit ||
		phase1.Topic != evidence.Topic || phase1.GroupID != evidence.GroupID ||
		phase1.SourceSHA256 != sourceSHA || phase1.SourceCommit != sourceCommit ||
		phase1.SourceTree != sourceTree || phase1.ExecutableSha256 != evidence.ExecutableSha256 {
		t.Fatalf("phase 1 evidence is not a matching source-bound uncommitted boundary: %+v", phase1)
	}
	booking1, err := uuid.Parse(phase1.Booking1ID)
	if err != nil {
		t.Fatal(err)
	}
	booking2, err := uuid.Parse(phase1.Booking2ID)
	if err != nil {
		t.Fatal(err)
	}
	if phase1.Phase1Booking1RecordID == "" || phase1.Phase1Booking2RecordID == "" ||
		phase1.Phase1Booking2CreatedAt == "" {
		t.Fatalf("phase 1 evidence lacks the persisted record identity: %+v", phase1)
	}
	beforeReplay1 := readNotificationRecord(t, reader, booking1)
	beforeReplay2 := readNotificationRecord(t, reader, booking2)
	if beforeReplay1.ID != phase1.Phase1Booking1RecordID ||
		beforeReplay2.ID != phase1.Phase1Booking2RecordID ||
		beforeReplay2.CreatedAt != phase1.Phase1Booking2CreatedAt {
		t.Fatalf("phase 1 record was missing or recreated before replay: before=%+v %+v", beforeReplay1, beforeReplay2)
	}
	evidence.Phase1RecordReadBefore = true
	evidence.Phase1Booking1RecordID = phase1.Phase1Booking1RecordID
	evidence.Phase1Booking2RecordID = phase1.Phase1Booking2RecordID
	evidence.Phase1Booking2CreatedAt = phase1.Phase1Booking2CreatedAt
	var phase2Calls atomic.Int64
	var phase2HandlerErr atomic.Value
	consumer := pkgkafka.NewConsumerWithObservation(run.brokers, run.topic, run.groupID,
		func(ctx context.Context, event pkgkafka.Event) error {
			phase2Calls.Add(1)
			err := nc.handleBookingEvent(ctx, event)
			if err != nil {
				phase2HandlerErr.Store(err.Error())
			}
			return err
		}, zap.NewNop(), &pkgkafka.ConsumerObservation{
			DialFunc: run.collector.WrapDial("normal-consumer",
				(&net.Dialer{Timeout: 5 * time.Second}).DialContext),
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = consumer.Start(ctx) }()
	evidence.CommittedAfterPhase2 = waitCommitted(t, run, phase1.Event2Offset+1, 45*time.Second)
	deadline := time.Now().Add(30 * time.Second)
	for phase2Calls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if phase2Calls.Load() < 1 {
		t.Fatal("phase 2 consumer never invoked the product handler")
	}
	evidence.Phase2HandlerCalls = phase2Calls.Load()
	evidence.RowsAfterEvent2Phase2 = waitRows(t, reader, booking2, 1, 5*time.Second)
	evidence.RowsAfterEvent1 = waitRows(t, reader, booking1, 1, 5*time.Second)
	afterReplay2 := readNotificationRecord(t, reader, booking2)
	evidence.RestartRecordPreserved = afterReplay2.ID == phase1.Phase1Booking2RecordID &&
		afterReplay2.CreatedAt == phase1.Phase1Booking2CreatedAt
	if !evidence.RestartRecordPreserved {
		t.Fatalf("replay recreated the persisted notification record: before=%s/%s after=%s/%s",
			phase1.Phase1Booking2RecordID, phase1.Phase1Booking2CreatedAt,
			afterReplay2.ID, afterReplay2.CreatedAt)
	}
	evidence.DuplicateSuppressed = evidence.RowsAfterEvent2Phase2 == phase1.RowsAfterEvent2Phase1
	evidence.Phase2CommitFacts = normalCommitFacts(run.collector.Snapshot(), "normal-consumer")
	evidence.Event1ID = phase1.Event1ID
	evidence.Event1Offset = phase1.Event1Offset
	evidence.Event2ID = phase1.Event2ID
	evidence.Event2Offset = phase1.Event2Offset
	evidence.Booking1ID = phase1.Booking1ID
	evidence.Booking2ID = phase1.Booking2ID
	evidence.RowsAfterEvent2Phase1 = phase1.RowsAfterEvent2Phase1
	evidence.CommittedAfterEvent1 = phase1.CommittedAfterEvent1
	evidence.CommittedAtPhase1Exit = phase1.CommittedAtPhase1Exit
	evidence.Phase1BoundaryObserved = phase1.Phase1BoundaryObserved
	evidence.Phase1ExitCode = phase1.Phase1ExitCode
	evidence.Phase1HandlerCalls = phase1.Phase1HandlerCalls
	evidence.Phase1CommitFacts = phase1.Phase1CommitFacts
	if handlerErr := phase2HandlerErr.Load(); handlerErr != nil {
		evidence.Phase2HandlerError = handlerErr.(string)
		t.Fatalf("phase 2 product handler rejected the replay: %s", evidence.Phase2HandlerError)
	}
	evidence.Phase2HandlerAccepted = true
	if !evidence.DuplicateSuppressed || !evidence.RestartRecordPreserved ||
		evidence.CommittedAfterPhase2 != phase1.Event2Offset+1 ||
		evidence.RowsAfterEvent2Phase2 != phase1.RowsAfterEvent2Phase1 ||
		evidence.RowsAfterEvent1 != phase1.RowsAfterEvent1 {
		t.Fatalf("restart did not prove idempotent replay: %+v", evidence)
	}
	evidence.Status = "NORMAL_CONSUMER_RESTART_COMMIT_CAPTURED"
	writeNormalConsumerEvidence(t, normalConsumerEvidencePath(run.evidence, run.runID, "2"), evidence)
}

type notificationRecordFact struct {
	ID        string
	CreatedAt string
}

func readNotificationRecord(t *testing.T, reader *pgxpool.Pool, bookingID uuid.UUID) notificationRecordFact {
	t.Helper()
	var fact notificationRecordFact
	var createdAt time.Time
	if err := reader.QueryRow(context.Background(),
		`SELECT id::text, created_at FROM notifications WHERE payload->>'booking_id'=$1`,
		bookingID.String()).Scan(&fact.ID, &createdAt); err != nil {
		t.Fatalf("phase-1 notification record unavailable for %s: %v", bookingID, err)
	}
	fact.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	return fact
}

func notificationRecordMatches(expected, actual notificationRecordFact) bool {
	return expected.ID != "" && expected.ID == actual.ID &&
		expected.CreatedAt != "" && expected.CreatedAt == actual.CreatedAt
}

func TestNotificationRestartRecordOracle(t *testing.T) {
	expected := notificationRecordFact{ID: "id-1", CreatedAt: "2026-11-17T00:00:00Z"}
	if !notificationRecordMatches(expected, expected) {
		t.Fatal("identical persisted record must match")
	}
	if notificationRecordMatches(expected, notificationRecordFact{ID: "id-2", CreatedAt: expected.CreatedAt}) {
		t.Fatal("recreated record ID must fail")
	}
	if notificationRecordMatches(expected, notificationRecordFact{ID: expected.ID, CreatedAt: "2026-11-17T00:00:01Z"}) {
		t.Fatal("recreated timestamp must fail")
	}
	if notificationRecordMatches(notificationRecordFact{}, expected) {
		t.Fatal("missing phase-1 identity must fail")
	}
}

func executableSHA256(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(sha256Sum(data))
}

func writeNormalConsumerEvidence(t *testing.T, path string, evidence normalConsumerEvidence) {
	t.Helper()
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func normalConsumerEvidencePath(dir, runID, phase string) string {
	sum := sha256.Sum256([]byte(runID))
	name := "normal-consumer-phase" + phase + "-" + hex.EncodeToString(sum[:6]) + ".json"
	return filepath.Join(dir, name)
}
