//go:build reliability_component

package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/recoverycomponent/kafkacollector"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type notificationOwnerManifest struct {
	ManifestVersion        string `json:"manifest_version"`
	ResourceID             string `json:"resource_id"`
	ContainerID            string `json:"container_id"`
	Database               string `json:"database"`
	Host                   string `json:"host"`
	Port                   int    `json:"port"`
	DSNSHA256              string `json:"dsn_sha256"`
	RunID                  string `json:"run_id"`
	OwnerNonce             string `json:"owner_nonce"`
	ServerSystemIdentifier string `json:"server_system_identifier"`
	DatabaseMarker         string `json:"database_marker"`
}

type notificationKafkaEvidence struct {
	Status                 string                    `json:"status"`
	RunID                  string                    `json:"runId"`
	Attempt                int                       `json:"attempt"`
	SourceRevision         string                    `json:"sourceRevision"`
	ComponentSourceSHA256  string                    `json:"componentSourceSha256"`
	ClusterID              string                    `json:"clusterId"`
	BootstrapBrokers       []string                  `json:"bootstrapBrokers"`
	Topic                  string                    `json:"topic"`
	GroupID                string                    `json:"groupId"`
	OldMember              map[string]any            `json:"oldMember"`
	NewMember              map[string]any            `json:"newMember"`
	OldHandlerInvoked      bool                      `json:"oldHandlerInvoked"`
	NewHandlerInvoked      bool                      `json:"newHandlerInvoked"`
	EventID                string                    `json:"eventId"`
	Partition              int                       `json:"partition"`
	RecordOffset           int64                     `json:"recordOffset"`
	OldCommitBrokerCode    *int16                    `json:"oldCommitBrokerErrorCode,omitempty"`
	NewCommitBrokerCode    *int16                    `json:"newCommitBrokerErrorCode,omitempty"`
	LeaveGroupRequestCount int                       `json:"leaveGroupRequestCount"`
	EffectSource           string                    `json:"effectSource"`
	SourceCompleteness     string                    `json:"sourceCompleteness"`
	ProbeGeneration        int32                     `json:"probeGeneration"`
	ProbeBeforeLeave       bool                      `json:"probeBeforeLeave"`
	NotificationRowsOld    int                       `json:"notificationRowsAfterOldHandler"`
	NotificationRowsNew    int                       `json:"notificationRowsAfterNewHandler"`
	DuplicateSuppressed    bool                      `json:"duplicateSuppressedByProductSemanticKey"`
	OldCommitAttempted     bool                      `json:"oldCommitAttempted"`
	OldCommitOutcome       string                    `json:"oldCommitOutcome"`
	OldCommitError         string                    `json:"oldCommitError,omitempty"`
	NewCommitAttempted     bool                      `json:"newCommitAttempted"`
	NewCommitOutcome       string                    `json:"newCommitOutcome"`
	NewCommitError         string                    `json:"newCommitError,omitempty"`
	OffsetFetchBefore      []offsetFact              `json:"offsetFetchBefore"`
	OffsetFetchAfter       []offsetFact              `json:"offsetFetchAfter"`
	OldCommitFrontierAfter []offsetFact              `json:"oldCommitFrontierAfter,omitempty"`
	OldCommitResults       []commitResult            `json:"oldCommitResults,omitempty"`
	NewCommitResults       []commitResult            `json:"newCommitResults,omitempty"`
	HeartbeatProbe         *kafkacollector.Exchange  `json:"heartbeatProbe,omitempty"`
	HeartbeatCodeAgreement string                    `json:"heartbeatCodeAgreement,omitempty"`
	Exchanges              []kafkacollector.Exchange `json:"exchanges"`
	Boundary               string                    `json:"boundary"`
	FailureStage           string                    `json:"failureStage,omitempty"`
	FailureType            string                    `json:"failureType,omitempty"`
}

type offsetFact struct {
	GroupID         string `json:"groupId"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	CommittedOffset int64  `json:"committedOffset"`
	ErrorCode       int16  `json:"errorCode"`
}

type commitResult struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	ErrorCode int16  `json:"errorCode"`
}

func TestNotificationProductHandlerFencesOldGenerationAndAdvancesNewOwner(t *testing.T) {
	brokers := splitBrokers(os.Getenv("RECOVERY_KAFKA_BROKERS"))
	runID := strings.TrimSpace(os.Getenv("RECOVERY_KAFKA_RUN_ID"))
	evidenceDir := strings.TrimSpace(os.Getenv("RECOVERY_KAFKA_EVIDENCE_DIR"))
	if os.Getenv("RECOVERY_KAFKA_ALLOW_ISOLATED_BROKER") != "1" || len(brokers) == 0 ||
		runID == "" || evidenceDir == "" {
		t.Skip("requires isolated broker, run ID, evidence directory, and RECOVERY_KAFKA_ALLOW_ISOLATED_BROKER=1")
	}
	sourceSHA := strings.TrimSpace(os.Getenv("NOTIFICATION_COMPONENT_SOURCE_SHA256"))
	if len(sourceSHA) != 64 {
		t.Fatal("NOTIFICATION_COMPONENT_SOURCE_SHA256 is required")
	}
	attempt := 1
	if raw := strings.TrimSpace(os.Getenv("RECOVERY_KAFKA_ATTEMPT")); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &attempt); err != nil || attempt < 1 {
			t.Fatal("RECOVERY_KAFKA_ATTEMPT must be a positive integer")
		}
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	evidencePath := filepath.Join(evidenceDir, "notification-kafka-"+hex.EncodeToString(
		sha256Sum([]byte(runID + "|" + fmt.Sprint(attempt)))[:8])+".json")
	if _, err := os.Stat(evidencePath); err == nil {
		t.Fatalf("evidence target already exists: %s", evidencePath)
	}

	pool, reader, _, _ := notificationComponentPools(t)
	repo := repository.NewPostgresNotificationRepository(pool)
	svc := service.NewNotificationService(repo, nil, zap.NewNop())
	consumer := &NotificationConsumer{service: svc, logger: zap.NewNop()}

	collector := kafkacollector.NewCollector(kafkacollector.Identity{
		RunID: runID, SourceRevision: strings.TrimSpace(os.Getenv("RECOVERY_KAFKA_SOURCE_REVISION")),
		Candidate: "notification-product-handler-sha256-" + sourceSHA,
	})
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	client := &kafkago.Client{Addr: kafkago.TCP(brokers[0]), Timeout: 5 * time.Second,
		Transport: &kafkago.Transport{Dial: collector.WrapDial("admin", dialer.DialContext)}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	metadata, err := client.Metadata(ctx, &kafkago.MetadataRequest{})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	suffix := hex.EncodeToString(sha256Sum([]byte(runID + "|" + fmt.Sprint(attempt)))[:6])
	topic := topics.TopicBookingEvents + ".rc-" + suffix
	groupID := "notification-booking-group.rc-" + suffix
	if created, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	}); err != nil || (created.Errors[topic] != nil &&
		!errors.Is(created.Errors[topic], kafkago.TopicAlreadyExists)) {
		t.Fatalf("create booking.events topic: %v %v", err, created.Errors[topic])
	}
	evidence := notificationKafkaEvidence{
		Status: "RUNNING", RunID: runID, Attempt: attempt,
		SourceRevision:        os.Getenv("RECOVERY_KAFKA_SOURCE_REVISION"),
		ComponentSourceSHA256: sourceSHA, ClusterID: metadata.ClusterID,
		BootstrapBrokers: append([]string(nil), brokers...), Topic: topic, GroupID: groupID,
		Boundary: "The real Notification product handler processes a BookingConfirmed event. Old and new group generations are broker-assigned; business effects are read from the product notification database. No provider/email request occurs. This is component-only evidence.",
	}
	defer func() {
		if evidence.Status == "RUNNING" {
			evidence.Status = "INCOMPLETE"
		}
		evidence.Exchanges = collector.Snapshot()
		payload, _ := json.MarshalIndent(evidence, "", "  ")
		if err := os.WriteFile(evidencePath, append(payload, '\n'), 0o600); err != nil {
			t.Errorf("write notification kafka evidence: %v", err)
		}
		t.Logf("NOTIFICATION_KAFKA_EVIDENCE=%s", evidencePath)
	}()

	newGroup := func(role string) (*kafkago.ConsumerGroup, error) {
		return kafkago.NewConsumerGroup(kafkago.ConsumerGroupConfig{
			ID: groupID, Brokers: brokers, Topics: []string{topic},
			Dialer:            &kafkago.Dialer{Timeout: 5 * time.Second, DialFunc: collector.WrapDial(role, dialer.DialContext)},
			StartOffset:       kafkago.FirstOffset,
			HeartbeatInterval: time.Second, SessionTimeout: 10 * time.Second,
			RebalanceTimeout: 15 * time.Second, JoinGroupBackoff: 100 * time.Millisecond,
		})
	}
	oldGroup, err := newGroup("old-product-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer oldGroup.Close()
	oldGeneration, err := nextGenerationWithCoordinatorRetry(ctx, oldGroup)
	if err != nil {
		t.Fatalf("old generation: %v", err)
	}
	oldCtx, oldCancel := context.WithCancel(ctx)
	oldStarted := make(chan struct{})
	oldDone := make(chan struct{})
	go func() {
		close(oldStarted)
		oldGeneration.Start(func(genCtx context.Context) {
			select {
			case <-genCtx.Done():
			case <-oldCtx.Done():
			}
		})
		close(oldDone)
	}()
	<-oldStarted

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers[0]), Topic: topic, RequiredAcks: kafkago.RequireAll,
		Transport: &kafkago.Transport{Dial: collector.WrapDial("synthetic-booking-producer", dialer.DialContext)}}
	bookingID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	eventIDKafka := uuid.New()
	payload := map[string]any{
		"booking_id": bookingID.String(), "user_id": userID.String(),
		"event_id": eventID.String(), "total": 725, "currency": "USD",
		"email": "candidate@example.test",
	}
	payloadRaw, _ := json.Marshal(payload)
	envelope, _ := json.Marshal(map[string]any{
		"id": eventIDKafka.String(), "type": topics.TypeBookingConfirmed,
		"timestamp": time.Now().UTC(), "data": payload,
	})
	recordOffset := topicNextOffset(t, collector, dialer, brokers[0], topic, 0)
	if err := producer.WriteMessages(ctx, kafkago.Message{Key: []byte(bookingID.String()), Value: envelope, Time: time.Now()}); err != nil {
		t.Fatalf("produce BookingConfirmed: %v", err)
	}
	_ = producer.Close()

	oldEffectCount := notificationRowCount(t, reader, bookingID)
	oldMessage := readRecordAt(t, collector, dialer, brokers, topic, 0, recordOffset, "old-product-handler")
	if oldMessage.Offset != recordOffset || oldMessage.Key != bookingID.String() {
		t.Fatalf("old handler record mismatch: %+v want offset %d key %s", oldMessage, recordOffset, bookingID)
	}
	evidence.EventID = oldMessage.ID
	evidence.Partition = 0
	evidence.RecordOffset = oldMessage.Offset
	if err := consumer.handleBookingEvent(ctx, kafka.Event{ID: oldMessage.ID, Type: topics.TypeBookingConfirmed, Timestamp: time.Now(), Data: payloadRaw}); err != nil {
		t.Fatalf("old product handler: %v", err)
	}
	evidence.OldHandlerInvoked = true
	evidence.NotificationRowsOld = notificationRowCount(t, reader, bookingID)
	if evidence.NotificationRowsOld != oldEffectCount+1 {
		t.Fatalf("old product handler wrote %d notifications, want one", evidence.NotificationRowsOld-oldEffectCount)
	}
	evidence.OffsetFetchBefore = offsetFacts(t, client, groupID, topic, 0)
	oldAssignment := assignmentFacts(collector.Snapshot(), "old-product-consumer", oldGeneration)
	evidence.OldMember = map[string]any{
		"memberId": oldGeneration.MemberID, "generationId": oldGeneration.ID,
		"syncGroup": oldAssignment,
	}

	newGroupB, err := newGroup("new-product-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer newGroupB.Close()
	// B joins first; A still holds the only partition. Attempt A's normal
	// commit under the superseded generation, then remove A so B receives the
	// assignment in the next generation.
	fencingGeneration, err := nextGenerationWithCoordinatorRetry(ctx, newGroupB)
	if err != nil {
		t.Fatalf("new generation before old leave: %v", err)
	}
	evidence.ProbeGeneration = fencingGeneration.ID
	oldCommit := map[string]map[int]int64{topic: {0: recordOffset + 1}}
	evidence.OldCommitAttempted = true
	if err := oldGeneration.CommitOffsets(oldCommit); err != nil {
		// A broker response code is stronger than a local/transport error and
		// must not be mislabeled as the latter. Only record the code when the
		// matched response actually carries a non-zero error.
		if code := commitBrokerCode(collector.Snapshot(), "old-product-consumer", oldGeneration); code != nil && *code != 0 {
			evidence.OldCommitOutcome = "BROKER_REJECTED"
			evidence.OldCommitBrokerCode = code
		} else {
			evidence.OldCommitOutcome = "COMMIT_ATTEMPT_ERROR"
		}
		evidence.OldCommitError = fmt.Sprintf("%T", err)
	} else {
		evidence.OldCommitOutcome = "COMMIT_ACCEPTED"
		t.Fatal("old generation commit was accepted after the new generation joined")
	}
	evidence.OldCommitResults = commitResults(collector.Snapshot(), "old-product-consumer", oldGeneration)
	evidence.OldCommitFrontierAfter = offsetFacts(t, client, groupID, topic, 0)
	probe := probeOldGeneration(t, collector, dialer, brokers, groupID, oldGeneration)
	evidence.HeartbeatProbe = probe
	evidence.HeartbeatCodeAgreement = probe.ResponseWireSchemaAgreement
	if probe.ResponseWireSchemaErrorCode == nil ||
		*probe.ResponseWireSchemaErrorCode != int16(kafkago.IllegalGeneration) {
		t.Fatalf("probe wire error is not ILLEGAL_GENERATION: %#v", probe)
	}
	// The old tuple was already rejected before this LeaveGroup. Leaving after
	// the probe frees the only partition for the new owner; it cannot explain
	// the earlier ILLEGAL_GENERATION response.
	oldCancel()
	if err := oldGroup.Close(); err != nil {
		t.Fatalf("close old group after probe: %v", err)
	}
	select {
	case <-oldDone:
	case <-time.After(10 * time.Second):
		t.Fatal("old generation did not retire")
	}
	newGeneration, err := nextGenerationWithCoordinatorRetry(ctx, newGroupB)
	if err != nil {
		t.Fatalf("new owner generation: %v", err)
	}
	if assignmentFacts(collector.Snapshot(), "new-product-owner", newGeneration) == nil {
		t.Fatal("new owner never received the partition assignment after old-owner expiry")
	}
	newCtx, newCancel := context.WithCancel(ctx)
	defer newCancel()
	go func() {
		newGeneration.Start(func(genCtx context.Context) {
			select {
			case <-genCtx.Done():
			case <-newCtx.Done():
			}
		})
	}()
	evidence.NewMember = map[string]any{
		"memberId": newGeneration.MemberID, "generationId": newGeneration.ID,
		"syncGroup": assignmentFacts(collector.Snapshot(), "new-product-owner", newGeneration),
	}
	newMessage := readRecordAt(t, collector, dialer, brokers, topic, 0, recordOffset, "new-product-handler")
	if newMessage.Offset != recordOffset {
		t.Fatalf("new product handler read offset %d, want %d", newMessage.Offset, recordOffset)
	}
	if err := consumer.handleBookingEvent(ctx, kafka.Event{ID: newMessage.ID, Type: topics.TypeBookingConfirmed, Timestamp: time.Now(), Data: payloadRaw}); err != nil {
		t.Fatalf("new product handler: %v", err)
	}
	evidence.NewHandlerInvoked = true
	evidence.NotificationRowsNew = notificationRowCount(t, reader, bookingID)
	evidence.DuplicateSuppressed = evidence.NotificationRowsNew == evidence.NotificationRowsOld
	if !evidence.DuplicateSuppressed {
		t.Fatalf("product semantic key did not suppress replay: old=%d new=%d", evidence.NotificationRowsOld, evidence.NotificationRowsNew)
	}
	evidence.NewCommitAttempted = true
	if err := newGeneration.CommitOffsets(map[string]map[int]int64{topic: {0: recordOffset + 1}}); err != nil {
		evidence.NewCommitOutcome = "COMMIT_ATTEMPT_ERROR"
		evidence.NewCommitError = fmt.Sprintf("%T", err)
		t.Fatalf("new owner normal commit failed: %v", err)
	}
	evidence.NewCommitOutcome = "COMMIT_ACCEPTED"
	snapshot := collector.Snapshot()
	evidence.NewCommitResults = commitResults(snapshot, "new-product-owner", newGeneration)
	if evidence.OldCommitOutcome == "BROKER_REJECTED" {
		if code := commitBrokerCode(snapshot, "old-product-consumer", oldGeneration); code != nil && *code != 0 {
			evidence.OldCommitBrokerCode = code
		}
	}
	evidence.NewCommitBrokerCode = commitBrokerCode(snapshot, "new-product-owner", newGeneration)
	evidence.LeaveGroupRequestCount = countLeaveGroups(snapshot)
	evidence.ProbeBeforeLeave = probePrecedesLeave(snapshot, evidence.HeartbeatProbe)
	if !evidence.ProbeBeforeLeave {
		t.Fatal("LeaveGroup preceded the old-tuple rejection")
	}
	evidence.OffsetFetchAfter = offsetFacts(t, client, groupID, topic, 0)
	if len(evidence.OffsetFetchAfter) != 1 || evidence.OffsetFetchAfter[0].CommittedOffset != recordOffset+1 {
		t.Fatalf("new owner frontier: %#v want %d", evidence.OffsetFetchAfter, recordOffset+1)
	}

	evidence.EffectSource = "REAL_NOTIFICATION_PRODUCT_HANDLER_AND_PG16"
	evidence.SourceCompleteness = "COMPLETE"
	evidence.Status = "NOTIFICATION_PRODUCT_HANDLER_GENERATION_FENCE_CAPTURED"
}

func notificationComponentPools(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool, notificationOwnerManifest, string) {
	t.Helper()
	dsn := os.Getenv("NOTIFICATION_COMPONENT_DATABASE_URL")
	manifestPath := os.Getenv("NOTIFICATION_COMPONENT_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" || !filepath.IsAbs(manifestPath) {
		t.Fatal("NOTIFICATION_COMPONENT_DATABASE_URL and absolute owner manifest are required")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("read notification owner manifest")
	}
	var manifest notificationOwnerManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal("parse notification owner manifest")
	}
	expectedMarker := "codex-notification-component:v1:" + manifest.ResourceID + ":" +
		manifest.RunID + ":" + manifest.OwnerNonce
	if manifest.ManifestVersion != "rushgo.notification.component-pg-owner.v1" ||
		manifest.DatabaseMarker != expectedMarker || manifest.ResourceID == "" ||
		manifest.ContainerID != manifest.ResourceID || manifest.Database == "" ||
		manifest.Port < 1 || manifest.ServerSystemIdentifier == "" {
		t.Fatal("notification owner manifest is incomplete or unbound")
	}
	sum := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(sum[:]) != manifest.DSNSHA256 {
		t.Fatal("notification DSN does not match owner manifest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var versionNum int
	var database, systemID, marker string
	if err := admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer,
		current_database(), system_identifier::text,
		COALESCE(shobj_description(oid,'pg_database'),'')
		FROM pg_database CROSS JOIN LATERAL pg_control_system()
		WHERE datname=current_database()`).Scan(&versionNum, &database, &systemID, &marker); err != nil {
		t.Fatal(err)
	}
	if versionNum < 160000 || versionNum >= 170000 || database != manifest.Database ||
		systemID != manifest.ServerSystemIdentifier || marker != manifest.DatabaseMarker {
		t.Fatal("notification database identity differs from owner manifest")
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := strings.TrimSpace(os.Getenv("NOTIFICATION_COMPONENT_SCHEMA"))
	persistentSchema := schema != ""
	if persistentSchema {
		if !regexp.MustCompile(`^[a-z0-9_]{1,63}$`).MatchString(schema) {
			t.Fatal("notification component schema override is invalid")
		}
		if _, err := admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
			t.Fatal(err)
		}
	} else {
		schema = "notification_component_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
				t.Errorf("drop notification component schema: %v", err)
			}
		})
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolated := parsed.String()
	migrateBinary, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("migrate CLI is not on PATH")
	}
	migrationPath := filepath.ToSlash(filepath.Join("..", "..", "migrations"))
	cmd := exec.CommandContext(ctx, migrateBinary, "-path", migrationPath, "-database", isolated, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		safe := strings.ReplaceAll(string(output), isolated, "[isolated DSN]")
		t.Fatalf("notification migrations failed: %s", safe)
	}
	writerConfig, _ := pgxpool.ParseConfig(isolated)
	writerConfig.ConnConfig.RuntimeParams["application_name"] = "notification_candidate_writer"
	writer, err := pgxpool.NewWithConfig(ctx, writerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writer.Close)
	readerConfig, _ := pgxpool.ParseConfig(isolated)
	readerConfig.ConnConfig.RuntimeParams["application_name"] = "notification_candidate_observer"
	reader, err := pgxpool.NewWithConfig(ctx, readerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	return writer, reader, manifest, schema
}

type recordFact struct {
	ID     string
	Offset int64
	Key    string
}

func topicNextOffset(t *testing.T, collector *kafkacollector.Collector, dialer *net.Dialer, broker, topic string, partition int) int64 {
	t.Helper()
	d := &kafkago.Dialer{Timeout: 5 * time.Second, DialFunc: collector.WrapDial("admin-last-offset", dialer.DialContext)}
	conn, err := d.DialLeader(context.Background(), "tcp", broker, topic, partition)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	offset, err := conn.ReadLastOffset()
	if err != nil {
		t.Fatal(err)
	}
	return offset
}

func readRecordAt(t *testing.T, collector *kafkacollector.Collector, dialer *net.Dialer, brokers []string, topic string, partition int, offset int64, role string) recordFact {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: partition,
		Dialer:   &kafkago.Dialer{Timeout: 5 * time.Second, DialFunc: collector.WrapDial(role, dialer.DialContext)},
		MinBytes: 1, MaxBytes: 1 << 20, MaxWait: time.Second,
	})
	defer reader.Close()
	if err := reader.SetOffset(offset); err != nil {
		t.Fatal(err)
	}
	message, err := reader.FetchMessage(context.Background())
	if err != nil {
		t.Fatalf("fetch record at %d: %v", partition, err)
	}
	var envelope struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(message.Value, &envelope)
	return recordFact{ID: envelope.ID, Offset: message.Offset, Key: string(message.Key)}
}

func notificationRowCount(t *testing.T, pool *pgxpool.Pool, bookingID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*)::int FROM notifications WHERE payload->>'booking_id'=$1`, bookingID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assignmentFacts(exchanges []kafkacollector.Exchange, role string, generation *kafkago.Generation) map[string]any {
	var selected *kafkacollector.Exchange
	for index := range exchanges {
		exchange := &exchanges[index]
		if exchange.Role == role && exchange.APIKey == int16(protocol.SyncGroup) &&
			exchange.MemberID == generation.MemberID && exchange.GenerationID == generation.ID &&
			exchange.ResponseErrorCode != nil && *exchange.ResponseErrorCode == 0 &&
			len(exchange.ResponseAssignments) > 0 {
			selected = exchange
		}
	}
	if selected != nil {
		exchange := *selected
		var assignment []map[string]any
		for _, item := range exchange.ResponseAssignments {
			partitions := append([]int32(nil), item.Partitions...)
			assignment = append(assignment, map[string]any{"topic": item.Topic, "partitions": partitions})
		}
		protocolType, protocolName := exchange.ResponseProtocolType, exchange.ResponseProtocolName
		protocolSource := "SYNCGROUP_RESPONSE"
		if protocolType == "" {
			protocolType, protocolName = exchange.RequestProtocolType, exchange.RequestProtocolName
			protocolSource = "SYNCGROUP_REQUEST"
		}
		if protocolType == "" {
			for _, join := range exchanges {
				if join.Role == role && join.APIKey == int16(protocol.JoinGroup) &&
					join.ResponseErrorCode != nil && *join.ResponseErrorCode == 0 &&
					join.ResponseGenerationID == generation.ID &&
					join.ResponseMemberID == generation.MemberID {
					protocolType, protocolName = join.RequestProtocolType, join.RequestProtocolName
					if protocolType == "" {
						protocolType, protocolName = join.ResponseProtocolType, join.ResponseProtocolName
					}
					protocolSource = "JOINGROUP_PROTOCOL"
					break
				}
			}
		}
		return map[string]any{
			"role": role, "matched": exchange.Matched, "errorCode": *exchange.ResponseErrorCode,
			"groupId": generation.GroupID, "memberId": generation.MemberID,
			"generationId":   generation.ID,
			"correlationId":  fmt.Sprint(exchange.CorrelationID),
			"protocolType":   protocolType,
			"protocolName":   protocolName,
			"protocolSource": protocolSource,
			"assignment":     assignment,
		}
	}
	return nil
}

func nextGenerationWithCoordinatorRetry(ctx context.Context, group *kafkago.ConsumerGroup) (*kafkago.Generation, error) {
	for attempt := 1; attempt <= 10; attempt++ {
		generation, err := group.Next(ctx)
		if err == nil {
			return generation, nil
		}
		var brokerError kafkago.Error
		if !errors.As(err, &brokerError) ||
			(brokerError != kafkago.GroupCoordinatorNotAvailable &&
				brokerError != kafkago.GroupLoadInProgress) || attempt == 10 {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, errors.New("unreachable coordinator retry state")
}

func commitResults(exchanges []kafkacollector.Exchange, role string, generation *kafkago.Generation) []commitResult {
	var results []commitResult
	for _, exchange := range exchanges {
		if exchange.Role != role || exchange.APIKey != int16(protocol.OffsetCommit) ||
			exchange.MemberID != generation.MemberID || exchange.GenerationID != generation.ID ||
			!exchange.Matched {
			continue
		}
		for _, item := range exchange.ResponseOffsetCommitResults {
			results = append(results, commitResult{Topic: item.Topic, Partition: int(item.Partition), ErrorCode: item.ErrorCode})
		}
	}
	return results
}

func commitBrokerCode(exchanges []kafkacollector.Exchange, role string, generation *kafkago.Generation) *int16 {
	for _, exchange := range exchanges {
		if exchange.Role == role && exchange.APIKey == int16(protocol.OffsetCommit) &&
			exchange.MemberID == generation.MemberID && exchange.GenerationID == generation.ID &&
			exchange.Matched && len(exchange.ResponseOffsetCommitResults) == 1 {
			code := exchange.ResponseOffsetCommitResults[0].ErrorCode
			return &code
		}
	}
	return nil
}

func probePrecedesLeave(exchanges []kafkacollector.Exchange, probe *kafkacollector.Exchange) bool {
	if probe == nil || probe.ResponseAt == nil {
		return false
	}
	for _, exchange := range exchanges {
		if exchange.APIKey == int16(protocol.LeaveGroup) &&
			exchange.RequestAt.Before(*probe.ResponseAt) {
			return false
		}
	}
	return true
}

func countLeaveGroups(exchanges []kafkacollector.Exchange) int {
	count := 0
	for _, exchange := range exchanges {
		if exchange.APIKey == int16(protocol.LeaveGroup) {
			count++
		}
	}
	return count
}

func probeOldGeneration(t *testing.T, collector *kafkacollector.Collector, dialer *net.Dialer, brokers []string, groupID string, generation *kafkago.Generation) *kafkacollector.Exchange {
	t.Helper()
	probeClient := &kafkago.Client{Addr: kafkago.TCP(brokers[0]), Timeout: 5 * time.Second,
		Transport: &kafkago.Transport{Dial: collector.WrapDial("probe-client", dialer.DialContext)}}
	if _, err := probeClient.Metadata(context.Background(), &kafkago.MetadataRequest{}); err != nil {
		t.Fatal(err)
	}
	coordinator, err := probeClient.FindCoordinator(context.Background(), &kafkago.FindCoordinatorRequest{Key: groupID})
	if err != nil || coordinator.Coordinator.Host == "" {
		t.Fatalf("find coordinator: %v %v", coordinator, err)
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, heartbeatErr := probeClient.Heartbeat(probeCtx, &kafkago.HeartbeatRequest{
		GroupID: groupID, MemberID: generation.MemberID, GenerationID: generation.ID,
	})
	probeCancel()
	if heartbeatErr != nil {
		t.Fatalf("old tuple heartbeat probe transport: %v", heartbeatErr)
	}
	probe := findProbe(collector.Snapshot())
	if probe == nil {
		t.Fatal("no captured old-tuple heartbeat exchange")
	}
	return probe
}

func findProbe(exchanges []kafkacollector.Exchange) *kafkacollector.Exchange {
	for index := range exchanges {
		if exchanges[index].Role == "probe-client" && exchanges[index].APIKey == int16(protocol.Heartbeat) && exchanges[index].Matched {
			copy := exchanges[index]
			return &copy
		}
	}
	return nil
}

func offsetFacts(t *testing.T, client *kafkago.Client, groupID, topic string, partition int) []offsetFact {
	t.Helper()
	response, err := client.OffsetFetch(context.Background(), &kafkago.OffsetFetchRequest{
		GroupID: groupID, Topics: map[string][]int{topic: {partition}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	var result []offsetFact
	for _, item := range response.Topics[topic] {
		result = append(result, offsetFact{GroupID: groupID, Topic: topic, Partition: item.Partition,
			CommittedOffset: item.CommittedOffset, ErrorCode: offsetFetchErrorCode(t, item.Error)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Partition < result[j].Partition })
	return result
}

func offsetFetchErrorCode(t *testing.T, err error) int16 {
	t.Helper()
	if err == nil {
		return 0
	}
	var kafkaErr kafkago.Error
	if !errors.As(err, &kafkaErr) {
		t.Fatalf("unexpected OffsetFetch partition error: %v", err)
	}
	return int16(kafkaErr)
}

func TestOffsetFetchPartitionErrorCodeIsPreserved(t *testing.T) {
	if got := offsetFetchErrorCode(t, nil); got != 0 {
		t.Fatalf("nil error code=%d", got)
	}
	if got := offsetFetchErrorCode(t, fmt.Errorf("partition: %w", kafkago.NotCoordinatorForGroup)); got != 16 {
		t.Fatalf("NotCoordinatorForGroup code=%d want 16", got)
	}
}

func splitBrokers(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}
