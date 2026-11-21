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
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
)

func clearReliabilityEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RELIABILITY_ACCEPTANCE_MODE", "false")
	t.Setenv("RELIABILITY_FAULT_TOKEN", "")
	t.Setenv("RELIABILITY_CONTROL_DIR", "")
	t.Setenv("RELIABILITY_KAFKA_TOPIC_SUFFIX", "")
}

func TestDecodeEventRejectsMalformedPayload(t *testing.T) {
	if _, err := decodeEvent([]byte(`{"type":`)); err == nil {
		t.Fatal("malformed Kafka payload must be rejected")
	}
}

func TestCandidateKafkaTopicRequiresAValidCampaignSuffixOnlyWhenEnabled(t *testing.T) {
	base := "booking.events"
	suffix := "0123456789abcdef01234567"
	got, err := candidateKafkaTopic(base, suffix, true)
	if err != nil || got != base+".t23-"+suffix {
		t.Fatalf("candidate topic = %q err=%v", got, err)
	}
	got, err = candidateKafkaTopic(base, "", false)
	if err != nil || got != base {
		t.Fatalf("normal topic changed while reliability mode is disabled: %q err=%v", got, err)
	}
	for _, invalid := range []string{"", "0123456789abcdef0123456", "0123456789ABCDEF01234567", "../../topic"} {
		if _, err := candidateKafkaTopic(base, invalid, true); err == nil {
			t.Fatalf("enabled candidate accepted invalid topic suffix %q", invalid)
		}
	}
}

func TestAcceptanceModeWithoutCampaignTopicSuffixFailsClosed(t *testing.T) {
	t.Setenv("RELIABILITY_ACCEPTANCE_MODE", "true")
	t.Setenv("RELIABILITY_FAULT_TOKEN", "t23-private-token-0123456789")
	t.Setenv("RELIABILITY_CONTROL_DIR", t.TempDir())
	t.Setenv("RELIABILITY_KAFKA_TOPIC_SUFFIX", "")
	consumer := NewConsumer([]string{"127.0.0.1:1"}, "booking.events",
		"notification-booking-group", func(context.Context, Event) error { return nil }, zap.NewNop())
	producer := NewProducer([]string{"127.0.0.1:1"}, []string{"booking.events"}, zap.NewNop())
	if consumer.reliabilityErr == nil || producer.configErr == nil {
		t.Fatal("acceptance Kafka clients did not fail closed without an isolated campaign topic")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := consumer.Start(ctx); err == nil {
		t.Fatal("consumer started with invalid acceptance topic configuration")
	}
	if err := producer.Publish(context.Background(), "booking.events", "key", Event{}); err == nil {
		t.Fatal("invalid acceptance producer attempted a write")
	}
}

func TestConsumerKafkaHookIsDisabledByDefault(t *testing.T) {
	t.Setenv("RELIABILITY_ACCEPTANCE_MODE", "false")
	t.Setenv("RELIABILITY_FAULT_TOKEN", "")
	t.Setenv("RELIABILITY_CONTROL_DIR", "")
	c := NewConsumer([]string{"127.0.0.1:1"}, "booking.events.t23-fixture",
		"notification-booking-group", func(context.Context, Event) error { return nil }, zap.NewNop())
	injected, err := c.awaitT23KafkaBoundary(context.Background(), 3,
		kafkago.Message{Topic: "booking.events.t23-fixture", Partition: 0, Offset: 0},
		Event{ID: "11111111-1111-4111-8111-111111111111", Type: "booking.created"})
	if err != nil || injected {
		t.Fatalf("disabled T23 hook changed Kafka behavior: injected=%v err=%v", injected, err)
	}
}

func TestConsumerRecordsFailOnceOnlyAfterExactKafkaBoundaryReturnsIt(t *testing.T) {
	dir := t.TempDir()
	token := "t23-private-token-0123456789"
	hook, err := reliabilityhook.New(reliabilityhook.Config{
		Enabled: true, Token: token, Directory: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &Consumer{reliability: hook, groupID: "notification-booking-group"}
	windowID := "kafka.partition.A_failure_offset_frontier"
	eventID := "11111111-1111-4111-8111-111111111111"
	topic := "booking.events.t23-0123456789abcdef01234567"
	command := map[string]any{
		"window_id": windowID, "operation_id": eventID,
		"checkpoint": "kafka_before_handler", "action": "pause",
		"token": token, "nonce": "nonce-kafka-failure-test",
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "command.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan struct {
		injected bool
		err      error
	}, 1)
	message := kafkago.Message{Topic: topic, Partition: 0, Offset: 9}
	event := Event{ID: eventID, Type: "BookingCancelled"}
	go func() {
		injected, err := c.awaitT23KafkaBoundary(context.Background(), 5, message, event)
		result <- struct {
			injected bool
			err      error
		}{injected, err}
	}()
	digest := sha256.Sum256([]byte(windowID + "\x00" + eventID + "\x00kafka_before_handler"))
	key := hex.EncodeToString(digest[:16])
	barrierPath := filepath.Join(dir, "barrier-"+key+".json")
	var barrier map[string]any
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(barrierPath)
		if readErr == nil {
			if err := json.Unmarshal(data, &barrier); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if barrier == nil {
		t.Fatal("Kafka consumer did not publish the exact pre-handler barrier")
	}
	decision := map[string]any{
		"token": token, "nonce": barrier["nonce"], "decision": "fail_once",
	}
	encoded, err = json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	decisionPath := filepath.Join(dir, "decision-"+key+".json")
	if err := os.WriteFile(decisionPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.injected {
			t.Fatalf("exact fail-once decision was not applied: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Kafka consumer did not leave the failure boundary")
	}
	if _, err := os.Stat(filepath.Join(dir, "kafka-failure-"+key+".json")); err != nil {
		t.Fatalf("applied transient failure has no private evidence: %v", err)
	}

	retryCheckpoint := "kafka_retry_before_handler"
	retryCommand := map[string]any{
		"window_id": windowID, "operation_id": eventID,
		"checkpoint": retryCheckpoint, "action": "pause",
		"token": token, "nonce": "nonce-kafka-retry-test",
	}
	encoded, err = json.Marshal(retryCommand)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "command.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	retryResult := make(chan struct {
		matched bool
		err     error
	}, 1)
	go func() {
		matched, err := c.awaitT23KafkaRetryBoundary(
			context.Background(), 5, message, event)
		retryResult <- struct {
			matched bool
			err     error
		}{matched, err}
	}()
	retryDigest := sha256.Sum256([]byte(windowID + "\x00" + eventID + "\x00" + retryCheckpoint))
	retryKey := hex.EncodeToString(retryDigest[:16])
	retryBarrierPath := filepath.Join(dir, "barrier-"+retryKey+".json")
	var retryBarrier map[string]any
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(retryBarrierPath)
		if readErr == nil {
			if err := json.Unmarshal(data, &retryBarrier); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if retryBarrier == nil {
		t.Fatal("Kafka consumer did not publish the exact post-failure retry barrier")
	}
	retryDecision := map[string]any{
		"token": token, "nonce": retryBarrier["nonce"], "decision": "continue",
	}
	encoded, err = json.Marshal(retryDecision)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "decision-"+retryKey+".json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-retryResult:
		if got.err != nil || !got.matched {
			t.Fatalf("same-record Kafka retry did not wait on the exact boundary: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Kafka consumer did not resume after the retry boundary decision")
	}
}

func TestConsumerCancellationDoesNotCommitUnhandledRecordWithoutBrokerProof(t *testing.T) {
	dir := t.TempDir()
	token := "t23-private-token-0123456789"
	hook, err := reliabilityhook.New(reliabilityhook.Config{
		Enabled: true, Token: token, Directory: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	windowID := "kafka.partition.rebalance_old_generation_fenced"
	eventID := "11111111-1111-4111-8111-111111111111"
	topic := "booking.events.t23-0123456789abcdef01234567"
	command := map[string]any{
		"window_id": windowID, "operation_id": eventID,
		"checkpoint": "kafka_before_handler", "action": "pause",
		"token": token, "nonce": "nonce-kafka-rebalance-commit-test",
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "command.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	paused := make(chan error, 1)
	go func() {
		_, _, err := hook.AwaitKafkaRecord(ctx, "notification-booking-group",
			topic, 7, 0, 9, eventID, "BookingCancelled")
		paused <- err
	}()
	digest := sha256.Sum256([]byte(windowID + "\x00" + eventID + "\x00kafka_before_handler"))
	key := hex.EncodeToString(digest[:16])
	barrierPath := filepath.Join(dir, "barrier-"+key+".json")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(barrierPath); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Stat(barrierPath); err != nil {
		t.Fatal("old Kafka generation did not claim its exact boundary")
	}
	cancel()
	select {
	case err := <-paused:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old-generation pause returned %v after cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old-generation pause did not stop")
	}
	c := &Consumer{reliability: hook, groupID: "notification-booking-group"}
	var attemptedGeneration int32
	var attemptedOffset int64
	var commitCalls int
	c.commit = func(g *kafkago.Generation, offsets map[string]map[int]int64) error {
		commitCalls++
		attemptedGeneration = g.ID
		attemptedOffset = offsets[topic][0]
		return kafkago.IllegalGeneration
	}
	if err := c.reportKafkaRevocationProofGap(&kafkago.Generation{ID: 7},
		kafkago.Message{Topic: topic, Partition: 0, Offset: 9},
		Event{ID: eventID, Type: "BookingCancelled"}); !errors.Is(err, reliabilityhook.ErrKafkaBrokerRevocationProofMissing) {
		t.Fatalf("canceled generation did not surface the required human gate: %v", err)
	}
	if commitCalls != 0 {
		t.Fatalf("context cancellation without broker revocation evidence attempted %d offset commits (generation=%d offset=%d)",
			commitCalls, attemptedGeneration, attemptedOffset)
	}
	if _, err := os.Stat(filepath.Join(dir, "kafka-rebalance-"+key+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shutdown cancellation created a broker rejection receipt without a broker response: %v", err)
	}
}

func TestCommitBudgetInterruptsCoordinatorAndRejectsLateSuccess(t *testing.T) {
	clearReliabilityEnv(t)
	for _, cancelEarly := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		client, server := net.Pipe()
		c := NewConsumer([]string{"unused"}, "topic", "group", func(context.Context, Event) error { return nil }, zap.NewNop())
		entered := make(chan struct{})
		c.commit = func(*kafkago.Generation, map[string]map[int]int64) error {
			close(entered)
			var b [1]byte
			_, _ = client.Read(b[:])
			return nil
		}
		done := make(chan error, 1)
		started := time.Now()
		go func() {
			done <- c.commitNext(ctx, &kafkago.Generation{}, "topic", 0, 1, make(chan struct{}, 1), func() { _ = client.Close() })
		}()
		<-entered
		if cancelEarly {
			cancel()
		}
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("accepted late commit result", err)
			}
			if elapsed := time.Since(started); elapsed > 5500*time.Millisecond {
				t.Fatal("unbounded commit", elapsed)
			}
		case <-time.After(6 * time.Second):
			t.Error("commit I/O did not stop")
		}
		cancel()
		client.Close()
		server.Close()
	}
}

func TestPartitionCancellationInterruptsReaderInitialization(t *testing.T) {
	clearReliabilityEnv(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewConsumer([]string{listener.Addr().String()}, "topic", "group", func(context.Context, Event) error { t.Error("handler invoked on uninitialized reader"); return nil }, zap.NewNop())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.partitionLoop(ctx, &kafkago.Generation{}, "topic", kafkago.PartitionAssignment{ID: 0, Offset: 0}, make(chan struct{}, 1), make(chan struct{}, 1), func() {})
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("reader not connected")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader initialization survived generation cancellation")
	}
}
