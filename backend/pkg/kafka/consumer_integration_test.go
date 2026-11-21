package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func kafkaFixture(t *testing.T, partitions int) (string, string) {
	t.Helper()
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker != "127.0.0.1:29093" {
		t.Fatal("BLOCKED: isolated Kafka broker required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	control, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	topic := "reliability-t15-" + uuid.NewString()
	if err := control.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err == nil {
			defer c.Close()
			_ = c.DeleteTopics(topic)
		}
	})
	return broker, topic
}

func writeFixtureEvent(t *testing.T, broker, topic string, partition int, name string) {
	raw, _ := json.Marshal(Event{ID: uuid.NewString(), Type: name, Timestamp: time.Now(), Data: json.RawMessage(`{"booking_id":"fixture"}`)})
	writeFixtureRaw(t, broker, topic, partition, raw)
}

func writeFixtureRaw(t *testing.T, broker, topic string, partition int, raw []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafkago.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.WriteMessages(kafkago.Message{Value: raw}); err != nil {
		t.Fatal(err)
	}
}

func committedFixtureOffset(t *testing.T, broker, topic, group string, partition int) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &kafkago.Client{Addr: kafkago.TCP(broker), Timeout: 3 * time.Second}
	res, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: {partition}}})
	if err != nil {
		t.Fatal("broker offset read", err)
	}
	if res.Error != nil {
		t.Fatal("broker offset read", err, res)
	}
	for _, p := range res.Topics[topic] {
		if p.Partition == partition {
			if p.Error != nil {
				t.Fatal(p.Error)
			}
			return p.CommittedOffset
		}
	}
	t.Fatal("partition offset absent")
	return 0
}

func TestRebalanceDuringEarlierRecordRetryIntegration(t *testing.T) {
	broker, topic := kafkaFixture(t, 2)
	group := "reliability-t15-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	firstA := make(chan struct{}, 4)
	retryA := make(chan context.Context, 8)
	bHandled := make(chan struct{}, 4)
	cHandled := make(chan struct{}, 4)
	release := make(chan struct{})
	var attempts atomic.Int32
	handler := func(ctx context.Context, e Event) error {
		switch e.Type {
		case "A":
			if attempts.Add(1) == 1 {
				firstA <- struct{}{}
				return errors.New("transient A")
			}
			retryA <- ctx
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case "B":
			bHandled <- struct{}{}
		case "C":
			cHandled <- struct{}{}
		}
		return nil
	}
	consumer := NewConsumer([]string{broker}, topic, group, handler, zap.NewNop())
	var mu sync.Mutex
	var commits []int64
	var received []int32
	commit2 := make(chan struct{}, 1)
	consumer.observe = func(g int32, p int, o int64, result string) {
		mu.Lock()
		defer mu.Unlock()
		if p == 0 && o == 0 && result == "received" {
			received = append(received, g)
		}
	}
	commit := func(g *kafkago.Generation, o map[string]map[int]int64) error {
		err := g.CommitOffsets(o)
		if err == nil {
			if next, ok := o[topic][0]; ok {
				mu.Lock()
				commits = append(commits, next)
				mu.Unlock()
				if next == 2 {
					commit2 <- struct{}{}
				}
			}
		}
		return err
	}
	consumer.commit = commit
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
	})
	writeFixtureEvent(t, broker, topic, 0, "A")
	writeFixtureEvent(t, broker, topic, 0, "B")
	writeFixtureEvent(t, broker, topic, 1, "C")
	select {
	case <-firstA:
	case <-ctx.Done():
		t.Fatal("A not read")
	}
	var oldHandlerCtx context.Context
	select {
	case <-bHandled:
		t.Fatal("later B handled across earlier failed A")
	case oldHandlerCtx = <-retryA:
	case <-ctx.Done():
		t.Fatal("A not retried")
	}
	if offset := committedFixtureOffset(t, broker, topic, group, 0); offset > 0 {
		t.Fatal("offset crossed unresolved A", offset)
	}
	select {
	case <-cHandled:
	case <-ctx.Done():
		t.Fatal("unrelated partition stalled")
	}
	second := NewConsumer([]string{broker}, topic, group, handler, zap.NewNop())
	second.observe = consumer.observe
	second.commit = commit
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-secondDone:
		case <-time.After(10 * time.Second):
			t.Error("second consumer did not stop")
		}
	})
	select {
	case <-oldHandlerCtx.Done():
	case <-ctx.Done():
		t.Fatal("rebalance did not revoke old handler")
	}
	select {
	case <-retryA:
	case <-ctx.Done():
		t.Fatal("new generation did not reread A")
	}
	if offset := committedFixtureOffset(t, broker, topic, group, 0); offset > 0 {
		t.Fatal("revoked generation crossed A", offset)
	}
	select {
	case <-bHandled:
		t.Fatal("B handled before new generation resolved A")
	default:
	}
	mu.Lock()
	if len(received) < 2 || received[0] == received[len(received)-1] {
		t.Fatal("generation did not change", received)
	}
	t.Logf("topic=%s partition=0 offset=0 group=%s generations=%v attempts=%d", topic, group, received, attempts.Load())
	mu.Unlock()
	close(release)
	select {
	case <-bHandled:
	case <-ctx.Done():
		t.Fatal("B never handled after A succeeded")
	}
	waitFixtureOffset(t, ctx, broker, topic, group, 0, 2)
	select {
	case <-commit2:
	case <-ctx.Done():
		t.Fatal("local commit2 completion absent")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commits) != 2 || commits[0] != 1 || commits[1] != 2 {
		t.Fatal("noncontiguous or revoked commits", commits)
	}
}

func waitFixtureOffset(t *testing.T, ctx context.Context, broker, topic, group string, partition int, want int64) {
	t.Helper()
	for committedFixtureOffset(t, broker, topic, group, partition) != want {
		if !consumerWait(ctx, 10*time.Millisecond) {
			t.Fatal("broker committed offset absent", want)
		}
	}
}

func TestPartitionPoisonRestartAndCommitAckLossIntegration(t *testing.T) {
	for _, kind := range []string{"decode", "permanent", "exhausted"} {
		t.Run(kind, func(t *testing.T) {
			broker, topic := kafkaFixture(t, 1)
			group := "reliability-t15-" + uuid.NewString()
			writeFixtureEvent(t, broker, topic, 0, "A")
			if kind == "decode" {
				// Use a fresh topic so offset0 is the malformed envelope.
				broker, topic = kafkaFixture(t, 1)
				writeFixtureRaw(t, broker, topic, 0, []byte(`{"type":`))
			}
			writeFixtureEvent(t, broker, topic, 0, "B")
			var attempts atomic.Int32
			for restart := 0; restart < 2; restart++ {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				stalled := make(chan struct{}, 1)
				c := NewConsumer([]string{broker}, topic, group, func(ctx context.Context, e Event) error {
					if e.Type != "A" {
						t.Error("poison skipped to later event", e.Type)
						return ErrPermanent
					}
					attempts.Add(1)
					if kind == "permanent" {
						return ErrPermanent
					}
					return errors.New("transient")
				}, zap.NewNop())
				c.observe = func(g int32, p int, o int64, result string) {
					if result == "failed" || result == "decode_failed" {
						t.Logf("topic=%s partition=%d offset=%d group=%s generation=%d state=%s", topic, p, o, group, g, result)
						stalled <- struct{}{}
					}
				}
				done := make(chan error, 1)
				go func() { done <- c.Start(ctx) }()
				select {
				case <-stalled:
				case <-ctx.Done():
					t.Error("stall not observed")
				}
				if offset := committedFixtureOffset(t, broker, topic, group, 0); offset > 0 {
					t.Error("poison committed", offset)
				}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("stalled consumer did not cancel")
				}
			}
			want := int32(0)
			if kind == "permanent" {
				want = 2
			}
			if kind == "exhausted" {
				want = 10
			}
			if attempts.Load() != want {
				t.Fatal("retry budget", attempts.Load(), want)
			}
		})
	}
	for _, appliedCommit := range []bool{false, true} {
		t.Run("commit_ack_loss_"+strconv.FormatBool(appliedCommit), func(t *testing.T) {
			broker, topic := kafkaFixture(t, 1)
			group := "reliability-t15-" + uuid.NewString()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			dsn := os.Getenv("BOOKING_TEST_DATABASE_URL")
			if dsn != "postgres://reliability_fixture@127.0.0.1:25435/ticketbox_booking_test?sslmode=disable" {
				t.Fatal("BLOCKED: isolated durable destination required")
			}
			pool, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			table := "t15_destination_" + uuid.NewString()
			table = "\"" + table + "\""
			if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (event_id text PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			defer func() {
				clean, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				_, err := pool.Exec(clean, "DROP TABLE "+table)
				if err != nil {
					t.Error(err)
				}
			}()
			var attempts atomic.Int32
			var lostGeneration atomic.Int32
			c := NewConsumer([]string{broker}, topic, group, func(ctx context.Context, e Event) error {
				if e.Type == "A" {
					attempts.Add(1)
				}
				_, err := pool.Exec(ctx, "INSERT INTO "+table+" (event_id) VALUES ($1) ON CONFLICT DO NOTHING", e.ID)
				return err
			}, zap.NewNop())
			c.commit = func(g *kafkago.Generation, o map[string]map[int]int64) error {
				lostGeneration.CompareAndSwap(0, g.ID)
				if g.ID == lostGeneration.Load() {
					if appliedCommit {
						if err := g.CommitOffsets(o); err != nil {
							return err
						}
					}
					return errors.New("offset response lost")
				}
				return g.CommitOffsets(o)
			}
			c.observe = func(g int32, p int, o int64, result string) {
				if result != "received" {
					t.Logf("topic=%s partition=%d offset=%d group=%s generation=%d state=%s", topic, p, o, group, g, result)
				}
			}
			done := make(chan error, 1)
			go func() { done <- c.Start(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("ack-loss consumer did not stop")
				}
			}()
			writeFixtureEvent(t, broker, topic, 0, "A")
			writeFixtureEvent(t, broker, topic, 0, "B")
			waitFixtureOffset(t, ctx, broker, topic, group, 0, 2)
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 2 {
				t.Fatal("durable effect duplicated or skipped", count)
			}
			want := int32(2)
			if appliedCommit {
				want = 1
			}
			if attempts.Load() != want {
				t.Fatal("broker offset not reloaded", attempts.Load(), want)
			}
		})
	}
}
