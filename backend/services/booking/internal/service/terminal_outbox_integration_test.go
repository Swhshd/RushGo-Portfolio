package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/ticketbox/booking/internal/domain"
	bookingkafka "github.com/ticketbox/booking/internal/kafka"
	"github.com/ticketbox/booking/internal/repository"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

// The wrapper faults only after the real broker has acknowledged publication.
// Storage, relay transaction, producer and wire envelope remain production code.
type terminalPublishFault struct {
	publisher OutboxRelayPublisher
	after     func() error
}

func (p terminalPublishFault) PublishOutboxEvent(ctx context.Context, e repository.OutboxEvent) error {
	if err := p.publisher.PublishOutboxEvent(ctx, e); err != nil {
		return err
	}
	return p.after()
}

func TestTerminalOutboxStableRepublishIntegration(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker != "127.0.0.1:29093" {
		t.Fatal("BLOCKED: isolated Kafka required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	controller, err := conn.Controller()
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	control, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}
	if err := control.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	err = control.CreateTopics(kafkago.TopicConfig{Topic: topics.TopicBookingEvents, NumPartitions: 1, ReplicationFactor: 1})
	control.Close()
	if err != nil {
		t.Fatal(err)
	}
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	s := NewBookingService(r, nil, nil, zap.NewNop())
	producer := bookingkafka.NewBookingEventProducer([]string{broker}, zap.NewNop())
	defer producer.Close()
	for _, kind := range []string{"cancel", "fail"} {
		for _, fault := range []string{"ack", "marker"} {
			t.Run(kind+"_"+fault, func(t *testing.T) {
				id, user, event := uuid.New(), uuid.New(), uuid.New()
				if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline) VALUES($1,$2,$3,'PENDING',725,1,true,1,'OPEN','usd','pessimistic',$4)`, id, user, event, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				d := domain.LifecycleDecision{Ref: domain.LifecycleRef{BookingID: id, Generation: 1}, DecisionID: domain.BookingDecisionID(id, kind), Kind: kind, ExpectedVersion: 1, Reason: "controlled_failure"}
				for range 5 {
					if _, err := s.DecideLifecycle(ctx, d); err != nil {
						t.Fatal(err)
					}
				}
				stable := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking-event:"+id.String()+":g1:"+kind))
				assertPending := func(want bool) {
					t.Helper()
					var pending bool
					if err := p.QueryRow(ctx, `SELECT NOT published FROM outbox WHERE id=$1`, stable).Scan(&pending); err != nil {
						t.Fatal(err)
					}
					if pending != want {
						t.Fatal("lost durable publication responsibility", pending, want)
					}
				}
				// An unreachable broker is a real transport failure, not an injected callback.
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				badAddr := listener.Addr().String()
				listener.Close()
				bad := bookingkafka.NewBookingEventProducer([]string{badAddr}, zap.NewNop())
				outageCtx, outageCancel := context.WithTimeout(ctx, 250*time.Millisecond)
				n, err := NewOutboxRelay(r, bad, zap.NewNop()).RelayBatch(outageCtx, 1)
				outageCancel()
				bad.Close()
				if err == nil || n != 0 {
					t.Fatal("broker outage lost intent", n, err)
				}
				assertPending(true)
				relayCtx, relayCancel := context.WithCancel(ctx)
				faultPublisher := terminalPublishFault{publisher: producer, after: func() error {
					if fault == "marker" {
						relayCancel()
						return nil
					}
					return errors.New("controlled lost publish ACK")
				}}
				n, err = NewOutboxRelay(r, faultPublisher, zap.NewNop()).RelayBatch(relayCtx, 1)
				relayCancel()
				if err == nil || n != 0 {
					t.Fatal("controlled uncertainty not retained", n, err)
				}
				assertPending(true)
				if n, err := NewOutboxRelay(r, producer, zap.NewNop()).RelayBatch(ctx, 1); err != nil || n != 1 {
					t.Fatal("recovery did not drain", n, err)
				}
				assertPending(false)
				reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{broker}, Topic: topics.TopicBookingEvents, Partition: 0, MinBytes: 1, MaxBytes: 1e6, MaxWait: 100 * time.Millisecond})
				defer reader.Close()
				var first []byte
				found := 0
				readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
				defer readCancel()
				for found < 2 {
					m, err := reader.ReadMessage(readCtx)
					if err != nil {
						t.Fatal("actual broker replay", err)
					}
					var envelope pkgkafka.Event
					if err := json.Unmarshal(m.Value, &envelope); err != nil {
						t.Fatal(err)
					}
					if envelope.ID != stable.String() {
						continue
					}
					if string(m.Key) != stable.String() {
						t.Fatal("publisher key differs from stable envelope ID")
					}
					if first == nil {
						first = append([]byte(nil), m.Value...)
					} else if !bytes.Equal(first, m.Value) {
						t.Fatal("technical replay mutated stored envelope")
					}
					var data map[string]any
					if err := json.Unmarshal(envelope.Data, &data); err != nil {
						t.Fatal(err)
					}
					if data["booking_id"] != id.String() || data["user_id"] != user.String() || data["event_id"] != event.String() || data["email"] != "" {
						t.Fatal("incompatible terminal payload", data)
					}
					wantType := topics.TypeBookingCancelled
					wantKeys := 4
					if kind == "fail" {
						wantType = topics.TypeBookingFailed
						wantKeys = 5
						if data["reason"] != "controlled_failure" {
							t.Fatal("failure reason omitted")
						}
					}
					if envelope.Type != wantType || len(data) != wantKeys {
						t.Fatal("terminal event contract changed", envelope.Type, data)
					}
					found++
				}
				var count int
				if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_key=$1`, id.String()).Scan(&count); err != nil || count != 1 {
					t.Fatal("replay created another semantic intent", count, err)
				}
			})
		}
	}
}
