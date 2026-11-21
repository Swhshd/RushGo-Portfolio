package kafka

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type issuanceAuthorityStub struct {
	mu       sync.Mutex
	calls    int
	eligible bool
}

func (s *issuanceAuthorityStub) CheckTicketAuthorities(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate) (domain.TicketAuthorityEvidence, error) {
	s.mu.Lock()
	s.calls++
	eligible := s.eligible
	s.mu.Unlock()
	if !eligible {
		return domain.TicketAuthorityEvidence{SagaEligible: true, EventEligible: true}, nil
	}
	return domain.TicketAuthorityEvidence{
		SagaEligible: true, PaymentEligible: true, EventEligible: true, PaymentDecisionKnown: true,
		DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund,
		ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
	}, nil
}

func (s *issuanceAuthorityStub) setEligible(value bool) {
	s.mu.Lock()
	s.eligible = value
	s.mu.Unlock()
}

func (s *issuanceAuthorityStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type issuanceBookingFixture struct {
	bookingID, eventID, sagaID, paymentID, seatID, tierID, itemID uuid.UUID
}

func bookingIssuanceRecoveryFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("BOOKING_TEST_DATABASE_URL")
	parsed, err := url.Parse(raw)
	if raw == "" || err != nil || parsed.Hostname() != "127.0.0.1" || parsed.Port() != "25435" || parsed.Path != "/ticketbox_booking_test" {
		t.Fatal("BLOCKED: isolated Booking PG required (127.0.0.1:25435/ticketbox_booking_test)")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatalf("connect Booking test PG: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "issuance_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, dropErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if dropErr != nil {
			t.Errorf("drop disposable schema: %v", dropErr)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolated := parsed.String()
	command := exec.Command("migrate", "-path", "../../migrations", "-database", isolated, "up")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("booking migrations failed: %s", strings.ReplaceAll(string(output), isolated, "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(isolated)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func seedIssuanceRecoveryBooking(t *testing.T, pool *pgxpool.Pool) issuanceBookingFixture {
	t.Helper()
	f := issuanceBookingFixture{
		bookingID: uuid.New(), eventID: uuid.New(), sagaID: uuid.New(), paymentID: uuid.New(),
		seatID: uuid.New(), tierID: uuid.New(), itemID: uuid.New(),
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',725,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+interval '1 hour',$5)`,
		f.bookingID, uuid.New(), f.eventID, uuid.New(), f.sagaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,1,725,$4)`,
		f.itemID, f.bookingID, f.tierID, []uuid.UUID{f.seatID}); err != nil {
		t.Fatal(err)
	}
	return f
}

func issuanceRecoveryEnvelope(f issuanceBookingFixture) pkgkafka.Event {
	issuanceID := uuid.New()
	payload, _ := json.Marshal(map[string]any{
		"version": 1, "ticket_issuance_id": issuanceID.String(), "saga_id": f.sagaID.String(),
		"booking_id": f.bookingID.String(), "payment_id": f.paymentID.String(),
		"event_id": f.eventID.String(), "generation": 1,
	})
	return pkgkafka.Event{ID: issuanceID.String(), Type: topics.TypeTicketIssuanceRequestedV1, Timestamp: time.Now().UTC(), Data: payload}
}

func publishIssuanceRecord(t *testing.T, broker, topic string, record pkgkafka.Event) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := kafkago.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("dial issuance topic leader: %v", err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.WriteMessages(kafkago.Message{Value: raw}); err != nil {
		t.Fatalf("publish issuance record: %v", err)
	}
}

func committedIssuanceOffset(t *testing.T, broker, topic, group string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &kafkago.Client{Addr: kafkago.TCP(broker), Timeout: 3 * time.Second}
	response, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: group, Topics: map[string][]int{topic: {0}},
	})
	if err != nil || response.Error != nil {
		t.Fatalf("read committed offset: %v %v", err, response)
	}
	for _, partition := range response.Topics[topic] {
		if partition.Partition == 0 {
			if partition.Error != nil {
				t.Fatal(partition.Error)
			}
			return partition.CommittedOffset
		}
	}
	t.Fatal("committed offset absent")
	return -1
}

func issuanceCounts(t *testing.T, pool *pgxpool.Pool, bookingID uuid.UUID) (int64, int64) {
	t.Helper()
	var tickets, receipts int64
	if err := pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM tickets WHERE booking_id=$1),
		(SELECT count(*) FROM ticket_issuance_receipts WHERE booking_id=$1)`, bookingID).Scan(&tickets, &receipts); err != nil {
		t.Fatal(err)
	}
	return tickets, receipts
}

func waitForIssuance(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not reached before timeout")
}

func startIssuanceConsumer(t *testing.T, broker, topic, group string, processor TicketIssuanceProcessor) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	// Same handler and consumer contract as NewTicketIssuanceConsumer, but on a
	// per-run topic so leftover records cannot mask or poison this scenario.
	consumer := pkgkafka.NewConsumer([]string{broker}, topic, group, TicketIssuanceHandler(processor, zap.NewNop()), zap.NewNop())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()
	return cancel, done
}

func stopIssuanceConsumer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer stop: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stalled consumer did not stop")
	}
}

// This is the single new real recovery scenario for this round. It uses a real
// Booking PostgreSQL schema, a real Kafka broker, the real issuance handler and
// transaction, and only the ticket authority is a controllable double. The
// consumer's existing five-failure/no-commit contract is preserved.
func TestStalledTicketIssuanceRecoversOnSameGroupRestartIntegration(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker != "127.0.0.1:29093" {
		t.Fatal("BLOCKED: isolated Kafka broker required at 127.0.0.1:29093")
	}
	pool := bookingIssuanceRecoveryFixture(t)
	authority := &issuanceAuthorityStub{}
	issuanceService := service.NewTicketIssuanceService(repository.NewPostgresBookingRepository(pool), authority)
	first := seedIssuanceRecoveryBooking(t, pool)
	second := seedIssuanceRecoveryBooking(t, pool)
	third := seedIssuanceRecoveryBooking(t, pool)
	firstRecord := issuanceRecoveryEnvelope(first)
	secondRecord := issuanceRecoveryEnvelope(second)
	thirdRecord := issuanceRecoveryEnvelope(third)
	topic := topics.TopicTicketIssuanceEvents + "-recovery-" + uuid.NewString()
	publishIssuanceRecord(t, broker, topic, firstRecord)
	publishIssuanceRecord(t, broker, topic, secondRecord)
	group := "booking-ticket-issuance-v1-recovery-" + uuid.NewString()

	// Phase A: five UNKNOWN attempts must stall without committing or issuing.
	cancel, done := startIssuanceConsumer(t, broker, topic, group, issuanceService)
	waitForIssuance(t, 20*time.Second, func() bool { return authority.callCount() >= 5 })
	time.Sleep(300 * time.Millisecond)
	if offset := committedIssuanceOffset(t, broker, topic, group); offset > 0 {
		t.Fatalf("stalled consumer committed offset %d", offset)
	}
	for _, fixture := range []issuanceBookingFixture{first, second, third} {
		if tickets, receipts := issuanceCounts(t, pool, fixture.bookingID); tickets != 0 || receipts != 0 {
			t.Fatalf("stalled consumer issued booking %s: tickets=%d receipts=%d", fixture.bookingID, tickets, receipts)
		}
	}
	t.Logf("after 5 UNKNOWN attempts: authority_calls=%d committed_offset=%d tickets=0 receipts=0", authority.callCount(), committedIssuanceOffset(t, broker, topic, group))
	stopIssuanceConsumer(t, cancel, done)

	// Phase B: restore the authority and restart the same group. The original
	// uncommitted record and the following record are both processed.
	authority.setEligible(true)
	cancel, done = startIssuanceConsumer(t, broker, topic, group, issuanceService)
	waitForIssuance(t, 30*time.Second, func() bool {
		firstTickets, firstReceipts := issuanceCounts(t, pool, first.bookingID)
		secondTickets, secondReceipts := issuanceCounts(t, pool, second.bookingID)
		return firstTickets == 1 && firstReceipts == 1 && secondTickets == 1 && secondReceipts == 1
	})
	waitForIssuance(t, 10*time.Second, func() bool { return committedIssuanceOffset(t, broker, topic, group) == 2 })
	callsAfterRecovery := authority.callCount()
	firstTickets, firstReceipts := issuanceCounts(t, pool, first.bookingID)
	secondTickets, secondReceipts := issuanceCounts(t, pool, second.bookingID)
	t.Logf("after same-group restart: committed_offset=%d first_tickets=%d first_receipts=%d second_tickets=%d second_receipts=%d authority_calls=%d",
		committedIssuanceOffset(t, broker, topic, group), firstTickets, firstReceipts, secondTickets, secondReceipts, callsAfterRecovery)

	// Phase C: a byte-identical replay of the first event must not issue again
	// and must not invoke the authority (receipt fast path).
	stopIssuanceConsumer(t, cancel, done)
	cancel, done = startIssuanceConsumer(t, broker, topic, group, issuanceService)
	publishIssuanceRecord(t, broker, topic, firstRecord)
	waitForIssuance(t, 20*time.Second, func() bool { return committedIssuanceOffset(t, broker, topic, group) == 3 })
	if tickets, receipts := issuanceCounts(t, pool, first.bookingID); tickets != 1 || receipts != 1 {
		t.Fatalf("duplicate replay changed counts: tickets=%d receipts=%d", tickets, receipts)
	}
	if calls := authority.callCount(); calls != callsAfterRecovery {
		t.Fatalf("duplicate replay invoked authority: before=%d after=%d", callsAfterRecovery, calls)
	}
	replayTickets, replayReceipts := issuanceCounts(t, pool, first.bookingID)
	t.Logf("after duplicate replay: committed_offset=%d first_tickets=%d first_receipts=%d authority_calls=%d",
		committedIssuanceOffset(t, broker, topic, group), replayTickets, replayReceipts, authority.callCount())

	// Phase D: the partition continues past the duplicate.
	publishIssuanceRecord(t, broker, topic, thirdRecord)
	waitForIssuance(t, 20*time.Second, func() bool {
		tickets, receipts := issuanceCounts(t, pool, third.bookingID)
		return tickets == 1 && receipts == 1
	})
	waitForIssuance(t, 10*time.Second, func() bool { return committedIssuanceOffset(t, broker, topic, group) == 4 })
	thirdTickets, thirdReceipts := issuanceCounts(t, pool, third.bookingID)
	t.Logf("following record after duplicate: committed_offset=%d third_tickets=%d third_receipts=%d",
		committedIssuanceOffset(t, broker, topic, group), thirdTickets, thirdReceipts)

	// Phase E: a bounded scan of a fully retained range that does not contain
	// the target must return "not found", not block waiting for a new record.
	scanCtx, scanCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scanCancel()
	scanStart := time.Now()
	offset, found, scanned, scanErr := pkgkafka.FindRecord(scanCtx, []string{broker}, topic, 0, 0, 4, 100,
		func([]byte) bool { return false })
	if scanErr != nil || found || offset != -1 || scanned != 4 || time.Since(scanStart) > 4*time.Second {
		t.Fatalf("bounded missing-record scan = offset:%d found:%v scanned:%d elapsed:%s err:%v",
			offset, found, scanned, time.Since(scanStart), scanErr)
	}
	t.Logf("bounded missing-record scan returned without blocking: scanned=%d elapsed=%s", scanned, time.Since(scanStart).Round(time.Millisecond))
	stopIssuanceConsumer(t, cancel, done)
}
