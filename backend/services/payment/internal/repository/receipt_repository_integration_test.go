package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/ticketbox/payment/internal/domain"
	paymentkafka "github.com/ticketbox/payment/internal/kafka"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/pkg/typed"
	"go.uber.org/zap"
)

func TestReceiptRelayAckLossAndCorrelationIntegration(t *testing.T) {
	if os.Getenv("KAFKA_TEST_BROKERS") != "127.0.0.1:29093" {
		t.Fatal("isolated real Kafka required")
	}
	pool, _ := paymentFinancialFixture(t)
	repo := NewPostgresPaymentRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	op := financialFixtureOperation()
	eventID := "evt_correlation_" + uuid.NewString()
	receiptID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+op.Identity.ProviderAccount+":test:"+eventID))
	safe, _ := json.Marshal(map[string]any{"id": "pi_correlation_" + uuid.NewString(), "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false, "metadata": map[string]string{"ticketbox_payment_id": op.PaymentID.String(), "ticketbox_booking_id": op.Identity.BookingID.String(), "ticketbox_generation": "1"}})
	var object struct{ ID string }
	_ = json.Unmarshal(safe, &object)
	r := domain.ProviderReceipt{ID: receiptID, EnvelopeID: receiptID.String(), EventType: "payment_intent.succeeded", CorrelationState: "pending", RelayState: "pending", Observation: domain.ProviderObservation{Account: op.Identity.ProviderAccount, EventID: eventID, IntentID: object.ID, PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now(), SafeObject: safe, EvidenceHash: sha256.Sum256(safe)}}
	accepted, inserted, err := repo.AcceptProviderReceipt(ctx, r)
	if err != nil || !inserted || accepted.CorrelationState != "pending" {
		t.Fatalf("unmatched responsibility: %+v %v", accepted, err)
	}
	duplicate, inserted, err := repo.AcceptProviderReceipt(ctx, r)
	if err != nil || inserted || duplicate.ID != receiptID {
		t.Fatal("duplicate receipt identity changed")
	}
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	claims, err := repo.ClaimReceiptWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatalf("receipt claim: %d %v", len(claims), err)
	}
	c := claims[0]
	if applied, err := repo.ApplyReceiptObservation(ctx, c, c.Receipt.Observation); err != nil || !applied {
		t.Fatalf("correlation merge: %v %v", applied, err)
	}
	view, err := repo.GetFinancial(ctx, op.PaymentID)
	if err != nil || view.ProviderOutcome != "collected" || view.Payment.PaymentIntentId != object.ID {
		t.Fatalf("correlation lost collection: %+v %v", view, err)
	}
	// Read from the actual broker's tail before publishing this receipt twice.
	conn, err := kafkago.DialLeader(ctx, "tcp", "127.0.0.1:29093", topics.TopicPaymentEvents, 0)
	if err != nil {
		t.Fatal(err)
	}
	start, err := conn.ReadLastOffset()
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:29093"}, Topic: topics.TopicPaymentEvents, Partition: 0, MinBytes: 1, MaxBytes: 1e6})
	defer reader.Close()
	if err := reader.SetOffset(start); err != nil {
		t.Fatal(err)
	}
	publisher := paymentkafka.NewPaymentProducer([]string{"127.0.0.1:29093"}, zap.NewNop())
	defer publisher.Close()
	data, _ := json.Marshal(typed.VerifiedPaymentEvent(eventID, safe))
	envelope := pkgkafka.Event{ID: r.EnvelopeID, Type: topics.TypePaymentSucceed, Timestamp: r.ReceivedAt, Data: data}
	if err := publisher.PublishEvent(ctx, topics.TopicPaymentEvents, r.EnvelopeID, envelope); err != nil {
		t.Fatal(err)
	}
	// Broker ACK is proven by returned publish and observed record. Fail only
	// the subsequent marker transaction; the durable receipt must remain due.
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(message.Key) != r.EnvelopeID {
		t.Fatal("broker key lost stable receipt identity")
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION fail_receipt_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.relay_state='published' THEN RAISE EXCEPTION 'fixture lost marker'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER receipt_marker_fault BEFORE UPDATE ON payment_provider_events FOR EACH ROW EXECUTE FUNCTION fail_receipt_marker()`); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishReceiptWork(ctx, c, "applied", "published", "", time.Now()); err == nil {
		t.Fatal("marker fault not reached")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER receipt_marker_fault ON payment_provider_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payment_provider_events SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE id=$1`, receiptID); err != nil {
		t.Fatal(err)
	}
	claims, err = repo.ClaimReceiptWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 || claims[0].Fence <= c.Fence {
		t.Fatal("receipt did not recover after marker death")
	}
	if owned, err := repo.FinishReceiptWork(ctx, c, "applied", "published", "", time.Now()); err != nil || owned {
		t.Fatal("old receipt claimant finished new claim")
	}
	if err := publisher.PublishEvent(ctx, topics.TopicPaymentEvents, r.EnvelopeID, envelope); err != nil {
		t.Fatal(err)
	}
	replay, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var first, second pkgkafka.Event
	if json.Unmarshal(message.Value, &first) != nil || json.Unmarshal(replay.Value, &second) != nil || first.ID != r.EnvelopeID || first.ID != second.ID || string(replay.Key) != r.EnvelopeID || string(first.Data) != string(second.Data) {
		t.Fatal("ACK loss replay changed semantic envelope")
	}
	if owned, err := repo.FinishReceiptWork(ctx, claims[0], "applied", "published", "", time.Now()); err != nil || !owned {
		t.Fatalf("current receipt completion: %v %v", owned, err)
	}
	view, err = repo.GetFinancial(ctx, op.PaymentID)
	if err != nil || view.ProviderOutcome != "collected" || view.ReconciliationState != "pending" {
		t.Fatal("published marker erased financial responsibility")
	}
	if claims, err := repo.ClaimReceiptWork(ctx, 1, 30*time.Second); err != nil || len(claims) != 0 {
		t.Fatal("published receipt remains relay-claimable")
	}
}

func TestReceiptAssociationRollbackAndRetentionIntegration(t *testing.T) {
	pool, dsn := paymentFinancialFixture(t)
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(pool)
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	makeReceipt := func(intent string) domain.ProviderReceipt {
		eventID := "evt_atomic_" + uuid.NewString()
		id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+op.Identity.ProviderAccount+":test:"+eventID))
		safe := json.RawMessage(`{"id":"` + intent + `","status":"succeeded"}`)
		return domain.ProviderReceipt{ID: id, EnvelopeID: id.String(), EventType: "payment_intent.succeeded", CorrelationState: "pending", RelayState: "pending", Observation: domain.ProviderObservation{Account: op.Identity.ProviderAccount, EventID: eventID, IntentID: intent, PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now(), SafeObject: safe, EvidenceHash: sha256.Sum256(safe)}}
	}
	r := makeReceipt("pi_atomic_fixture")
	if _, err := pool.Exec(ctx, `CREATE FUNCTION fail_receipt_association() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.correlation_state='applied' THEN RAISE EXCEPTION 'fixture association failure'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER association_fault BEFORE UPDATE ON payment_provider_events FOR EACH ROW EXECUTE FUNCTION fail_receipt_association()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.AcceptProviderReceipt(ctx, r); err == nil {
		t.Fatal("association fault did not reject transaction")
	}
	var count int
	var outcome string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_provider_events`).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial receipt committed")
	}
	if err := pool.QueryRow(ctx, `SELECT provider_outcome FROM payments WHERE id=$1`, op.PaymentID).Scan(&outcome); err != nil || outcome != "unknown" {
		t.Fatal("collection escaped association rollback")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER association_fault ON payment_provider_events`); err != nil {
		t.Fatal(err)
	}
	if accepted, _, err := repo.AcceptProviderReceipt(ctx, r); err != nil || accepted.CorrelationState != "applied" {
		t.Fatalf("atomic replay: %+v %v", accepted, err)
	}
	// A second event for the same provider object is a separate receipt but
	// the local money lifecycle and collection remain the original ones.
	second := makeReceipt(r.Observation.IntentID)
	if accepted, _, err := repo.AcceptProviderReceipt(ctx, second); err != nil || accepted.CorrelationState != "applied" {
		t.Fatal("second object event failed")
	}
	conflict := makeReceipt("pi_different_fixture")
	if accepted, _, err := repo.AcceptProviderReceipt(ctx, conflict); err != nil || accepted.CorrelationState != "attention" {
		t.Fatalf("mismatched intent responsibility dropped: %+v %v", accepted, err)
	}
	amountMismatch := makeReceipt(r.Observation.IntentID)
	amountMismatch.Observation.AmountCents = 800
	if accepted, _, err := repo.AcceptProviderReceipt(ctx, amountMismatch); err != nil || accepted.CorrelationState != "attention" {
		t.Fatal("amount mismatch applied or lost")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate object forked local Payment")
	}
	// Receipt down is safe for empty schemas only; retained signed evidence
	// cannot disappear through an ordinary migration rollback.
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", dsn, "goto", "202610110001").CombinedOutput(); err == nil || !bytes.Contains(output, []byte("retained verified provider receipts")) {
		t.Fatal("receipt migration down lost responsibility")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_provider_events`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("retained receipts=%d %v", count, err)
	}
}
