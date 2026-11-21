package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
)

type capturedReceiptPublisher struct {
	events []pkgkafka.Event
	keys   []string
	fail   bool
}

func (p *capturedReceiptPublisher) PublishEvent(ctx context.Context, topic, key string, event pkgkafka.Event) error {
	if topic != topics.TopicPaymentEvents {
		return errors.New("wrong topic")
	}
	p.events = append(p.events, event)
	p.keys = append(p.keys, key)
	if p.fail {
		return errors.New("fixture publish ACK unknown")
	}
	return nil
}

func TestReceiptWorkerRecoversUnmatchedAndUnknownPublishIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	eventID := "evt_worker_" + uuid.NewString()
	receiptID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:acct_fixture:test:"+eventID))
	safe := json.RawMessage(`{"id":"pi_worker_fixture","status":"succeeded"}`)
	r := domain.ProviderReceipt{ID: receiptID, EnvelopeID: receiptID.String(), EventType: "payment_intent.succeeded", CorrelationState: "pending", RelayState: "pending", Observation: domain.ProviderObservation{Account: "acct_fixture", EventID: eventID, IntentID: "pi_worker_fixture", PaymentID: id, BookingID: booking, Generation: 1, AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now(), SafeObject: safe, EvidenceHash: sha256.Sum256(safe)}}
	if _, _, err := repo.AcceptProviderReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	publisher := &capturedReceiptPublisher{fail: true}
	worker := &FinancialWorker{repo: repo, publisher: publisher}
	if n, err := worker.RunReceiptsOnce(ctx, 1); err != nil || n != 1 || len(publisher.events) != 0 {
		t.Fatalf("unmatched receipt got published: %d %v", n, err)
	}
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payment_provider_events SET due_at=NOW()`); err != nil {
		t.Fatal(err)
	}
	if n, err := worker.RunReceiptsOnce(ctx, 1); err != nil || n != 1 || len(publisher.events) != 1 {
		t.Fatalf("unknown publish not attempted: %d %v", n, err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT relay_state FROM payment_provider_events`).Scan(&state); err != nil || state != "pending" {
		t.Fatal("unknown publish lost retry")
	}
	publisher.fail = false
	if _, err := pool.Exec(ctx, `UPDATE payment_provider_events SET due_at=NOW()`); err != nil {
		t.Fatal(err)
	}
	if n, err := worker.RunReceiptsOnce(ctx, 1); err != nil || n != 1 || len(publisher.events) != 2 {
		t.Fatalf("publish retry missing: %d %v", n, err)
	}
	if publisher.events[0].ID != receiptID.String() || publisher.events[1].ID != receiptID.String() || publisher.keys[0] != publisher.keys[1] || string(publisher.events[0].Data) != string(publisher.events[1].Data) {
		t.Fatal("worker retry changed receipt envelope")
	}
	if err := pool.QueryRow(ctx, `SELECT relay_state FROM payment_provider_events`).Scan(&state); err != nil || state != "published" {
		t.Fatal("successful relay not durable")
	}
	view, err := repo.GetFinancial(ctx, id)
	if err != nil || view.ProviderOutcome != "collected" || view.ReconciliationState != "pending" {
		t.Fatal("relay removed financial responsibility")
	}
}
