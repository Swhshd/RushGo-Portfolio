package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/pkg/topics"
)

type relayBoundaryRepository struct {
	repository.FinancialRepository
	claim      domain.ReceiptClaim
	relayState string
	category   string
	order      *[]string
}

func (r *relayBoundaryRepository) ClaimReceiptWork(context.Context, int, time.Duration) ([]domain.ReceiptClaim, error) {
	return []domain.ReceiptClaim{r.claim}, nil
}

func (r *relayBoundaryRepository) FinishReceiptWork(_ context.Context, _ domain.ReceiptClaim,
	correlation, relay, category string, _ time.Time) (bool, error) {
	if correlation != "applied" {
		return false, errors.New("correlation marker changed")
	}
	if r.order != nil {
		*r.order = append(*r.order, "marker")
	}
	r.relayState, r.category = relay, category
	return true, nil
}

type relayBoundaryPublisher struct {
	calls int
	order *[]string
}

func (p *relayBoundaryPublisher) PublishEvent(_ context.Context, topic, key string, event kafka.Event) error {
	p.calls++
	if p.order != nil {
		*p.order = append(*p.order, "publish")
	}
	if topic != topics.TopicPaymentEvents || key == "" || event.ID != key {
		return errors.New("receipt relay identity changed")
	}
	return errors.New("fixture publish acknowledgement unknown")
}

type relayBoundaryHook struct {
	boundaries    []reliabilityhook.Boundary
	publicationAt []int
	publisher     *relayBoundaryPublisher
	order         *[]string
}

func (h *relayBoundaryHook) Await(_ context.Context, boundary reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error) {
	h.boundaries = append(h.boundaries, boundary)
	h.publicationAt = append(h.publicationAt, h.publisher.calls)
	if h.order != nil {
		*h.order = append(*h.order, boundary.Checkpoint)
	}
	return reliabilityhook.DecisionContinue, true, nil
}

func (*relayBoundaryHook) Enabled() bool { return true }

func TestReceiptRelayFaultBarriersBracketTheRealProduceAttempt(t *testing.T) {
	paymentID, bookingID, receiptID := uuid.New(), uuid.New(), uuid.New()
	claimToken := uuid.New()
	eventID := "evt_t23_ack_gate"
	envelopeID := uuid.NewString()
	leaseUntil := time.Now().Add(30 * time.Second)
	order := []string{}
	repository := &relayBoundaryRepository{claim: domain.ReceiptClaim{
		ReceiptID: receiptID, Token: claimToken, Fence: 7, LeaseUntil: leaseUntil,
		Receipt: domain.ProviderReceipt{
			ID: receiptID, EventType: "payment_intent.succeeded", CorrelationState: "applied",
			RelayState: "claimed", EnvelopeID: envelopeID,
			Observation: domain.ProviderObservation{PaymentID: paymentID, BookingID: bookingID,
				Generation: 1, EventID: eventID, IntentID: "pi_t23_ack_gate",
				Collected: true, Status: "succeeded", AmountCents: 725, Currency: "usd"},
		},
	}, order: &order}
	publisher := &relayBoundaryPublisher{order: &order}
	hook := &relayBoundaryHook{publisher: publisher, order: &order}
	worker := &FinancialWorker{repo: repository, publisher: publisher, reliability: hook}
	if count, err := worker.RunReceiptsOnce(context.Background(), 1); err != nil || count != 1 {
		t.Fatalf("receipt ACK gate did not complete its retryable path: count=%d err=%v", count, err)
	}
	if repository.relayState != "pending" || repository.category != "publish_outcome_unknown" {
		t.Fatalf("unknown Produce outcome was not recorded as retryable: relay=%q category=%q", repository.relayState, repository.category)
	}
	if publisher.calls != 1 || len(hook.boundaries) != 2 || len(hook.publicationAt) != 2 ||
		hook.publicationAt[0] != 0 || hook.publicationAt[1] != 1 {
		t.Fatalf("fault barriers did not bracket exactly one Produce attempt: calls=%d at=%v boundaries=%+v", publisher.calls, hook.publicationAt, hook.boundaries)
	}
	wantOrder := []string{"payment_receipt_before_broker_publish", "publish", "payment_broker_ack_dropped_before_relay_marker", "marker"}
	if len(order) != len(wantOrder) {
		t.Fatalf("receipt relay boundary ordering changed: %v", order)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("receipt relay boundary order = %v, want %v", order, wantOrder)
		}
	}
	before, after := hook.boundaries[0], hook.boundaries[1]
	if before.WindowID != "payment.relay_before_marker" ||
		before.Checkpoint != "payment_receipt_before_broker_publish" ||
		before.Action != reliabilityhook.ActionPause || before.OperationID != paymentID.String() ||
		after.Checkpoint != "payment_broker_ack_dropped_before_relay_marker" ||
		after.Action != reliabilityhook.ActionAckGate || after.OperationID != paymentID.String() {
		t.Fatalf("reliability barriers lost Payment/transport identity: before=%+v after=%+v", before, after)
	}
	for _, boundary := range hook.boundaries {
		if boundary.Facts["payment_id"] != paymentID.String() ||
			boundary.Facts["receipt_event_id"] != eventID ||
			boundary.Facts["booking_id"] != bookingID.String() ||
			boundary.Facts["envelope_id"] != envelopeID ||
			boundary.Facts["topic"] != topics.TopicPaymentEvents ||
			boundary.Facts["claim_token"] != claimToken.String() ||
			boundary.Facts["claim_fence"] != "7" ||
			boundary.Facts["lease_until"] != leaseUntil.UTC().Format(time.RFC3339Nano) {
			t.Fatalf("reliability boundary lost exact durable receipt claim: %+v", boundary)
		}
	}
}

func TestReceiptRelayRunsWithoutHookByDefault(t *testing.T) {
	paymentID, bookingID, receiptID := uuid.New(), uuid.New(), uuid.New()
	repository := &relayBoundaryRepository{claim: domain.ReceiptClaim{
		ReceiptID: receiptID, Token: uuid.New(), Fence: 1, LeaseUntil: time.Now().Add(30 * time.Second),
		Receipt: domain.ProviderReceipt{
			ID: receiptID, EventType: "payment_intent.succeeded", CorrelationState: "applied",
			RelayState: "claimed", EnvelopeID: uuid.NewString(),
			Observation: domain.ProviderObservation{PaymentID: paymentID, BookingID: bookingID,
				Generation: 1, EventID: "evt_default_receipt", IntentID: "pi_default_receipt",
				Collected: true, Status: "succeeded", AmountCents: 725, Currency: "usd"},
		},
	}}
	publisher := &relayBoundaryPublisher{}
	worker := &FinancialWorker{repo: repository, publisher: publisher}
	if count, err := worker.RunReceiptsOnce(context.Background(), 1); err != nil || count != 1 {
		t.Fatalf("default receipt relay changed behavior: count=%d err=%v", count, err)
	}
	if publisher.calls != 1 || repository.relayState != "pending" || repository.category != "publish_outcome_unknown" {
		t.Fatalf("default path did not preserve publish and retry behavior: calls=%d state=%q category=%q",
			publisher.calls, repository.relayState, repository.category)
	}
}

func TestEnabledReceiptHookRejectsMissingPaymentIdentity(t *testing.T) {
	hook := &relayBoundaryHook{publisher: &relayBoundaryPublisher{}}
	worker := &FinancialWorker{reliability: hook}
	claim := domain.ReceiptClaim{Token: uuid.New(), Fence: 1, LeaseUntil: time.Now().Add(time.Minute)}
	_, _, err := worker.awaitReceiptReliabilityBoundary(context.Background(), claim,
		domain.ProviderReceipt{EnvelopeID: uuid.NewString(), Observation: domain.ProviderObservation{
			BookingID: uuid.New(), EventID: "evt_missing_payment_identity",
		}}, "payment_receipt_before_broker_publish", reliabilityhook.ActionPause)
	if err == nil || len(hook.boundaries) != 0 {
		t.Fatalf("enabled Payment hook silently skipped missing Payment identity: calls=%d err=%v", len(hook.boundaries), err)
	}
}

func TestDisabledReceiptHookLeavesMissingIdentityInert(t *testing.T) {
	disabled, err := reliabilityhook.New(reliabilityhook.Config{})
	if err != nil {
		t.Fatal(err)
	}
	worker := &FinancialWorker{reliability: disabled}
	_, matched, err := worker.awaitReceiptReliabilityBoundary(context.Background(), domain.ReceiptClaim{},
		domain.ProviderReceipt{}, "payment_receipt_before_broker_publish", reliabilityhook.ActionPause)
	if err != nil || matched {
		t.Fatalf("disabled Payment hook changed default behavior: matched=%v err=%v", matched, err)
	}
}
