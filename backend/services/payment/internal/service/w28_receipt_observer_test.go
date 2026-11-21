package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
)

type observedReceiptRepository struct {
	repository.PaymentRepositoryInterface
	stored   domain.ProviderReceipt
	inserted bool
	accepted bool
}

func (r *observedReceiptRepository) AcceptProviderReceipt(_ context.Context, _ domain.ProviderReceipt) (*domain.ProviderReceipt, bool, error) {
	r.accepted = true
	return &r.stored, r.inserted, nil
}

type duplicateReceiptHook struct {
	repo     *observedReceiptRepository
	observed []reliabilityhook.Boundary
	t        *testing.T
}

func (h *duplicateReceiptHook) Enabled() bool { return true }

func (h *duplicateReceiptHook) Await(_ context.Context, b reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error) {
	h.t.Helper()
	if h.repo != nil && !h.repo.accepted {
		h.t.Fatal("receipt evidence produced before repository acceptance")
	}
	h.observed = append(h.observed, b)
	return reliabilityhook.DecisionContinue, true, nil
}

func TestW28ReceiptObserverOnPublishedDuplicateIntegration(t *testing.T) {
	repo := repository.NewPostgresPaymentRepository(retryPaymentFixture(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payment, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":100,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{
		PaymentID: payment, Key: "ticketbox:payment:" + payment.String() + ":g1:create",
		FrozenParams: frozen, Identity: domain.FinancialIdentity{
			BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_w28fixture",
			AmountCents: 100, Currency: "usd",
		},
	}
	var err error
	op.Identity.Fingerprint, err = domain.FinancialFingerprint(op.Identity, frozen)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	const eventID = "evt_w28duplicate"
	receiptID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:acct_w28fixture:test:"+eventID))
	safe := json.RawMessage(`{"id":"pi_w28duplicate","status":"succeeded","amount":100,"amount_received":100,"currency":"usd","livemode":false}`)
	r := domain.ProviderReceipt{
		ID: receiptID, EnvelopeID: receiptID.String(), EventType: "payment_intent.succeeded",
		CorrelationState: "pending", RelayState: "pending",
		Observation: domain.ProviderObservation{
			Account: "acct_w28fixture", EventID: eventID, IntentID: "pi_w28duplicate",
			PaymentID: payment, BookingID: booking, Generation: 1, AmountCents: 100, Currency: "usd",
			Status: "succeeded", Collected: true, ObservedAt: time.Now(),
			SafeObject: safe, EvidenceHash: sha256.Sum256(safe),
		},
	}
	hook := &duplicateReceiptHook{t: t}
	svc := NewPaymentService(repo, zap.NewNop(), nil)
	svc.SetReceiptReliabilityHook(hook)
	if err := svc.AcceptProviderReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	publisher := &capturedReceiptPublisher{}
	worker := NewFinancialWorker(svc)
	worker.SetReceiptPublisher(publisher)
	if count, err := worker.RunReceiptsOnce(ctx, 1); err != nil || count != 1 || len(publisher.events) != 1 {
		t.Fatalf("initial receipt was not published exactly once: count=%d err=%v", count, err)
	}
	if err := svc.AcceptProviderReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	if count, err := worker.RunReceiptsOnce(ctx, 1); err != nil || count != 0 {
		t.Fatalf("an exact published duplicate became new work: count=%d err=%v", count, err)
	}
	if len(hook.observed) != 2 || hook.observed[0].Checkpoint != "payment_w28_initial_receipt" ||
		hook.observed[1].Facts["receipt_id"] != receiptID.String() ||
		hook.observed[1].Facts["relay_state"] != "published" || len(publisher.events) != 1 {
		t.Fatal("did not observe exactly one previously published duplicate")
	}
}

func TestW28ReceiptObserverDistinguishesInitialFromDuplicate(t *testing.T) {
	receipt := domain.ProviderReceipt{ID: uuid.New(), EventType: "payment_intent.succeeded",
		CorrelationState: "applied", RelayState: "pending", Observation: domain.ProviderObservation{
			Account: "acct_testing", EventID: "evt_w28", IntentID: "pi_w28",
			PaymentID: uuid.New(), BookingID: uuid.New(), Collected: true, Generation: 1,
			AmountCents: 100, Currency: "usd", EvidenceHash: sha256.Sum256([]byte("initial"))}}
	repo := &observedReceiptRepository{stored: receipt, inserted: true}
	hook := &duplicateReceiptHook{repo: repo, t: t}
	svc := NewPaymentService(repo, zap.NewNop(), nil)
	svc.SetReceiptReliabilityHook(hook)
	if err := svc.AcceptProviderReceipt(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if len(hook.observed) != 1 || hook.observed[0].Checkpoint != "payment_w28_initial_receipt" ||
		hook.observed[0].Facts["inserted"] != "true" || hook.observed[0].Facts["relay_state"] != "pending" {
		t.Fatal("the inserted, applied receipt was not observed as initial")
	}
}

func TestW28ReceiptObserverReportsOnlyExactAppliedDuplicate(t *testing.T) {
	receipt := domain.ProviderReceipt{ID: uuid.New(), EventType: "payment_intent.succeeded", CorrelationState: "applied",
		RelayState: "published", Observation: domain.ProviderObservation{
			Account: "acct_testing", EventID: "evt_w28", IntentID: "pi_w28",
			PaymentID: uuid.New(), BookingID: uuid.New(), Collected: true, Generation: 1, AmountCents: 100,
			Currency: "usd", EvidenceHash: sha256.Sum256([]byte("fixture observation"))}}
	tests := []struct {
		name   string
		change func(*observedReceiptRepository)
		want   bool
	}{
		{name: "exact previously applied receipt", want: true},
		{name: "first delivery", change: func(r *observedReceiptRepository) { r.inserted = true }},
		{name: "wrong evidence", change: func(r *observedReceiptRepository) { r.stored.Observation.EvidenceHash = sha256.Sum256([]byte("other")) }},
		{name: "wrong account", change: func(r *observedReceiptRepository) { r.stored.Observation.Account = "acct_other" }},
		{name: "wrong mode", change: func(r *observedReceiptRepository) { r.stored.Observation.LiveMode = true }},
		{name: "wrong payment", change: func(r *observedReceiptRepository) { r.stored.Observation.PaymentID = uuid.New() }},
		{name: "not applied", change: func(r *observedReceiptRepository) { r.stored.CorrelationState = "pending" }},
		{name: "correlated now but not previously published", change: func(r *observedReceiptRepository) { r.stored.RelayState = "pending" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &observedReceiptRepository{stored: receipt}
			if tc.change != nil {
				tc.change(repo)
			}
			hook := &duplicateReceiptHook{repo: repo, t: t}
			svc := NewPaymentService(repo, zap.NewNop(), nil)
			svc.SetReceiptReliabilityHook(hook)
			if err := svc.AcceptProviderReceipt(context.Background(), receipt); err != nil {
				t.Fatal(err)
			}
			if !tc.want {
				if len(hook.observed) != 0 {
					t.Fatal("an ineligible receipt was observed as an applied duplicate")
				}
				return
			}
			if len(hook.observed) != 1 {
				t.Fatalf("observed %d duplicate invocations", len(hook.observed))
			}
			boundary := hook.observed[0]
			if boundary.Checkpoint != "payment_w28_correlated_duplicate_receipt" ||
				boundary.OperationID != receipt.Observation.PaymentID.String() ||
				boundary.Facts["receipt_id"] != receipt.ID.String() ||
				boundary.Facts["event_id"] != receipt.Observation.EventID ||
				boundary.Facts["correlation_state"] != "applied" ||
				boundary.Facts["booking_id"] != receipt.Observation.BookingID.String() {
				t.Fatalf("unexpected observed duplicate: %+v", boundary)
			}
			if _, err := uuid.Parse(boundary.Facts["invocation_id"]); err != nil {
				t.Fatal("missing invocation identity")
			}
		})
	}
}
