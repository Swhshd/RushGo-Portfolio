//go:build admission_r4

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	payment_gateway "github.com/ticketbox/payment/internal/gateway"
)

type providerFixture struct {
	paymentID       uuid.UUID
	bookingID       uuid.UUID
	userID          uuid.UUID
	generation      int64
	intentID        string
	account         string
	amountCents     int64
	currency        string
	evidenceCalls   int64
	refundReadCalls int64
	refundCalls     int64
	refundEffects   int64
	refunded        bool
	refundID        string
	dynamic         bool
	createOperation domain.CreateOperation
	created         bool
	confirmed       bool
	cancelled       bool
	receiptAccepted bool
	frozenReceipt   *domain.ProviderReceipt
}

type localProvider struct {
	mu           sync.Mutex
	byPayment    map[uuid.UUID]*providerFixture
	byIntent     map[string]*providerFixture
	byRefund     map[string]*providerFixture
	evidenceCall int64
	runID        string
	dynamicUser  uuid.UUID
	dynamic      *providerFixture
	persistence  *providerPersistence
	persistError error
}

type providerStats struct {
	ProviderKind     string                 `json:"provider_kind"`
	PersistenceState string                 `json:"persistence_state"`
	StripeAPICalls   int64                  `json:"stripe_api_calls"`
	ProviderCalls    int64                  `json:"provider_calls"`
	Fixtures         []providerFixtureStats `json:"fixtures"`
	DynamicPurchase  *dynamicPurchaseStats  `json:"dynamic_purchase,omitempty"`
}

type dynamicPurchaseStats struct {
	Enabled         bool   `json:"enabled"`
	UserID          string `json:"user_id"`
	Created         bool   `json:"created"`
	Confirmed       bool   `json:"confirmed"`
	ReceiptAccepted bool   `json:"receipt_accepted"`
	PaymentID       string `json:"payment_id,omitempty"`
	Refunded        bool   `json:"refunded"`
	RefundEffects   int64  `json:"refund_effects"`
}

type providerFixtureStats struct {
	PaymentID       string `json:"payment_id"`
	PaymentIntentID string `json:"payment_intent_id"`
	ProviderCalls   int64  `json:"provider_calls"`
	RefundReadCalls int64  `json:"refund_read_calls"`
	RefundCalls     int64  `json:"refund_calls"`
	RefundEffects   int64  `json:"refund_effects"`
	Refunded        bool   `json:"refunded"`
}

func newLocalProvider(cfg bootstrapConfig) (*localProvider, error) {
	provider := &localProvider{
		byPayment: make(map[uuid.UUID]*providerFixture, len(cfg.Fixtures)),
		byIntent:  make(map[string]*providerFixture, len(cfg.Fixtures)),
		byRefund:  make(map[string]*providerFixture, len(cfg.Fixtures)),
		runID:     cfg.RunID,
	}
	for _, item := range cfg.Fixtures {
		paymentID, _ := uuid.Parse(item.PaymentID)
		bookingID, _ := uuid.Parse(item.BookingID)
		userID, _ := uuid.Parse(item.UserID)
		f := &providerFixture{
			paymentID: paymentID, bookingID: bookingID, userID: userID,
			generation: item.Generation, intentID: item.PaymentIntentID,
			account: item.ProviderAccount, amountCents: item.AmountCents, currency: item.Currency,
			refundID: "re_r4_" + paymentID.String(),
		}
		if _, exists := provider.byPayment[f.paymentID]; exists {
			return nil, errors.New("duplicate local provider Payment fixture")
		}
		if _, exists := provider.byIntent[f.intentID]; exists {
			return nil, errors.New("duplicate local provider intent fixture")
		}
		provider.byPayment[f.paymentID] = f
		provider.byIntent[f.intentID] = f
		provider.byRefund[f.refundID] = f
	}
	if cfg.DynamicPurchase != nil {
		if err := validateDynamicPurchaseConfig(*cfg.DynamicPurchase, cfg.Fixtures, cfg.KafkaBrokers); err != nil {
			return nil, err
		}
		buyer, err := uuid.Parse(cfg.DynamicPurchase.UserID)
		if err != nil || buyer == uuid.Nil || cfg.RunID == "" {
			return nil, errors.New("dynamic purchase run identity is invalid")
		}
		provider.dynamicUser = buyer
	}
	return provider, nil
}

func (p *localProvider) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, errors.New("test-only provider does not create PaymentIntents")
}

func (p *localProvider) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, errors.New("test-only provider does not handle webhooks")
}

func (p *localProvider) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return errors.New("test-only provider does not send legacy refunds")
}

func (p *localProvider) CreateIntent(ctx context.Context, op domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if p.dynamicUser == uuid.Nil || !validDynamicOperation(op, p.dynamicUser) {
		return nil, "", errors.New("dynamic PaymentIntent identity is outside the configured single purchase slot")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, "", p.persistError
	}
	if existing := p.byPayment[op.PaymentID]; existing != nil {
		if !existing.dynamic || !sameDynamicOperation(existing.createOperation, op) {
			return nil, "", errors.New("dynamic PaymentIntent replay identity mismatch")
		}
		return p.dynamicObservationLocked(existing), dynamicClientSecret(existing.intentID, existing.receiptAccepted || existing.cancelled), nil
	}
	if p.dynamic != nil || p.byPayment[op.PaymentID] != nil {
		return nil, "", errors.New("dynamic purchase slot has already been consumed")
	}
	intentID := "pi_r4_" + strings.ReplaceAll(op.PaymentID.String(), "-", "")
	if !intentPattern.MatchString(intentID) {
		return nil, "", errors.New("dynamic PaymentIntent identifier is invalid")
	}
	f := &providerFixture{
		paymentID: op.PaymentID, bookingID: op.Identity.BookingID, userID: op.Identity.UserID,
		generation: op.Identity.Generation, intentID: intentID, account: op.Identity.ProviderAccount,
		amountCents: op.Identity.AmountCents, currency: op.Identity.Currency,
		refundID: "re_r4_" + strings.ReplaceAll(op.PaymentID.String(), "-", ""),
		dynamic:  true, createOperation: cloneCreateOperation(op), created: true,
	}
	p.dynamic = f
	p.byPayment[f.paymentID] = f
	p.byIntent[f.intentID] = f
	p.byRefund[f.refundID] = f
	if err := p.persistLocked(); err != nil {
		return nil, "", err
	}
	return p.dynamicObservationLocked(f), dynamicClientSecret(f.intentID, false), nil
}

func (p *localProvider) RetrieveIntent(ctx context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, "", p.persistError
	}
	f := p.byIntent[intentID]
	if f == nil || f.account != account || live || f.dynamic && (p.dynamic != f || !f.created) {
		return nil, "", errors.New("test-only provider intent identity mismatch")
	}
	if !f.dynamic {
		return nil, "", errors.New("static admission fixtures do not expose checkout PaymentIntents")
	}
	return p.dynamicObservationLocked(f), dynamicClientSecret(f.intentID, f.receiptAccepted || f.cancelled), nil
}

func (p *localProvider) FindIntent(ctx context.Context, op domain.CreateOperation) ([]domain.ProviderObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.dynamicUser == uuid.Nil || !validDynamicOperation(op, p.dynamicUser) {
		return nil, errors.New("dynamic PaymentIntent search identity is outside the configured single purchase slot")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, p.persistError
	}
	if p.dynamic == nil || p.dynamic.paymentID != op.PaymentID || !sameDynamicOperation(p.dynamic.createOperation, op) {
		return nil, nil
	}
	o := p.dynamicObservationLocked(p.dynamic)
	return []domain.ProviderObservation{*o}, nil
}

func (p *localProvider) CancelIntent(ctx context.Context, claim domain.FinancialClaim, intent string) (*domain.ProviderObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, p.persistError
	}
	f := p.byPayment[claim.PaymentID]
	if f == nil || !f.dynamic || f.intentID != intent || f.confirmed || claim.Kind != "cancel" || claim.Key != "ticketbox:payment:"+f.paymentID.String()+":g1:cancel" || claim.Operation.Identity != f.createOperation.Identity {
		return nil, errors.New("simulated cancellation identity or state rejected")
	}
	f.cancelled = true
	if err := p.persistLocked(); err != nil {
		return nil, err
	}
	return p.dynamicObservationLocked(f), nil
}

func (p *localProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, paymentIntentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	if p.checkPersistenceLocked() != nil {
		p.mu.Unlock()
		return nil, p.persistError
	}
	f := p.byPayment[claim.PaymentID]
	if f == nil || !matchesFinancialClaim(f, claim) || paymentIntentID != f.intentID {
		p.mu.Unlock()
		return nil, errors.New("test-only refund identity mismatch")
	}
	if f.dynamic && (!f.confirmed || !f.receiptAccepted) {
		p.mu.Unlock()
		return nil, errors.New("dynamic refund requires a durably accepted collected purchase")
	}
	f.refundCalls++
	if f.refunded {
		observation := p.refundObservationLocked(f, "succeeded")
		p.mu.Unlock()
		return observation, nil
	}
	f.refunded = true
	f.refundEffects++
	if err := p.persistLocked(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	observation := p.refundObservationLocked(f, "succeeded")
	refundID := f.refundID
	account := f.account
	amount := f.amountCents
	p.mu.Unlock()

	if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
		live := false
		now := time.Now().UTC()
		capture.RecordResponse(domain.RefundAuditResponse{
			Classification:    domain.RefundAuditResponseObserved,
			ProviderRequestID: "req_r4_" + claim.PaymentID.String(),
			RefundID:          refundID, PaymentIntentID: paymentIntentID,
			ProviderStatus: "succeeded", Currency: "usd", AmountCents: &amount,
			ObservedProviderAccount: &account, ObservedLiveMode: &live,
			SummarySource: "transport_allowlist", ObservedAt: &now,
		})
	}
	return observation, nil
}

func matchesFinancialClaim(f *providerFixture, claim domain.FinancialClaim) bool {
	i := claim.Operation.Identity
	return claim.PaymentID == f.paymentID && claim.Operation.PaymentID == f.paymentID &&
		i.BookingID == f.bookingID && i.UserID == f.userID && i.Generation == f.generation &&
		i.ProviderAccount == f.account && !i.LiveMode && i.AmountCents == f.amountCents && i.Currency == f.currency &&
		claim.Refund != nil && claim.Refund.PaymentID == f.paymentID && claim.Refund.Generation == f.generation &&
		claim.Refund.AmountCents == f.amountCents && claim.Refund.Currency == f.currency &&
		claim.Refund.Key == "ticketbox:payment:"+f.paymentID.String()+":g1:refund"
}

func (p *localProvider) RetrieveRefund(_ context.Context, account string, live bool, refundID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, p.persistError
	}
	f := p.byRefund[refundID]
	if f == nil || f.account != account || live || !f.refunded {
		return nil, errors.New("test-only refund observation is unavailable")
	}
	return p.refundObservationLocked(f, "succeeded"), nil
}

func (p *localProvider) InspectSucceededRefunds(_ context.Context, account string, live bool, paymentIntentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return nil, p.persistError
	}
	f := p.byIntent[paymentIntentID]
	if f == nil || f.account != account || live {
		return nil, errors.New("test-only provider identity mismatch")
	}
	f.refundReadCalls++
	if f.refunded {
		return p.refundObservationLocked(f, "succeeded"), nil
	}
	observation := p.refundObservationLocked(f, "no_refunds")
	observation.SucceededRefundCents = 0
	observation.RefundID = ""
	return observation, nil
}

func (p *localProvider) VerifyWebhook(context.Context, []byte, string) (*domain.ProviderReceipt, error) {
	return nil, errors.New("test-only provider does not verify webhooks")
}

func (p *localProvider) CheckAdmissionEvidence(_ context.Context, identity domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return unknownProviderEvidence("provider_persistence_uncertain")
	}
	p.evidenceCall++
	f := p.byPayment[identity.PaymentID]
	if f == nil {
		return unknownProviderEvidence("fixture_identity_unknown")
	}
	if f.dynamic && (!f.confirmed || !f.receiptAccepted) {
		return unknownProviderEvidence("dynamic_purchase_not_confirmed")
	}
	f.evidenceCalls++
	if identity.BookingID != f.bookingID || identity.Generation != f.generation || identity.LiveMode ||
		identity.ProviderAccount != f.account || identity.PaymentIntentID != f.intentID ||
		identity.AmountCents != f.amountCents || identity.Currency != f.currency {
		return unknownProviderEvidence("fixture_identity_mismatch")
	}
	if f.refunded {
		return domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityDenied,
			DisputeState:     domain.DisputeEvidenceNoDispute,
			RefundState:      domain.RefundEvidencePresent,
			// The source label is required by AdmissionCoordinator; this
			// build-tagged R4 process reads only its local provider double.
			ObservedAt: time.Now().UTC(), Source: fixtureEvidenceSource, ReasonCode: "fixture_refund_present",
		}
	}
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		// This is the coordinator's evidence-source contract label, not a Stripe call.
		ObservedAt: time.Now().UTC(), Source: fixtureEvidenceSource, ReasonCode: "fixture_observed_clear",
	}
}

func unknownProviderEvidence(reason string) domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityUnknown,
		DisputeState:     domain.DisputeEvidenceUnknown,
		RefundState:      domain.RefundEvidenceUnknown,
		ReasonCode:       reason,
	}
}

func (p *localProvider) refundObservationLocked(f *providerFixture, state string) *domain.ProviderObservation {
	succeeded := int64(0)
	if f.refunded {
		succeeded = f.amountCents
	}
	return &domain.ProviderObservation{
		Account: f.account, LiveMode: false, EventID: "evt_r4_" + f.paymentID.String(),
		IntentID: f.intentID, RefundID: f.refundID, Status: state,
		PaymentID: f.paymentID, BookingID: f.bookingID, Generation: f.generation,
		AmountCents: f.amountCents, SucceededRefundCents: succeeded,
		Currency: f.currency, Collected: !f.dynamic || f.confirmed, ObservedAt: time.Now().UTC(),
	}
}

func (p *localProvider) stats() providerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := providerStats{ProviderKind: "local_test_double", StripeAPICalls: 0, ProviderCalls: p.evidenceCall, Fixtures: make([]providerFixtureStats, 0, len(p.byPayment))}
	stats.PersistenceState = "volatile_test_only"
	if p.persistence != nil {
		stats.PersistenceState = "durable"
	}
	if p.checkPersistenceLocked() != nil {
		stats.PersistenceState = "unknown"
	}
	for _, f := range p.byPayment {
		if f.dynamic {
			continue
		}
		stats.Fixtures = append(stats.Fixtures, providerFixtureStats{
			PaymentID: f.paymentID.String(), PaymentIntentID: f.intentID,
			ProviderCalls: f.evidenceCalls, RefundReadCalls: f.refundReadCalls,
			RefundCalls: f.refundCalls, RefundEffects: f.refundEffects, Refunded: f.refunded,
		})
	}
	if p.dynamicUser != uuid.Nil {
		dynamic := &dynamicPurchaseStats{Enabled: true, UserID: p.dynamicUser.String()}
		if p.dynamic != nil {
			dynamic.Created = p.dynamic.created
			dynamic.Confirmed = p.dynamic.confirmed
			dynamic.ReceiptAccepted = p.dynamic.receiptAccepted
			dynamic.PaymentID = p.dynamic.paymentID.String()
			dynamic.Refunded = p.dynamic.refunded
			dynamic.RefundEffects = p.dynamic.refundEffects
		}
		stats.DynamicPurchase = dynamic
	}
	sort.Slice(stats.Fixtures, func(i, j int) bool { return stats.Fixtures[i].PaymentID < stats.Fixtures[j].PaymentID })
	return stats
}

func validDynamicOperation(op domain.CreateOperation, buyer uuid.UUID) bool {
	i := op.Identity
	if buyer == uuid.Nil || op.PaymentID == uuid.Nil || i.BookingID == uuid.Nil || i.UserID != buyer || i.Generation != 1 ||
		i.ProviderAccount != "acct_r4_local" || i.LiveMode || i.AmountCents != 100 || i.Currency != "usd" ||
		op.Key != "ticketbox:payment:"+op.PaymentID.String()+":g1:create" {
		return false
	}
	fingerprint, err := domain.FinancialFingerprint(i, op.FrozenParams)
	return err == nil && fingerprint == i.Fingerprint
}

func sameDynamicOperation(a, b domain.CreateOperation) bool {
	var x, y any
	return a.PaymentID == b.PaymentID && a.Key == b.Key && a.Identity == b.Identity && json.Unmarshal(a.FrozenParams, &x) == nil && json.Unmarshal(b.FrozenParams, &y) == nil && reflect.DeepEqual(x, y)
}

func cloneCreateOperation(op domain.CreateOperation) domain.CreateOperation {
	op.FrozenParams = append(json.RawMessage(nil), op.FrozenParams...)
	return op
}

func dynamicClientSecret(intentID string, confirmed bool) string {
	if confirmed {
		return ""
	}
	return "simulated://SIMULATED_PROVIDER_AUTHORITY/" + intentID
}

func (p *localProvider) dynamicObservationLocked(f *providerFixture) *domain.ProviderObservation {
	status, collected := "requires_payment_method", false
	if f.receiptAccepted {
		status, collected = "succeeded", true
	}
	if f.cancelled {
		status, collected = "canceled", false
	}
	return &domain.ProviderObservation{
		Account: f.account, LiveMode: false,
		EventID:  "evt_r4_" + strings.ReplaceAll(f.paymentID.String(), "-", ""),
		IntentID: f.intentID, Status: status, PaymentID: f.paymentID, BookingID: f.bookingID,
		Generation: f.generation, AmountCents: f.amountCents, Currency: f.currency,
		Collected: collected, ObservedAt: time.Now().UTC(),
	}
}

func (p *localProvider) dynamicConfirmationReceipt(paymentID uuid.UUID) (domain.ProviderReceipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return domain.ProviderReceipt{}, p.persistError
	}
	f := p.dynamic
	if f == nil || !f.dynamic || !f.created || f.paymentID != paymentID || f.cancelled {
		return domain.ProviderReceipt{}, errors.New("dynamic purchase payment is not owned by the configured slot")
	}
	if f.frozenReceipt != nil {
		return cloneProviderReceipt(*f.frozenReceipt), nil
	}
	eventID := "evt_r4_" + strings.ReplaceAll(f.paymentID.String(), "-", "")
	safeObject, err := json.Marshal(struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}{ID: f.intentID, Object: "payment_intent"})
	if err != nil {
		return domain.ProviderReceipt{}, err
	}
	observedAt := time.Now().UTC()
	observation := domain.ProviderObservation{
		Account: f.account, LiveMode: false, EventID: eventID, IntentID: f.intentID,
		Status: "succeeded", PaymentID: f.paymentID, BookingID: f.bookingID,
		Generation: f.generation, AmountCents: f.amountCents, Currency: f.currency,
		Collected: true, ObservedAt: observedAt, EvidenceHash: sha256.Sum256(safeObject), SafeObject: safeObject,
	}
	receiptID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+f.account+":test:"+eventID))
	receipt := domain.ProviderReceipt{
		ID: receiptID, Observation: observation, EventType: "payment_intent.succeeded",
		CorrelationState: "pending", RelayState: "pending", EnvelopeID: receiptID.String(), ReceivedAt: observedAt,
	}
	f.confirmed = true
	cloned := cloneProviderReceipt(receipt)
	f.frozenReceipt = &cloned
	if err := p.persistLocked(); err != nil {
		return domain.ProviderReceipt{}, err
	}
	return cloneProviderReceipt(receipt), nil
}

func (p *localProvider) markDynamicReceiptAccepted(paymentID uuid.UUID, receipt domain.ProviderReceipt) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return p.persistError
	}
	f := p.dynamic
	if f == nil || !f.dynamic || f.paymentID != paymentID || !f.confirmed || f.frozenReceipt == nil || !sameFrozenReceipt(*f.frozenReceipt, receipt) {
		return errors.New("accepted dynamic receipt identity differs from the frozen provider confirmation")
	}
	f.receiptAccepted = true
	return p.persistLocked()
}

func (p *localProvider) dynamicOperationFor(paymentID uuid.UUID) (domain.CreateOperation, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkPersistenceLocked() != nil {
		return domain.CreateOperation{}, "", false
	}
	f := p.dynamic
	if f == nil || !f.dynamic || !f.created || paymentID == uuid.Nil || f.paymentID != paymentID {
		return domain.CreateOperation{}, "", false
	}
	return cloneCreateOperation(f.createOperation), f.intentID, true
}

func (p *localProvider) dynamicStatsSnapshot() dynamicPurchaseStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := dynamicPurchaseStats{Enabled: p.dynamicUser != uuid.Nil}
	if p.dynamicUser != uuid.Nil {
		stats.UserID = p.dynamicUser.String()
	}
	if p.dynamic != nil {
		stats.Created = p.dynamic.created
		stats.Confirmed = p.dynamic.confirmed
		stats.ReceiptAccepted = p.dynamic.receiptAccepted
		stats.PaymentID = p.dynamic.paymentID.String()
		stats.Refunded = p.dynamic.refunded
		stats.RefundEffects = p.dynamic.refundEffects
	}
	return stats
}

func cloneProviderReceipt(receipt domain.ProviderReceipt) domain.ProviderReceipt {
	receipt.Observation.SafeObject = append(json.RawMessage(nil), receipt.Observation.SafeObject...)
	return receipt
}

func sameFrozenReceipt(a, b domain.ProviderReceipt) bool {
	x, y := a.Observation, b.Observation
	return a.ID == b.ID && a.EventType == b.EventType && a.CorrelationState == b.CorrelationState && a.RelayState == b.RelayState &&
		a.EnvelopeID == b.EnvelopeID && a.ReceivedAt.Equal(b.ReceivedAt) && x.Account == y.Account && x.LiveMode == y.LiveMode &&
		x.EventID == y.EventID && x.IntentID == y.IntentID && x.RefundID == y.RefundID && x.Status == y.Status &&
		x.PaymentID == y.PaymentID && x.BookingID == y.BookingID && x.Generation == y.Generation && x.AmountCents == y.AmountCents &&
		x.SucceededRefundCents == y.SucceededRefundCents && x.Currency == y.Currency && x.Collected == y.Collected &&
		x.ObservedAt.Equal(y.ObservedAt) && x.EvidenceHash == y.EvidenceHash && bytes.Equal(x.SafeObject, y.SafeObject)
}

var _ payment_gateway.PaymentGatewayInterface = (*localProvider)(nil)
var _ payment_gateway.FinancialProvider = (*localProvider)(nil)
var _ payment_gateway.AdmissionEvidenceProvider = (*localProvider)(nil)
