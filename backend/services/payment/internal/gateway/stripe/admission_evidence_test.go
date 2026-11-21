package stripe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	"go.uber.org/zap"
)

func TestCheckAdmissionEvidenceUsesCompleteBoundProviderQueries(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	const paymentIntentID = "pi_admission_fixture"
	const latestChargeID = "ch_admission_success"
	const priorChargeID = "ch_admission_failed"

	var chargePages, disputePages int
	var finalDisputeResponseAt atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer sk_test_fixture" {
			t.Errorf("request did not use the configured TEST key safely")
		}
		if r.Header.Get("Stripe-Version") != stripego.APIVersion {
			t.Errorf("Stripe-Version = %q, want SDK version %q", r.Header.Get("Stripe-Version"), stripego.APIVersion)
		}

		switch r.URL.Path {
		case "/v1/payment_intents/" + paymentIntentID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": paymentIntentID, "object": "payment_intent", "status": "succeeded",
				"amount": 725, "amount_received": 725, "currency": "usd", "livemode": false,
				"latest_charge": latestChargeID,
				"metadata":      map[string]string{"ticketbox_payment_id": paymentID.String(), "ticketbox_booking_id": bookingID.String(), "ticketbox_generation": "1"},
			})
		case "/v1/charges":
			if r.URL.Query().Get("payment_intent") != paymentIntentID || r.URL.Query().Get("limit") != "100" {
				t.Errorf("charge query is not scoped and bounded: %s", r.URL.RawQuery)
			}
			chargePages++
			if chargePages == 1 {
				if r.URL.Query().Get("starting_after") != "" {
					t.Errorf("first charge page unexpectedly has cursor")
				}
				_, _ = w.Write([]byte(`{"object":"list","has_more":true,"data":[{"id":"ch_admission_failed","object":"charge","amount":725,"amount_captured":0,"amount_refunded":0,"currency":"usd","livemode":false,"status":"failed","paid":false,"captured":false,"disputed":false,"payment_intent":"pi_admission_fixture"}]}`))
				return
			}
			if r.URL.Query().Get("starting_after") != priorChargeID {
				t.Errorf("charge pagination cursor = %q, want previous page's last ID", r.URL.Query().Get("starting_after"))
			}
			_, _ = w.Write([]byte(`{"object":"list","has_more":false,"data":[{"id":"ch_admission_success","object":"charge","amount":725,"amount_captured":725,"amount_refunded":0,"currency":"usd","livemode":false,"status":"succeeded","paid":true,"captured":true,"disputed":false,"payment_intent":"pi_admission_fixture"}]}`))
		case "/v1/disputes":
			if r.URL.Query().Get("payment_intent") != paymentIntentID || r.URL.Query().Get("limit") != "100" {
				t.Errorf("dispute query is not scoped and bounded: %s", r.URL.RawQuery)
			}
			disputePages++
			_, _ = w.Write([]byte(`{"object":"list","has_more":false,"data":[]}`))
			finalDisputeResponseAt.Store(time.Now().UTC().UnixNano())
		default:
			t.Errorf("unexpected Stripe path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	client := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, client)
	provider.SetWebhookScope("acct_fixture", false)
	provider.SetAdmissionReadBackend(backend, "sk_test_fixture")

	got := provider.CheckAdmissionEvidence(context.Background(), domain.AdmissionFinancialIdentity{
		PaymentID: paymentID, BookingID: bookingID, Generation: 1, ProviderAccount: "acct_fixture",
		PaymentIntentID: paymentIntentID, AmountCents: 725, Currency: "usd",
	})
	if got.DisputeState != domain.DisputeEvidenceNoDispute {
		t.Fatalf("dispute state = %q, want provider-observed no dispute (reason %q)", got.DisputeState, got.ReasonCode)
	}
	if got.RefundState != domain.RefundEvidenceNoRefund {
		t.Fatalf("refund state = %q, want provider-observed no refund", got.RefundState)
	}
	if got.Source != "stripe_test_api" || got.ObservedAt.IsZero() || got.ObservedAt.Location() != time.UTC {
		t.Fatalf("incomplete observation metadata: source=%q observed_at=%v", got.Source, got.ObservedAt)
	}
	if chargePages != 2 || disputePages != 1 {
		t.Fatalf("provider pages: charges=%d disputes=%d, want 2 and 1", chargePages, disputePages)
	}
	if got.ObservedAt.UnixNano() < finalDisputeResponseAt.Load() {
		t.Fatalf("observed_at %s predates completion of the final Dispute response", got.ObservedAt)
	}
}

func TestCheckAdmissionEvidenceRejectsClosedDispute(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	const paymentIntentID = "pi_disputed_fixture"
	const chargeID = "ch_disputed_fixture"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/payment_intents/" + paymentIntentID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": paymentIntentID, "object": "payment_intent", "status": "succeeded", "amount": 725,
				"amount_received": 725, "currency": "usd", "livemode": false, "latest_charge": chargeID,
				"metadata": map[string]string{"ticketbox_payment_id": paymentID.String(), "ticketbox_booking_id": bookingID.String(), "ticketbox_generation": "1"},
			})
		case "/v1/charges":
			_, _ = w.Write([]byte(`{"object":"list","has_more":false,"data":[{"id":"ch_disputed_fixture","object":"charge","amount":725,"amount_captured":725,"amount_refunded":0,"currency":"usd","livemode":false,"status":"succeeded","paid":true,"captured":true,"disputed":false,"payment_intent":"pi_disputed_fixture"}]}`))
		case "/v1/disputes":
			_, _ = w.Write([]byte(`{"object":"list","has_more":false,"data":[{"id":"du_closed_fixture","object":"dispute","amount":725,"charge":"ch_disputed_fixture","created":1792600000,"currency":"usd","livemode":false,"payment_intent":"pi_disputed_fixture","status":"won"}]}`))
		default:
			t.Errorf("unexpected Stripe path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	provider.SetWebhookScope("acct_fixture", false)
	provider.SetAdmissionReadBackend(backend, "sk_test_fixture")

	got := provider.CheckAdmissionEvidence(context.Background(), domain.AdmissionFinancialIdentity{
		PaymentID: paymentID, BookingID: bookingID, Generation: 1, ProviderAccount: "acct_fixture",
		PaymentIntentID: paymentIntentID, AmountCents: 725, Currency: "usd",
	})
	if got.DisputeState != domain.DisputeEvidencePresent {
		t.Fatalf("closed dispute state = %q, want present (reason %q)", got.DisputeState, got.ReasonCode)
	}
}

func TestCheckAdmissionEvidenceDoesNotTreatIncompletePaginationAsClear(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	const paymentIntentID = "pi_incomplete_fixture"
	const chargeID = "ch_incomplete_fixture"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/payment_intents/" + paymentIntentID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": paymentIntentID, "object": "payment_intent", "status": "succeeded", "amount": 725,
				"amount_received": 725, "currency": "usd", "livemode": false, "latest_charge": chargeID,
				"metadata": map[string]string{"ticketbox_payment_id": paymentID.String(), "ticketbox_booking_id": bookingID.String(), "ticketbox_generation": "1"},
			})
		case "/v1/charges":
			_, _ = w.Write([]byte(`{"object":"list","has_more":false,"data":[{"id":"ch_incomplete_fixture","object":"charge","amount":725,"amount_captured":725,"amount_refunded":0,"currency":"usd","livemode":false,"status":"succeeded","paid":true,"captured":true,"disputed":false,"payment_intent":"pi_incomplete_fixture"}]}`))
		case "/v1/disputes":
			// has_more=true with an empty page cannot establish a complete no-dispute observation.
			_, _ = w.Write([]byte(`{"object":"list","has_more":true,"data":[]}`))
		default:
			t.Errorf("unexpected Stripe path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	provider.SetWebhookScope("acct_fixture", false)
	provider.SetAdmissionReadBackend(backend, "sk_test_fixture")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got := provider.CheckAdmissionEvidence(ctx, domain.AdmissionFinancialIdentity{
		PaymentID: paymentID, BookingID: bookingID, Generation: 1, ProviderAccount: "acct_fixture",
		PaymentIntentID: paymentIntentID, AmountCents: 725, Currency: "usd",
	})
	if got.DisputeState != domain.DisputeEvidenceUnknown {
		t.Fatalf("incomplete pagination state = %q, want unknown", got.DisputeState)
	}
}

func TestCheckAdmissionEvidenceScopesIdentityBeforeProviderCall(t *testing.T) {
	var called atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Store(true) }))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0)})
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	provider.SetWebhookScope("acct_fixture", false)
	provider.SetAdmissionReadBackend(backend, "sk_test_fixture")

	got := provider.CheckAdmissionEvidence(context.Background(), domain.AdmissionFinancialIdentity{
		PaymentID: uuid.New(), BookingID: uuid.New(), Generation: 1, ProviderAccount: "acct_other",
		PaymentIntentID: "pi_untrusted", AmountCents: 725, Currency: "usd",
	})
	if called.Load() || got.DisputeState != domain.DisputeEvidenceUnknown {
		t.Fatalf("untrusted provider identity triggered request or escaped unknown: called=%v evidence=%+v", called.Load(), got)
	}
}

type admissionFixtureOptions struct {
	statusByPath              map[string]int
	intentOverrides           map[string]any
	chargeOverrides           map[string]any
	disputes                  []map[string]any
	disputePageHasMore        bool
	disputeAppearsAfterCharge bool
}

func newAdmissionEvidenceFixture(t *testing.T, options admissionFixtureOptions) (*StripePaymentGateway, domain.AdmissionFinancialIdentity) {
	t.Helper()
	paymentID, bookingID := uuid.New(), uuid.New()
	const paymentIntentID = "pi_admission_fixture"
	const chargeID = "ch_admission_fixture"
	var disputeAppears atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := options.statusByPath[r.URL.Path]; code != 0 {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"error":{"message":"fixture provider failure"}}`))
			return
		}
		var body any
		switch r.URL.Path {
		case "/v1/payment_intents/" + paymentIntentID:
			intent := map[string]any{
				"id": paymentIntentID, "object": "payment_intent", "status": "succeeded", "amount": 725,
				"amount_received": 725, "currency": "usd", "livemode": false, "latest_charge": chargeID,
				"metadata": map[string]string{"ticketbox_payment_id": paymentID.String(), "ticketbox_booking_id": bookingID.String(), "ticketbox_generation": "1"},
			}
			for key, value := range options.intentOverrides {
				intent[key] = value
			}
			body = intent
		case "/v1/charges":
			charge := map[string]any{
				"id": chargeID, "object": "charge", "amount": 725, "amount_captured": 725, "amount_refunded": 0,
				"currency": "usd", "livemode": false, "status": "succeeded", "paid": true, "captured": true,
				"disputed": false, "payment_intent": paymentIntentID,
			}
			for key, value := range options.chargeOverrides {
				charge[key] = value
			}
			body = map[string]any{"object": "list", "has_more": false, "data": []any{charge}}
			if options.disputeAppearsAfterCharge {
				disputeAppears.Store(true)
			}
		case "/v1/disputes":
			if options.disputeAppearsAfterCharge && disputeAppears.Load() {
				options.disputes = []map[string]any{{
					"id": "du_appeared_after_charge", "object": "dispute", "charge": chargeID,
					"payment_intent": paymentIntentID, "status": "needs_response", "livemode": false,
				}}
			}
			if options.disputes == nil {
				options.disputes = []map[string]any{}
			}
			body = map[string]any{"object": "list", "has_more": options.disputePageHasMore, "data": options.disputes}
		default:
			t.Errorf("unexpected Stripe path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encode fixture response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	provider.SetWebhookScope("acct_fixture", false)
	provider.SetAdmissionReadBackend(backend, "sk_test_fixture")
	return provider, domain.AdmissionFinancialIdentity{
		PaymentID: paymentID, BookingID: bookingID, Generation: 1, ProviderAccount: "acct_fixture",
		PaymentIntentID: paymentIntentID, AmountCents: 725, Currency: "usd",
	}
}

func TestCheckAdmissionEvidenceDeniesDisputeVisibleAtFinalAuthorityCheck(t *testing.T) {
	provider, identity := newAdmissionEvidenceFixture(t, admissionFixtureOptions{disputeAppearsAfterCharge: true})
	got := provider.CheckAdmissionEvidence(context.Background(), identity)
	if got.EligibilityState != domain.AdmissionEligibilityDenied || got.DisputeState != domain.DisputeEvidencePresent || got.ReasonCode != "provider_dispute_present" {
		t.Fatalf("dispute visible to final authority check was not denied: %+v", got)
	}
}

func TestCheckAdmissionEvidenceProviderHTTPFailuresRemainUnknown(t *testing.T) {
	paths := []string{"/v1/payment_intents/pi_admission_fixture", "/v1/charges", "/v1/disputes"}
	for _, path := range paths {
		for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("%s_%d", strings.ReplaceAll(strings.Trim(path, "/"), "/", "_"), code), func(t *testing.T) {
				provider, identity := newAdmissionEvidenceFixture(t, admissionFixtureOptions{statusByPath: map[string]int{path: code}})
				got := provider.CheckAdmissionEvidence(context.Background(), identity)
				if got.EligibilityState != domain.AdmissionEligibilityUnknown || got.DisputeState != domain.DisputeEvidenceUnknown || got.RefundState != domain.RefundEvidenceUnknown {
					t.Fatalf("HTTP %d on %s yielded decision-grade evidence: %+v", code, path, got)
				}
			})
		}
	}
}

func TestCheckAdmissionEvidenceRejectsProviderIdentityAndAmountMismatch(t *testing.T) {
	for _, test := range []struct {
		name            string
		intentOverrides map[string]any
		chargeOverrides map[string]any
	}{
		{name: "wrong PaymentIntent identity", intentOverrides: map[string]any{"id": "pi_other"}},
		{name: "wrong amount", intentOverrides: map[string]any{"amount": 726}},
		{name: "wrong amount received", intentOverrides: map[string]any{"amount_received": 724}},
		{name: "wrong currency", intentOverrides: map[string]any{"currency": "cad"}},
		{name: "live PaymentIntent", intentOverrides: map[string]any{"livemode": true}},
		{name: "wrong Payment metadata", intentOverrides: map[string]any{"metadata": map[string]string{"ticketbox_payment_id": uuid.NewString(), "ticketbox_booking_id": uuid.NewString(), "ticketbox_generation": "1"}}},
		{name: "wrong latest Charge", intentOverrides: map[string]any{"latest_charge": "ch_other"}},
		{name: "wrong Charge PaymentIntent", chargeOverrides: map[string]any{"payment_intent": "pi_other"}},
		{name: "live Charge", chargeOverrides: map[string]any{"livemode": true}},
		{name: "wrong Charge amount", chargeOverrides: map[string]any{"amount": 726}},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider, identity := newAdmissionEvidenceFixture(t, admissionFixtureOptions{intentOverrides: test.intentOverrides, chargeOverrides: test.chargeOverrides})
			got := provider.CheckAdmissionEvidence(context.Background(), identity)
			if got.EligibilityState != domain.AdmissionEligibilityUnknown || got.DisputeState != domain.DisputeEvidenceUnknown {
				t.Fatalf("mismatched Stripe evidence was not fail-closed: %+v", got)
			}
		})
	}
}

func TestCheckAdmissionEvidenceDeniesAnyDisputeAndRefundSignal(t *testing.T) {
	for _, test := range []struct {
		name            string
		chargeOverrides map[string]any
		disputeStatus   string
		wantDispute     domain.DisputeEvidenceState
		wantRefund      domain.RefundEvidenceState
	}{
		{name: "Charge disputed flag", chargeOverrides: map[string]any{"disputed": true}, wantDispute: domain.DisputeEvidencePresent, wantRefund: domain.RefundEvidenceNoRefund},
		{name: "won Dispute remains denied", disputeStatus: "won", wantDispute: domain.DisputeEvidencePresent, wantRefund: domain.RefundEvidenceNoRefund},
		{name: "closed inquiry remains denied", disputeStatus: "warning_closed", wantDispute: domain.DisputeEvidencePresent, wantRefund: domain.RefundEvidenceNoRefund},
		{name: "unknown Dispute status fails closed", disputeStatus: "future_status", wantDispute: domain.DisputeEvidencePresent, wantRefund: domain.RefundEvidenceNoRefund},
		{name: "partial refund denied", chargeOverrides: map[string]any{"amount_refunded": 1}, wantDispute: domain.DisputeEvidenceNoDispute, wantRefund: domain.RefundEvidencePresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := admissionFixtureOptions{chargeOverrides: test.chargeOverrides}
			if test.disputeStatus != "" {
				options.disputes = []map[string]any{{
					"id": "du_admission_fixture", "object": "dispute", "charge": "ch_admission_fixture",
					"payment_intent": "pi_admission_fixture", "status": test.disputeStatus, "livemode": false,
				}}
			}
			provider, identity := newAdmissionEvidenceFixture(t, options)
			got := provider.CheckAdmissionEvidence(context.Background(), identity)
			if got.EligibilityState != domain.AdmissionEligibilityDenied || got.DisputeState != test.wantDispute || got.RefundState != test.wantRefund {
				t.Fatalf("provider risk signal did not deny: got=%+v want dispute=%s refund=%s", got, test.wantDispute, test.wantRefund)
			}
			if got.ObservedAt.IsZero() || got.Source != "stripe_test_api" {
				t.Fatalf("provider denial lost its observation evidence: %+v", got)
			}
		})
	}
}
