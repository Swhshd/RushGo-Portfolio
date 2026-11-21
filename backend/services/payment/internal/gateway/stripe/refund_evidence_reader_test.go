package stripe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	stripego "github.com/stripe/stripe-go/v84"
	"go.uber.org/zap"
)

func TestCollectRefundEvidenceWalksPagesAndRetainsEveryObject(t *testing.T) {
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Stripe-Account") != "acct_fixture" {
			t.Errorf("Stripe-Account=%q", r.Header.Get("Stripe-Account"))
		}
		requests = append(requests, r.URL.RequestURI())
		switch {
		case r.URL.Path == "/v1/payment_intents/pi_1":
			writeJSON(t, w, map[string]any{
				"id": "pi_1", "object": "payment_intent", "status": "succeeded",
				"amount": 725, "amount_received": 725, "currency": "usd",
				"latest_charge": "ch_1", "livemode": false,
			})
		case r.URL.Path == "/v1/charges":
			writeJSON(t, w, map[string]any{
				"object": "list", "has_more": false, "data": []any{map[string]any{
					"id": "ch_1", "object": "charge", "status": "succeeded",
					"amount": 725, "amount_captured": 725, "amount_refunded": 725,
					"currency": "usd", "payment_intent": "pi_1",
					"paid": true, "captured": true, "livemode": false,
				}},
			})
		case r.URL.Path == "/v1/refunds" && r.URL.Query().Get("starting_after") == "":
			writeJSON(t, w, map[string]any{
				"object": "list", "has_more": true, "data": []any{
					map[string]any{"id": "re_1", "object": "refund", "status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_1"},
					map[string]any{"id": "re_2", "object": "refund", "status": "failed", "amount": 725, "currency": "usd", "payment_intent": "pi_1"},
				},
			})
		case r.URL.Path == "/v1/refunds" && r.URL.Query().Get("starting_after") == "re_2":
			writeJSON(t, w, map[string]any{
				"object": "list", "has_more": false, "data": []any{
					map[string]any{"id": "re_3", "object": "refund", "status": "pending", "amount": 100, "currency": "usd", "payment_intent": "pi_1"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	provider := newRefundEvidenceProvider(t, server)
	collection, err := provider.CollectRefundEvidence(context.Background(), "acct_fixture", "pi_1")
	if err != nil {
		t.Fatal(err)
	}
	if !collection.PaginationComplete || collection.HasMore || collection.EndReason != "HAS_MORE_FALSE" {
		t.Fatalf("pagination completion mismatch: %+v", collection)
	}
	if len(collection.Refunds) != 3 || collection.Refunds[0].ID != "re_1" ||
		collection.Refunds[1].ID != "re_2" || collection.Refunds[2].ID != "re_3" {
		t.Fatalf("per-object refunds lost: %+v", collection.Refunds)
	}
	if len(collection.RefundPages) != 2 ||
		collection.RefundPages[0].RequestCursor != "" ||
		collection.RefundPages[0].ResponseLastID != "re_2" ||
		collection.RefundPages[1].RequestCursor != "re_2" ||
		collection.RefundPages[1].ResponseLastID != "re_3" {
		t.Fatalf("page cursor trace mismatch: %+v", collection.RefundPages)
	}
	if collection.Charge.ID != "ch_1" || collection.Charge.AmountCaptured != 725 ||
		collection.Intent.LatestCharge != "ch_1" {
		t.Fatalf("charge/intent association mismatch: %+v %+v", collection.Charge, collection.Intent)
	}
	if len(requests) < 4 || !strings.Contains(strings.Join(requests, " "), "limit=100") {
		t.Fatalf("unexpected request trace: %v", requests)
	}
}

func TestCollectRefundEvidenceRejectsMissingHasMore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/payment_intents/pi_1" {
			writeJSON(t, w, paymentIntentFixture())
			return
		}
		if r.URL.Path == "/v1/charges" {
			writeJSON(t, w, chargeListFixture())
			return
		}
		writeJSON(t, w, map[string]any{"object": "list", "data": []any{}})
	}))
	defer server.Close()
	provider := newRefundEvidenceProvider(t, server)
	if _, err := provider.CollectRefundEvidence(context.Background(), "acct_fixture", "pi_1"); err == nil {
		t.Fatal("missing has_more was accepted")
	}
}

func TestCollectRefundEvidenceRejectsRepeatedCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/payment_intents/pi_1":
			writeJSON(t, w, paymentIntentFixture())
		case "/v1/charges":
			writeJSON(t, w, chargeListFixture())
		default:
			writeJSON(t, w, map[string]any{"object": "list", "has_more": true, "data": []any{
				map[string]any{"id": "re_1", "object": "refund", "status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_1"},
			}})
		}
	}))
	defer server.Close()
	provider := newRefundEvidenceProvider(t, server)
	if _, err := provider.CollectRefundEvidence(context.Background(), "acct_fixture", "pi_1"); err == nil {
		t.Fatal("non-advancing cursor was accepted")
	}
}

func TestCollectRefundEvidenceRejectsDuplicateAndExtras(t *testing.T) {
	for name, refunds := range map[string][]any{
		"duplicate": {
			map[string]any{"id": "re_1", "object": "refund", "status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_1"},
			map[string]any{"id": "re_1", "object": "refund", "status": "failed", "amount": 725, "currency": "usd", "payment_intent": "pi_1"},
		},
		"wrong-link": {
			map[string]any{"id": "re_1", "object": "refund", "status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_other"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/payment_intents/pi_1":
					writeJSON(t, w, paymentIntentFixture())
				case "/v1/charges":
					writeJSON(t, w, chargeListFixture())
				default:
					writeJSON(t, w, map[string]any{"object": "list", "has_more": false, "data": refunds})
				}
			}))
			defer server.Close()
			provider := newRefundEvidenceProvider(t, server)
			if _, err := provider.CollectRefundEvidence(context.Background(), "acct_fixture", "pi_1"); err == nil {
				t.Fatal("invalid refund list was accepted")
			}
		})
	}
}

func newRefundEvidenceProvider(t *testing.T, server *httptest.Server) *StripePaymentGateway {
	t.Helper()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(),
		MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger:     &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, nil)
	provider.SetRefundEvidenceReadBackend(backend, "sk_test_fixture")
	return provider
}

func paymentIntentFixture() map[string]any {
	return map[string]any{
		"id": "pi_1", "object": "payment_intent", "status": "succeeded",
		"amount": 725, "amount_received": 725, "currency": "usd",
		"latest_charge": "ch_1", "livemode": false,
	}
}

func chargeListFixture() map[string]any {
	return map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{
		"id": "ch_1", "object": "charge", "status": "succeeded", "amount": 725,
		"amount_captured": 725, "amount_refunded": 725, "currency": "usd",
		"payment_intent": "pi_1", "paid": true, "captured": true, "livemode": false,
	}}}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}
