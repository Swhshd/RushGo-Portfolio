package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeTransport struct{ calls int }

func (f *fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls++
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
}
func TestReadGuardRejectsWritesForeignObjectsAndRepeatedReads(t *testing.T) {
	f := &fakeTransport{}
	g := newGuard(f)
	for _, url := range []string{"https://api.stripe.com/v1/refunds", "https://api.stripe.com/v1/payment_intents/pi_other", "https://example.com/v1/account"} {
		r, _ := http.NewRequest("GET", url, nil)
		if _, err := g.RoundTrip(r); err == nil {
			t.Fatal("out of scope allowed")
		}
	}
	r, _ := http.NewRequest("POST", "https://api.stripe.com/v1/account", nil)
	if _, err := g.RoundTrip(r); err == nil {
		t.Fatal("write allowed")
	}
	if f.calls != 0 {
		t.Fatal("rejected request reached network")
	}
	r, _ = http.NewRequest("GET", "https://api.stripe.com/v1/account", nil)
	if _, err := g.RoundTrip(r); err != nil {
		t.Fatal(err)
	}
	if _, err := g.RoundTrip(r); err == nil {
		t.Fatal("repeat read allowed")
	}
	if f.calls != 1 {
		t.Fatal("request budget exceeded")
	}
}

type evidenceTransport struct {
	calls        int
	denyDisputes bool
}

func (f *evidenceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls++
	code := 200
	var data any
	switch r.URL.Path {
	case "/v1/account":
		data = map[string]any{"id": account, "object": "account"}
	case "/v1/payment_intents/" + intent:
		data = map[string]any{"id": intent, "object": "payment_intent", "status": "succeeded", "amount": 100, "amount_received": 100, "currency": "usd", "livemode": false, "latest_charge": "ch_fixture", "metadata": map[string]string{"ticketbox_payment_id": "5431a3c2-d2d0-4710-97e7-a00a9f4a182e", "ticketbox_booking_id": "003217d4-d350-46b4-81df-7988b42a04ad", "ticketbox_generation": "1"}}
	case "/v1/charges":
		data = map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{"id": "ch_fixture", "object": "charge", "payment_intent": intent, "amount": 100, "amount_captured": 100, "amount_refunded": 0, "currency": "usd", "livemode": false, "status": "succeeded", "paid": true, "captured": true, "disputed": false}}}
	case "/v1/disputes":
		data = map[string]any{"object": "list", "has_more": false, "data": []any{}}
		if f.denyDisputes {
			code = 403
			data = map[string]any{"error": map[string]string{"type": "invalid_request_error", "message": "sk_test_PRIVATE"}}
		}
	default:
		panic("unexpected endpoint")
	}
	b, _ := json.Marshal(data)
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(string(b))), Header: http.Header{"Content-Type": []string{"application/json"}}}, nil
}
func TestProbeReusesReaderAndDoesNotExposeProviderError(t *testing.T) {
	for _, deny := range []bool{false, true} {
		f := &evidenceTransport{denyDisputes: deny}
		out := probe("sk_test_fixture", f)
		if f.calls != 4 {
			t.Fatalf("requests=%d result=%v", f.calls, out)
		}
		reason := "provider_observed_clear"
		if deny {
			reason = "provider_dispute_evidence_incomplete"
		}
		if out["reason"] != reason {
			t.Fatalf("result=%v", out)
		}
		b, _ := json.Marshal(out)
		if strings.Contains(string(b), "PRIVATE") || strings.Contains(string(b), "sk_test_fixture") {
			t.Fatal("secret leaked")
		}
	}
	f := &evidenceTransport{}
	out := probe("sk_live_fixture", f)
	if f.calls != 0 || out["status"] != "TEST_KEY_REQUIRED" {
		t.Fatal("live key accepted")
	}
}
