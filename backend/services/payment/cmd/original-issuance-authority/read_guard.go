// One-shot read-only probe of original TEST payment using the original evidence reader.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	gateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const intent = "pi_3UXwxSD3fWINmaYl2s0bueSl"
const account = "acct_1UN55BD3fWINmaYl"

type readGuard struct {
	next    http.RoundTripper
	mu      sync.Mutex
	seen    map[string]bool
	results map[string]int
	blocked int
}

func newGuard(next http.RoundTripper) *readGuard {
	return &readGuard{next: next, seen: map[string]bool{}, results: map[string]int{}}
}
func (g *readGuard) RoundTrip(r *http.Request) (*http.Response, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	allowed := r.Method == "GET" && r.URL.Scheme == "https" && r.URL.Host == "api.stripe.com" && r.URL.User == nil
	q := r.URL.Query()
	name := ""
	switch r.URL.Path {
	case "/v1/account":
		name = "account"
		allowed = allowed && len(q) == 0
	case "/v1/payment_intents/" + intent:
		name = "payment_intent"
		allowed = allowed && len(q) == 0
	case "/v1/charges", "/v1/disputes":
		name = strings.TrimPrefix(r.URL.Path, "/v1/")
		allowed = allowed && len(q) == 2 && len(q["payment_intent"]) == 1 && q.Get("payment_intent") == intent && len(q["limit"]) == 1 && q.Get("limit") == "100"
	default:
		allowed = false
	}
	if !allowed || g.seen[name] || len(g.seen) >= 4 {
		g.blocked++
		return nil, errors.New("read_scope_or_budget_refused")
	}
	g.seen[name] = true
	response, err := g.next.RoundTrip(r)
	if err != nil {
		g.results[name] = 0
		return nil, errors.New("read_transport_failed")
	}
	g.results[name] = response.StatusCode
	return response, nil
}
func probe(key string, next http.RoundTripper) map[string]any {
	out := map[string]any{"runId": "4b7a98d6-35d5-4fa3-ad46-8ed131621a47", "mode": "original_TEST_payment_read_only", "financialWrites": 0, "containersStarted": 0}
	if !(strings.HasPrefix(key, "sk_test_") || strings.HasPrefix(key, "rk_test_")) {
		out["status"] = "TEST_KEY_REQUIRED"
		return out
	}
	g := newGuard(next)
	defer func() {
		out["httpStatuses"] = g.results
		out["requestsSent"] = len(g.seen)
		out["requestsBlocked"] = g.blocked
	}()
	client := &http.Client{Transport: g, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect_refused") }}
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{HTTPClient: client, MaxNetworkRetries: stripego.Int64(0), EnableTelemetry: stripego.Bool(false), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	sdk := stripego.NewClient(key, stripego.WithBackends(&stripego.Backends{API: backend}))
	reader := gateway.NewStripePaymentGateway(zap.NewNop(), "", nil, sdk)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	verified, err := reader.VerifyTestAccount(ctx, key)
	if err != nil || verified != account {
		out["status"] = "ACCOUNT_CHECK_FAILED"
		return out
	}
	reader.SetWebhookScope(account, false)
	reader.SetAdmissionReadBackend(backend, key)
	evidence := reader.CheckAdmissionEvidence(ctx, domain.AdmissionFinancialIdentity{PaymentID: uuid.MustParse("5431a3c2-d2d0-4710-97e7-a00a9f4a182e"), BookingID: uuid.MustParse("003217d4-d350-46b4-81df-7988b42a04ad"), Generation: 1, ProviderAccount: account, LiveMode: false, PaymentIntentID: intent, AmountCents: 100, Currency: "usd"})
	out["status"] = "READ_COMPLETED"
	out["eligibility"] = evidence.EligibilityState
	out["reason"] = evidence.ReasonCode
	out["disputeState"] = evidence.DisputeState
	out["refundState"] = evidence.RefundState
	out["limitation"] = "Current provider evidence only; does not prove the original failure cause. Pagination and repeated requests are refused."
	return out
}
func providerOnlyProbeUnused() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 256), 512)
	if !scanner.Scan() {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "KEY_INPUT_REQUIRED"})
		os.Exit(2)
	}
	key := strings.TrimSpace(scanner.Text())
	_ = json.NewEncoder(os.Stdout).Encode(probe(key, http.DefaultTransport))
}
