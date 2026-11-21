package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	"go.uber.org/zap"
)

func TestCumulativeRefundRejectsConflictingAssociation(t *testing.T) {
	for _, field := range []string{"metadata", "intent", "currency", "amount", "duplicate", "list_error"} {
		t.Run(field, func(t *testing.T) {
			id, booking := uuid.New(), uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/payment_intents/pi_refund" {
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "pi_refund", "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false, "metadata": map[string]string{"ticketbox_payment_id": id.String(), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}})
					return
				}
				refund := map[string]any{"id": "re_refund", "object": "refund", "payment_intent": "pi_refund", "amount": 725, "currency": "usd", "status": "succeeded"}
				switch field {
				case "metadata":
					refund["metadata"] = map[string]string{"ticketbox_payment_id": uuid.NewString()}
				case "intent":
					refund["payment_intent"] = "pi_other"
				case "currency":
					refund["currency"] = "eur"
				case "amount":
					refund["amount"] = 800
				case "list_error":
					http.Error(w, `{"error":{"type":"api_error","message":"fixture unavailable"}}`, 500)
					return
				}
				data := []any{refund}
				if field == "duplicate" {
					data = append(data, refund)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
			}))
			defer server.Close()
			backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
			provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
			if observation, err := provider.InspectSucceededRefunds(context.Background(), "acct_fixture", false, "pi_refund"); err == nil || observation != nil {
				t.Fatal("invalid cumulative refund evidence accepted")
			}
		})
	}
}

func TestStripeFinancialMutationsUsePersistedKeysAndCancellation(t *testing.T) {
	for _, kind := range []string{"cancel", "refund"} {
		t.Run(kind, func(t *testing.T) {
			id, booking := uuid.New(), uuid.New()
			claim := domain.FinancialClaim{PaymentID: id, Kind: kind, Key: "ticketbox:payment:" + id.String() + ":g1:" + kind, Operation: domain.CreateOperation{Identity: domain.FinancialIdentity{BookingID: booking, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}, Refund: &domain.RefundObligation{PaymentID: id, Generation: 1, AmountCents: 725, Currency: "usd", Key: "ticketbox:payment:" + id.String() + ":g1:refund"}}
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Idempotency-Key") != claim.Key {
					t.Error("persisted operation key missing")
				}
				_ = r.ParseForm()
				if kind == "refund" && (r.PostForm.Get("amount") != "725" || r.PostForm.Get("payment_intent") != "pi_fixture") {
					t.Error("immutable full refund target missing")
				}
				close(entered)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
			provider := NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if kind == "cancel" {
					_, err = provider.CancelIntent(ctx, claim, "pi_fixture")
				} else {
					_, err = provider.CreateFullRefund(ctx, claim, "pi_fixture")
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("provider mutation not observed")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled mutation succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("provider ignored context")
			}
		})
	}
}

func TestCreateFullRefundCapturesResponseBeforeAdapterMapping(t *testing.T) {
	claim := refundCaptureTestClaim()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
			if got := r.Header.Get("Idempotency-Key"); got != claim.Key {
				t.Errorf("Idempotency-Key=%q; want persisted key", got)
			}
			w.Header().Set("Request-Id", "req_synthetic_refund")
			_, _ = w.Write([]byte(`{"id":"re_synthetic_refund","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"pi_synthetic_refund","charge":"ch_synthetic_refund","livemode":false}`))
			return
		}
		// Force adapter association to fail after the API returned a valid
		// refund object. The response facts must already be in this attempt's
		// capture before RetrieveIntent runs.
		http.Error(w, `{"error":{"type":"api_error"}}`, http.StatusInternalServerError)
	}))
	defer server.Close()
	provider := refundCaptureTestProvider(t, server, 0)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if observation, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err == nil || observation != nil {
		t.Fatalf("expected association mapping error, got observation=%+v err=%v", observation, err)
	}
	response := capture.Snapshot()
	if response.RefundID != "re_synthetic_refund" || response.PaymentIntentID != "pi_synthetic_refund" || response.ChargeID != "ch_synthetic_refund" || response.ProviderRequestID != "req_synthetic_refund" || response.ProviderStatus != "pending" || response.AmountCents == nil || *response.AmountCents != 725 || response.Currency != "usd" {
		t.Fatalf("pre-mapping response facts were lost: %+v", response)
	}
	if response.ObservedTransportCalls != -1 || response.TransportObservationState != "unknown" {
		t.Fatalf("unobserved transport count was reported as known: %+v", response)
	}
	if requests != 2 {
		t.Fatalf("fixture request count=%d; want one Create and one failed association read", requests)
	}
}

func TestCreateFullRefundCapturesObjectStatusWhenRequestIDIsMissing(t *testing.T) {
	for _, status := range []string{"pending", "succeeded", "failed", "canceled", "requires_action"} {
		t.Run(status, func(t *testing.T) {
			claim := refundCaptureTestClaim()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
					_, _ = w.Write([]byte(`{"id":"re_status_fixture","object":"refund","amount":725,"currency":"usd","status":"` + status + `","payment_intent":"pi_synthetic_refund","charge":"ch_synthetic_refund","livemode":false}`))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id": "pi_synthetic_refund", "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false,
					"metadata": map[string]string{
						"ticketbox_payment_id": claim.PaymentID.String(), "ticketbox_booking_id": claim.Operation.Identity.BookingID.String(), "ticketbox_generation": "1",
					},
				})
			}))
			defer server.Close()
			provider := refundCaptureTestProvider(t, server, 0)
			capture := domain.NewRefundResponseCapture(uuid.New())
			ctx := domain.WithRefundResponseCapture(context.Background(), capture)

			observation, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund")
			if err != nil || observation == nil || observation.Status != status {
				t.Fatalf("CreateFullRefund = %+v, %v; want status %q", observation, err, status)
			}
			response := capture.Snapshot()
			if response.ProviderRequestID != "" || response.RefundID != "re_status_fixture" || response.ProviderStatus != status || response.AmountCents == nil || *response.AmountCents != 725 || response.Currency != "usd" {
				t.Fatalf("captured response=%+v; want object facts with missing Request-ID left unknown", response)
			}
			if response.ObservedTransportCalls != -1 || response.ObservedLiveMode != nil {
				t.Fatalf("unobserved transport or object mode was promoted: %+v", response)
			}
		})
	}
}

func TestRefundAuditTransportCountsOnlyTheCreatePost(t *testing.T) {
	claim := refundCaptureTestClaim()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
			w.Header().Set("Request-Id", "req_create_only")
			_, _ = w.Write([]byte(`{"id":"re_create_only","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"pi_synthetic_refund","charge":"ch_create_only","livemode":false}`))
			return
		}
		writeRefundCaptureIntent(w, claim)
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithObserver(t, server, 0)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err != nil {
		t.Fatalf("CreateFullRefund returned error: %v", err)
	}
	response := capture.Snapshot()
	if requests != 2 {
		t.Fatalf("fixture request count=%d; want one Create and one RetrieveIntent", requests)
	}
	if response.ObservedTransportCalls != 1 || response.TransportObservationState != "observed" || len(response.TransportObservations) != 1 {
		t.Fatalf("transport observations=%+v; want exactly the Create POST", response)
	}
	if observation := response.TransportObservations[0]; observation.Ordinal != 1 || observation.StatusCode != http.StatusOK || observation.RequestID != "req_create_only" || observation.ResultClass != "http_response" {
		t.Fatalf("Create transport observation=%+v", observation)
	}
	if response.RefundID != "re_create_only" || response.ProviderRequestID != "req_create_only" || response.ObservedLiveMode == nil || *response.ObservedLiveMode || response.ObservedProviderAccount != nil {
		t.Fatalf("actual allowlisted response fields were not preserved: %+v", response)
	}
}

func TestRefundAuditTransportDoesNotPromoteAccountHeaders(t *testing.T) {
	capture := domain.NewRefundResponseCapture(uuid.New())
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/v1/refunds", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Stripe-Account", "acct_synthetic_request_scope")
	request = request.WithContext(domain.WithRefundResponseCapture(context.Background(), capture))
	transport := refundAuditRoundTripper{next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL == nil || !isLiteralLoopback(request.URL.Hostname()) {
			return nil, errors.New("SYNTHETIC_PROVIDER_HTTP request to non-loopback denied")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Request-Id":     []string{"req_account_fixture"},
				"Stripe-Account": []string{"acct_untrusted_response_header"},
			},
			Body: io.NopCloser(strings.NewReader(`{"id":"re_account_fixture","object":"refund","livemode":false}`)),
		}, nil
	})}
	if _, err := transport.RoundTrip(request); err != nil {
		t.Fatalf("loopback observer returned error: %v", err)
	}
	response := capture.Snapshot()
	if response.ObservedProviderAccount != nil {
		t.Fatalf("outbound or response Stripe-Account header was promoted to actual observed account: %+v", response)
	}
	if response.ObservedLiveMode == nil || *response.ObservedLiveMode || response.ProviderRequestID != "req_account_fixture" {
		t.Fatalf("response allowlist facts were not captured: %+v", response)
	}
}

func TestRefundAuditCapturePreservesSDKResponseBodyReadError(t *testing.T) {
	const payload = `{"id":"re_partial_fixture","object":"refund","livemode":false}`
	response := &http.Response{Body: &bodyThenError{payload: []byte(payload), err: io.ErrUnexpectedEOF}}
	capture := domain.NewRefundResponseCapture(uuid.New())
	captureRefundResponseBody(capture, response)

	replayed, err := io.ReadAll(response.Body)
	if string(replayed) != payload || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("response replay changed SDK body result: bytes=%q err=%v", replayed, err)
	}
	if got := capture.Snapshot(); got.RefundID != "" || got.ObservedLiveMode != nil {
		t.Fatalf("partial response was claimed as a complete allowlist result: %+v", got)
	}
}

func TestStripeSDKRetriesAfterCapturedResponseBodyReadError(t *testing.T) {
	claim := refundCaptureTestClaim()
	posts := 0
	keys := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/payment_intents/pi_synthetic_refund" {
			writeRefundCaptureIntent(w, claim)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" {
			t.Errorf("unexpected fixture request %s %s", r.Method, r.URL.Path)
			return
		}
		posts++
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		w.Header().Set("Request-Id", fmt.Sprintf("req_body_retry_%d", posts))
		if posts == 1 {
			payload := `{"id":"re_partial_body","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"pi_synthetic_refund","livemode":false}`
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)+16))
			_, _ = w.Write([]byte(payload))
			return
		}
		_, _ = w.Write([]byte(`{"id":"re_body_retry_success","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"pi_synthetic_refund","livemode":false}`))
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithDefaultRetries(t, server)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err != nil {
		t.Fatalf("CreateFullRefund did not recover through the SDK retry: %v", err)
	}
	if posts != 2 || len(keys) != posts || keys[0] != claim.Key || keys[1] != claim.Key {
		t.Fatalf("SDK retry count/keys=%d/%v; want one read-error retry with stable key", posts, keys)
	}
	response := capture.Snapshot()
	if response.ObservedTransportCalls != 2 || len(response.TransportObservations) != 2 || response.RefundID != "re_body_retry_success" {
		t.Fatalf("body-read retry was not preserved through the SDK: %+v", response)
	}
	if first, second := response.TransportObservations[0], response.TransportObservations[1]; first.ResultClass != "response_body_read_error" || first.RequestID != "req_body_retry_1" ||
		second.ResultClass != "http_response" || second.RequestID != "req_body_retry_2" {
		t.Fatalf("captured body-read retry observations=%+v", response.TransportObservations)
	}
}

type bodyThenError struct {
	payload   []byte
	err       error
	sentBody  bool
	sentError bool
}

func (b *bodyThenError) Read(p []byte) (int, error) {
	if !b.sentBody {
		b.sentBody = true
		return copy(p, b.payload), nil
	}
	if !b.sentError {
		b.sentError = true
		return 0, b.err
	}
	return 0, io.EOF
}

func (b *bodyThenError) Close() error { return nil }

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRefundAuditTransportRetriesGeneric500WithDefaultSDKAndStableKey(t *testing.T) {
	claim := refundCaptureTestClaim()
	posts := 0
	keys := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
			posts++
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			keys = append(keys, r.Header.Get("Idempotency-Key"))
			w.Header().Set("Request-Id", fmt.Sprintf("req_retry_%d", posts))
			if posts < 3 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"message":"synthetic failure"}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"re_retry_fixture","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"pi_synthetic_refund","livemode":false}`))
			return
		}
		writeRefundCaptureIntent(w, claim)
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithDefaultRetries(t, server)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err != nil {
		t.Fatalf("CreateFullRefund returned error: %v", err)
	}
	if posts != int(stripego.DefaultMaxNetworkRetries+1) {
		t.Fatalf("Stripe SDK made %d POSTs; want default %d retries plus the original request", posts, stripego.DefaultMaxNetworkRetries+1)
	}
	for _, key := range keys {
		if key != claim.Key {
			t.Fatalf("retry changed stable idempotency key: %q", key)
		}
	}
	response := capture.Snapshot()
	if response.ObservedTransportCalls != int64(posts) || len(response.TransportObservations) != posts {
		t.Fatalf("observed transport count=%d details=%d; want %d", response.ObservedTransportCalls, len(response.TransportObservations), posts)
	}
	for i, observation := range response.TransportObservations {
		if observation.Ordinal != int64(i+1) || observation.StatusCode != []int{500, 500, 200}[i] || observation.RequestID != fmt.Sprintf("req_retry_%d", i+1) {
			t.Fatalf("retry observation[%d]=%+v", i, observation)
		}
	}
}

func TestStripeSDKTypedAPI500RemainsUnknownWithoutRetry(t *testing.T) {
	claim := refundCaptureTestClaim()
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" {
			t.Errorf("unexpected fixture request %s %s", r.Method, r.URL.Path)
			return
		}
		posts++
		w.Header().Set("Request-Id", "req_typed_500")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"api_error","message":"synthetic API error"}}`))
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithDefaultRetries(t, server)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err == nil {
		t.Fatal("typed API 500 unexpectedly succeeded")
	}
	response := capture.Snapshot()
	if posts != 1 || response.Classification != domain.RefundAuditOutcomeUnknown || response.ErrorCategory != "provider_refund_unknown" ||
		response.ObservedTransportCalls != 1 || len(response.TransportObservations) != 1 ||
		response.TransportObservations[0].StatusCode != http.StatusInternalServerError || response.TransportObservations[0].RequestID != "req_typed_500" {
		t.Fatalf("typed API 500 did not remain an unknown single observed call: posts=%d response=%+v", posts, response)
	}
}

func TestRefundAuditTransportCapturesResponseDropAsUnknown(t *testing.T) {
	claim := refundCaptureTestClaim()
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" {
			t.Errorf("unexpected fixture request %s %s", r.Method, r.URL.Path)
			return
		}
		posts++
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack fixture connection: %v", err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithObserver(t, server, 0)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err == nil {
		t.Fatal("dropped provider response unexpectedly succeeded")
	}
	response := capture.Snapshot()
	if posts != 1 || response.ObservedTransportCalls != 1 || len(response.TransportObservations) != 1 {
		t.Fatalf("dropped-response capture=%+v fixture_posts=%d; want one observed round trip", response, posts)
	}
	observation := response.TransportObservations[0]
	if observation.StatusCode != 0 || observation.RequestID != "" || observation.ResultClass != "transport_error" {
		t.Fatalf("dropped response was overclaimed: %+v", observation)
	}
}

func TestRefundAuditTransportCapturesTimeoutAsUnknown(t *testing.T) {
	claim := refundCaptureTestClaim()
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" {
			t.Errorf("unexpected fixture request %s %s", r.Method, r.URL.Path)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("read fixture request body: %v", err)
		}
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	provider := refundCaptureTestProviderWithObserver(t, server, 0)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx, cancel := context.WithTimeout(domain.WithRefundResponseCapture(context.Background(), capture), 150*time.Millisecond)
	defer cancel()
	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err == nil {
		t.Fatal("timed out refund request unexpectedly succeeded")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timeout fixture did not receive the loopback request")
	}
	response := capture.Snapshot()
	if response.ObservedTransportCalls != 1 || len(response.TransportObservations) != 1 || response.TransportObservations[0].ResultClass != "transport_error" || response.TransportObservations[0].StatusCode != 0 {
		t.Fatalf("timeout was not recorded as unknown: %+v", response)
	}
}

func TestRefundAuditFixtureRejectsNonLoopbackRedirect(t *testing.T) {
	claim := refundCaptureTestClaim()
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		w.Header().Set("Request-Id", "req_redirect_fixture")
		w.Header().Set("Location", "https://api.stripe.com/v1/refunds")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	provider := refundCaptureTestProviderWithObserver(t, server, 0)
	capture := domain.NewRefundResponseCapture(uuid.New())
	ctx := domain.WithRefundResponseCapture(context.Background(), capture)

	if _, err := provider.CreateFullRefund(ctx, claim, "pi_synthetic_refund"); err == nil {
		t.Fatal("fixture followed a non-loopback redirect")
	}
	response := capture.Snapshot()
	if posts != 1 || response.ObservedTransportCalls != 1 || response.TransportObservations[0].StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("redirect fixture did not stay at loopback: posts=%d response=%+v", posts, response)
	}
}

func TestNewRefundAuditBackendKeepsLockedSDKDefaults(t *testing.T) {
	backend, ok := NewRefundAuditAPIBackend().(*stripego.BackendImplementation)
	if !ok {
		t.Fatalf("refund audit backend type=%T; want Stripe backend implementation", NewRefundAuditAPIBackend())
	}
	if backend.URL != stripego.APIURL || backend.MaxNetworkRetries != stripego.DefaultMaxNetworkRetries {
		t.Fatalf("production Stripe backend defaults changed: url=%q retries=%d", backend.URL, backend.MaxNetworkRetries)
	}
}

func TestRefundAuditCaptureDoesNotCrossConcurrentCreateResponses(t *testing.T) {
	claimA, claimB := refundCaptureTestClaim(), refundCaptureTestClaim()
	piA, piB := "pi_cross_a", "pi_cross_b"
	aEntered, bEntered, releaseA := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			intentID := r.PostForm.Get("payment_intent")
			if intentID == piA {
				close(aEntered)
				<-releaseA
			} else if intentID == piB {
				close(bEntered)
			} else {
				t.Errorf("unexpected PaymentIntent %q", intentID)
			}
			w.Header().Set("Request-Id", "req_"+intentID)
			_, _ = w.Write([]byte(`{"id":"re_` + intentID + `","object":"refund","amount":725,"currency":"usd","status":"pending","payment_intent":"` + intentID + `","livemode":false}`))
			return
		}
		intentID := strings.TrimPrefix(r.URL.Path, "/v1/payment_intents/")
		claim := claimA
		if intentID == piB {
			claim = claimB
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": intentID, "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false,
			"metadata": map[string]string{"ticketbox_payment_id": claim.PaymentID.String(), "ticketbox_booking_id": claim.Operation.Identity.BookingID.String(), "ticketbox_generation": "1"},
		})
	}))
	defer func() {
		select {
		case <-releaseA:
		default:
			close(releaseA)
		}
		server.Close()
	}()
	provider := refundCaptureTestProviderWithObserver(t, server, 0)
	captureA, captureB := domain.NewRefundResponseCapture(uuid.New()), domain.NewRefundResponseCapture(uuid.New())
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := provider.CreateFullRefund(domain.WithRefundResponseCapture(context.Background(), captureA), claimA, piA)
		doneA <- err
	}()
	go func() {
		_, err := provider.CreateFullRefund(domain.WithRefundResponseCapture(context.Background(), captureB), claimB, piB)
		doneB <- err
	}()
	for name, channel := range map[string]<-chan struct{}{"A": aEntered, "B": bEntered} {
		select {
		case <-channel:
		case <-time.After(3 * time.Second):
			t.Fatalf("Create %s did not reach loopback fixture", name)
		}
	}
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("Create B returned error before crossed response: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Create B did not finish before held Create A")
	}
	close(releaseA)
	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("Create A returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Create A did not finish after release")
	}

	responseA, responseB := captureA.Snapshot(), captureB.Snapshot()
	if responseA.RefundID != "re_"+piA || responseA.ProviderRequestID != "req_"+piA || responseA.PaymentIntentID != piA {
		t.Fatalf("Create A captured crossed facts: %+v", responseA)
	}
	if responseB.RefundID != "re_"+piB || responseB.ProviderRequestID != "req_"+piB || responseB.PaymentIntentID != piB {
		t.Fatalf("Create B captured crossed facts: %+v", responseB)
	}
}

func writeRefundCaptureIntent(w http.ResponseWriter, claim domain.FinancialClaim) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "pi_synthetic_refund", "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false,
		"metadata": map[string]string{
			"ticketbox_payment_id": claim.PaymentID.String(), "ticketbox_booking_id": claim.Operation.Identity.BookingID.String(), "ticketbox_generation": "1",
		},
	})
}

func refundCaptureTestProvider(t *testing.T, server *httptest.Server, retries int64) *StripePaymentGateway {
	return refundCaptureTestProviderWithConfig(t, server, stripego.Int64(retries), false)
}

func refundCaptureTestProviderWithObserver(t *testing.T, server *httptest.Server, retries int64) *StripePaymentGateway {
	return refundCaptureTestProviderWithConfig(t, server, stripego.Int64(retries), true)
}

func refundCaptureTestProviderWithDefaultRetries(t *testing.T, server *httptest.Server) *StripePaymentGateway {
	return refundCaptureTestProviderWithConfig(t, server, nil, true)
}

func refundCaptureTestProviderWithConfig(t *testing.T, server *httptest.Server, retries *int64, observe bool) *StripePaymentGateway {
	t.Helper()
	baseClient := server.Client()
	client := *baseClient
	baseTransport := baseClient.Transport
	var next http.RoundTripper = loopbackOnlyRoundTripper{next: baseTransport}
	if observe {
		next = refundAuditRoundTripper{next: next}
	}
	client.Transport = next
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !isLiteralLoopback(req.URL.Hostname()) {
			return errors.New("SYNTHETIC_PROVIDER_HTTP redirect to non-loopback denied")
		}
		if len(via) >= 5 {
			return errors.New("SYNTHETIC_PROVIDER_HTTP redirect bound exceeded")
		}
		return nil
	}
	backendConfig := &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: &client, LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}}
	if retries != nil {
		backendConfig.MaxNetworkRetries = retries
	}
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, backendConfig)
	if retries == nil {
		backend.(*stripego.BackendImplementation).SetNetworkRetriesSleep(false)
	}
	stripeClient := stripego.NewClient("sk_test_synthetic_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	return NewStripePaymentGateway(zap.NewNop(), "", nil, stripeClient)
}

type loopbackOnlyRoundTripper struct {
	next http.RoundTripper
}

func (t loopbackOnlyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil || !isLiteralLoopback(req.URL.Hostname()) {
		return nil, errors.New("SYNTHETIC_PROVIDER_HTTP request to non-loopback denied")
	}
	return t.next.RoundTrip(req)
}

func isLiteralLoopback(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func refundCaptureTestClaim() domain.FinancialClaim {
	paymentID, bookingID, userID, decisionID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	key := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	return domain.FinancialClaim{
		PaymentID: paymentID, Kind: "refund", Key: key,
		Operation: domain.CreateOperation{PaymentID: paymentID, Key: key, Identity: domain.FinancialIdentity{
			BookingID: bookingID, UserID: userID, Generation: 1, ProviderAccount: "acct_synthetic_fixture", AmountCents: 725, Currency: "usd",
		}},
		Refund: &domain.RefundObligation{PaymentID: paymentID, DecisionID: decisionID, Generation: 1, AmountCents: 725, Currency: "usd", Key: key},
	}
}
