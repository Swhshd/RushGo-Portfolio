package stripe

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
)

const maxRefundResponseCaptureBytes = 1 << 20

// NewRefundAuditAPIBackend returns the normal Stripe API backend with a
// per-backend, context-scoped observer. It leaves the SDK URL and retry count
// unset so stripe-go v84.4.1 supplies its defaults.
func NewRefundAuditAPIBackend() stripego.Backend {
	logger := &stripego.LeveledLogger{Level: stripego.LevelNull}
	defaultBackend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{LeveledLogger: logger})
	defaultClient := defaultBackend.(*stripego.BackendImplementation).HTTPClient
	client := *defaultClient
	next := client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	client.Transport = refundAuditRoundTripper{next: next}
	return stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		HTTPClient: &client, LeveledLogger: logger,
	})
}

type refundAuditRoundTripper struct {
	next http.RoundTripper
}

func (t refundAuditRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.Method != http.MethodPost || request.URL.EscapedPath() != "/v1/refunds" {
		return t.next.RoundTrip(request)
	}
	capture := domain.RefundResponseCaptureFromContext(request.Context())
	if capture == nil {
		return t.next.RoundTrip(request)
	}

	capture.MarkTransportObserverAttached()
	response, err := t.next.RoundTrip(request)
	observedAt := time.Now().UTC()
	observation := domain.RefundHTTPObservation{ObservedAt: &observedAt, ResultClass: "http_response"}
	if err != nil {
		observation.ResultClass = "transport_error"
	}
	if response != nil {
		observation.StatusCode = response.StatusCode
		observation.RequestID = response.Header.Get("Request-Id")
		if response.StatusCode >= http.StatusBadRequest {
			observation.ResultClass = "http_error"
		} else if response.StatusCode >= http.StatusMultipleChoices {
			observation.ResultClass = "http_redirect"
		} else {
			if captureRefundResponseBody(capture, response) {
				observation.ResultClass = "response_body_read_error"
			}
		}
	}
	capture.RecordHTTPObservation(observation)
	return response, err
}

type responseReplayBody struct {
	prefix              *bytes.Reader
	remainder           io.Reader
	terminalErr         error
	terminalErrReturned bool
	closer              io.Closer
}

func (b *responseReplayBody) Read(p []byte) (int, error) {
	if b.prefix != nil {
		if n, err := b.prefix.Read(p); n > 0 {
			return n, nil
		} else if err != nil && err != io.EOF {
			return n, err
		}
	}
	if b.terminalErr != nil {
		if !b.terminalErrReturned {
			b.terminalErrReturned = true
			return 0, b.terminalErr
		}
		return 0, io.EOF
	}
	if b.remainder == nil {
		return 0, io.EOF
	}
	return b.remainder.Read(p)
}

func (b *responseReplayBody) Close() error { return b.closer.Close() }

func captureRefundResponseBody(capture *domain.RefundResponseCapture, response *http.Response) bool {
	if capture == nil || response == nil || response.Body == nil {
		return false
	}
	original := response.Body
	body, readErr := io.ReadAll(io.LimitReader(original, maxRefundResponseCaptureBytes+1))
	replay := &responseReplayBody{prefix: bytes.NewReader(body), closer: original}
	if readErr != nil {
		replay.terminalErr = readErr
	} else {
		replay.remainder = original
	}
	response.Body = replay
	if readErr != nil || len(body) > maxRefundResponseCaptureBytes {
		return readErr != nil
	}
	var fields struct {
		ID            *string         `json:"id"`
		Status        *string         `json:"status"`
		Currency      *string         `json:"currency"`
		Amount        *int64          `json:"amount"`
		Livemode      *bool           `json:"livemode"`
		PaymentIntent json.RawMessage `json:"payment_intent"`
		Charge        json.RawMessage `json:"charge"`
	}
	if err := json.Unmarshal(body, &fields); err != nil {
		return false
	}
	observedAt := time.Now().UTC()
	refund := domain.RefundAuditResponse{
		ProviderRequestID: response.Header.Get("Request-Id"),
		PaymentIntentID:   stripeObjectID(fields.PaymentIntent),
		ChargeID:          stripeObjectID(fields.Charge),
		SummarySource:     "http_response_allowlist",
		ObservedAt:        &observedAt,
	}
	if fields.ID != nil {
		refund.RefundID = *fields.ID
	}
	if fields.Status != nil {
		refund.ProviderStatus = *fields.Status
	}
	if fields.Currency != nil {
		refund.Currency = *fields.Currency
	}
	if fields.Amount != nil {
		amount := *fields.Amount
		refund.AmountCents = &amount
	}
	if fields.Livemode != nil {
		mode := *fields.Livemode
		refund.ObservedLiveMode = &mode
	}
	capture.RecordResponse(refund)
	return false
}

func stripeObjectID(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var id string
	if err := json.Unmarshal(raw, &id); err == nil {
		return id
	}
	var expanded struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &expanded); err == nil {
		return expanded.ID
	}
	return ""
}
