package stripe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	"go.uber.org/zap"
)

func TestStripeCreateUsesFrozenKeyAndContext(t *testing.T) {
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"},"receipt_email":"original@example.test"}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}, FrozenParams: frozen}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, op.FrozenParams)
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		for key, want := range map[string]string{"amount": "725", "currency": "usd", "receipt_email": "original@example.test", "automatic_payment_methods[enabled]": "true", "automatic_payment_methods[allow_redirects]": "always", "metadata[ticketbox_payment_id]": id.String(), "metadata[ticketbox_booking_id]": booking.String(), "metadata[ticketbox_generation]": "1"} {
			if r.PostForm.Get(key) != want {
				t.Errorf("frozen field %s mismatch", key)
			}
		}
		if r.Header.Get("Idempotency-Key") != op.Key {
			t.Error("stable key missing")
		}
		close(entered)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	client := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := provider.CreateIntent(ctx, op); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("SDK request not observed")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled provider call succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("SDK ignored context cancellation")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not reach HTTP boundary")
	}
}
