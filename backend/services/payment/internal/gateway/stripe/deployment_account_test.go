package stripe

import (
	"context"
	stripego "github.com/stripe/stripe-go/v84"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestDeploymentAccountBoundBeforeFinancialActivation(t *testing.T) {
	var calls atomic.Int32
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/v1/account" {
			t.Error("unexpected fixture request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"acct_fixture","object":"account"}`))
	}))
	defer fixture.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(fixture.URL), HTTPClient: fixture.Client(), MaxNetworkRetries: stripego.Int64(0)})
	client := stripego.NewClient("sk_test_synthetic", stripego.WithBackends(&stripego.Backends{API: backend}))
	provider := NewStripePaymentGateway(zap.NewNop(), "", nil, client)
	if _, err := provider.VerifyTestAccount(context.Background(), "sk_live_synthetic", "acct_fixture"); err == nil || calls.Load() != 0 {
		t.Fatal("LIVE reached provider")
	}
	if _, err := provider.VerifyTestAccount(context.Background(), "sk_test_synthetic", "acct_wrong"); err == nil {
		t.Fatal("wrong account accepted")
	}
	if id, err := provider.VerifyTestAccount(context.Background(), "sk_test_synthetic", "acct_fixture"); err != nil || id != "acct_fixture" {
		t.Fatal("matching account failed")
	}
}
