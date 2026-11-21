package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	stripego "github.com/stripe/stripe-go/v84"
	gateway "github.com/ticketbox/payment/internal/gateway/stripe"
	paymentgrpc "github.com/ticketbox/payment/internal/grpc"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/originalissuance"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	originalissuance.Main(func(ctx context.Context) error {
		pool, err := originalissuance.Open(ctx, "payment", true)
		if err != nil {
			return err
		}
		defer pool.Close()
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 512), 512)
		if !scanner.Scan() {
			return errors.New("test_key_required")
		}
		key := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(key, "sk_test_") && !strings.HasPrefix(key, "rk_test_") {
			return errors.New("test_key_required")
		}
		guard := newGuard(http.DefaultTransport)
		defer func() {
			guard.mu.Lock()
			defer guard.mu.Unlock()
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "PROVIDER_READ_SUMMARY", "requestsSent": len(guard.seen), "requestsBlocked": guard.blocked, "httpStatuses": guard.results, "financialWrites": 0})
		}()
		client := &http.Client{Transport: guard, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect_refused") }}
		backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{HTTPClient: client, MaxNetworkRetries: stripego.Int64(0), EnableTelemetry: stripego.Bool(false), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
		sdk := stripego.NewClient(key, stripego.WithBackends(&stripego.Backends{API: backend}))
		reader := gateway.NewStripePaymentGateway(zap.NewNop(), "", nil, sdk)
		verified, err := reader.VerifyTestAccount(ctx, key)
		if err != nil || verified != originalissuance.Account {
			return errors.New("test_account_mismatch")
		}
		reader.SetWebhookScope(verified, false)
		reader.SetAdmissionReadBackend(backend, key)
		svc := service.NewPaymentService(repository.NewPostgresPaymentRepository(pool), zap.NewNop(), reader)
		svc.SetFinancialAuthority(nil, verified, false)
		return originalissuance.Serve(ctx, "payment", paymentv1.PaymentService_ServiceDesc, paymentgrpc.NewPaymentServer(svc, zap.NewNop()), paymentv1.PaymentService_CheckAdmissionFinancialEligibility_FullMethodName)
	})
}
