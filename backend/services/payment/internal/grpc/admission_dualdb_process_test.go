//go:build admission_integration

package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

const (
	dualDBChildEnv   = "RUSHGO_ADMISSION_PAYMENT_PROCESS"
	dualDBConfigEnv  = "RUSHGO_ADMISSION_PAYMENT_CONFIG"
	dualDBChildValue = "1"
)

type dualDBPaymentProcessConfig struct {
	Version                int    `json:"version"`
	DatabaseURL            string `json:"database_url"`
	BookingAddress         string `json:"booking_address"`
	BookingServerName      string `json:"booking_server_name"`
	BookingClientCert      string `json:"booking_client_cert"`
	BookingClientKey       string `json:"booking_client_key"`
	PaymentServerCert      string `json:"payment_server_cert"`
	PaymentServerKey       string `json:"payment_server_key"`
	CAFile                 string `json:"ca_file"`
	PaymentID              string `json:"payment_id"`
	BookingID              string `json:"booking_id"`
	UserID                 string `json:"user_id"`
	AmountCents            int64  `json:"amount_cents"`
	PaymentIntentID        string `json:"payment_intent_id"`
	ProviderAccount        string `json:"provider_account"`
	SeedFinancial          bool   `json:"seed_financial"`
	DropPrepareReplyOnce   bool   `json:"drop_prepare_reply_once"`
	DropReconcileReplyOnce bool   `json:"drop_reconcile_reply_once"`
}

type dualDBPaymentReady struct {
	GRPCAddress    string `json:"grpc_address"`
	ControlAddress string `json:"control_address"`
}

type dualDBPaymentControlResponse struct {
	OK            bool   `json:"ok"`
	Processed     int    `json:"processed,omitempty"`
	ProviderCalls int64  `json:"provider_calls,omitempty"`
	RefundCalls   int64  `json:"refund_calls,omitempty"`
	Error         string `json:"error,omitempty"`
}

// TestAdmissionPaymentProcess is the independently compiled helper process
// launched by Booking's admission_integration tests. It is intentionally
// inert when invoked by ordinary package test runs.
func TestAdmissionPaymentProcess(t *testing.T) {
	if os.Getenv(dualDBChildEnv) != dualDBChildValue {
		t.Skip("Payment admission integration process is started only by the dual-PostgreSQL harness")
	}
	if os.Getenv(admissiontest.OptInEnv) != admissiontest.OptInValue {
		t.Fatal("BLOCKED: child process is missing the dual-PostgreSQL test-only opt-in")
	}
	configPath := os.Getenv(dualDBConfigEnv)
	if configPath == "" {
		t.Fatal("BLOCKED: Payment process config is required")
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("read Payment process config")
	}
	var config dualDBPaymentProcessConfig
	if err := json.Unmarshal(contents, &config); err != nil || config.Version != 1 {
		t.Fatal("BLOCKED: Payment process config version is invalid")
	}
	if config.DatabaseURL == "" || config.BookingAddress == "" || config.BookingServerName == "" || config.AmountCents < 1 ||
		config.PaymentIntentID == "" || config.ProviderAccount == "" {
		t.Fatal("BLOCKED: Payment process config is incomplete")
	}
	paymentID, err := uuid.Parse(config.PaymentID)
	if err != nil || paymentID == uuid.Nil {
		t.Fatal("BLOCKED: Payment fixture identity is invalid")
	}
	bookingID, err := uuid.Parse(config.BookingID)
	if err != nil || bookingID == uuid.Nil {
		t.Fatal("BLOCKED: Booking fixture identity is invalid")
	}
	userID, err := uuid.Parse(config.UserID)
	if err != nil || userID == uuid.Nil {
		t.Fatal("BLOCKED: user fixture identity is invalid")
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		startupCancel()
		t.Fatal("parse isolated Payment database config")
	}
	poolConfig.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(startupCtx, poolConfig)
	if err != nil {
		startupCancel()
		t.Fatal("connect isolated Payment PostgreSQL")
	}
	if err := pool.Ping(startupCtx); err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("ping isolated Payment PostgreSQL")
	}
	repo := repository.NewPostgresPaymentRepository(pool)
	provider := &dualDBAdmissionProvider{
		paymentID: paymentID, bookingID: bookingID, intentID: config.PaymentIntentID,
		account: config.ProviderAccount, amountCents: config.AmountCents,
	}
	paymentService := service.NewPaymentService(repo, zap.NewNop(), provider)
	paymentService.SetFinancialAuthority(nil, config.ProviderAccount, false)
	if config.SeedFinancial {
		if err := seedDualDBFinancial(startupCtx, pool, repo, paymentID, bookingID, userID, config); err != nil {
			pool.Close()
			startupCancel()
			t.Fatal("seed isolated Payment financial row")
		}
	}

	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral test CA")
	}
	paymentServerCert, err := os.ReadFile(config.PaymentServerCert)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Payment server certificate")
	}
	paymentServerKey, err := os.ReadFile(config.PaymentServerKey)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Payment server key")
	}
	paymentServerCreds, err := grpcsecurity.NewServerCredentials(paymentServerCert, paymentServerKey, caPEM, []string{grpcsecurity.ServiceIdentityBooking})
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("create Payment mTLS server credentials")
	}
	bookingClientCert, err := os.ReadFile(config.BookingClientCert)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Booking client certificate")
	}
	bookingClientKey, err := os.ReadFile(config.BookingClientKey)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Booking client key")
	}
	bookingClientCreds, err := grpcsecurity.NewClientCredentials(bookingClientCert, bookingClientKey, caPEM, config.BookingServerName, grpcsecurity.ServiceIdentityPayment)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("create Payment-to-Booking mTLS credentials")
	}
	bookingConn, err := grpc.NewClient(config.BookingAddress, grpc.WithTransportCredentials(bookingClientCreds))
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("create Payment-to-Booking gRPC client")
	}
	bookingConn.Connect()
	if err := waitDualDBGRPCReady(startupCtx, bookingConn); err != nil {
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		t.Fatal("connect Payment to Booking admission authority")
	}
	// The deadline bounds initialization only. Admission RPCs and the serving
	// process use their caller-owned contexts and can outlive this setup window.
	startupCancel()

	coordinator := service.NewAdmissionCoordinator(repo, paymentService, admission.NewOutcomeClient(bookingConn))
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		t.Fatal("listen for isolated Payment admission RPC")
	}
	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = grpcListener.Close()
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		t.Fatal("listen for loopback-only test control")
	}
	var dropPrepare atomic.Bool
	var dropReconcile atomic.Bool
	dropPrepare.Store(config.DropPrepareReplyOnce)
	dropReconcile.Store(config.DropReconcileReplyOnce)
	server := grpc.NewServer(
		grpc.Creds(paymentServerCreds),
		grpc.UnaryInterceptor(dualDBDropAdmissionReplyOnce(&dropPrepare, &dropReconcile)),
	)
	admissionv1.RegisterPaymentAdmissionCoordinatorServer(server, NewAdmissionServer(coordinator))
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(grpcListener) }()

	controlMux := http.NewServeMux()
	controlMux.HandleFunc("POST /v1/activate", func(w http.ResponseWriter, r *http.Request) {
		if err := repo.ActivatePaymentAdmission(r.Context()); err != nil {
			writeDualDBControl(w, dualDBPaymentControlResponse{Error: "activate Payment admission failed"}, http.StatusConflict)
			return
		}
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true}, http.StatusOK)
	})
	controlMux.HandleFunc("POST /v1/recover-once", func(w http.ResponseWriter, r *http.Request) {
		processed, err := coordinator.RunAdmissionRecoveryOnce(r.Context())
		if err != nil {
			writeDualDBControl(w, dualDBPaymentControlResponse{Processed: processed, ProviderCalls: provider.calls.Load(), RefundCalls: provider.refundCalls.Load(), Error: "Payment admission recovery returned an error"}, http.StatusConflict)
			return
		}
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true, Processed: processed, ProviderCalls: provider.calls.Load(), RefundCalls: provider.refundCalls.Load()}, http.StatusOK)
	})
	controlMux.HandleFunc("POST /v1/require-refund", func(w http.ResponseWriter, r *http.Request) {
		decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:expire"))
		_, err := repo.RequireRefund(r.Context(), domain.RefundObligation{
			PaymentID: paymentID, DecisionID: decisionID, Generation: 1,
			AmountCents: config.AmountCents, Currency: "usd", Reason: "EXPIRED",
			Key: "ticketbox:payment:" + paymentID.String() + ":g1:refund",
		})
		if err != nil {
			writeDualDBControl(w, dualDBPaymentControlResponse{Error: "create isolated refund obligation failed"}, http.StatusConflict)
			return
		}
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true}, http.StatusOK)
	})
	controlMux.HandleFunc("POST /v1/refund-once", func(w http.ResponseWriter, r *http.Request) {
		processed, err := service.NewFinancialWorker(paymentService).RunOnce(r.Context(), 1)
		if err != nil {
			writeDualDBControl(w, dualDBPaymentControlResponse{Processed: processed, ProviderCalls: provider.calls.Load(), RefundCalls: provider.refundCalls.Load(), Error: "local refund worker returned an error"}, http.StatusConflict)
			return
		}
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true, Processed: processed, ProviderCalls: provider.calls.Load(), RefundCalls: provider.refundCalls.Load()}, http.StatusOK)
	})
	controlMux.HandleFunc("GET /v1/provider-count", func(w http.ResponseWriter, _ *http.Request) {
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true, ProviderCalls: provider.calls.Load(), RefundCalls: provider.refundCalls.Load()}, http.StatusOK)
	})
	stopOnce := make(chan struct{})
	controlMux.HandleFunc("POST /v1/shutdown", func(w http.ResponseWriter, _ *http.Request) {
		writeDualDBControl(w, dualDBPaymentControlResponse{OK: true}, http.StatusAccepted)
		go func() {
			select {
			case <-stopOnce:
			default:
				close(stopOnce)
				startupCancel()
				server.GracefulStop()
			}
		}()
	})
	httpServer := &http.Server{Handler: controlMux, ReadHeaderTimeout: 2 * time.Second}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpServer.Serve(controlListener) }()
	ready := dualDBPaymentReady{GRPCAddress: grpcListener.Addr().String(), ControlAddress: controlListener.Addr().String()}
	encodedReady, err := json.Marshal(ready)
	if err != nil {
		server.Stop()
		_ = httpServer.Close()
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		t.Fatal("encode Payment process readiness")
	}
	if _, err := fmt.Fprintf(os.Stdout, "ADMISSION_PAYMENT_READY %s\n", encodedReady); err != nil {
		server.Stop()
		_ = httpServer.Close()
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		t.Fatal("publish Payment process readiness")
	}
	select {
	case <-stopOnce:
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
		_ = httpListenerClose(controlListener)
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
	case err := <-serveErr:
		_ = httpServer.Close()
		_ = httpListenerClose(controlListener)
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Fatalf("Payment gRPC test process stopped: %v", err)
		}
	case err := <-httpErr:
		server.Stop()
		_ = bookingConn.Close()
		pool.Close()
		startupCancel()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Payment test control stopped: %v", err)
		}
	}
}

type dualDBAdmissionProvider struct {
	calls       atomic.Int64
	refundCalls atomic.Int64
	refunded    atomic.Bool
	paymentID   uuid.UUID
	bookingID   uuid.UUID
	intentID    string
	account     string
	amountCents int64
}

func (p *dualDBAdmissionProvider) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, errors.New("local admission provider does not create PaymentIntents")
}
func (p *dualDBAdmissionProvider) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, errors.New("local admission provider does not handle webhooks")
}
func (p *dualDBAdmissionProvider) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return errors.New("local admission provider does not send refunds")
}
func (p *dualDBAdmissionProvider) CreateIntent(context.Context, domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("local admission provider does not create PaymentIntents")
}
func (p *dualDBAdmissionProvider) RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("local admission provider does not retrieve PaymentIntents")
}
func (p *dualDBAdmissionProvider) FindIntent(context.Context, domain.CreateOperation) ([]domain.ProviderObservation, error) {
	return nil, errors.New("local admission provider does not search PaymentIntents")
}
func (p *dualDBAdmissionProvider) CancelIntent(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("local admission provider does not cancel PaymentIntents")
}
func (p *dualDBAdmissionProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, paymentIntentID string) (*domain.ProviderObservation, error) {
	p.refundCalls.Add(1)
	p.refunded.Store(true)
	now := time.Now().UTC()
	refundID := "re_admission_integration_local"
	if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
		live := false
		amount := p.amountCents
		capture.RecordResponse(domain.RefundAuditResponse{
			Classification:    domain.RefundAuditResponseObserved,
			ProviderRequestID: "req_admission_integration_local", RefundID: refundID,
			PaymentIntentID: paymentIntentID, ProviderStatus: "succeeded", Currency: "usd",
			AmountCents: &amount, ObservedProviderAccount: &p.account, ObservedLiveMode: &live,
			SummarySource: "transport_allowlist", ObservedAt: &now,
		})
	}
	return &domain.ProviderObservation{
		Account: p.account, IntentID: paymentIntentID, RefundID: refundID, Status: "succeeded",
		PaymentID: claim.PaymentID, BookingID: claim.Operation.Identity.BookingID, Generation: claim.Operation.Identity.Generation,
		AmountCents: claim.Operation.Identity.AmountCents, SucceededRefundCents: claim.Refund.AmountCents,
		Currency: claim.Operation.Identity.Currency, Collected: true, ObservedAt: now,
	}, nil
}
func (p *dualDBAdmissionProvider) RetrieveRefund(context.Context, string, bool, string) (*domain.ProviderObservation, error) {
	if !p.refunded.Load() {
		return nil, errors.New("no local refund response has been created")
	}
	return p.refundObservation("succeeded"), nil
}
func (p *dualDBAdmissionProvider) InspectSucceededRefunds(_ context.Context, account string, live bool, paymentIntentID string) (*domain.ProviderObservation, error) {
	if account != p.account || live || paymentIntentID != p.intentID {
		return nil, errors.New("local provider identity mismatch")
	}
	if p.refunded.Load() {
		return p.refundObservation("succeeded"), nil
	}
	observation := p.refundObservation("no_refunds")
	observation.SucceededRefundCents = 0
	return observation, nil
}
func (p *dualDBAdmissionProvider) VerifyWebhook(context.Context, []byte, string) (*domain.ProviderReceipt, error) {
	return nil, errors.New("local admission provider does not verify webhooks")
}

func (p *dualDBAdmissionProvider) refundObservation(status string) *domain.ProviderObservation {
	now := time.Now().UTC()
	return &domain.ProviderObservation{
		Account: p.account, LiveMode: false, EventID: "evt_admission_integration_local",
		IntentID: p.intentID, RefundID: "re_admission_integration_local", Status: status,
		PaymentID: p.paymentID, BookingID: p.bookingID, Generation: 1,
		AmountCents: p.amountCents, SucceededRefundCents: p.amountCents,
		Currency: "usd", Collected: true, ObservedAt: now,
	}
}
func (p *dualDBAdmissionProvider) CheckAdmissionEvidence(_ context.Context, identity domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	p.calls.Add(1)
	if identity.PaymentID != p.paymentID || identity.BookingID != p.bookingID || identity.Generation != 1 || identity.LiveMode ||
		identity.ProviderAccount != p.account || identity.PaymentIntentID != p.intentID || identity.AmountCents != p.amountCents || identity.Currency != "usd" {
		return domain.AdmissionProviderEvidence{EligibilityState: domain.AdmissionEligibilityUnknown, DisputeState: domain.DisputeEvidenceUnknown, RefundState: domain.RefundEvidenceUnknown, ReasonCode: "invalid_local_fixture_identity"}
	}
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(),
		Source:           "stripe_test_api",
		ReasonCode:       "fixture_observed_clear",
	}
}

func seedDualDBFinancial(ctx context.Context, pool *pgxpool.Pool, repo *repository.PostgresPaymentRepository, paymentID, bookingID, userID uuid.UUID, config dualDBPaymentProcessConfig) error {
	frozen := []byte(fmt.Sprintf(`{"version":1,"amount":%d,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`, config.AmountCents))
	identity := domain.FinancialIdentity{
		BookingID: bookingID, UserID: userID, Generation: 1, ProviderAccount: config.ProviderAccount,
		LiveMode: false, AmountCents: config.AmountCents, Currency: "usd",
	}
	fingerprint, err := domain.FinancialFingerprint(identity, frozen)
	if err != nil {
		return err
	}
	identity.Fingerprint = fingerprint
	operation := domain.CreateOperation{
		Identity: identity, PaymentID: paymentID,
		Key: "ticketbox:payment:" + paymentID.String() + ":g1:create", FrozenParams: frozen,
	}
	if _, _, err := repo.CreateOrGet(ctx, operation); err != nil {
		return err
	}
	observedAt := time.Now().UTC()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: config.ProviderAccount, LiveMode: false, EventID: uuid.NewString(), IntentID: config.PaymentIntentID,
		Status: "succeeded", PaymentID: paymentID, BookingID: bookingID, Generation: 1,
		AmountCents: config.AmountCents, Currency: "usd", Collected: true, ObservedAt: observedAt,
	}); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified' WHERE id=$1`, paymentID)
	return err
}

func waitDualDBGRPCReady(ctx context.Context, conn *grpc.ClientConn) error {
	for conn.GetState() != connectivity.Ready {
		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
	return nil
}

func dualDBDropAdmissionReplyOnce(dropPrepare, dropReconcile *atomic.Bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		if err != nil {
			return response, err
		}
		switch info.FullMethod {
		case admissionv1.PaymentAdmissionCoordinator_PrepareAdmission_FullMethodName:
			if dropPrepare.CompareAndSwap(true, false) {
				return nil, status.Error(codes.Unavailable, "test-only: committed PrepareAdmission reply was lost")
			}
		case admissionv1.PaymentAdmissionCoordinator_ReconcileAdmission_FullMethodName:
			if dropReconcile.CompareAndSwap(true, false) {
				return nil, status.Error(codes.Unavailable, "test-only: committed ReconcileAdmission reply was lost")
			}
		}
		return response, nil
	}
}

func writeDualDBControl(w http.ResponseWriter, response dualDBPaymentControlResponse, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(response)
}

func httpListenerClose(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	return listener.Close()
}
