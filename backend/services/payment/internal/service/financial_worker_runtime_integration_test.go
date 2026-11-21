package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/paymentruntime"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/admission"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
)

func TestFinancialWorkerRuntimeAuthorityWiringUsesHTTPProvider(t *testing.T) {
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	op := domain.CreateOperation{
		PaymentID: id,
		Key:       "ticketbox:payment:" + id.String() + ":g1:create",
		Identity: domain.FinancialIdentity{
			BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd",
		},
	}
	var requestMu sync.Mutex
	intentReads, refundListReads := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.Contains(req.URL.Path, "payment_intents"):
			requestMu.Lock()
			intentReads++
			requestMu.Unlock()
			_ = json.NewEncoder(w).Encode(runtimeIntentJSON(op, "succeeded"))
		case req.URL.Path == "/v1/refunds":
			requestMu.Lock()
			refundListReads++
			requestMu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []any{}, "has_more": false})
		default:
			t.Errorf("unexpected local provider request: %s %s", req.Method, req.URL.Path)
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	provider := runtimeStripeGateway(server)
	service := newFinancialWorkerRuntimeService(nil, op, provider)
	worker := NewFinancialWorker(service)
	configuredProvider, providerOK := worker.provider.(*stripegateway.StripePaymentGateway)
	if !providerOK || configuredProvider != provider || worker.booking == nil {
		t.Fatalf("worker authority wiring incomplete: provider=%v booking=%v", worker.provider != nil, worker.booking != nil)
	}
	bookingReader, ok := worker.booking.(*resolutionBookingReader)
	if !ok || bookingReader.view == nil || bookingReader.view.Booking == nil ||
		bookingReader.view.Booking.Id != booking.String() || bookingReader.view.Booking.UserId != user.String() ||
		bookingReader.view.Booking.TotalAmountCents != op.Identity.AmountCents || bookingReader.view.Generation != op.Identity.Generation {
		t.Fatal("worker Booking authority does not match its Payment identity")
	}

	observation, err := worker.provider.InspectSucceededRefunds(context.Background(), op.Identity.ProviderAccount, false, runtimeIntentID(op))
	if err != nil || observation == nil || !observation.Collected || observation.Status != "no_refunds" {
		t.Fatalf("configured FinancialProvider InspectSucceededRefunds = %+v, %v", observation, err)
	}
	requestMu.Lock()
	defer requestMu.Unlock()
	if intentReads != 1 || refundListReads != 1 {
		t.Fatalf("local provider requests: intent=%d refunds=%d; want one each", intentReads, refundListReads)
	}
}

func TestFinancialWorkerStartPollsDurableRefundToPreparedIntegration(t *testing.T) {
	requireFinancialWorkerRuntimeDatabase(t)
	pool, repo, op, worker, server := financialWorkerRuntimeFixture(t)
	if _, err := repo.RequireRefund(context.Background(), financialWorkerRuntimeRefund(op)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	posts := 0
	keys := []string{}
	accepted := false
	evidence := newWorkerRuntimeEvidence()
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		evidence.observeClaim(pool, op.PaymentID)
		if strings.Contains(req.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(runtimeIntentJSON(op, "succeeded"))
			return
		}
		if req.Method == http.MethodPost {
			if err := req.ParseForm(); err != nil {
				t.Error(err)
			}
			evidence.recordCreate(req, op)
			mu.Lock()
			posts++
			keys = append(keys, req.Header.Get("Idempotency-Key"))
			accepted = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(runtimeRefundJSON(op, "re_runtime_success", "succeeded"))
			return
		}
		mu.Lock()
		wasAccepted := accepted
		mu.Unlock()
		data := []any{}
		if wasAccepted {
			data = append(data, runtimeRefundJSON(op, "re_runtime_success", "succeeded"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
	})

	group := paymentruntime.Start(context.Background(), worker.Start)
	registerFinancialWorkerGroupCleanup(t, group)
	waitForRuntimeRefundState(t, pool, op.PaymentID, "succeeded", 8*time.Second)
	assertRuntimePreparedRefund(t, pool, op.PaymentID, 1)
	stopFinancialWorkerGroup(t, group)
	evidence.log(t, "TestFinancialWorkerStartPollsDurableRefundToPreparedIntegration", pool, op.PaymentID)

	mu.Lock()
	defer mu.Unlock()
	if posts != 1 || len(keys) != 1 || keys[0] != "ticketbox:payment:"+op.PaymentID.String()+":g1:refund" {
		t.Fatalf("Start loop provider refund calls=%d keys=%v", posts, keys)
	}
}

func TestFinancialWorkerStartWaitsForPendingAdmissionThenRefundsIntegration(t *testing.T) {
	requireFinancialWorkerRuntimeDatabase(t)
	pool, repo, op, worker, server := financialWorkerRuntimeFixture(t)
	ctx := context.Background()
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	identity := runtimeAdmissionIdentity(op)
	permit, err := repo.GrantAdmission(ctx, identity, domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(),
		Source:           "stripe_test_api",
	})
	if err != nil || permit.State != admission.Pending {
		t.Fatalf("create active pending admission permit: %+v %v", permit, err)
	}
	if _, err := repo.RequireRefund(ctx, financialWorkerRuntimeRefund(op)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	posts := 0
	accepted := false
	evidence := newWorkerRuntimeEvidence()
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		evidence.observeClaim(pool, op.PaymentID)
		if strings.Contains(req.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(runtimeIntentJSON(op, "succeeded"))
			return
		}
		if req.Method == http.MethodPost {
			if err := req.ParseForm(); err != nil {
				t.Error(err)
			}
			evidence.recordCreate(req, op)
			mu.Lock()
			posts++
			accepted = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(runtimeRefundJSON(op, "re_runtime_admission", "succeeded"))
			return
		}
		mu.Lock()
		wasAccepted := accepted
		mu.Unlock()
		data := []any{}
		if wasAccepted {
			data = append(data, runtimeRefundJSON(op, "re_runtime_admission", "succeeded"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
	})

	group := paymentruntime.Start(ctx, worker.Start)
	registerFinancialWorkerGroupCleanup(t, group)
	time.Sleep(1250 * time.Millisecond)
	select {
	case <-group.Done():
		t.Fatal("FinancialWorker.Start exited while the pending admission was active")
	default:
	}
	var blockedState string
	var blockedFence int64
	var blockedToken bool
	if err := pool.QueryRow(ctx, `SELECT refund_state,COALESCE(claim_fence,0),claim_token IS NOT NULL FROM payments WHERE id=$1`, op.PaymentID).
		Scan(&blockedState, &blockedFence, &blockedToken); err != nil {
		t.Fatal(err)
	}
	if blockedState != "pending" || blockedFence != 0 || blockedToken {
		t.Fatalf("pending admission did not hold preclaim refund work: state=%q fence=%d token=%v", blockedState, blockedFence, blockedToken)
	}
	assertRuntimePreparedRefund(t, pool, op.PaymentID, 0)
	mu.Lock()
	if posts != 0 {
		mu.Unlock()
		t.Fatalf("pending admission allowed provider POST: %d", posts)
	}
	mu.Unlock()

	settled, err := repo.SettleAdmission(ctx, identity, permit.Token, permit.Fence, admission.Aborted)
	if err != nil || settled.State != admission.Aborted {
		t.Fatalf("settle pending admission: %+v %v", settled, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	waitForRuntimeRefundState(t, pool, op.PaymentID, "succeeded", 8*time.Second)
	assertRuntimePreparedRefund(t, pool, op.PaymentID, 1)
	stopFinancialWorkerGroup(t, group)
	evidence.log(t, "TestFinancialWorkerStartWaitsForPendingAdmissionThenRefundsIntegration", pool, op.PaymentID)
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatalf("provider refund calls after admission settled=%d; want 1", posts)
	}
}

func TestFinancialWorkerStartRestartRecoversAcceptedRefundWithoutDuplicateIntegration(t *testing.T) {
	requireFinancialWorkerRuntimeDatabase(t)
	pool, repo, op, worker, server := financialWorkerRuntimeFixture(t)
	if _, err := repo.RequireRefund(context.Background(), financialWorkerRuntimeRefund(op)); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	var mu sync.Mutex
	posts := 0
	keys := []string{}
	accepted := false
	evidence := newWorkerRuntimeEvidence()
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		evidence.observeClaim(pool, op.PaymentID)
		if strings.Contains(req.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(runtimeIntentJSON(op, "succeeded"))
			return
		}
		if req.Method == http.MethodPost {
			if err := req.ParseForm(); err != nil {
				t.Error(err)
			}
			evidence.recordCreate(req, op)
			mu.Lock()
			posts++
			keys = append(keys, req.Header.Get("Idempotency-Key"))
			accepted = true
			mu.Unlock()
			close(entered)
			<-req.Context().Done()
			return
		}
		mu.Lock()
		wasAccepted := accepted
		mu.Unlock()
		data := []any{}
		if wasAccepted {
			data = append(data, runtimeRefundJSON(op, "re_runtime_recovered", "succeeded"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
	})

	first := paymentruntime.Start(context.Background(), worker.Start)
	registerFinancialWorkerGroupCleanup(t, first)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		stopFinancialWorkerGroup(t, first)
		t.Fatal("FinancialWorker.Start did not reach the provider mutation")
	}
	assertRuntimePreparedRefund(t, pool, op.PaymentID, 1)
	stopFinancialWorkerGroup(t, first)

	var state string
	if err := pool.QueryRow(context.Background(), `SELECT refund_state FROM payments WHERE id=$1`, op.PaymentID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "retryable" {
		t.Fatalf("first worker pre-restart refund_state=%q; want durable retryable uncertainty", state)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE payments SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}

	provider, ok := worker.provider.(paymentgateway.PaymentGatewayInterface)
	if !ok {
		t.Fatal("fixture provider does not implement PaymentGatewayInterface")
	}
	restarted := NewFinancialWorker(newFinancialWorkerRuntimeService(repo, op, provider))
	second := paymentruntime.Start(context.Background(), restarted.Start)
	registerFinancialWorkerGroupCleanup(t, second)
	waitForRuntimeRefundState(t, pool, op.PaymentID, "succeeded", 8*time.Second)
	assertRuntimePreparedRefund(t, pool, op.PaymentID, 1)
	stopFinancialWorkerGroup(t, second)
	evidence.log(t, "TestFinancialWorkerStartRestartRecoversAcceptedRefundWithoutDuplicateIntegration", pool, op.PaymentID)

	mu.Lock()
	defer mu.Unlock()
	if posts != 1 || len(keys) != 1 || keys[0] != "ticketbox:payment:"+op.PaymentID.String()+":g1:refund" {
		t.Fatalf("provider created duplicate or unstable refund: posts=%d keys=%v", posts, keys)
	}
}

func requireFinancialWorkerRuntimeDatabase(t *testing.T) {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("PAYMENT_TEST_DATABASE_URL"))
	if err != nil || parsed.Port() != "25439" || parsed.Path != "/ticketbox_payment_test" ||
		(parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Skip("set PAYMENT_TEST_DATABASE_URL to the dedicated loopback Payment test database at port 25439")
	}
}

func financialWorkerRuntimeFixture(t *testing.T) (*pgxpool.Pool, *repository.PostgresPaymentRepository, domain.CreateOperation, *FinancialWorker, *httptest.Server) {
	t.Helper()
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{
		BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd",
	}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_runtime_" + id.String(),
		AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	provider := runtimeStripeGateway(server)
	service := newFinancialWorkerRuntimeService(repo, op, provider)
	return pool, repo, op, NewFinancialWorker(service), server
}

func newFinancialWorkerRuntimeService(repo repository.PaymentRepositoryInterface, op domain.CreateOperation, gateway paymentgateway.PaymentGatewayInterface) *PaymentService {
	service := NewPaymentService(repo, zap.NewNop(), gateway)
	service.SetFinancialAuthority(runtimeBookingReader(op), op.Identity.ProviderAccount, op.Identity.LiveMode)
	return service
}

func runtimeBookingReader(op domain.CreateOperation) *resolutionBookingReader {
	refund := financialWorkerRuntimeRefund(op)
	return &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id: op.Identity.BookingID.String(), UserId: op.Identity.UserID.String(),
			TotalAmountCents: op.Identity.AmountCents, Status: refund.Reason,
		},
		Generation: op.Identity.Generation, Phase: "TERMINAL", Currency: op.Identity.Currency, DecisionId: refund.DecisionID.String(),
	}}
}

func runtimeStripeGateway(server *httptest.Server) *stripegateway.StripePaymentGateway {
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	client := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	return stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, client)
}

func financialWorkerRuntimeRefund(op domain.CreateOperation) domain.RefundObligation {
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	return domain.RefundObligation{
		PaymentID: op.PaymentID, DecisionID: decision, Generation: 1, AmountCents: op.Identity.AmountCents,
		Currency: op.Identity.Currency, Reason: "EXPIRED", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund",
	}
}

func runtimeAdmissionIdentity(op domain.CreateOperation) admission.Identity {
	actor, ticket, event := uuid.New(), uuid.New(), uuid.New()
	keyDigest := sha256.Sum256([]byte("worker runtime admission key:" + uuid.NewString()))
	fingerprint := sha256.Sum256([]byte("worker runtime admission request:" + uuid.NewString()))
	return admission.Identity{
		RequestID: admission.RequestIDFor(actor, keyDigest), PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		TicketID: ticket, EventID: event, UserID: op.Identity.UserID, ActorID: actor, Generation: op.Identity.Generation,
		KeyDigest: keyDigest, Fingerprint: fingerprint,
	}
}

func runtimeIntentID(op domain.CreateOperation) string {
	return "pi_worker_runtime_" + op.PaymentID.String()
}

func runtimeIntentJSON(op domain.CreateOperation, status string) map[string]any {
	amountReceived := 0
	if status == "succeeded" {
		amountReceived = 725
	}
	return map[string]any{"id": runtimeIntentID(op), "object": "payment_intent", "amount": 725, "amount_received": amountReceived,
		"currency": "usd", "status": status, "livemode": false, "metadata": map[string]string{
			"ticketbox_payment_id": op.PaymentID.String(), "ticketbox_booking_id": op.Identity.BookingID.String(), "ticketbox_generation": "1",
		}}
}

func runtimeRefundJSON(op domain.CreateOperation, id, status string) map[string]any {
	return map[string]any{"id": id, "object": "refund", "payment_intent": runtimeIntentID(op), "amount": 725, "currency": "usd", "status": status}
}

func waitForRuntimeRefundState(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var state string
		if err := pool.QueryRow(context.Background(), `SELECT refund_state FROM payments WHERE id=$1`, paymentID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	var state string
	_ = pool.QueryRow(context.Background(), `SELECT refund_state FROM payments WHERE id=$1`, paymentID).Scan(&state)
	t.Fatalf("FinancialWorker.Start state=%q; want %q", state, want)
}

func assertRuntimePreparedRefund(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID, want int) {
	t.Helper()
	count := runtimePreparedRefundCount(t, pool, paymentID)
	if count != want {
		t.Fatalf("durable refund PREPARED rows=%d; want %d", count, want)
	}
}

func runtimePreparedRefundCount(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='PREPARED'`, paymentID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type workerRuntimeEvidence struct {
	mu                  sync.Mutex
	workerStarted       bool
	claimFence          int64
	providerCreateCalls int
	stableKeyPreserved  bool
}

func newWorkerRuntimeEvidence() *workerRuntimeEvidence {
	return &workerRuntimeEvidence{stableKeyPreserved: true}
}

func (e *workerRuntimeEvidence) observeClaim(pool *pgxpool.Pool, paymentID uuid.UUID) {
	var fence int64
	var tokenPresent bool
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(claim_fence,0),claim_token IS NOT NULL FROM payments WHERE id=$1`, paymentID).Scan(&fence, &tokenPresent); err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.workerStarted = true
	if tokenPresent && fence > e.claimFence {
		e.claimFence = fence
	}
}

func (e *workerRuntimeEvidence) recordCreate(req *http.Request, op domain.CreateOperation) {
	wantKey := "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"
	e.mu.Lock()
	defer e.mu.Unlock()
	e.providerCreateCalls++
	if req.Header.Get("Idempotency-Key") != wantKey || req.PostForm.Get("amount") != "725" || req.PostForm.Get("payment_intent") != runtimeIntentID(op) {
		e.stableKeyPreserved = false
	}
}

func (e *workerRuntimeEvidence) log(t *testing.T, scenario string, pool *pgxpool.Pool, paymentID uuid.UUID) {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(), `SELECT refund_state FROM payments WHERE id=$1`, paymentID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedRefundCount(t, pool, paymentID)
	e.mu.Lock()
	defer e.mu.Unlock()
	sourceCommit := runtimeSourceCommit()
	if sourceCommit == "UNBOUND" || !e.workerStarted || e.claimFence < 1 || prepared != 1 ||
		e.providerCreateCalls != 1 || state != "succeeded" || !e.stableKeyPreserved {
		t.Fatalf("incomplete runtime evidence: source=%q started=%v fence=%d prepared=%d creates=%d state=%q stable=%v",
			sourceCommit, e.workerStarted, e.claimFence, prepared, e.providerCreateCalls, state, e.stableKeyPreserved)
	}
	marker := map[string]any{
		"scenario": scenario, "source_commit": sourceCommit, "pid": os.Getpid(),
		"worker_started": e.workerStarted, "claim_fence": e.claimFence,
		"prepared_count": prepared, "provider_create_calls": e.providerCreateCalls,
		"refund_state": state, "stable_key_preserved": e.stableKeyPreserved,
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("RUNTIME_EVIDENCE %s\n", encoded)
}

func runtimeSourceCommit() string {
	commit := strings.TrimSpace(os.Getenv("RUNTIME_SOURCE_COMMIT"))
	if len(commit) != 40 {
		return "UNBOUND"
	}
	for _, character := range commit {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return "UNBOUND"
		}
	}
	return commit
}

func registerFinancialWorkerGroupCleanup(t *testing.T, group *paymentruntime.Group) {
	t.Helper()
	t.Cleanup(func() { stopFinancialWorkerGroup(t, group) })
}

func stopFinancialWorkerGroup(t *testing.T, group *paymentruntime.Group) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := group.StopAndWait(ctx); err != nil {
		t.Errorf("stop and join FinancialWorker.Start: %v", err)
		// Test fixture cleanup must not close the isolated database while the
		// worker could still be finalizing a durable result.
		<-group.Done()
	}
}
