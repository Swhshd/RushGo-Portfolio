//go:build stripe_minimal_test

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
)

const defaultWorkerOptIn = "I_UNDERSTAND_ONE_TEST_PAYMENT_AND_REFUND"

func TestDefaultWorkerConfigRequiresDedicatedExplicitOptIn(t *testing.T) {
	for _, optIn := range []string{"", "1", "I_UNDERSTAND_ONE_TEST_PAYMENT"} {
		t.Run("rejects "+strings.ReplaceAll(optIn, "_", "-"), func(t *testing.T) {
			_, _, err := loadRuntimeTestConfig(t, "default_worker", optIn)
			if err == nil || !strings.Contains(err.Error(), "default-worker explicit opt-in required") {
				t.Fatalf("loadTestConfig error=%v; want dedicated default-worker opt-in rejection", err)
			}
		})
	}

	t.Run("accepts exact opt-in", func(t *testing.T) {
		_, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
		if err != nil {
			t.Fatalf("loadTestConfig rejected exact default-worker opt-in: %v", err)
		}
	})

	t.Run("rejects default-worker opt-in for legacy config", func(t *testing.T) {
		_, _, err := loadRuntimeTestConfig(t, "", defaultWorkerOptIn)
		if err == nil || !strings.Contains(err.Error(), "default-worker opt-in requires runtime_mode") {
			t.Fatalf("loadTestConfig error=%v; want opt-in/config mode mismatch rejection", err)
		}
	})
}

func TestDefaultWorkerControlExposesIdentityAndOmitsManualMutationRoutes(t *testing.T) {
	cfg, token, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
	if err != nil {
		t.Fatalf("load default-worker config: %v", err)
	}
	state, err := openState(filepath.Join(t.TempDir(), "state.json"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	control := &testControl{cfg: cfg, token: token, state: state}
	handler := control.handler()

	request := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s; want 200", response.Code, response.Body.String())
	}
	var health map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health["runtime_mode"] != "default_worker" || health["financial_worker_mode"] != "background_default_worker" ||
		health["run_id"] != cfg.RunID || health["source_commit"] != cfg.SourceCommit {
		t.Fatalf("health identity=%v; want default worker mode and the configured run/source identity", health)
	}

	for _, path := range []string{"/v1/receipts-once", "/v1/run-once"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"run_id":"ignored","source_commit":"ignored"}`))
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("POST %s status=%d; want route absent (404), body=%s", path, response.Code, response.Body.String())
		}
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/health", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/health status=%d; want GET-only 405", response.Code)
	}
}

func TestLegacyConfigKeepsManualControlRouteAndRuntimeIdentity(t *testing.T) {
	cfg, token, err := loadRuntimeTestConfig(t, "", "")
	if err != nil {
		t.Fatalf("load legacy config: %v", err)
	}
	state, err := openState(filepath.Join(t.TempDir(), "state.json"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&testControl{cfg: cfg, token: token, state: state}).handler()

	request := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var health map[string]any
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &health) != nil ||
		health["runtime_mode"] != "manual" || health["financial_worker_mode"] != "manual_control" {
		t.Fatalf("legacy health status=%d body=%s; want manual-control identity", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/run-once", strings.NewReader(`{"run_id":"`+cfg.RunID+`","source_commit":"`+cfg.SourceCommit+`"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("legacy POST /v1/run-once status=%d body=%s; want existing manual route response", response.Code, response.Body.String())
	}
}

func TestDefaultWorkerStateCannotReopenUnderLegacyMode(t *testing.T) {
	defaultConfig, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "state.json")
	if _, err := openState(path, defaultConfig); err != nil {
		t.Fatalf("initialize default-worker state: %v", err)
	}
	manualConfig, _, err := loadRuntimeTestConfig(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openState(path, manualConfig); err == nil {
		t.Fatal("default-worker state reopened under legacy manual mode")
	}
}

func TestDefaultWorkerRefundRejectsMalformedAndForeignClaimsBeforeProviderCreate(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(domain.FinancialClaim) domain.FinancialClaim
	}{
		{name: "missing approved-unused decision and scope", change: func(claim domain.FinancialClaim) domain.FinancialClaim {
			claim.Refund.DecisionID = uuid.Nil
			claim.Refund.Reason = ""
			claim.View = nil
			return claim
		}},
		{name: "foreign payment", change: func(claim domain.FinancialClaim) domain.FinancialClaim {
			claim.PaymentID = uuid.New()
			return claim
		}},
		{name: "foreign booking", change: func(claim domain.FinancialClaim) domain.FinancialClaim {
			claim.Operation.Identity.BookingID = uuid.New()
			return claim
		}},
		{name: "foreign approval decision", change: func(claim domain.FinancialClaim) domain.FinancialClaim {
			decisionID := uuid.New()
			claim.Refund.DecisionID = decisionID
			claim.View.RefundDecisionID = &decisionID
			return claim
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
			if err != nil {
				t.Fatal(err)
			}
			state, op := prepareCollectedPaymentAt(t, filepath.Join(t.TempDir(), "state.json"), cfg)
			if _, err := state.beginFinancialWork(); err != nil {
				t.Fatal(err)
			}
			claim := testApprovedUnusedRefundClaim(op, uuid.New())
			if test.change != nil {
				claim = test.change(claim)
			}
			provider := &countingRefundProvider{}
			authority := &approvedUnusedRefundAuthorityStub{view: testApprovedUnusedRefundClaim(op, uuid.New()).View}
			bounded := &boundedProvider{next: provider, state: state, approvedUnusedRefund: authority, verifyAppliedReceipt: func(context.Context, runState) error { return nil }}
			_, err = bounded.CreateFullRefund(context.Background(), claim, "pi_test_approved")
			if err == nil || provider.calls != 0 {
				t.Fatalf("CreateFullRefund err=%v; provider creates=%d, want rejection before any provider create", err, provider.calls)
			}
			if authority.calls != 0 && test.name != "foreign approval decision" {
				t.Fatalf("approved-unused authority calls=%d; malformed claim should fail before authority", authority.calls)
			}
		})
	}
}

func TestDefaultWorkerRefundUsesDurableApprovedUnusedDecisionAndRejectsDuplicateScope(t *testing.T) {
	cfg, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
	if err != nil {
		t.Fatal(err)
	}
	state, op := prepareCollectedPaymentAt(t, filepath.Join(t.TempDir(), "state.json"), cfg)
	if _, err := state.beginFinancialWork(); err != nil {
		t.Fatal(err)
	}
	claim := testApprovedUnusedRefundClaim(op, uuid.New())
	provider := &countingRefundProvider{}
	authority := &approvedUnusedRefundAuthorityStub{view: claim.View}
	verifiedReceipt := 0
	bounded := &boundedProvider{next: provider, state: state, approvedUnusedRefund: authority, verifyAppliedReceipt: func(_ context.Context, got runState) error {
		verifiedReceipt++
		if got.WebhookEventID != "evt_test_approved" || got.VerifiedWebhookType != "payment_intent.succeeded" {
			return errors.New("different receipt")
		}
		return nil
	}}
	observation, err := bounded.CreateFullRefund(context.Background(), claim, "pi_test_approved")
	if err != nil || observation == nil || provider.calls != 1 || authority.calls != 1 || verifiedReceipt != 1 {
		t.Fatalf("approved-unused refund observation=%+v err=%v creates=%d authority=%d receipts=%d", observation, err, provider.calls, authority.calls, verifiedReceipt)
	}
	if authority.bookingID != op.Identity.BookingID || authority.decisionID != claim.Refund.DecisionID ||
		authority.paymentID != op.PaymentID || authority.generation != 1 {
		t.Fatalf("approved-unused authority selectors=(%s,%s,%s,%d), want exact claim identity", authority.bookingID, authority.decisionID, authority.paymentID, authority.generation)
	}
	if got := state.snapshot(); got.RefundDecisionID != claim.Refund.DecisionID.String() || got.RefundKey != claim.Key || !got.Collected || got.ReceiptWorkState != "done" {
		t.Fatalf("default worker did not persist the applied receipt and stable refund decision: %+v", got)
	}

	for _, duplicate := range []domain.FinancialClaim{claim, testApprovedUnusedRefundClaim(op, uuid.New())} {
		if _, err := bounded.CreateFullRefund(context.Background(), duplicate, "pi_test_approved"); err == nil {
			t.Fatal("duplicate refund decision/scope was accepted")
		}
	}
	if provider.calls != 1 || authority.calls != 1 || verifiedReceipt != 1 {
		t.Fatalf("duplicate refund scope reached downstream work: creates=%d authority=%d receipts=%d", provider.calls, authority.calls, verifiedReceipt)
	}
}

func TestDefaultWorkerObserverPreservesInflightRefundForWebhookFirst(t *testing.T) {
	cfg, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
	if err != nil {
		t.Fatal(err)
	}
	state, op := prepareCollectedPaymentAt(t, filepath.Join(t.TempDir(), "state.json"), cfg)
	if _, err := state.beginFinancialWork(); err != nil {
		t.Fatal(err)
	}
	claim := testApprovedUnusedRefundClaim(op, uuid.New())
	if err := state.reserveRefund(claim, "pi_test_approved", true); err != nil {
		t.Fatalf("reserve exact approved-unused refund: %v", err)
	}
	if err := state.confirmBackgroundRefundApplied(claim.View); err != nil {
		t.Fatalf("observe claimed Payment row during provider call: %v", err)
	}
	if got := state.snapshot(); got.RefundState != "creating" || got.RefundID != "" {
		t.Fatalf("background observation replaced in-flight provider identity: %+v", got)
	}

	refund := &domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1,
		IntentID: "pi_test_approved", RefundID: "re_test_fixture", Status: "pending",
		AmountCents: 100, Currency: "usd", Account: approvedAccount,
	}
	if err := state.webhook(domain.ProviderReceipt{EnvelopeID: "evt_test_refund", EventType: "refund.created", Observation: *refund}); err != nil {
		t.Fatalf("refund.created arriving before CreateFullRefund response: %v", err)
	}
	if err := state.refundCreated(refund); err != nil {
		t.Fatalf("provider response after early refund.created: %v", err)
	}
	if got := state.snapshot(); got.Blocked || got.RefundID != "re_test_fixture" || got.RefundState != "pending" {
		t.Fatalf("webhook-first refund identity was not preserved: %+v", got)
	}
}

func TestDefaultWorkerReadsAppliedReceiptBeforePaymentProjection(t *testing.T) {
	view := &domain.FinancialView{ProviderOutcome: "not_collected"}
	receiptApplied := false
	var order []string
	got, err := readAppliedPaymentFinancialView(context.Background(), runState{},
		func(context.Context, runState) error {
			order = append(order, "receipt")
			receiptApplied = true
			return nil
		},
		func(context.Context, runState) (*domain.FinancialView, error) {
			order = append(order, "payment")
			if receiptApplied {
				view.ProviderOutcome = "collected"
			}
			return view, nil
		})
	if err != nil || got.ProviderOutcome != "collected" || strings.Join(order, ",") != "receipt,payment" {
		t.Fatalf("receipt/payment evidence order=%v view=%+v err=%v", order, got, err)
	}
}

func TestDefaultWorkerObserverDoesNotApplyPreapprovalViewToConcurrentRefund(t *testing.T) {
	cfg, _, err := loadRuntimeTestConfig(t, "default_worker", defaultWorkerOptIn)
	if err != nil {
		t.Fatal(err)
	}
	state, err := openState(filepath.Join(t.TempDir(), "state.json"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	op := testCreateOperation()
	if err := state.reserve(op); err != nil {
		t.Fatal(err)
	}
	if err := state.created(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, IntentID: "pi_test_approved",
		AmountCents: 100, Currency: "usd", Account: approvedAccount, Status: "requires_payment_method",
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.webhook(domain.ProviderReceipt{EnvelopeID: "evt_test_approved", EventType: "payment_intent.succeeded", Observation: domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, IntentID: "pi_test_approved",
		AmountCents: 100, Currency: "usd", Account: approvedAccount, Status: "succeeded", Collected: true,
	}}); err != nil {
		t.Fatal(err)
	}
	observed := state.snapshot()
	preapproval := receiptRecoveryView(op)
	claim := testApprovedUnusedRefundClaim(op, uuid.New())
	if err := state.confirmBackgroundReceiptApplied(preapproval); err != nil {
		t.Fatalf("concurrent receipt completion: %v", err)
	}
	if err := state.reserveRefund(claim, "pi_test_approved", true); err != nil {
		t.Fatalf("concurrent approved-unused refund reservation: %v", err)
	}
	if err := applyObservedDefaultWorkerView(state, observed, preapproval); err != nil {
		t.Fatalf("apply observer's stale preapproval projection: %v", err)
	}
	if got := state.snapshot(); got.Blocked || got.RefundState != "creating" || got.RefundDecisionID != claim.Refund.DecisionID.String() {
		t.Fatalf("stale preapproval projection damaged concurrent refund state: %+v", got)
	}
}

type approvedUnusedRefundAuthorityStub struct {
	view       *domain.FinancialView
	bookingID  uuid.UUID
	decisionID uuid.UUID
	paymentID  uuid.UUID
	generation int64
	calls      int
}

func (a *approvedUnusedRefundAuthorityStub) AcceptApprovedUnusedRefundDecision(_ context.Context, bookingID, decisionID, paymentID uuid.UUID, generation int64) (*domain.FinancialView, error) {
	a.bookingID, a.decisionID, a.paymentID, a.generation = bookingID, decisionID, paymentID, generation
	a.calls++
	return a.view, nil
}

func testApprovedUnusedRefundClaim(op domain.CreateOperation, decisionID uuid.UUID) domain.FinancialClaim {
	claim := testRefundClaim(op)
	claim.Refund.DecisionID = decisionID
	claim.Refund.Reason = domain.RefundReasonConfirmedUnusedTicket
	claim.View = &domain.FinancialView{
		Payment: &domain.Payment{ID: op.PaymentID, BookingId: &op.Identity.BookingID, UserId: op.Identity.UserID,
			Price: 100, Currency: "usd", PaymentIntentId: "pi_test_approved", Status: domain.PaymentSuccess},
		Generation: 1, Managed: true, ProviderAccount: approvedAccount, LiveMode: boolPointer(false),
		ProviderOutcome: "collected", BookingDisposition: "CONFIRMED", ReconciliationState: "refund_required",
		RefundState: "claimed", RefundAmountCents: 100, RefundReason: domain.RefundReasonConfirmedUnusedTicket,
		RefundDecisionID: &decisionID,
	}
	return claim
}

type countingRefundProvider struct {
	paymentgateway.FinancialProvider
	calls int
}

func (p *countingRefundProvider) CreateFullRefund(_ context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	p.calls++
	return &domain.ProviderObservation{
		PaymentID: claim.PaymentID, BookingID: claim.Operation.Identity.BookingID, Generation: 1,
		IntentID: intentID, RefundID: "re_test_fixture", Account: approvedAccount,
		AmountCents: 100, Currency: "usd", Status: "pending",
	}, nil
}

func loadRuntimeTestConfig(t *testing.T, mode, optIn string) (testConfig, string, error) {
	t.Helper()
	for _, name := range []string{
		"PGHOST", "PGHOSTADDR", "PGPORT", "PGUSER", "PGPASSWORD", "PGPASSFILE", "PGDATABASE", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS", "PGSSLMODE", "PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT", "PGSSLCRL", "PGCONNECT_TIMEOUT", "PGAPPNAME", "PGTARGETSESSIONATTRS", "DATABASE_URL", "PAYMENT_DATABASE_URL", "STRIPE_API_KEY", "STRIPE_LIVE_SECRET_KEY",
	} {
		unsetTestEnvironment(t, name)
	}

	cfg := testStateConfig()
	compactRun := strings.ReplaceAll(cfg.RunID, "-", "")
	config := map[string]any{
		"version": 1, "run_id": cfg.RunID, "source_commit": cfg.SourceCommit,
		"database_url":      "postgres://payment_" + compactRun[:12] + "@127.0.0.1/rushgo_" + compactRun[:12],
		"expected_database": "rushgo_" + compactRun[:12], "expected_user": "payment_" + compactRun[:12],
		"expected_stripe_account": approvedAccount, "expected_user_id": cfg.ExpectedUserID,
		"booking_address": "booking.test:50053", "event_address": "event.test:50052", "kafka_brokers": "kafka.test:9092",
		"listen_address": "0.0.0.0:50054", "webhook_address": "0.0.0.0:8081", "control_address": "0.0.0.0:8090",
	}
	if mode != "" {
		config["runtime_mode"] = mode
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "test-config.json")
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "state.json")
	token := strings.Repeat("t", 40)
	t.Setenv(optInEnv, "1")
	t.Setenv(configEnv, configPath)
	t.Setenv(runIDEnv, cfg.RunID)
	t.Setenv(sourceEnv, cfg.SourceCommit)
	t.Setenv(receiptRecoveryEnv, "")
	t.Setenv(tokenEnv, token)
	t.Setenv(stateEnv, statePath)
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_local_fixture")
	t.Setenv("STRIPE_SECRET_WEBHOOK", "whsec_local_fixture")
	t.Setenv("RUSHGO_STRIPE_UNUSED_REFUND_TEST_ONLY", optIn)
	loaded, _, _, err := loadTestConfig(cfg.SourceCommit)
	if err != nil {
		return testConfig{}, token, err
	}
	return loaded, token, nil
}

func unsetTestEnvironment(t *testing.T, name string) {
	t.Helper()
	value, present := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("unset %s: %v", name, err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, value)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}
