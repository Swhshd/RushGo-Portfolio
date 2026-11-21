//go:build admission_r4

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestHasForbiddenEnvironmentAllowsPostgresImageMetadata(t *testing.T) {
	clearR4EnvironmentForTest(t)
	t.Setenv("PG_MAJOR", "16")
	t.Setenv("PG_VERSION", "16.4")
	t.Setenv("PGDATA", "/var/lib/postgresql/data")

	if hasForbiddenEnvironment() {
		t.Fatal("PostgreSQL image metadata was treated as a libpq connection override")
	}
}

func TestHasForbiddenEnvironmentRejectsLibpqConnectionOverrides(t *testing.T) {
	for _, name := range []string{"PGHOST", "PGPASSWORD", "PGSERVICEFILE", "PGSSLMODE"} {
		t.Run(name, func(t *testing.T) {
			clearR4EnvironmentForTest(t)
			t.Setenv(name, "unexpected-override")
			if !hasForbiddenEnvironment() {
				t.Fatalf("%s connection override was accepted", name)
			}
		})
	}
}

func TestControlRejectsBadTokenRunAndSourceBeforeRefundMutation(t *testing.T) {
	runID := uuid.NewString()
	sourceCommit := strings.Repeat("a", 40)
	paymentID := uuid.New()
	runtime := &serviceRuntime{
		cfg:   bootstrapConfig{RunID: runID, SourceCommit: sourceCommit},
		token: "test-control-token-that-is-not-used-outside-this-test",
		fixtures: map[uuid.UUID]fixtureConfig{
			paymentID: {PaymentID: paymentID.String()},
		},
	}
	for _, testCase := range []struct {
		name       string
		requestRun string
		requestSrc string
		token      string
		wantStatus int
	}{
		{name: "bad token", requestRun: runID, requestSrc: sourceCommit, token: "wrong", wantStatus: http.StatusUnauthorized},
		{name: "wrong run", requestRun: uuid.NewString(), requestSrc: sourceCommit, token: runtime.token, wantStatus: http.StatusForbidden},
		{name: "wrong source", requestRun: runID, requestSrc: strings.Repeat("b", 40), token: runtime.token, wantStatus: http.StatusForbidden},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body, err := json.Marshal(controlRequest{RunID: testCase.requestRun, SourceCommit: testCase.requestSrc, PaymentID: paymentID.String()})
			if err != nil {
				t.Fatal("encode control test request")
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/require-refund", strings.NewReader(string(body)))
			request.Header.Set("Authorization", "Bearer "+testCase.token)
			response := httptest.NewRecorder()
			runtime.controlHandler().ServeHTTP(response, request)
			if response.Code != testCase.wantStatus {
				t.Fatalf("control status=%d, want %d", response.Code, testCase.wantStatus)
			}
		})
	}
}

func TestDefaultWorkerModeExposesNoManualFinancialMutationControls(t *testing.T) {
	runtime := &serviceRuntime{cfg: bootstrapConfig{RuntimeMode: "default_worker"}}
	for _, path := range []string{"/v1/require-refund", "/v1/refund-once", "/v1/recover-once", "/v1/recovery/resume"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			response := httptest.NewRecorder()
			runtime.controlHandler().ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("manual financial control %s status=%d, want 404", path, response.Code)
			}
		})
	}
}

func TestDefaultWorkerModeRequiresIsolatedOptInAndBoundedFixtures(t *testing.T) {
	cfg := bootstrapConfig{
		RuntimeMode: "default_worker", EventAddress: "event-service:50051",
		Fixtures: []fixtureConfig{
			{ProviderAccount: "acct_r4_local", AmountCents: 100},
			{ProviderAccount: "acct_r4_local", AmountCents: 100},
		},
	}
	if err := validateRuntimeMode(&cfg, ""); err == nil {
		t.Fatal("default-worker mode started without its explicit test-only opt-in")
	}
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err != nil {
		t.Fatalf("valid bounded default-worker test configuration: %v", err)
	}
	cfg.Fixtures = cfg.Fixtures[:1]
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("default-worker mode accepted fewer than two fixture identities")
	}
	cfg.Fixtures = append(cfg.Fixtures, fixtureConfig{ProviderAccount: "acct_r4_local", AmountCents: 101})
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("default-worker mode accepted a fixture above the USD 1.00 test cap")
	}
}

func TestDynamicPurchaseOptInRequiresBoundBuyerAndInternalKafkaBroker(t *testing.T) {
	static := []fixtureConfig{
		{PaymentID: uuid.NewString(), BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_0123456789abcdef0123456789abcdef", ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true},
		{PaymentID: uuid.NewString(), BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_1123456789abcdef0123456789abcdef", ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true},
	}
	buyerID := uuid.NewString()
	cfg := bootstrapConfig{
		RuntimeMode: "default_worker", EventAddress: "event-service:50051", Fixtures: static,
		DynamicPurchase: &dynamicPurchaseConfig{Enabled: true, UserID: buyerID},
		KafkaBrokers:    []string{"kafka:9092"},
	}
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err != nil {
		t.Fatalf("valid dynamic purchase opt-in rejected: %v", err)
	}

	cfg.KafkaBrokers = nil
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("dynamic purchase accepted without its explicit Kafka broker list")
	}
	cfg.KafkaBrokers = []string{"kafka:9092"}
	cfg.DynamicPurchase.UserID = static[0].UserID
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("dynamic buyer reused a static fixture identity")
	}
	cfg.DynamicPurchase.UserID = buyerID
	cfg.DynamicPurchase.Enabled = false
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("present but disabled dynamic purchase object was accepted")
	}
	cfg.DynamicPurchase.Enabled = true
	cfg.KafkaBrokers = []string{"kafka.example.com:9092"}
	if err := validateRuntimeMode(&cfg, fullServiceOptInValue); err == nil {
		t.Fatal("dynamic purchase accepted a non-isolated Kafka broker")
	}
}

func TestDynamicPurchaseProviderUsesOneFrozenIntentAndReceipt(t *testing.T) {
	static := []fixtureConfig{
		{PaymentID: uuid.NewString(), BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_0123456789abcdef0123456789abcdef", ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true},
		{PaymentID: uuid.NewString(), BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_1123456789abcdef0123456789abcdef", ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true},
	}
	buyerID, err := uuid.Parse(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	cfg := bootstrapConfig{
		RunID: uuid.NewString(), Fixtures: static,
		DynamicPurchase: &dynamicPurchaseConfig{Enabled: true, UserID: buyerID.String()},
		KafkaBrokers:    []string{"kafka:9092"},
	}
	provider, err := newLocalProvider(cfg)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}
	op := dynamicPurchaseTestOperation(t, buyerID)
	first, secret, err := provider.CreateIntent(context.Background(), op)
	if err != nil {
		t.Fatalf("create dynamic intent: %v", err)
	}
	second, replaySecret, err := provider.CreateIntent(context.Background(), op)
	if err != nil {
		t.Fatalf("replay identical dynamic intent: %v", err)
	}
	wantIntent := "pi_r4_" + strings.ReplaceAll(op.PaymentID.String(), "-", "")
	wantSecret := "simulated://SIMULATED_PROVIDER_AUTHORITY/" + wantIntent
	if first.IntentID != wantIntent || second.IntentID != wantIntent || secret != wantSecret || replaySecret != wantSecret ||
		first.Status != "requires_payment_method" || first.Collected || second.PaymentID != op.PaymentID {
		t.Fatalf("intent identity or replay changed: first=%+v second=%+v", first, second)
	}

	wrongBuyer := op
	wrongBuyer.Identity.UserID = uuid.New()
	wrongBuyer.Identity.Fingerprint, err = domain.FinancialFingerprint(wrongBuyer.Identity, wrongBuyer.FrozenParams)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := provider.CreateIntent(context.Background(), wrongBuyer); err == nil {
		t.Fatal("intent was created for a buyer outside the configured slot")
	}
	secondSale := dynamicPurchaseTestOperation(t, buyerID)
	secondSale.PaymentID = uuid.New()
	secondSale.Key = "ticketbox:payment:" + secondSale.PaymentID.String() + ":g1:create"
	if _, _, err := provider.CreateIntent(context.Background(), secondSale); err == nil {
		t.Fatal("second dynamic payment was created for the single buyer slot")
	}

	before := provider.CheckAdmissionEvidence(context.Background(), dynamicAdmissionIdentity(op, wantIntent))
	if before.EligibilityState != domain.AdmissionEligibilityUnknown || before.ReasonCode != "dynamic_purchase_not_confirmed" {
		t.Fatalf("unconfirmed dynamic purchase evidence=%+v", before)
	}
	receipt1, err := provider.dynamicConfirmationReceipt(op.PaymentID)
	if err != nil {
		t.Fatalf("build dynamic receipt: %v", err)
	}
	receipt2, err := provider.dynamicConfirmationReceipt(op.PaymentID)
	if err != nil {
		t.Fatalf("retry dynamic receipt: %v", err)
	}
	if receipt1.ID != receipt2.ID || receipt1.EnvelopeID != receipt2.EnvelopeID || receipt1.Observation.EventID != receipt2.Observation.EventID ||
		receipt1.Observation.ObservedAt != receipt2.Observation.ObservedAt || receipt1.ReceivedAt != receipt2.ReceivedAt ||
		receipt1.Observation.EvidenceHash != receipt2.Observation.EvidenceHash || !bytes.Equal(receipt1.Observation.SafeObject, receipt2.Observation.SafeObject) {
		t.Fatalf("retry changed frozen receipt: first=%+v second=%+v", receipt1, receipt2)
	}
	middle := provider.CheckAdmissionEvidence(context.Background(), dynamicAdmissionIdentity(op, wantIntent))
	if middle.EligibilityState != domain.AdmissionEligibilityUnknown {
		t.Fatalf("provider confirmation alone became admission eligible before receipt persistence: %+v", middle)
	}
	if err := provider.markDynamicReceiptAccepted(op.PaymentID, receipt1); err != nil {
		t.Fatalf("mark accepted receipt: %v", err)
	}
	after := provider.CheckAdmissionEvidence(context.Background(), dynamicAdmissionIdentity(op, wantIntent))
	if after.EligibilityState != domain.AdmissionEligibilityEligible || after.RefundState != domain.RefundEvidenceNoRefund {
		t.Fatalf("persisted confirmed receipt did not unlock bounded admission evidence: %+v", after)
	}
	stats := provider.stats()
	if stats.DynamicPurchase == nil || !stats.DynamicPurchase.Created || !stats.DynamicPurchase.Confirmed ||
		!stats.DynamicPurchase.ReceiptAccepted || stats.DynamicPurchase.PaymentID != op.PaymentID.String() {
		t.Fatalf("dynamic slot stats do not reflect bounded effects: %+v", stats.DynamicPurchase)
	}
}

func TestDefaultWorkerDoesNotExposeDynamicPurchaseControlRoute(t *testing.T) {
	runtime := &serviceRuntime{cfg: bootstrapConfig{RuntimeMode: "default_worker"}, token: "test-control-token-that-is-not-used-outside-this-test"}
	request := httptest.NewRequest(http.MethodPost, "/v1/confirm-purchase", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	runtime.controlHandler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("default worker exposed dynamic purchase control route: HTTP %d", response.Code)
	}
}

func TestDynamicPurchaseRefundRequiresAcceptedReceiptAndReplayDeniesAdmission(t *testing.T) {
	buyerID := uuid.New()
	provider, err := newLocalProvider(bootstrapConfig{
		RunID:           uuid.NewString(),
		DynamicPurchase: &dynamicPurchaseConfig{Enabled: true, UserID: buyerID.String()},
		KafkaBrokers:    []string{"kafka:9092"},
	})
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}

	op := dynamicPurchaseTestOperation(t, buyerID)
	created, _, err := provider.CreateIntent(context.Background(), op)
	if err != nil {
		t.Fatalf("create dynamic intent: %v", err)
	}
	claim := dynamicPurchaseTestClaim(op)
	if _, err := provider.CreateFullRefund(context.Background(), claim, created.IntentID); err == nil {
		t.Fatal("refund was accepted before provider confirmation")
	}

	receipt, err := provider.dynamicConfirmationReceipt(op.PaymentID)
	if err != nil {
		t.Fatalf("build provider confirmation receipt: %v", err)
	}
	if stats := provider.dynamicStatsSnapshot(); !stats.Confirmed || stats.ReceiptAccepted {
		t.Fatalf("test setup did not leave confirmation unpersisted: %+v", stats)
	}
	if _, err := provider.CreateFullRefund(context.Background(), claim, created.IntentID); err == nil {
		t.Fatal("refund was accepted after provider confirmation but before receipt acceptance")
	}
	if stats := provider.dynamicStatsSnapshot(); stats.Refunded || stats.RefundEffects != 0 {
		t.Fatalf("rejected pre-receipt refund changed provider state: %+v", stats)
	}

	if err := provider.markDynamicReceiptAccepted(op.PaymentID, receipt); err != nil {
		t.Fatalf("mark durable provider receipt accepted: %v", err)
	}
	first, err := provider.CreateFullRefund(context.Background(), claim, created.IntentID)
	if err != nil {
		t.Fatalf("first accepted dynamic refund: %v", err)
	}
	second, err := provider.CreateFullRefund(context.Background(), claim, created.IntentID)
	if err != nil {
		t.Fatalf("replay accepted dynamic refund: %v", err)
	}
	if first == nil || second == nil || first.RefundID == "" || first.RefundID != second.RefundID ||
		first.Status != "succeeded" || second.Status != "succeeded" ||
		first.SucceededRefundCents != op.Identity.AmountCents || second.SucceededRefundCents != op.Identity.AmountCents {
		t.Fatalf("dynamic refund replay changed its result: first=%+v second=%+v", first, second)
	}
	stats := provider.dynamicStatsSnapshot()
	if !stats.Refunded || stats.RefundEffects != 1 || !stats.ReceiptAccepted {
		t.Fatalf("dynamic refund replay did not retain one accepted effect: %+v", stats)
	}

	evidence := provider.CheckAdmissionEvidence(context.Background(), dynamicAdmissionIdentity(op, created.IntentID))
	if evidence.EligibilityState != domain.AdmissionEligibilityDenied || evidence.RefundState != domain.RefundEvidencePresent ||
		evidence.ReasonCode != "fixture_refund_present" {
		t.Fatalf("refunded dynamic purchase remained admission eligible: %+v", evidence)
	}
}

func dynamicPurchaseTestClaim(op domain.CreateOperation) domain.FinancialClaim {
	return domain.FinancialClaim{
		PaymentID: op.PaymentID,
		Operation: op,
		Refund: &domain.RefundObligation{
			PaymentID: op.PaymentID, Generation: op.Identity.Generation,
			AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
			Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund",
		},
	}
}

func dynamicPurchaseTestOperation(t *testing.T, buyer uuid.UUID) domain.CreateOperation {
	t.Helper()
	booking, payment := uuid.New(), uuid.New()
	identity := domain.FinancialIdentity{
		BookingID: booking, UserID: buyer, Generation: 1, ProviderAccount: "acct_r4_local",
		AmountCents: 100, Currency: "usd",
	}
	frozen, err := json.Marshal(domain.FrozenCreateParameters{
		Version: 1, Amount: 100, Currency: "usd",
		AutomaticPaymentMethods: domain.AutomaticPaymentMethods{Enabled: true, AllowRedirects: "always"},
	})
	if err != nil {
		t.Fatal(err)
	}
	identity.Fingerprint, err = domain.FinancialFingerprint(identity, frozen)
	if err != nil {
		t.Fatal(err)
	}
	return domain.CreateOperation{
		Identity: identity, PaymentID: payment,
		Key: "ticketbox:payment:" + payment.String() + ":g1:create", FrozenParams: frozen,
	}
}

func dynamicAdmissionIdentity(op domain.CreateOperation, intent string) domain.AdmissionFinancialIdentity {
	return domain.AdmissionFinancialIdentity{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		ProviderAccount: op.Identity.ProviderAccount, LiveMode: op.Identity.LiveMode,
		PaymentIntentID: intent, AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
	}
}

func TestLocalProviderReturnsUnknownForMismatchedFixtureIdentity(t *testing.T) {
	paymentID, bookingID, userID := uuid.New(), uuid.New(), uuid.New()
	fixture := fixtureConfig{
		PaymentID: paymentID.String(), BookingID: bookingID.String(), UserID: userID.String(),
		Generation: 1, PaymentIntentID: "pi_r4_0123456789abcdef0123456789abcdef",
		ProviderAccount: "acct_r4_local", AmountCents: 725, Currency: "usd", SeedFinancial: true,
	}
	provider, err := newLocalProvider(bootstrapConfig{Fixtures: []fixtureConfig{fixture}})
	if err != nil {
		t.Fatal("create local provider fixture")
	}
	evidence := provider.CheckAdmissionEvidence(context.Background(), domain.AdmissionFinancialIdentity{
		PaymentID: paymentID, BookingID: uuid.New(), Generation: fixture.Generation,
		ProviderAccount: fixture.ProviderAccount, LiveMode: false, PaymentIntentID: fixture.PaymentIntentID,
		AmountCents: fixture.AmountCents, Currency: fixture.Currency,
	})
	if evidence.EligibilityState != domain.AdmissionEligibilityUnknown || evidence.ReasonCode != "fixture_identity_mismatch" {
		t.Fatalf("mismatched fixture evidence=%+v, want unknown identity mismatch", evidence)
	}
	stats := provider.stats()
	if len(stats.Fixtures) != 1 || stats.Fixtures[0].ProviderCalls != 1 || stats.Fixtures[0].RefundCalls != 0 {
		t.Fatalf("mismatched identity changed provider state: %+v", stats)
	}
}

func TestLocalProviderCountsRefundReplaysWithoutRepeatingEffect(t *testing.T) {
	paymentID, bookingID, userID := uuid.New(), uuid.New(), uuid.New()
	fixture := fixtureConfig{
		PaymentID: paymentID.String(), BookingID: bookingID.String(), UserID: userID.String(),
		Generation: 1, PaymentIntentID: "pi_r4_0123456789abcdef0123456789abcdef",
		ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true,
	}
	provider, err := newLocalProvider(bootstrapConfig{Fixtures: []fixtureConfig{fixture}})
	if err != nil {
		t.Fatal("create local provider fixture")
	}
	identity := domain.FinancialIdentity{
		BookingID: bookingID, UserID: userID, Generation: fixture.Generation,
		ProviderAccount: fixture.ProviderAccount, AmountCents: fixture.AmountCents, Currency: fixture.Currency,
	}
	claim := domain.FinancialClaim{
		PaymentID: paymentID,
		Operation: domain.CreateOperation{PaymentID: paymentID, Identity: identity},
		Refund: &domain.RefundObligation{
			PaymentID: paymentID, Generation: fixture.Generation,
			AmountCents: fixture.AmountCents, Currency: fixture.Currency,
			Key: "ticketbox:payment:" + paymentID.String() + ":g1:refund",
		},
	}
	first, err := provider.CreateFullRefund(context.Background(), claim, fixture.PaymentIntentID)
	if err != nil {
		t.Fatalf("first refund invocation: %v", err)
	}
	second, err := provider.CreateFullRefund(context.Background(), claim, fixture.PaymentIntentID)
	if err != nil {
		t.Fatalf("identical refund replay: %v", err)
	}
	if first == nil || second == nil || first.RefundID != second.RefundID || first.RefundID == "" ||
		first.Status != "succeeded" || second.Status != "succeeded" || first.SucceededRefundCents != fixture.AmountCents ||
		second.SucceededRefundCents != fixture.AmountCents {
		t.Fatalf("refund replay observations differ: first=%+v second=%+v", first, second)
	}
	stats := provider.stats()
	if len(stats.Fixtures) != 1 || stats.Fixtures[0].RefundCalls != 2 || stats.Fixtures[0].RefundEffects != 1 || !stats.Fixtures[0].Refunded {
		t.Fatalf("refund invocation/effect counts=%+v, want calls=2 effects=1 refunded=true", stats)
	}
}

func clearR4EnvironmentForTest(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		if !strings.HasPrefix(upper, "PG") && upper != "DATABASE_URL" && upper != "PAYMENT_DATABASE_URL" &&
			!(strings.HasPrefix(upper, "STRIPE_") && (strings.Contains(upper, "SECRET") || upper == "STRIPE_API_KEY")) {
			continue
		}
		value, present := os.LookupEnv(name)
		_ = os.Unsetenv(name)
		savedName, savedValue, savedPresent := name, value, present
		t.Cleanup(func() {
			if savedPresent {
				_ = os.Setenv(savedName, savedValue)
			} else {
				_ = os.Unsetenv(savedName)
			}
		})
	}
}
