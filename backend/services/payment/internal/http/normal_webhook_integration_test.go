//go:build reliability_normal_http

package payment_http_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/stripe/stripe-go/v84/webhook"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	payment_http "github.com/ticketbox/payment/internal/http"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"go.uber.org/zap"
)

type webhookOwnerManifest struct {
	ManifestVersion        json.RawMessage `json:"manifest_version"`
	ResourceID             string          `json:"resource_id"`
	ContainerID            string          `json:"container_id"`
	Database               string          `json:"database"`
	Host                   string          `json:"host"`
	Port                   int             `json:"port"`
	DSNSHA256              string          `json:"dsn_sha256"`
	RunID                  string          `json:"run_id"`
	OwnerNonce             string          `json:"owner_nonce"`
	ServerSystemIdentifier string          `json:"server_system_identifier"`
	DatabaseMarker         string          `json:"database_marker"`
}

type webhookEvidence struct {
	Status                    string `json:"status"`
	Phase                     string `json:"phase"`
	RunID                     string `json:"runId"`
	SourceSHA256              string `json:"sourceSha256"`
	SourceCommit              string `json:"sourceCommit"`
	SourceTree                string `json:"sourceTree"`
	EvaluatorCommit           string `json:"evaluatorCommit"`
	ExecutableSha256          string `json:"executableSha256"`
	Event1ID                  string `json:"event1Id"`
	Event1Type                string `json:"event1Type"`
	Event1Created             int64  `json:"event1Created"`
	Event1HTTPStatus          int    `json:"event1HttpStatus"`
	RefundID                  string `json:"refundId"`
	PaymentID                 string `json:"paymentId"`
	BookingID                 string `json:"bookingId"`
	PaymentIntentID           string `json:"paymentIntentId"`
	ReceiptsAfterEvent1       int    `json:"receiptsAfterEvent1"`
	AuditsAfterEvent1         int    `json:"verifiedEventAuditsAfterEvent1"`
	DuplicateHTTPStatus       int    `json:"duplicateHttpStatus"`
	ReceiptsAfterDuplicate    int    `json:"receiptsAfterDuplicate"`
	AuditsAfterDuplicate      int    `json:"verifiedEventAuditsAfterDuplicate"`
	Event2ID                  string `json:"event2Id"`
	Event2Type                string `json:"event2Type"`
	Event2Created             int64  `json:"event2Created"`
	Event2HTTPStatus          int    `json:"event2HttpStatus"`
	ReceiptsAfterEvent2       int    `json:"receiptsAfterEvent2"`
	AuditsAfterEvent2         int    `json:"verifiedEventAuditsAfterEvent2"`
	Phase1ReceiptID           string `json:"phase1ReceiptId,omitempty"`
	Phase1AuditID             string `json:"phase1AuditId,omitempty"`
	Phase1ReceivedAt          string `json:"phase1ReceivedAt,omitempty"`
	Phase1AuditRecordedAt     string `json:"phase1AuditRecordedAt,omitempty"`
	Phase1RecordReadBefore    bool   `json:"phase1RecordReadBeforeReplay"`
	RestartRecordPreserved    bool   `json:"restartRecordIdentityPreserved"`
	ReconciliationAfterEvent1 string `json:"reconciliationStateAfterEvent1,omitempty"`
	Phase1RefundState         string `json:"phase1RefundState,omitempty"`
	Phase1RefundProviderID    string `json:"phase1RefundProviderId,omitempty"`
	Phase1RefundStatus        string `json:"phase1RefundProviderStatus,omitempty"`
	ReconciliationAfterEvent2 string `json:"reconciliationStateAfterEvent2,omitempty"`
	OutOfOrderByCreatedAt     bool   `json:"outOfOrderByCreatedAt"`
	DuplicateSuppressed       bool   `json:"duplicateSuppressedByProviderEventIdentity"`
	OutOfOrderPersisted       bool   `json:"outOfOrderEventPersisted"`
	HTTPHandlerPath           string `json:"httpHandlerPath"`
	SignatureVerification     string `json:"signatureVerification"`
	Provider                  string `json:"provider"`
	Phase1ExitCode            int    `json:"phase1ExitCode"`
	Phase1EvidencePath        string `json:"phase1EvidencePath,omitempty"`
	Phase1EvidenceSha256      string `json:"phase1EvidenceSha256,omitempty"`
	Composition               string `json:"composition"`
	FormalEvidence            bool   `json:"formalEvidence"`
	Boundary                  string `json:"boundary"`
}

func webhookDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	dsn := os.Getenv("REFUND_HTTP_DATABASE_URL")
	manifestPath := os.Getenv("REFUND_HTTP_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" {
		t.Fatal("REFUND_HTTP_DATABASE_URL and REFUND_HTTP_OWNER_MANIFEST are required")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest webhookOwnerManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	marker := "codex-refund-component:v1:" + manifest.ResourceID + ":" + manifest.RunID + ":" + manifest.OwnerNonce
	if manifest.ResourceID == "" || manifest.ContainerID != manifest.ResourceID ||
		manifest.Database == "" || manifest.DatabaseMarker != marker ||
		manifest.ServerSystemIdentifier == "" {
		t.Fatal("refund owner manifest is incomplete or unbound")
	}
	sum := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(sum[:]) != manifest.DSNSHA256 {
		t.Fatal("refund DSN does not match owner manifest")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(parsed.Port())
	if parsed.Hostname() != manifest.Host || port != manifest.Port ||
		strings.TrimPrefix(parsed.Path, "/") != manifest.Database {
		t.Fatal("refund DSN endpoint differs from owner manifest")
	}
	setupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(setupCtx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var database, systemID, databaseMarker string
	var versionNum int
	if err := admin.QueryRow(setupCtx, `SELECT current_setting('server_version_num')::integer,
		current_database(), system_identifier::text,
		COALESCE(shobj_description(oid,'pg_database'),'')
		FROM pg_database CROSS JOIN LATERAL pg_control_system()
		WHERE datname=current_database()`).Scan(&versionNum, &database, &systemID, &databaseMarker); err != nil {
		t.Fatal(err)
	}
	if versionNum < 160000 || versionNum >= 170000 || database != manifest.Database ||
		systemID != manifest.ServerSystemIdentifier || databaseMarker != manifest.DatabaseMarker {
		t.Fatal("refund database identity differs from owner manifest")
	}
	if _, err := admin.Exec(setupCtx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := strings.TrimSpace(os.Getenv("REFUND_HTTP_SCHEMA"))
	if schema == "" {
		t.Fatal("REFUND_HTTP_SCHEMA is required for the restart phases")
	}
	for _, r := range schema {
		if r != '_' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			t.Fatal("REFUND_HTTP_SCHEMA is invalid")
		}
	}
	if _, err := admin.Exec(setupCtx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolated := parsed.String()
	migrate, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("migrate CLI is not on PATH")
	}
	migrationPath := filepath.ToSlash(filepath.Join("..", "..", "migrations"))
	cmd := exec.CommandContext(setupCtx, migrate, "-path", migrationPath, "-database", isolated, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		safe := strings.ReplaceAll(string(output), isolated, "[isolated DSN]")
		t.Fatalf("payment migrations failed: %s", safe)
	}
	config, err := pgxpool.ParseConfig(isolated)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(setupCtx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, context.Background()
}

func seedWebhookPayment(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *repository.PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := domain.CreateOperation{
		PaymentID: uuid.New(),
		Key:       "",
		Identity: domain.FinancialIdentity{BookingID: uuid.New(), UserID: uuid.New(),
			Generation: 1, ProviderAccount: "acct_fixture_test", AmountCents: 725, Currency: "usd"},
		FrozenParams: json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`),
	}
	op.Key = "ticketbox:payment:" + op.PaymentID.String() + ":g1:create"
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, op.FrozenParams)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded",
		IntentID: "pi_normal_http", PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
		Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='succeeded',
		refund_provider_id='re_normal_http_1', refund_provider_status='succeeded'
		WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	return op
}

func refundEventBody(eventID, eventType, refundID, status string, created int64,
	paymentID, bookingID uuid.UUID, amount int64) []byte {
	object := map[string]any{
		"id": refundID, "object": "refund", "status": status, "amount": amount,
		"currency": "usd", "livemode": false, "payment_intent": "pi_normal_http",
		"metadata": map[string]string{
			"ticketbox_payment_id": paymentID.String(),
			"ticketbox_booking_id": bookingID.String(),
			"ticketbox_generation": "1",
		},
	}
	body, _ := json.Marshal(map[string]any{
		"id": eventID, "object": "event", "type": eventType,
		"livemode": false, "account": "acct_fixture_test", "created": created,
		"api_version": "2026-02-25.clover",
		"data":        map[string]any{"object": object},
	})
	return body
}

func signedWebhook(t *testing.T, body []byte) (*webhook.SignedPayload, string) {
	t.Helper()
	secret := strings.TrimSpace(os.Getenv("REFUND_HTTP_WEBHOOK_SECRET"))
	if secret == "" {
		t.Fatal("REFUND_HTTP_WEBHOOK_SECRET is required")
	}
	return webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
		Payload: body, Secret: secret,
	}), secret
}

func postWebhook(t *testing.T, server *httptest.Server, body []byte, signature string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/webhooks/stripe", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Stripe-Signature", signature)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func newWebhookServer(secret string, pool *pgxpool.Pool) *httptest.Server {
	client := stripego.NewClient("sk_test_fixture")
	gateway := stripegateway.NewStripePaymentGateway(zap.NewNop(), secret, nil, client, nil)
	gateway.SetWebhookScope("acct_fixture_test", false)
	repo := repository.NewPostgresPaymentRepository(pool)
	service := service.NewPaymentService(repo, zap.NewNop(), gateway)
	handler := payment_http.NewPaymentHttpHandler(service, zap.NewNop(), gateway)
	mux := http.NewServeMux()
	mux.Handle("/webhooks/stripe", http.HandlerFunc(handler.WebhookHandler))
	return httptest.NewServer(mux)
}

type webhookRecordFact struct {
	ReceiptID            string
	AuditID              string
	ReceivedAt           string
	AuditRecordedAt      string
	CorrelationState     string
	RelayState           string
	Reconciliation       string
	RefundState          string
	RefundProviderID     string
	RefundProviderStatus string
}

func readWebhookRecord(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	eventID string, paymentID uuid.UUID) webhookRecordFact {
	t.Helper()
	var fact webhookRecordFact
	var receivedAt, auditRecordedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT id::text, received_at, correlation_state, relay_state
		FROM payment_provider_events WHERE event_id=$1`, eventID).
		Scan(&fact.ReceiptID, &receivedAt, &fact.CorrelationState, &fact.RelayState); err != nil {
		t.Fatalf("persisted receipt unavailable for %s: %v", eventID, err)
	}
	if err := pool.QueryRow(ctx, `SELECT record_id::text, recorded_at
		FROM payment_refund_audit WHERE record_kind='VERIFIED_EVENT' AND provider_event_id=$1`, eventID).
		Scan(&fact.AuditID, &auditRecordedAt); err != nil {
		t.Fatalf("persisted audit unavailable for %s: %v", eventID, err)
	}
	if err := pool.QueryRow(ctx, `SELECT reconciliation_state,
		COALESCE(refund_state,''), COALESCE(refund_provider_id,''),
		COALESCE(refund_provider_status,'') FROM payments WHERE id=$1`, paymentID).
		Scan(&fact.Reconciliation, &fact.RefundState, &fact.RefundProviderID,
			&fact.RefundProviderStatus); err != nil {
		t.Fatalf("payment financial state unavailable: %v", err)
	}
	if receivedAt == nil || auditRecordedAt == nil {
		t.Fatalf("persisted timestamps missing for %s", eventID)
	}
	fact.ReceivedAt = receivedAt.UTC().Format(time.RFC3339Nano)
	fact.AuditRecordedAt = auditRecordedAt.UTC().Format(time.RFC3339Nano)
	return fact
}

func webhookRecordMatches(expected, actual webhookRecordFact) bool {
	return expected.ReceiptID != "" && expected.AuditID != "" &&
		expected.ReceiptID == actual.ReceiptID && expected.AuditID == actual.AuditID &&
		expected.ReceivedAt == actual.ReceivedAt &&
		expected.AuditRecordedAt == actual.AuditRecordedAt
}

func webhookFinancialStateMatches(actual webhookRecordFact, state, providerID, status string) bool {
	return actual.RefundState == state && actual.RefundProviderID == providerID &&
		actual.RefundProviderStatus == status
}

func TestWebhookRestartRecordOracle(t *testing.T) {
	expected := webhookRecordFact{ReceiptID: "r1", AuditID: "a1",
		ReceivedAt: "2026-11-17T00:00:00Z", AuditRecordedAt: "2026-11-17T00:00:01Z"}
	if !webhookRecordMatches(expected, expected) {
		t.Fatal("identical persisted record must match")
	}
	if webhookRecordMatches(expected, webhookRecordFact{ReceiptID: "r2", AuditID: expected.AuditID,
		ReceivedAt: expected.ReceivedAt, AuditRecordedAt: expected.AuditRecordedAt}) {
		t.Fatal("recreated receipt ID must fail")
	}
	if webhookRecordMatches(expected, webhookRecordFact{ReceiptID: expected.ReceiptID, AuditID: "a2",
		ReceivedAt: expected.ReceivedAt, AuditRecordedAt: expected.AuditRecordedAt}) {
		t.Fatal("recreated audit ID must fail")
	}
	if webhookRecordMatches(webhookRecordFact{}, expected) {
		t.Fatal("missing phase-1 identity must fail")
	}
}

func webhookCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) (int, int) {
	t.Helper()
	var receipts, audits int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_provider_events WHERE event_id=$1`, eventID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_refund_audit WHERE record_kind='VERIFIED_EVENT' AND provider_event_id=$1`, eventID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	return receipts, audits
}

func TestNormalWebhookHandlerPersistsAcrossProcessRestart(t *testing.T) {
	phase := strings.TrimSpace(os.Getenv("REFUND_HTTP_PHASE"))
	if phase != "1" && phase != "2" {
		t.Fatal("REFUND_HTTP_PHASE must be 1 or 2")
	}
	sourceSHA := strings.TrimSpace(os.Getenv("REFUND_HTTP_SOURCE_SHA256"))
	sourceCommit := strings.TrimSpace(os.Getenv("REFUND_HTTP_SOURCE_COMMIT"))
	sourceTree := strings.TrimSpace(os.Getenv("REFUND_HTTP_SOURCE_TREE"))
	runID := strings.TrimSpace(os.Getenv("REFUND_HTTP_RUN_ID"))
	evidenceDir := strings.TrimSpace(os.Getenv("REFUND_HTTP_EVIDENCE_DIR"))
	if len(sourceSHA) != 64 || len(sourceCommit) != 40 || len(sourceTree) != 40 ||
		runID == "" || evidenceDir == "" {
		t.Fatal("source identity, run ID and evidence directory are required")
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	pool, ctx := webhookDB(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	evidence := webhookEvidence{
		Status: "RUNNING", Phase: phase, RunID: runID,
		SourceSHA256: sourceSHA, SourceCommit: sourceCommit, SourceTree: sourceTree,
		EvaluatorCommit:       os.Getenv("REFUND_HTTP_EVALUATOR_COMMIT"),
		ExecutableSha256:      executableSHA256(t),
		HTTPHandlerPath:       "payment_http.NewPaymentHttpHandler/WebhookHandler",
		SignatureVerification: "STRIPE_SDK_CONSTRUCT_EVENT_WITH_LOCAL_SECRET",
		Provider:              "LOCAL_HTTP_REPLACEMENT_NOT_STRIPE",
		Composition:           "REAL_HTTP_HANDLER_AND_SERVICE_WITH_LOCAL_SECRET_PROVIDER",
		FormalEvidence:        false,
		Boundary: "A real HTTP listener invokes the production Payment webhook handler, verifies a locally signed payload " +
			"with the production Stripe SDK verifier, persists the provider receipt and verified-event audit in PG16, and " +
			"survives a process restart. The secret and provider are synthetic; no Stripe request is made.",
	}
	phase1Path := filepath.Join(evidenceDir, "webhook-phase1-"+hex.EncodeToString(
		sha256Sum([]byte(runID))[:6])+".json")
	evidence.Phase1EvidencePath = phase1Path

	if phase == "1" {
		op := seedWebhookPayment(t, ctx, pool, repo)
		evidence.PaymentID = op.PaymentID.String()
		evidence.BookingID = op.Identity.BookingID.String()
		evidence.PaymentIntentID = "pi_normal_http"
		eventID := "evt_normal_http_" + hex.EncodeToString(sha256Sum([]byte(runID))[:6])
		created := time.Now().Add(-10 * time.Second).Unix()
		body := refundEventBody(eventID, "refund.updated", "re_normal_http_1", "succeeded",
			created, op.PaymentID, op.Identity.BookingID, 725)
		signed, secret := signedWebhook(t, body)
		server := newWebhookServer(secret, pool)
		evidence.Event1HTTPStatus = postWebhook(t, server, body, signed.Header)
		server.Close()
		evidence.Event1ID, evidence.Event1Type, evidence.Event1Created = eventID, "refund.updated", created
		evidence.RefundID = "re_normal_http_1"
		evidence.ReceiptsAfterEvent1, evidence.AuditsAfterEvent1 = webhookCounts(t, ctx, pool, eventID)
		record := readWebhookRecord(t, ctx, pool, eventID, op.PaymentID)
		evidence.Phase1ReceiptID = record.ReceiptID
		evidence.Phase1AuditID = record.AuditID
		evidence.Phase1ReceivedAt = record.ReceivedAt
		evidence.Phase1AuditRecordedAt = record.AuditRecordedAt
		evidence.ReconciliationAfterEvent1 = record.Reconciliation
		evidence.Phase1RefundState = record.RefundState
		evidence.Phase1RefundProviderID = record.RefundProviderID
		evidence.Phase1RefundStatus = record.RefundProviderStatus
		evidence.Phase1ExitCode = 137
		evidence.Status = "NORMAL_WEBHOOK_PHASE1_PERSISTED_EXIT"
		writeWebhookEvidence(t, phase1Path, evidence)
		os.Exit(137)
	}

	raw, err := os.ReadFile(phase1Path)
	if err != nil {
		t.Fatalf("phase 1 evidence unavailable: %v", err)
	}
	var phase1 webhookEvidence
	if err := json.Unmarshal(raw, &phase1); err != nil {
		t.Fatal(err)
	}
	evidence.Phase1EvidenceSha256 = hex.EncodeToString(sha256Sum(raw))
	if phase1.Status != "NORMAL_WEBHOOK_PHASE1_PERSISTED_EXIT" ||
		phase1.Event1HTTPStatus != http.StatusOK || phase1.ReceiptsAfterEvent1 != 1 ||
		phase1.AuditsAfterEvent1 != 1 || phase1.SourceSHA256 != sourceSHA ||
		phase1.SourceCommit != sourceCommit || phase1.SourceTree != sourceTree ||
		phase1.ExecutableSha256 != evidence.ExecutableSha256 {
		t.Fatalf("phase 1 did not persist one exact source-bound receipt: %+v", phase1)
	}
	evidence.PaymentID = phase1.PaymentID
	evidence.BookingID = phase1.BookingID
	evidence.PaymentIntentID = phase1.PaymentIntentID
	evidence.Event1ID = phase1.Event1ID
	evidence.Event1Type = phase1.Event1Type
	evidence.Event1Created = phase1.Event1Created
	evidence.Event1HTTPStatus = phase1.Event1HTTPStatus
	evidence.RefundID = phase1.RefundID
	evidence.ReceiptsAfterEvent1 = phase1.ReceiptsAfterEvent1
	evidence.AuditsAfterEvent1 = phase1.AuditsAfterEvent1
	evidence.Phase1ExitCode = phase1.Phase1ExitCode
	paymentID, _ := uuid.Parse(phase1.PaymentID)
	bookingID, _ := uuid.Parse(phase1.BookingID)
	if phase1.Phase1RefundState != "succeeded" ||
		phase1.Phase1RefundProviderID != "re_normal_http_1" ||
		phase1.Phase1RefundStatus != "succeeded" {
		t.Fatalf("phase 1 changed the seeded terminal refund state: %+v", phase1)
	}
	expectedPhase1 := webhookRecordFact{ReceiptID: phase1.Phase1ReceiptID, AuditID: phase1.Phase1AuditID,
		ReceivedAt: phase1.Phase1ReceivedAt, AuditRecordedAt: phase1.Phase1AuditRecordedAt}
	beforeReplay := readWebhookRecord(t, ctx, pool, phase1.Event1ID, paymentID)
	if !webhookRecordMatches(expectedPhase1, beforeReplay) {
		t.Fatalf("phase 1 record was missing or recreated before replay: before=%+v", beforeReplay)
	}
	evidence.Phase1RecordReadBefore = true
	evidence.Phase1ReceiptID = phase1.Phase1ReceiptID
	evidence.Phase1AuditID = phase1.Phase1AuditID
	evidence.Phase1ReceivedAt = phase1.Phase1ReceivedAt
	evidence.Phase1AuditRecordedAt = phase1.Phase1AuditRecordedAt
	evidence.ReconciliationAfterEvent1 = phase1.ReconciliationAfterEvent1
	evidence.Phase1RefundState = phase1.Phase1RefundState
	evidence.Phase1RefundProviderID = phase1.Phase1RefundProviderID
	evidence.Phase1RefundStatus = phase1.Phase1RefundStatus
	body := refundEventBody(phase1.Event1ID, phase1.Event1Type, phase1.RefundID, "succeeded",
		phase1.Event1Created, paymentID, bookingID, 725)
	signed, secret := signedWebhook(t, body)
	server := newWebhookServer(secret, pool)
	evidence.DuplicateHTTPStatus = postWebhook(t, server, body, signed.Header)
	evidence.ReceiptsAfterDuplicate, evidence.AuditsAfterDuplicate = webhookCounts(t, ctx, pool, phase1.Event1ID)
	afterDuplicate := readWebhookRecord(t, ctx, pool, phase1.Event1ID, paymentID)
	evidence.RestartRecordPreserved = webhookRecordMatches(expectedPhase1, afterDuplicate) &&
		webhookFinancialStateMatches(afterDuplicate, phase1.Phase1RefundState,
			phase1.Phase1RefundProviderID, phase1.Phase1RefundStatus) &&
		afterDuplicate.Reconciliation == evidence.ReconciliationAfterEvent1
	evidence.DuplicateSuppressed = evidence.ReceiptsAfterDuplicate == phase1.ReceiptsAfterEvent1 &&
		evidence.AuditsAfterDuplicate == phase1.AuditsAfterEvent1 && evidence.RestartRecordPreserved

	secondID := "evt_normal_http_created_" + hex.EncodeToString(sha256Sum([]byte(runID))[:6])
	event2Created := phase1.Event1Created - 60
	secondBody := refundEventBody(secondID, "refund.created", phase1.RefundID, "pending",
		event2Created, paymentID, bookingID, 725)
	secondSigned, _ := signedWebhook(t, secondBody)
	evidence.Event2ID, evidence.Event2Type, evidence.Event2Created = secondID, "refund.created", event2Created
	evidence.Event2HTTPStatus = postWebhook(t, server, secondBody, secondSigned.Header)
	evidence.ReceiptsAfterEvent2, evidence.AuditsAfterEvent2 = webhookCounts(t, ctx, pool, secondID)
	event2Record := readWebhookRecord(t, ctx, pool, secondID, paymentID)
	evidence.ReconciliationAfterEvent2 = event2Record.Reconciliation
	evidence.OutOfOrderByCreatedAt = event2Created < phase1.Event1Created
	evidence.OutOfOrderPersisted = evidence.OutOfOrderByCreatedAt &&
		evidence.ReceiptsAfterEvent2 == 1 && evidence.AuditsAfterEvent2 == 1 &&
		event2Record.Reconciliation == phase1.ReconciliationAfterEvent1 &&
		webhookFinancialStateMatches(event2Record, phase1.Phase1RefundState,
			phase1.Phase1RefundProviderID, phase1.Phase1RefundStatus)
	server.Close()
	if evidence.DuplicateHTTPStatus != http.StatusOK || !evidence.DuplicateSuppressed ||
		!evidence.RestartRecordPreserved || evidence.Event2HTTPStatus != http.StatusOK ||
		!evidence.OutOfOrderPersisted {
		t.Fatalf("webhook restart did not prove duplicate/out-of-order persistence: %+v", evidence)
	}
	evidence.Status = "NORMAL_WEBHOOK_RESTART_PERSISTED"
	writeWebhookEvidence(t, filepath.Join(evidenceDir, "webhook-phase2-"+hex.EncodeToString(
		sha256Sum([]byte(runID))[:6])+".json"), evidence)
}

func executableSHA256(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(sha256Sum(data))
}

func writeWebhookEvidence(t *testing.T, path string, evidence webhookEvidence) {
	t.Helper()
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

var _ = fmt.Sprintf
