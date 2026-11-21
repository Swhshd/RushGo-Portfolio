//go:build recoverycomponents

package service

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
)

const (
	refundProvenanceProcessChildEnv = "REFUND_PROVENANCE_PROCESS_CHILD"
	refundProvenanceChildPrepared   = "prepared"
	refundProvenanceChildProcessed  = "fixture_processed"
	refundProvenanceFixtureTable    = "refund_provenance_provider_double_effects"
)

var refundProvenanceWorkerSchemaPattern = regexp.MustCompile(`^refund_worker_[a-f0-9]{32}$`)

// TestRefundProvenanceProcessCrashRecoveryIntegration kills two owned child
// processes at durable boundaries. The provider is a local synthetic double;
// its deduplication fixture says nothing about Stripe behavior.
func TestRefundProvenanceProcessCrashRecoveryIntegration(t *testing.T) {
	if scenario := os.Getenv(refundProvenanceProcessChildEnv); scenario != "" {
		runRefundProvenanceCrashChild(t, scenario)
		return
	}

	// Validate the selected component PG identity in the parent before starting
	// children. Each child independently repeats this gate before opening a schema.
	validated := refundWorkerManifestedDatabase(t)
	_ = validated
	baseDSN := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal("BLOCKED: manifested refund database URL is invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatal("BLOCKED: cannot connect to manifested refund database")
	}
	defer admin.Close()

	t.Run("committed preparation then process exit before Create", func(t *testing.T) {
		result := runOwnedRefundProvenanceChild(t, refundProvenanceChildPrepared)
		t.Cleanup(func() {
			if err := dropOwnedRefundProvenanceSchema(t, context.Background(), admin, result.schema); err != nil {
				t.Errorf("cannot remove only the test-owned terminated-child schema: %v", err)
			}
		})
		schemaPool := refundProvenancePoolForProcessSchema(t, ctx, parsed, result.schema)
		defer schemaPool.Close()
		assertRefundProvenanceAdmissionActive(t, ctx, schemaPool, "prepared_after_crash")
		var prepared, response int
		if err := schemaPool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE record_kind='PREPARED'),
			count(*) FILTER (WHERE record_kind='RESPONSE') FROM payment_refund_audit
			WHERE application_attempt_id=$1`, result.attemptID).Scan(&prepared, &response); err != nil {
			t.Fatal("cannot inspect committed preparation after child process termination")
		}
		if prepared != 1 || response != 0 {
			t.Fatalf("post-crash audit rows for attempt %s: PREPARED=%d RESPONSE=%d; want 1/0", result.attemptID, prepared, response)
		}
		recovered, capture := recoverRefundProvenanceAfterCrash(t, ctx, schemaPool, result, false)
		assertRefundProvenanceAdmissionActive(t, ctx, schemaPool, "prepared_after_recovery")
		if capture.AttemptID == uuid.Nil || capture.AttemptID == result.attemptID {
			t.Fatalf("fresh retry attempt ID=%s; original crashed attempt=%s", capture.AttemptID, result.attemptID)
		}
		if recovered.Key != result.stableKey || recovered.Refund == nil || recovered.Refund.Key != result.stableKey {
			t.Fatalf("recovery changed the frozen logical refund key: claim=%+v original=%q", recovered, result.stableKey)
		}
		var effectRows, effectCalls int
		if err := schemaPool.QueryRow(ctx, `SELECT count(*),COALESCE(max(process_count),0) FROM `+refundProvenanceFixtureTable+`
			WHERE idempotency_key=$1`, result.stableKey).Scan(&effectRows, &effectCalls); err != nil {
			t.Fatal("cannot inspect retry provider-double effect")
		}
		if effectRows != 1 || effectCalls != 1 {
			t.Fatalf("retry double result rows=%d calls=%d; want one synthetic effect from one Create", effectRows, effectCalls)
		}
		assertRefundProvenanceAttemptAudit(t, ctx, schemaPool, result.attemptID, 1, 0)
		assertRefundProvenanceAttemptAudit(t, ctx, schemaPool, capture.AttemptID, 1, 1)
		var commandDigests, keyDigests int
		if err := schemaPool.QueryRow(ctx, `SELECT count(DISTINCT encode(command_digest,'hex')),
			count(DISTINCT encode(idempotency_key_digest,'hex')) FROM payment_refund_audit
			WHERE payment_id=$1 AND record_kind='PREPARED'`, result.paymentID).Scan(&commandDigests, &keyDigests); err != nil {
			t.Fatal("cannot compare frozen logical command identities across application attempts")
		}
		if commandDigests != 1 || keyDigests != 1 {
			t.Fatalf("legal retry command identity changed: distinct command digests=%d key digests=%d; want 1/1", commandDigests, keyDigests)
		}
	})

	t.Run("local fixture processes stable-key refund before response persistence", func(t *testing.T) {
		result := runOwnedRefundProvenanceChild(t, refundProvenanceChildProcessed)
		t.Cleanup(func() {
			if err := dropOwnedRefundProvenanceSchema(t, context.Background(), admin, result.schema); err != nil {
				t.Errorf("cannot remove only the test-owned terminated-child schema: %v", err)
			}
		})
		schemaPool := refundProvenancePoolForProcessSchema(t, ctx, parsed, result.schema)
		defer schemaPool.Close()
		assertRefundProvenanceAdmissionActive(t, ctx, schemaPool, "processed_after_crash")
		var effects, fixtureCalls int
		if err := schemaPool.QueryRow(ctx, `SELECT count(*), COALESCE(max(process_count),0)
			FROM `+refundProvenanceFixtureTable+` WHERE idempotency_key=$1`, result.stableKey).Scan(&effects, &fixtureCalls); err != nil {
			t.Fatal("cannot inspect local provider-double fixture after child process termination")
		}
		if effects != 1 || fixtureCalls != 2 {
			t.Fatalf("SYNTHETIC_PROVIDER_DOUBLE fixture rows=%d calls=%d; want one deduped effect from two same-key double calls", effects, fixtureCalls)
		}
		var prepared, response int
		if err := schemaPool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE record_kind='PREPARED'),
			count(*) FILTER (WHERE record_kind='RESPONSE') FROM payment_refund_audit
			WHERE application_attempt_id=$1`, result.attemptID).Scan(&prepared, &response); err != nil {
			t.Fatal("cannot inspect response audit absence after child process termination")
		}
		if prepared != 1 || response != 0 {
			t.Fatalf("post-crash audit rows for attempt %s: PREPARED=%d RESPONSE=%d; want 1/0", result.attemptID, prepared, response)
		}
		recovered, _ := recoverRefundProvenanceAfterCrash(t, ctx, schemaPool, result, true)
		assertRefundProvenanceAdmissionActive(t, ctx, schemaPool, "processed_after_recovery")
		if recovered.Key != result.stableKey || recovered.Refund == nil || recovered.Refund.Key != result.stableKey {
			t.Fatalf("existing synthetic result recovery changed the frozen logical refund key: claim=%+v original=%q", recovered, result.stableKey)
		}
		if recovered.Attempts < 1 {
			t.Fatalf("recovery claim did not advance the owned financial attempt: %+v", recovered)
		}
		var state, refundID string
		if err := schemaPool.QueryRow(ctx, `SELECT refund_state,refund_provider_id FROM payments WHERE id=$1`, result.paymentID).Scan(&state, &refundID); err != nil {
			t.Fatal("cannot inspect financial result after local fixture recovery")
		}
		var fixtureRefundID string
		if err := schemaPool.QueryRow(ctx, `SELECT refund_id FROM `+refundProvenanceFixtureTable+` WHERE idempotency_key=$1`, result.stableKey).Scan(&fixtureRefundID); err != nil {
			t.Fatal("cannot read the existing synthetic refund identity")
		}
		if state != "succeeded" || refundID != fixtureRefundID {
			t.Fatalf("synthetic inspected result did not finish existing financial state: state=%q refund_id=%q expected=%q", state, refundID, fixtureRefundID)
		}
		var finalPrepared, finalResponse int
		if err := schemaPool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE record_kind='PREPARED'),
			count(*) FILTER (WHERE record_kind='RESPONSE') FROM payment_refund_audit
			WHERE application_attempt_id=$1`, result.attemptID).Scan(&finalPrepared, &finalResponse); err != nil {
			t.Fatal("cannot confirm original response remains unrecorded after synthetic recovery")
		}
		if finalPrepared != 1 || finalResponse != 0 {
			t.Fatalf("crashed attempt was rewritten during recovery: PREPARED=%d RESPONSE=%d; want 1/0", finalPrepared, finalResponse)
		}
	})
}

type refundProvenanceChildResult struct {
	schema           string
	paymentID        uuid.UUID
	attemptID        uuid.UUID
	stableKey        string
	claimTokenDigest string
	claimFence       int64
}

func runOwnedRefundProvenanceChild(t *testing.T, scenario string) refundProvenanceChildResult {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRefundProvenanceProcessCrashRecoveryIntegration$", "-test.v")
	cmd.Env = replaceRefundProvenanceEnv(os.Environ(), refundProvenanceProcessChildEnv, scenario)
	diagnostic := &refundProvenanceChildDiagnostic{}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal("cannot open child stdout pipe")
	}
	// The go test child reports setup failures on stdout as well as stderr.
	// Export only finite categories, never its DSN, environment or provider data.
	cmd.Stderr = diagnostic
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start owned Payment child process: %v", err)
	}
	ownedProcess := cmd.Process
	if ownedProcess == nil || ownedProcess.Pid <= 0 {
		t.Fatal("child process did not expose its owned process handle")
	}
	waitForExit := func() {
		t.Helper()
		if cmd.ProcessState == nil {
			if err := ownedProcess.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("cannot terminate the original owned child process handle: %v", err)
			}
			_ = cmd.Wait()
		}
	}
	t.Cleanup(waitForExit)
	reader := bufio.NewReader(stdout)
	readMarker := func(prefix string) string {
		t.Helper()
		deadline := time.NewTimer(45 * time.Second)
		defer deadline.Stop()
		for {
			line := make(chan string, 1)
			readErr := make(chan error, 1)
			go func() {
				value, err := reader.ReadString('\n')
				line <- value
				readErr <- err
			}()
			select {
			case value := <-line:
				err := <-readErr
				trimmed := strings.TrimSpace(value)
				if strings.HasPrefix(trimmed, prefix+" ") {
					return trimmed
				}
				_, _ = diagnostic.Write([]byte(value))
				if err != nil {
					t.Fatalf("child exited before its %s boundary marker: %v; diagnostic=%s", prefix, err, diagnostic.String())
				}
			case <-deadline.C:
				t.Fatalf("owned child process did not reach %q boundary; diagnostic=%s", scenario, diagnostic.String())
				return ""
			}
		}
	}
	schemaLine := readMarker("SCHEMA")
	schema := strings.TrimPrefix(schemaLine, "SCHEMA ")
	if !refundProvenanceWorkerSchemaPattern.MatchString(schema) {
		t.Fatalf("child reported an unexpected disposable schema name %q", schema)
	}
	wantMarker := "PREPARED"
	if scenario == refundProvenanceChildProcessed {
		wantMarker = "PROCESSED"
	}
	boundary := strings.Fields(readMarker(wantMarker))
	if len(boundary) != 5 {
		t.Fatalf("unexpected child boundary marker %q", strings.Join(boundary, " "))
	}
	if boundary[0] != wantMarker {
		t.Fatalf("child boundary marker=%q; want %q", boundary[0], wantMarker)
	}
	paymentID, err := uuid.Parse(boundary[1])
	if err != nil {
		t.Fatal("child boundary marker has invalid Payment identity")
	}
	attemptID, err := uuid.Parse(boundary[2])
	if err != nil {
		t.Fatal("child boundary marker has invalid application attempt identity")
	}
	stableKey := fmt.Sprintf("ticketbox:payment:%s:g1:refund", paymentID)
	claimTokenDigest := boundary[3]
	if len(claimTokenDigest) != 64 {
		t.Fatal("child boundary marker has invalid claim-token digest")
	}
	claimFence, err := strconv.ParseInt(boundary[4], 10, 64)
	if err != nil || claimFence < 1 {
		t.Fatal("child boundary marker has invalid claim fence")
	}

	// Terminate using the Process value returned by Start. The numeric PID is
	// recorded only for audit output and is never used as the kill target.
	if err := ownedProcess.Kill(); err != nil {
		t.Fatalf("cannot kill the original owned child-process handle (pid=%d): %v", ownedProcess.Pid, err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child unexpectedly exited cleanly before controlled termination")
	}
	t.Logf("terminated owned child through original process handle (pid=%d), scenario=%s", ownedProcess.Pid, scenario)
	return refundProvenanceChildResult{schema: schema, paymentID: paymentID, attemptID: attemptID, stableKey: stableKey, claimTokenDigest: claimTokenDigest, claimFence: claimFence}
}

// It is intentionally not a raw log buffer: child failures may contain a
// connection URL or credentials. The fixed category set also bounds memory.
type refundProvenanceChildDiagnostic struct {
	mu   sync.Mutex
	tail string
	seen [4]bool
}

func (d *refundProvenanceChildDiagnostic) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, b := range p {
		d.tail += string(b)
		if len(d.tail) > 128 {
			d.tail = d.tail[len(d.tail)-128:]
		}
		for i, match := range []string{"uuid_generate_v4", "Payment migrations failed", "BLOCKED:", "--- FAIL:"} {
			if strings.Contains(d.tail, match) {
				d.seen[i] = true
			}
		}
	}
	return len(p), nil
}

func (d *refundProvenanceChildDiagnostic) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var codes []string
	for i, code := range []string{"MIGRATION_UUID_FUNCTION_UNAVAILABLE", "PAYMENT_MIGRATION_FAILED", "COMPONENT_IDENTITY_BLOCKED", "CHILD_TEST_FAILED"} {
		if d.seen[i] {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "CHILD_EXIT_WITHOUT_RECOGNIZED_DIAGNOSTIC"
	}
	return strings.Join(codes, ",")
}

func TestRefundProvenanceChildDiagnosticIsBoundedAndSecretSafe(t *testing.T) {
	d := &refundProvenanceChildDiagnostic{}
	_, _ = d.Write([]byte("Payment migrations failed: postgres://user:secret@127.0.0.1/db function uuid_"))
	_, _ = d.Write([]byte("generate_v4() does not exist; --- FAIL: fixture\n"))
	want := "MIGRATION_UUID_FUNCTION_UNAVAILABLE,PAYMENT_MIGRATION_FAILED,CHILD_TEST_FAILED"
	if got := d.String(); got != want {
		t.Fatalf("safe child diagnostic=%q; want %q", got, want)
	}
	_, _ = d.Write([]byte(strings.Repeat("secret", 100000)))
	if len(d.tail) > 128 || d.String() != want {
		t.Fatal("child diagnostic lost bounded category-only behavior")
	}
}

func runRefundProvenanceCrashChild(t *testing.T, scenario string) {
	pool := refundWorkerManifestedDatabase(t)
	defer pool.Close()
	searchPath := pool.Config().ConnConfig.RuntimeParams["search_path"]
	schema := strings.Split(searchPath, ",")[0]
	fmt.Fprintf(os.Stdout, "SCHEMA %s\n", schema)

	ctx := context.Background()
	repo := repository.NewPostgresPaymentRepository(pool)
	op := seedRefundWorkerPayment(t, ctx, repo)
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal("activate Payment admission mode in isolated refund child:", err)
	}
	claim := claimRefundProvenanceWorker(t, ctx, repo, op.PaymentID)
	provider := &refundProvenanceCrashProvider{pool: pool, mode: scenario}
	workerRepo := repository.FinancialRepository(repo)
	if scenario == refundProvenanceChildPrepared {
		workerRepo = &refundProvenancePreparedBarrierRepository{FinancialRepository: repo}
	} else if scenario != refundProvenanceChildProcessed {
		t.Fatalf("unknown refund process-child scenario %q", scenario)
	}
	worker := &FinancialWorker{repo: workerRepo, provider: provider}
	if err := worker.executeRefund(ctx, claim); err != nil {
		t.Fatalf("actual Payment FinancialWorker refund execution failed before boundary: %v", err)
	}
	t.Fatal("controlled refund boundary returned instead of remaining killable")
}

func claimRefundProvenanceWorker(t *testing.T, ctx context.Context, repo *repository.PostgresPaymentRepository, paymentID uuid.UUID) domain.FinancialClaim {
	t.Helper()
	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Kind != "refund" || claims[0].PaymentID != paymentID || claims[0].Refund == nil {
		t.Fatalf("refund child claim count=%d err=%v; want one claim for %s", len(claims), err, paymentID)
	}
	return claims[0]
}

func recoverRefundProvenanceAfterCrash(t *testing.T, ctx context.Context, pool *pgxpool.Pool, crashed refundProvenanceChildResult, effectAlreadyProcessed bool) (domain.FinancialClaim, *domain.RefundResponseCapture) {
	t.Helper()
	var token uuid.UUID
	var fence int64
	if err := pool.QueryRow(ctx, `SELECT claim_token,claim_fence FROM payments WHERE id=$1`, crashed.paymentID).Scan(&token, &fence); err != nil {
		t.Fatal("cannot inspect crashed worker ownership before expiring its own lease")
	}
	if refundProvenanceClaimTokenDigest(token) != crashed.claimTokenDigest || fence != crashed.claimFence {
		t.Fatalf("payment claim changed before recovery: token digest mismatch=%v fence=%d; child owned fence=%d", refundProvenanceClaimTokenDigest(token) != crashed.claimTokenDigest, fence, crashed.claimFence)
	}
	updated, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp()
		WHERE id=$1 AND claim_token=$2 AND claim_fence=$3 AND operation_kind='refund'`, crashed.paymentID, token, crashed.claimFence)
	if err != nil || updated.RowsAffected() != 1 {
		t.Fatalf("could not expire only the terminated child's refund claim: rows=%d err=%v", updated.RowsAffected(), err)
	}
	repo := repository.NewPostgresPaymentRepository(pool)
	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].PaymentID != crashed.paymentID || claims[0].Kind != "refund" || claims[0].Refund == nil {
		t.Fatalf("fresh worker recovery claim count=%d err=%v", len(claims), err)
	}
	fresh := claims[0]
	if refundProvenanceClaimTokenDigest(fresh.Token) == crashed.claimTokenDigest || fresh.Fence <= crashed.claimFence || fresh.Key != crashed.stableKey || fresh.Refund.Key != crashed.stableKey {
		t.Fatalf("recovery did not get fresh ownership for the original frozen command: fresh=%+v crashed=%+v", fresh, crashed)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+refundProvenanceFixtureTable+` (
		idempotency_key TEXT PRIMARY KEY, refund_id TEXT NOT NULL, process_count INTEGER NOT NULL,
		first_processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())`); err != nil {
		t.Fatal("cannot create synthetic provider-double fixture ledger")
	}
	provider := &refundProvenanceRecoveryProvider{pool: pool, claim: fresh, effectAlreadyProcessed: effectAlreadyProcessed}
	worker := &FinancialWorker{repo: repo, provider: provider}
	if err := worker.executeRefund(ctx, fresh); err != nil {
		t.Fatalf("Payment worker recovery returned error: %v", err)
	}
	if effectAlreadyProcessed {
		if provider.createCalls != 0 {
			t.Fatalf("recovery issued %d CreateFullRefund calls despite an already processed local fixture effect", provider.createCalls)
		}
	} else if provider.createCalls != 1 || provider.capture == nil {
		t.Fatalf("unknown-effect recovery creates=%d capture=%v; want one Create and new application capture", provider.createCalls, provider.capture != nil)
	}
	return fresh, provider.capture
}

func refundProvenanceClaimTokenDigest(token uuid.UUID) string {
	digest := sha256.Sum256([]byte(token.String()))
	return hex.EncodeToString(digest[:])
}

func assertRefundProvenanceAdmissionActive(t *testing.T, ctx context.Context, pool *pgxpool.Pool, phase string) {
	t.Helper()
	var active bool
	if err := pool.QueryRow(ctx, `SELECT active FROM payment_admission_mode WHERE singleton=TRUE`).Scan(&active); err != nil {
		t.Fatalf("read Payment admission mode at %s: %v", phase, err)
	}
	if !active {
		t.Fatalf("Payment admission mode at %s is inactive", phase)
	}
	t.Logf("ADMISSION_CRASH_BOUNDARY phase=%s active=true observed_utc=%s", phase, time.Now().UTC().Format(time.RFC3339Nano))
}

func assertRefundProvenanceAttemptAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt uuid.UUID, wantPrepared, wantResponse int) {
	t.Helper()
	var prepared, response int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE record_kind='PREPARED'),
		count(*) FILTER (WHERE record_kind='RESPONSE') FROM payment_refund_audit
		WHERE application_attempt_id=$1`, attempt).Scan(&prepared, &response); err != nil {
		t.Fatal("cannot inspect application attempt audit rows")
	}
	if prepared != wantPrepared || response != wantResponse {
		t.Fatalf("attempt %s audit rows PREPARED=%d RESPONSE=%d; want %d/%d", attempt, prepared, response, wantPrepared, wantResponse)
	}
}

type refundProvenanceRecoveryProvider struct {
	paymentgateway.FinancialProvider
	pool                   *pgxpool.Pool
	claim                  domain.FinancialClaim
	effectAlreadyProcessed bool
	createCalls            int
	capture                *domain.RefundResponseCapture
}

func (p *refundProvenanceRecoveryProvider) InspectSucceededRefunds(ctx context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	var refundID string
	err := p.pool.QueryRow(ctx, `SELECT refund_id FROM `+refundProvenanceFixtureTable+` WHERE idempotency_key=$1`, p.claim.Refund.Key).Scan(&refundID)
	if errors.Is(err, pgx.ErrNoRows) {
		return refundProvenanceObservation(p.claim, account, live, intentID, "no_refunds", "", 0), nil
	}
	if err != nil {
		return nil, err
	}
	if p.effectAlreadyProcessed {
		return refundProvenanceObservation(p.claim, account, live, intentID, "succeeded", refundID, p.claim.Refund.AmountCents), nil
	}
	if p.createCalls > 0 {
		return refundProvenanceObservation(p.claim, account, live, intentID, "pending", refundID, 0), nil
	}
	return refundProvenanceObservation(p.claim, account, live, intentID, "no_refunds", "", 0), nil
}

func (p *refundProvenanceRecoveryProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	if claim.PaymentID != p.claim.PaymentID || claim.Key != p.claim.Key || claim.Refund == nil || claim.Refund.Key != p.claim.Refund.Key || intentID != p.claim.View.Payment.PaymentIntentId {
		return nil, errors.New("recovery changed original refund command identity")
	}
	capture := domain.RefundResponseCaptureFromContext(ctx)
	if capture == nil || capture.AttemptID == uuid.Nil {
		return nil, errors.New("recovery CreateFullRefund lacks application-scoped capture")
	}
	p.capture = capture
	p.createCalls++
	refundID := "re_synthetic_recovery_" + uuid.NewString()
	if _, err := p.pool.Exec(ctx, `INSERT INTO `+refundProvenanceFixtureTable+`
		(idempotency_key,refund_id,process_count) VALUES ($1,$2,1)
		ON CONFLICT (idempotency_key) DO UPDATE SET process_count=`+refundProvenanceFixtureTable+`.process_count+1`, claim.Refund.Key, refundID); err != nil {
		return nil, err
	}
	return refundProvenanceObservation(claim, claim.Operation.Identity.ProviderAccount, claim.Operation.Identity.LiveMode, intentID, "pending", refundID, 0), nil
}

func refundProvenanceObservation(claim domain.FinancialClaim, account string, live bool, intentID, status, refundID string, succeededCents int64) *domain.ProviderObservation {
	return &domain.ProviderObservation{Account: account, LiveMode: live, IntentID: intentID, RefundID: refundID, Status: status,
		PaymentID: claim.PaymentID, BookingID: claim.Operation.Identity.BookingID, Generation: claim.Operation.Identity.Generation,
		AmountCents: claim.Refund.AmountCents, SucceededRefundCents: succeededCents, Currency: claim.Refund.Currency,
		Collected: true, ObservedAt: time.Now().UTC()}
}

type refundProvenancePreparedBarrierRepository struct {
	repository.FinancialRepository
}

func (r *refundProvenancePreparedBarrierRepository) CheckRefundPreSend(ctx context.Context, claim domain.FinancialClaim) (repository.RefundPreSendResult, error) {
	result, err := r.FinancialRepository.CheckRefundPreSend(ctx, claim)
	if err != nil || result.Decision != repository.RefundPreSendAllow {
		return result, err
	}
	fmt.Fprintf(os.Stdout, "PREPARED %s %s %s %d\n", claim.PaymentID, result.AttemptID, refundProvenanceClaimTokenDigest(claim.Token), claim.Fence)
	<-make(chan struct{})
	return result, nil
}

type refundProvenanceCrashProvider struct {
	paymentgateway.FinancialProvider
	pool *pgxpool.Pool
	mode string
}

func (p *refundProvenanceCrashProvider) InspectSucceededRefunds(_ context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	return &domain.ProviderObservation{Account: account, LiveMode: live, IntentID: intentID, Status: "no_refunds", Collected: true}, nil
}

func (p *refundProvenanceCrashProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	if p.mode != refundProvenanceChildProcessed || intentID == "" || claim.Refund == nil {
		return nil, errors.New("unexpected synthetic Provider double invocation")
	}
	capture := domain.RefundResponseCaptureFromContext(ctx)
	if capture == nil || capture.AttemptID == uuid.Nil {
		return nil, errors.New("CreateFullRefund did not receive this application's scoped response capture")
	}
	_, err := p.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+refundProvenanceFixtureTable+` (
		idempotency_key TEXT PRIMARY KEY, refund_id TEXT NOT NULL, process_count INTEGER NOT NULL,
		first_processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())`)
	if err != nil {
		return nil, err
	}
	refundID := "re_synthetic_" + uuid.NewString()
	for i := 0; i < 2; i++ {
		if _, err := p.pool.Exec(ctx, `INSERT INTO `+refundProvenanceFixtureTable+`
			(idempotency_key,refund_id,process_count) VALUES ($1,$2,1)
			ON CONFLICT (idempotency_key) DO UPDATE SET process_count=`+refundProvenanceFixtureTable+`.process_count+1`,
			claim.Refund.Key, refundID); err != nil {
			return nil, err
		}
	}
	fmt.Fprintf(os.Stdout, "PROCESSED %s %s %s %d\n", claim.PaymentID, capture.AttemptID, refundProvenanceClaimTokenDigest(claim.Token), claim.Fence)
	<-make(chan struct{})
	return &domain.ProviderObservation{Account: claim.Operation.Identity.ProviderAccount, LiveMode: claim.Operation.Identity.LiveMode,
		IntentID: intentID, RefundID: refundID, Status: "pending", PaymentID: claim.PaymentID,
		BookingID: claim.Operation.Identity.BookingID, Generation: claim.Operation.Identity.Generation,
		AmountCents: claim.Refund.AmountCents, Currency: claim.Refund.Currency, Collected: true, ObservedAt: time.Now().UTC()}, nil
}

func refundProvenancePoolForProcessSchema(t *testing.T, ctx context.Context, parsed *url.URL, schema string) *pgxpool.Pool {
	t.Helper()
	if !refundProvenanceWorkerSchemaPattern.MatchString(schema) {
		t.Fatal("refusing to connect to an unrecognized test schema")
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	copyURL := *parsed
	copyURL.RawQuery = query.Encode()
	pool, err := pgxpool.New(ctx, copyURL.String())
	if err != nil {
		t.Fatal("cannot open the terminated child's disposable schema")
	}
	return pool
}

func dropOwnedRefundProvenanceSchema(t *testing.T, ctx context.Context, admin *pgxpool.Pool, schema string) error {
	t.Helper()
	if !refundProvenanceWorkerSchemaPattern.MatchString(schema) {
		return errors.New("schema identity is outside the worker test's exact UUID pattern")
	}
	_, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	return err
}

func replaceRefundProvenanceEnv(env []string, key, value string) []string {
	result := make([]string, 0, len(env)+1)
	prefix := key + "="
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}

var _ repository.FinancialRepository = (*repository.PostgresPaymentRepository)(nil)
