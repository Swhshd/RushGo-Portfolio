//go:build recoverycomponents

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
)

func TestRefundBStaleSnapshotGuardSuppressesCreateBarrier(t *testing.T) {
	pool := refundWorkerManifestedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo := repository.NewPostgresPaymentRepository(pool)
	op := seedRefundWorkerPayment(t, ctx, repo)

	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Kind != "refund" {
		t.Fatalf("A initial refund claim: count=%d err=%v", len(claims), err)
	}
	staleA := claims[0]
	if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}

	provider := newRefundWorkerBarrierProvider(op)
	t.Cleanup(provider.release)
	worker := &FinancialWorker{repo: repo, provider: provider}
	type runResult struct {
		count int
		err   error
	}
	done := make(chan runResult, 1)
	go func() {
		count, err := worker.RunOnce(ctx, 1)
		done <- runResult{count: count, err: err}
	}()

	select {
	case <-provider.inspectEntered:
	case <-ctx.Done():
		t.Fatal("worker did not reach the local no_refunds inspection barrier")
	}
	before := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if before.token == nil || *before.token == staleA.Token || before.fence <= staleA.Fence || before.kind == nil || *before.kind != "refund" || before.refundState != "claimed" {
		t.Fatalf("DB does not show B owning the blocked worker claim: %+v", before)
	}
	inspectionsBeforeA, createsBeforeA := provider.counts()
	if inspectionsBeforeA != 1 || createsBeforeA != 0 {
		t.Fatalf("before A's late observation: InspectSucceededRefunds=%d CreateFullRefund=%d; want 1/0", inspectionsBeforeA, createsBeforeA)
	}

	owned, err := repo.FinishFinancialWork(ctx, staleA, refundWorkerLateFullResult(op))
	if err != nil || owned {
		t.Fatalf("A late full result = owned:%v err:%v; want fact retained while B remains owner", owned, err)
	}
	afterLateA := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if !sameRefundWorkerClaim(before, afterLateA) {
		t.Fatalf("A's late fact changed B's active claim:\nbefore=%+v\nafter=%+v", before, afterLateA)
	}
	if afterLateA.refundID == nil || *afterLateA.refundID != "re_late_A" || afterLateA.refundProviderStatus == nil || *afterLateA.refundProviderStatus != "succeeded" ||
		afterLateA.refundState != "succeeded" || afterLateA.completedAt == nil || afterLateA.reconciliationState != "resolved" {
		t.Fatalf("A's full fact did not persist its current Payment fields: %+v", afterLateA)
	}
	provider.releaseInspection()
	select {
	case result := <-done:
		if result.err != nil || result.count != 1 {
			t.Fatalf("B worker completion: count=%d err=%v", result.count, result.err)
		}
	case <-provider.createEntered:
		// Let the old behavior complete so RED is asserted on the actual
		// forbidden Provider create, rather than a blocked worker or compile error.
		provider.releaseCreate()
		select {
		case result := <-done:
			if result.err != nil || result.count != 1 {
				t.Fatalf("B worker completion after obsolete create: count=%d err=%v", result.count, result.err)
			}
		case <-ctx.Done():
			t.Fatal("B worker did not complete after releasing the provider double")
		}
	case <-ctx.Done():
		t.Fatal("B worker did not finish after the no_refunds inspection was released")
	}

	inspectCalls, createCalls := provider.counts()
	if inspectCalls != 1 || createCalls != 0 {
		t.Fatalf("after A's committed full refund and B's stale no_refunds inspection: InspectSucceededRefunds=%d CreateFullRefund=%d; want 1/0", inspectCalls, createCalls)
	}
	final := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if final.refundID == nil || *final.refundID != "re_late_A" || final.refundProviderStatus == nil || *final.refundProviderStatus != "succeeded" || final.refundState != "succeeded" ||
		final.completedAt == nil || final.reconciliationState != "resolved" || final.token != nil || final.lease != nil || final.kind != nil {
		t.Fatalf("B completion did not preserve A's full fact and release B's claim: %+v", final)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	remaining, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("resolved succeeded refund remained claimable: claims=%d err=%v", len(remaining), err)
	}
}

func TestRefundPostGuardSuccessRemainsExternalRaceIntegration(t *testing.T) {
	pool := refundWorkerManifestedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo := repository.NewPostgresPaymentRepository(pool)
	op := seedRefundWorkerPayment(t, ctx, repo)
	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Kind != "refund" {
		t.Fatalf("A initial refund claim: count=%d err=%v", len(claims), err)
	}
	staleA := claims[0]
	if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}

	provider := newRefundWorkerBarrierProvider(op)
	t.Cleanup(provider.release)
	guardBarrier := &refundWorkerPostGuardBarrierRepository{
		PostgresPaymentRepository: repo, entered: make(chan struct{}), release: make(chan struct{}),
	}
	t.Cleanup(guardBarrier.releaseCheck)
	worker := &FinancialWorker{repo: guardBarrier, provider: provider}
	type runResult struct {
		count int
		err   error
	}
	done := make(chan runResult, 1)
	go func() {
		count, err := worker.RunOnce(ctx, 1)
		done <- runResult{count: count, err: err}
	}()
	select {
	case <-provider.inspectEntered:
	case <-ctx.Done():
		t.Fatal("B did not reach the local no_refunds inspection")
	}
	provider.releaseInspection()
	select {
	case <-guardBarrier.entered:
	case <-ctx.Done():
		t.Fatal("B did not reach the barrier after its committed allow check")
	}
	if guardBarrier.err != nil || guardBarrier.result.Decision != repository.RefundPreSendAllow {
		t.Fatalf("B pre-send guard result = %+v err=%v; want committed allow", guardBarrier.result, guardBarrier.err)
	}

	owned, err := repo.FinishFinancialWork(ctx, staleA, refundWorkerLateFullResult(op))
	if err != nil || owned {
		t.Fatalf("A success after B's guard = owned:%v err:%v; want late fact committed while B still owns claim", owned, err)
	}
	afterA := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if afterA.refundState != "succeeded" || afterA.reconciliationState != "resolved" || afterA.token == nil ||
		*afterA.token == staleA.Token || afterA.fence <= staleA.Fence {
		t.Fatalf("A success was not committed under B's still-current claim: %+v", afterA)
	}

	guardBarrier.releaseCheck()
	select {
	case <-provider.createEntered:
	case <-ctx.Done():
		t.Fatal("B did not continue to the Provider boundary after the post-guard race")
	}
	createClaim := provider.claim()
	wantKey := "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"
	if createClaim.Key != wantKey || createClaim.Operation.Key != wantKey || createClaim.Refund == nil || createClaim.Refund.Key != wantKey {
		t.Fatalf("post-guard race changed the stable command key: claim=%+v", createClaim)
	}
	inspectCalls, createCalls := provider.counts()
	if inspectCalls != 1 || createCalls != 1 {
		t.Fatalf("post-guard Provider double calls=%d inspections/%d creates; want 1/1", inspectCalls, createCalls)
	}
	provider.releaseCreate()
	select {
	case result := <-done:
		if result.err != nil || result.count != 1 {
			t.Fatalf("B worker completion: count=%d err=%v", result.count, result.err)
		}
	case <-ctx.Done():
		t.Fatal("B worker did not finish after Provider double release")
	}
	final := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if final.refundState != "succeeded" || final.reconciliationState != "resolved" || final.token != nil || final.lease != nil || final.kind != nil {
		t.Fatalf("post-guard race did not preserve terminal success and release B claim: %+v", final)
	}
}

type refundWorkerPostGuardBarrierRepository struct {
	*repository.PostgresPaymentRepository
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	result      repository.RefundPreSendResult
	err         error
}

func (r *refundWorkerPostGuardBarrierRepository) CheckRefundPreSend(ctx context.Context, claim domain.FinancialClaim) (repository.RefundPreSendResult, error) {
	result, err := r.PostgresPaymentRepository.CheckRefundPreSend(ctx, claim)
	r.result, r.err = result, err
	if err != nil || result.Decision != repository.RefundPreSendAllow {
		return result, err
	}
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
		return result, nil
	case <-ctx.Done():
		return repository.RefundPreSendResult{}, ctx.Err()
	}
}

func (r *refundWorkerPostGuardBarrierRepository) releaseCheck() {
	r.releaseOnce.Do(func() { close(r.release) })
}

type refundWorkerOwnerManifest struct {
	ManifestVersion        int    `json:"manifest_version"`
	ResourceID             string `json:"resource_id"`
	ContainerID            string `json:"container_id"`
	Database               string `json:"database"`
	Host                   string `json:"host"`
	Port                   int    `json:"port"`
	DSNSHA256              string `json:"dsn_sha256"`
	RunID                  string `json:"run_id"`
	OwnerNonce             string `json:"owner_nonce"`
	ServerSystemIdentifier string `json:"server_system_identifier"`
	DatabaseMarker         string `json:"database_marker"`
}

func refundWorkerManifestedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	manifestPath := os.Getenv("REFUND_COMPONENT_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" {
		t.Fatal("BLOCKED: manifested refund component database environment is required")
	}
	rawManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("BLOCKED: cannot read manifested refund component owner record")
	}
	var manifest refundWorkerOwnerManifest
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		t.Fatal("BLOCKED: invalid manifested refund component owner record")
	}
	if manifest.ManifestVersion != 1 || manifest.ResourceID == "" || manifest.ContainerID == "" || manifest.Database == "" || manifest.Host == "" ||
		manifest.Port < 1 || manifest.Port > 65535 || manifest.DSNSHA256 == "" || manifest.RunID == "" || manifest.OwnerNonce == "" ||
		manifest.ServerSystemIdentifier == "" || manifest.DatabaseMarker == "" {
		t.Fatal("BLOCKED: manifested refund component owner record is incomplete")
	}
	if manifest.DatabaseMarker != "codex-refund-component:v1:"+manifest.ResourceID+":"+manifest.RunID+":"+manifest.OwnerNonce {
		t.Fatal("BLOCKED: database marker is not bound to this resource, run, and owner nonce")
	}
	dsnHash := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(dsnHash[:]) != manifest.DSNSHA256 {
		t.Fatal("BLOCKED: Payment DSN does not match owner record")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("BLOCKED: Payment DSN must be a PostgreSQL URL")
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || parsed.Hostname() != manifest.Host || port != manifest.Port || strings.TrimPrefix(parsed.Path, "/") != manifest.Database {
		t.Fatal("BLOCKED: Payment DSN endpoint does not match owner record")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("BLOCKED: cannot connect to manifested Payment database")
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatal("BLOCKED: manifested Payment database is unavailable")
	}
	var versionNum int
	var database, systemIdentifier, databaseMarker string
	if err := admin.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&versionNum); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment PostgreSQL version")
	}
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		t.Fatal("BLOCKED: cannot identify current Payment database")
	}
	if err := admin.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&systemIdentifier); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment PostgreSQL cluster")
	}
	if err := admin.QueryRow(ctx, "SELECT pg_catalog.shobj_description(oid, 'pg_database') FROM pg_catalog.pg_database WHERE datname=current_database()").Scan(&databaseMarker); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment database marker")
	}
	if versionNum < 160000 || versionNum >= 170000 || database != manifest.Database || systemIdentifier != manifest.ServerSystemIdentifier || databaseMarker != manifest.DatabaseMarker {
		t.Fatal("BLOCKED: Payment database identity differs from the owner record")
	}
	ensureRefundWorkerUUIDOSSPublic(t, ctx, admin)

	schema := "refund_worker_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("cannot create disposable refund worker schema")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error("cannot drop disposable refund worker schema")
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolatedDSN := parsed.String()
	migrate, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("BLOCKED: migrate CLI is unavailable for isolated Payment schema setup")
	}
	cmd := exec.CommandContext(ctx, migrate, "-path", "../../migrations", "-database", isolatedDSN, "up")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if safe := strings.ReplaceAll(string(output), isolatedDSN, "[isolated DSN]"); safe != "" {
			t.Fatalf("Payment migrations failed: %s", safe)
		}
		t.Fatal("Payment migrations failed")
	}
	config, err := pgxpool.ParseConfig(isolatedDSN)
	if err != nil {
		t.Fatal("cannot parse isolated Payment schema connection")
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("cannot connect to isolated Payment schema")
	}
	t.Cleanup(pool.Close)
	return pool
}

func ensureRefundWorkerUUIDOSSPublic(t *testing.T, ctx context.Context, admin *pgxpool.Pool) {
	t.Helper()
	const namespaceQuery = `SELECT COALESCE((
		SELECT n.nspname
		FROM pg_catalog.pg_extension e
		JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
		WHERE e.extname='uuid-ossp'
	), '')`
	var extensionSchema string
	if err := admin.QueryRow(ctx, namespaceQuery).Scan(&extensionSchema); err != nil {
		t.Fatal("BLOCKED: cannot inspect uuid-ossp extension namespace")
	}
	if extensionSchema != "" && extensionSchema != "public" {
		t.Fatal("BLOCKED: uuid-ossp is installed outside public; refusing to move or replace it")
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal("BLOCKED: cannot provision uuid-ossp in public")
	}
	extensionSchema = ""
	if err := admin.QueryRow(ctx, namespaceQuery).Scan(&extensionSchema); err != nil || extensionSchema != "public" {
		t.Fatal("BLOCKED: uuid-ossp was not verified in public after provisioning")
	}
}

func seedRefundWorkerPayment(t *testing.T, ctx context.Context, repo *repository.PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := domain.CreateOperation{
		PaymentID: uuid.New(), Key: "",
		FrozenParams: json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`),
		Identity:     domain.FinancialIdentity{BookingID: uuid.New(), UserID: uuid.New(), Generation: 1, ProviderAccount: "acct_refund_worker_fixture", AmountCents: 725, Currency: "usd"},
	}
	op.Key = "ticketbox:payment:" + op.PaymentID.String() + ":g1:create"
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, op.FrozenParams)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_refund_worker_fixture"
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, LiveMode: op.Identity.LiveMode, Status: "succeeded", IntentID: intentID,
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	if _, err := repo.RequireRefund(ctx, domain.RefundObligation{
		PaymentID: op.PaymentID, DecisionID: decisionID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Reason: "EXPIRED",
		Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund",
	}); err != nil {
		t.Fatal(err)
	}
	return op
}

type refundWorkerBarrierProvider struct {
	op                    domain.CreateOperation
	intentID              string
	inspectEntered        chan struct{}
	inspectionGate        chan struct{}
	createEntered         chan struct{}
	createGate            chan struct{}
	inspectOnce           sync.Once
	createOnce            sync.Once
	releaseInspectionOnce sync.Once
	releaseCreateOnce     sync.Once
	mu                    sync.Mutex
	inspectCalls          int
	createClaims          []domain.FinancialClaim
}

func newRefundWorkerBarrierProvider(op domain.CreateOperation) *refundWorkerBarrierProvider {
	return &refundWorkerBarrierProvider{
		op: op, intentID: "pi_refund_worker_fixture", inspectEntered: make(chan struct{}), inspectionGate: make(chan struct{}),
		createEntered: make(chan struct{}), createGate: make(chan struct{}),
	}
}

func (p *refundWorkerBarrierProvider) InspectSucceededRefunds(ctx context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	p.inspectCalls++
	call := p.inspectCalls
	p.mu.Unlock()
	if account != p.op.Identity.ProviderAccount || live != p.op.Identity.LiveMode || intentID != p.intentID {
		return nil, errors.New("provider double received unexpected inspection identity")
	}
	if call == 1 {
		staleNoRefunds := p.observation("no_refunds", "", 0)
		p.inspectOnce.Do(func() { close(p.inspectEntered) })
		select {
		case <-p.inspectionGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return staleNoRefunds, nil
	}
	if call == 2 {
		return p.observation("succeeded", "re_late_A", p.op.Identity.AmountCents), nil
	}
	return nil, errors.New("provider double received an unexpected extra refund inspection")
}

func (p *refundWorkerBarrierProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	p.createClaims = append(p.createClaims, claim)
	p.mu.Unlock()
	p.createOnce.Do(func() { close(p.createEntered) })
	select {
	case <-p.createGate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if intentID != p.intentID {
		return nil, errors.New("provider double received unexpected PaymentIntent")
	}
	return p.observation("pending", "re_late_A", 0), nil
}

func (p *refundWorkerBarrierProvider) claim() domain.FinancialClaim {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.createClaims) != 1 {
		panic(fmt.Sprintf("provider double has %d create claims", len(p.createClaims)))
	}
	return p.createClaims[0]
}

func (p *refundWorkerBarrierProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inspectCalls, len(p.createClaims)
}

func (p *refundWorkerBarrierProvider) releaseInspection() {
	p.releaseInspectionOnce.Do(func() { close(p.inspectionGate) })
}

func (p *refundWorkerBarrierProvider) releaseCreate() {
	p.releaseCreateOnce.Do(func() { close(p.createGate) })
}

func (p *refundWorkerBarrierProvider) release() {
	p.releaseInspection()
	p.releaseCreate()
}

func (p *refundWorkerBarrierProvider) observation(status, refundID string, succeededCents int64) *domain.ProviderObservation {
	return &domain.ProviderObservation{
		Account: p.op.Identity.ProviderAccount, LiveMode: p.op.Identity.LiveMode, Status: status, IntentID: p.intentID,
		RefundID: refundID, PaymentID: p.op.PaymentID, BookingID: p.op.Identity.BookingID, Generation: p.op.Identity.Generation,
		AmountCents: p.op.Identity.AmountCents, SucceededRefundCents: succeededCents, Currency: p.op.Identity.Currency,
		Collected: true, ObservedAt: time.Now().UTC(),
	}
}

func (p *refundWorkerBarrierProvider) CreateIntent(context.Context, domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("unexpected CreateIntent on refund worker double")
}
func (p *refundWorkerBarrierProvider) RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error) {
	return nil, "", errors.New("unexpected RetrieveIntent on refund worker double")
}
func (p *refundWorkerBarrierProvider) FindIntent(context.Context, domain.CreateOperation) ([]domain.ProviderObservation, error) {
	return nil, errors.New("unexpected FindIntent on refund worker double")
}
func (p *refundWorkerBarrierProvider) CancelIntent(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("unexpected CancelIntent on refund worker double")
}
func (p *refundWorkerBarrierProvider) RetrieveRefund(context.Context, string, bool, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("unexpected RetrieveRefund on refund worker double")
}
func (p *refundWorkerBarrierProvider) VerifyWebhook(context.Context, []byte, string) (*domain.ProviderReceipt, error) {
	return nil, errors.New("unexpected VerifyWebhook on refund worker double")
}

var _ paymentgateway.FinancialProvider = (*refundWorkerBarrierProvider)(nil)

func refundWorkerLateFullResult(op domain.CreateOperation) domain.FinancialResult {
	observation := &domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, LiveMode: op.Identity.LiveMode, Status: "succeeded", IntentID: "pi_refund_worker_fixture",
		RefundID: "re_late_A", PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, SucceededRefundCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
		Collected: true, ObservedAt: time.Now().UTC(),
	}
	return domain.FinancialResult{State: "succeeded", Observation: observation, NextAttemptAt: time.Now().UTC().Add(time.Second)}
}

type refundWorkerRowSnapshot struct {
	token                *uuid.UUID
	fence                int64
	lease                *time.Time
	kind                 *string
	refundID             *string
	refundProviderStatus *string
	refundState          string
	completedAt          *time.Time
	reconciliationState  string
}

func refundWorkerSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) refundWorkerRowSnapshot {
	t.Helper()
	var row refundWorkerRowSnapshot
	err := pool.QueryRow(ctx, `SELECT claim_token,claim_fence,lease_until,operation_kind,refund_provider_id,refund_provider_status,refund_state,refund_completed_at,reconciliation_state
		FROM payments WHERE id=$1`, paymentID).Scan(&row.token, &row.fence, &row.lease, &row.kind, &row.refundID, &row.refundProviderStatus, &row.refundState, &row.completedAt, &row.reconciliationState)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func sameRefundWorkerClaim(a, b refundWorkerRowSnapshot) bool {
	return sameRefundWorkerUUID(a.token, b.token) && a.fence == b.fence && sameRefundWorkerTime(a.lease, b.lease) && sameRefundWorkerString(a.kind, b.kind)
}

func sameRefundWorkerUUID(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameRefundWorkerString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameRefundWorkerTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
