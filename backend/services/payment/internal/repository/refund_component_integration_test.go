//go:build recoverycomponents

package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/recoverycomponent"
)

type refundComponentOwnerManifest struct {
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

func refundComponentDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	manifestPath := os.Getenv("REFUND_COMPONENT_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" {
		t.Fatal("BLOCKED: REFUND_COMPONENT_DATABASE_URL and REFUND_COMPONENT_OWNER_MANIFEST are required")
	}

	rawManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("BLOCKED: cannot read refund component owner manifest")
	}
	var manifest refundComponentOwnerManifest
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		t.Fatal("BLOCKED: invalid refund component owner manifest")
	}
	if manifest.ManifestVersion != 1 || manifest.ResourceID == "" || manifest.ContainerID == "" ||
		manifest.Database == "" || manifest.Host == "" || manifest.Port < 1 || manifest.Port > 65535 ||
		manifest.DSNSHA256 == "" || manifest.RunID == "" || manifest.OwnerNonce == "" ||
		manifest.ServerSystemIdentifier == "" || manifest.DatabaseMarker == "" {
		t.Fatal("BLOCKED: refund component owner manifest is incomplete")
	}
	expectedMarker := "codex-refund-component:v1:" + manifest.ResourceID + ":" + manifest.RunID + ":" + manifest.OwnerNonce
	if manifest.DatabaseMarker != expectedMarker {
		t.Fatal("BLOCKED: database marker is not bound to this resource, run, and owner nonce")
	}
	sum := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(sum[:]) != manifest.DSNSHA256 {
		t.Fatal("BLOCKED: Payment DSN does not match owner manifest")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("BLOCKED: Payment DSN must be a PostgreSQL URL")
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || parsed.Hostname() != manifest.Host || port != manifest.Port ||
		strings.TrimPrefix(parsed.Path, "/") != manifest.Database {
		t.Fatal("BLOCKED: Payment DSN endpoint does not match owner manifest")
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
	var currentDatabase, systemIdentifier, databaseMarker string
	if err := admin.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&versionNum); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment PostgreSQL version")
	}
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil {
		t.Fatal("BLOCKED: cannot identify current Payment database")
	}
	if err := admin.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&systemIdentifier); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment PostgreSQL cluster")
	}
	if err := admin.QueryRow(ctx, "SELECT pg_catalog.shobj_description(oid, 'pg_database') FROM pg_catalog.pg_database WHERE datname=current_database()").Scan(&databaseMarker); err != nil {
		t.Fatal("BLOCKED: cannot identify Payment database marker")
	}
	if versionNum < 160000 || versionNum >= 170000 || currentDatabase != manifest.Database ||
		systemIdentifier != manifest.ServerSystemIdentifier || databaseMarker != manifest.DatabaseMarker {
		t.Fatal("BLOCKED: Payment database identity differs from owner manifest")
	}
	ensureRefundComponentUUIDOSSPublic(t, ctx, admin)

	schema := "refund_component_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("cannot create disposable refund component schema")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("cannot drop disposable refund component schema")
		}
	})

	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolatedDSN := parsed.String()
	migrationPath := filepath.ToSlash(filepath.Join("..", "..", "migrations"))
	migrate, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("BLOCKED: migrate CLI is not available for isolated Payment schema setup")
	}
	cmd := exec.CommandContext(ctx, migrate, "-path", migrationPath, "-database", isolatedDSN, "up")
	output, err := cmd.CombinedOutput()
	if err != nil {
		safeOutput := strings.ReplaceAll(string(output), isolatedDSN, "[isolated DSN]")
		if safeOutput != "" {
			t.Fatalf("Payment migrations failed: %s", safeOutput)
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

func ensureRefundComponentUUIDOSSPublic(t *testing.T, ctx context.Context, admin *pgxpool.Pool) {
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

func seedRefundComponentPayment(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded", IntentID: "pi_component_fixture",
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Collected: true,
		ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	_, err := repo.RequireRefund(ctx, domain.RefundObligation{
		PaymentID: op.PaymentID, DecisionID: decisionID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Reason: "EXPIRED",
		Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund",
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func syntheticRefundForSource(source recoverycomponent.ProductSource) recoverycomponent.ProviderEvidence {
	liveMode := false
	paid, captured := true, true
	requested := time.Now().UTC().Add(-time.Second)
	request := func(id, objectID, operation string) recoverycomponent.RequestObservation {
		return recoverycomponent.RequestObservation{
			ID: id, Operation: operation, AccountID: source.ProviderAccount, LiveMode: &liveMode,
			ObjectID: objectID, RequestedAt: requested, ObservedAt: requested.Add(time.Second),
		}
	}
	return recoverycomponent.ProviderEvidence{
		Class: recoverycomponent.EvidenceSynthetic,
		PaymentIntent: recoverycomponent.PaymentIntentFact{
			ID: source.PaymentIntentID, Status: "succeeded", AmountCents: source.AmountCents,
			AmountReceivedCents: source.AmountCents, Currency: source.Currency, LatestChargeID: "ch_component_fixture",
			LiveMode: &liveMode, Request: request("req_pi_component_fixture", source.PaymentIntentID, "retrieve_payment_intent"),
		},
		Charge: recoverycomponent.ChargeFact{
			ID: "ch_component_fixture", PaymentIntentID: source.PaymentIntentID, AmountCents: source.AmountCents,
			Currency: source.Currency, Paid: &paid, Captured: &captured, LiveMode: &liveMode,
			Request: request("req_charge_component_fixture", "ch_component_fixture", "retrieve_charge"),
		},
		Refund: recoverycomponent.RefundFact{
			ID: "re_component_fixture", PaymentIntentID: source.PaymentIntentID, ChargeID: "ch_component_fixture",
			Status: "succeeded", AmountCents: source.AmountCents, Currency: source.Currency, LiveMode: &liveMode,
			Request: request("req_refund_component_fixture", "re_component_fixture", "retrieve_refund"),
		},
	}
}

func TestRecoveryComponentRefundSourceUsesPersistedPaymentCommand(t *testing.T) {
	pool := refundComponentDatabase(t)
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(pool)
	op := seedRefundComponentPayment(t, ctx, repo)

	source, err := recoverycomponent.ReadProductSource(ctx, pool, op.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	result := recoverycomponent.ValidateSingleFullRefund(source, syntheticRefundForSource(source))
	if result.ProviderFacts != recoverycomponent.StatusPass || result.ProductCommand != recoverycomponent.StatusPass {
		t.Fatalf("persisted Payment/refund command did not match the synthetic fixture: provider=%s command=%s findings=%v",
			result.ProviderFacts, result.ProductCommand, result.Findings)
	}
	if result.Provenance != recoverycomponent.StatusUnknown || result.Overall != recoverycomponent.StatusUnknown ||
		!containsRecoveryFinding(result.Findings, recoverycomponent.FindingChargeAssociationNotPersisted) {
		t.Fatalf("missing persisted Charge association must remain UNKNOWN: provenance=%s overall=%s findings=%v",
			result.Provenance, result.Overall, result.Findings)
	}
}

func TestRecoveryComponentRefundMarkerUsesExistingOwnerFencePredicate(t *testing.T) {
	t.Run("expired_lease", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		claim := claimRefundComponent(t, ctx, repo)
		if _, err := pool.Exec(ctx, "UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=$1", op.PaymentID); err != nil {
			t.Fatal(err)
		}
		owned, err := repo.FinishFinancialWork(ctx, claim, componentRetryableResult())
		if err != nil || owned {
			t.Fatalf("expired owner marker result = owned:%v err:%v; want rejected", owned, err)
		}
		assertRefundComponentMarker(t, ctx, pool, op.PaymentID, "claimed", claim.Fence, &claim.Token)
	})

	t.Run("wrong_token_and_fence", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		claim := claimRefundComponent(t, ctx, repo)

		wrongToken := claim
		wrongToken.Token = uuid.New()
		owned, err := repo.FinishFinancialWork(ctx, wrongToken, componentRetryableResult())
		if err != nil || owned {
			t.Fatalf("wrong token marker result = owned:%v err:%v; want rejected", owned, err)
		}
		wrongFence := claim
		wrongFence.Fence++
		owned, err = repo.FinishFinancialWork(ctx, wrongFence, componentRetryableResult())
		if err != nil || owned {
			t.Fatalf("wrong fence marker result = owned:%v err:%v; want rejected", owned, err)
		}
		assertRefundComponentMarker(t, ctx, pool, op.PaymentID, "claimed", claim.Fence, &claim.Token)
	})

	t.Run("owner_takeover_and_stale_worker", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		oldClaim := claimRefundComponent(t, ctx, repo)
		if _, err := pool.Exec(ctx, "UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1", op.PaymentID); err != nil {
			t.Fatal(err)
		}
		newClaim := claimRefundComponent(t, ctx, repo)
		if newClaim.Token == oldClaim.Token || newClaim.Fence <= oldClaim.Fence {
			t.Fatal("takeover did not issue a new token and larger fence")
		}
		owned, err := repo.FinishFinancialWork(ctx, newClaim, componentRetryableResult())
		if err != nil || !owned {
			t.Fatalf("current owner marker result = owned:%v err:%v; want accepted", owned, err)
		}
		owned, err = repo.FinishFinancialWork(ctx, oldClaim, componentRetryableResult())
		if err != nil || owned {
			t.Fatalf("stale owner marker result = owned:%v err:%v; want rejected", owned, err)
		}
		assertRefundComponentMarker(t, ctx, pool, op.PaymentID, "retryable", newClaim.Fence, nil)
	})

	t.Run("wrong_business_identity", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		claim := claimRefundComponent(t, ctx, repo)
		liveMode := false
		observation := &domain.ProviderObservation{
			Account: op.Identity.ProviderAccount, LiveMode: liveMode, Status: "succeeded", IntentID: "pi_component_fixture",
			RefundID: "re_component_fixture", PaymentID: op.PaymentID, BookingID: uuid.New(),
			Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents, SucceededRefundCents: op.Identity.AmountCents,
			Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
		}
		owned, err := repo.FinishFinancialWork(ctx, claim, domain.FinancialResult{
			State: "succeeded", Observation: observation, NextAttemptAt: time.Now().UTC().Add(time.Second),
		})
		if owned || !errors.Is(err, ErrProviderEvidenceConflict) {
			t.Fatalf("wrong Booking marker result = owned:%v err:%v; want evidence conflict", owned, err)
		}
		assertRefundComponentMarker(t, ctx, pool, op.PaymentID, "claimed", claim.Fence, &claim.Token)
	})

	t.Run("repeat_marker_operation", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		claim := claimRefundComponent(t, ctx, repo)
		owned, err := repo.FinishFinancialWork(ctx, claim, componentRetryableResult())
		if err != nil || !owned {
			t.Fatalf("initial marker result = owned:%v err:%v; want accepted", owned, err)
		}
		owned, err = repo.FinishFinancialWork(ctx, claim, domain.FinancialResult{
			State: "provider_pending", NextAttemptAt: time.Now().UTC().Add(time.Second),
		})
		if err != nil || owned {
			t.Fatalf("repeated stale marker result = owned:%v err:%v; want rejected", owned, err)
		}
		assertRefundComponentMarker(t, ctx, pool, op.PaymentID, "retryable", claim.Fence, nil)
	})
}

func TestRecoveryComponentRefundSyntheticLateFullObservationRetention(t *testing.T) {
	t.Run("expired_lease_retains_full_refund_without_ownership", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		claim := claimRefundComponent(t, ctx, repo)
		if _, err := pool.Exec(ctx, "UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=$1", op.PaymentID); err != nil {
			t.Fatal(err)
		}
		markerBefore := readRefundComponentClaimMarker(t, ctx, pool, op.PaymentID)
		if markerBefore.token == nil || *markerBefore.token != claim.Token || markerBefore.fence != claim.Fence || markerBefore.lease == nil {
			t.Fatal("expired lease fixture does not retain the expired claimant marker")
		}

		owned, err := repo.FinishFinancialWork(ctx, claim, syntheticLateFullRefundResult(op))
		if err != nil || owned {
			t.Fatalf("expired synthetic full-refund observation = owned:%v err:%v; want evidence retained without ownership", owned, err)
		}
		assertRefundComponentClaimMarkerUnchanged(t, readRefundComponentClaimMarker(t, ctx, pool, op.PaymentID), markerBefore)
		assertRefundComponentFullRefundFact(t, ctx, pool, op.PaymentID)
	})

	t.Run("stale_worker_after_takeover_retains_full_refund_without_replacing_owner", func(t *testing.T) {
		pool, ctx, repo := newRefundComponentRepository(t)
		op := seedRefundComponentPayment(t, ctx, repo)
		staleClaim := claimRefundComponent(t, ctx, repo)
		if _, err := pool.Exec(ctx, "UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1", op.PaymentID); err != nil {
			t.Fatal(err)
		}
		currentClaim := claimRefundComponent(t, ctx, repo)
		if currentClaim.Token == staleClaim.Token || currentClaim.Fence <= staleClaim.Fence {
			t.Fatal("takeover did not issue a new token and larger fence")
		}
		markerBefore := readRefundComponentClaimMarker(t, ctx, pool, op.PaymentID)
		if markerBefore.token == nil || *markerBefore.token != currentClaim.Token || markerBefore.fence != currentClaim.Fence || markerBefore.lease == nil {
			t.Fatal("takeover marker does not belong to the current claimant")
		}

		owned, err := repo.FinishFinancialWork(ctx, staleClaim, syntheticLateFullRefundResult(op))
		if err != nil || owned {
			t.Fatalf("stale synthetic full-refund observation = owned:%v err:%v; want evidence retained without ownership", owned, err)
		}
		assertRefundComponentClaimMarkerUnchanged(t, readRefundComponentClaimMarker(t, ctx, pool, op.PaymentID), markerBefore)
		assertRefundComponentFullRefundFact(t, ctx, pool, op.PaymentID)
	})
}

func newRefundComponentRepository(t *testing.T) (*pgxpool.Pool, context.Context, *PostgresPaymentRepository) {
	t.Helper()
	pool := refundComponentDatabase(t)
	ctx := context.Background()
	return pool, ctx, NewPostgresPaymentRepository(pool)
}
func claimRefundComponent(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository) domain.FinancialClaim {
	t.Helper()
	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Kind != "refund" || claims[0].Refund == nil {
		t.Fatalf("claimed %d financial items, want one durable refund command", len(claims))
	}
	return claims[0]
}

func componentRetryableResult() domain.FinancialResult {
	return domain.FinancialResult{State: "retryable", ErrorCategory: "component_test", NextAttemptAt: time.Now().UTC().Add(time.Second)}
}

// syntheticLateFullRefundResult is synthetic normalized provider evidence; it makes no provider request and is not source-authenticated.
func syntheticLateFullRefundResult(op domain.CreateOperation) domain.FinancialResult {
	observation := domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded", IntentID: "pi_component_fixture",
		RefundID: "re_component_fixture", PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents,
		SucceededRefundCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
		Collected: true, ObservedAt: time.Now().UTC(),
	}
	return domain.FinancialResult{
		State: "retryable", ErrorCategory: "synthetic_late_refund_observation",
		Observation: &observation, NextAttemptAt: time.Now().UTC().Add(time.Second),
	}
}

type refundComponentClaimMarker struct {
	token *uuid.UUID
	fence int64
	lease *time.Time
}

func readRefundComponentClaimMarker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) refundComponentClaimMarker {
	t.Helper()
	var marker refundComponentClaimMarker
	if err := pool.QueryRow(ctx, "SELECT claim_token,claim_fence,lease_until FROM payments WHERE id=$1", paymentID).
		Scan(&marker.token, &marker.fence, &marker.lease); err != nil {
		t.Fatal(err)
	}
	return marker
}

func assertRefundComponentClaimMarkerUnchanged(t *testing.T, got, want refundComponentClaimMarker) {
	t.Helper()
	if got.fence != want.fence {
		t.Fatalf("late observation changed current claim fence from %d to %d", want.fence, got.fence)
	}
	if !sameRefundComponentUUID(got.token, want.token) {
		t.Fatal("late observation changed the current claim token")
	}
	if !sameRefundComponentTime(got.lease, want.lease) {
		t.Fatal("late observation changed the current claim lease")
	}
}

func sameRefundComponentUUID(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameRefundComponentTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func assertRefundComponentFullRefundFact(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) {
	t.Helper()
	var refundID, refundStatus, refundState, reconciliationState string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT refund_provider_id,refund_provider_status,refund_state,refund_completed_at,reconciliation_state
		FROM payments WHERE id=$1`, paymentID).
		Scan(&refundID, &refundStatus, &refundState, &completedAt, &reconciliationState); err != nil {
		t.Fatal(err)
	}
	if refundID != "re_component_fixture" || refundStatus != "succeeded" || refundState != "succeeded" ||
		completedAt == nil || reconciliationState != "resolved" {
		t.Fatalf("full synthetic refund fact persisted as id=%q provider_status=%q refund_state=%q completed_at=%v reconciliation=%q",
			refundID, refundStatus, refundState, completedAt, reconciliationState)
	}
}

func assertRefundComponentMarker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID, wantState string, wantFence int64, wantToken *uuid.UUID) {
	t.Helper()
	var state string
	var token *uuid.UUID
	var fence int64
	var lease *time.Time
	if err := pool.QueryRow(ctx, "SELECT refund_state,claim_token,claim_fence,lease_until FROM payments WHERE id=$1", paymentID).
		Scan(&state, &token, &fence, &lease); err != nil {
		t.Fatal(err)
	}
	if state != wantState || fence != wantFence {
		t.Fatalf("refund marker state/fence = %s/%d, want %s/%d", state, fence, wantState, wantFence)
	}
	if wantToken == nil && (token != nil || lease != nil) {
		t.Fatal("stale marker operation retained or replaced a claim token or lease")
	}
	if wantToken != nil && (token == nil || *token != *wantToken || lease == nil) {
		t.Fatal("rejected marker operation changed the current claim identity")
	}
}

func containsRecoveryFinding(findings []string, want string) bool {
	for _, finding := range findings {
		if finding == want {
			return true
		}
	}
	return false
}
