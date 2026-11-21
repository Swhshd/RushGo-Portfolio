//go:build admission_r4

package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func durableProviderFixture(t *testing.T) (*pgxpool.Pool, bootstrapConfig) {
	t.Helper()
	raw := os.Getenv("MVP_PAYMENT_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated MVP_PAYMENT_DATABASE_URL required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/rushgo_mvp_") {
		t.Fatal("refusing non-isolated database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	name := "rushgo_mvp_provider_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, `DROP DATABASE "`+name+`"`); admin.Close() })
	dir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("migrate", "-path", ".", "-database", u.String(), "up")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migration: %v %s", err, out)
	}
	cfg := bootstrapConfig{RunID: uuid.NewString(), SourceCommit: strings.Repeat("a", 40), RuntimeMode: "default_worker", DynamicPurchase: &dynamicPurchaseConfig{Enabled: true, UserID: uuid.NewString()}, KafkaBrokers: []string{"kafka:9092"}}
	for i := 0; i < 2; i++ {
		id := uuid.NewString()
		cfg.Fixtures = append(cfg.Fixtures, fixtureConfig{PaymentID: id, BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_" + strings.ReplaceAll(id, "-", ""), ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true})
	}
	return pool, cfg
}
func TestDurableProviderRecoversOriginalConfirmationAndRefund(t *testing.T) {
	pool, cfg := durableProviderFixture(t)
	ctx := context.Background()
	repo := repository.NewPostgresPaymentRepository(pool)
	p, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatal(err)
	}
	op := dynamicPurchaseTestOperation(t, uuid.MustParse(cfg.DynamicPurchase.UserID))
	if _, _, err = repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	observation, _, err := p.CreateIntent(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.RecordObservation(ctx, *observation); err != nil {
		t.Fatal(err)
	}
	resumed, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatalf("pending restart: %v", err)
	}
	receipt, err := resumed.dynamicConfirmationReceipt(op.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	// A second process's stale snapshot cannot authorize admission after another
	// process advances the durable provider revision.
	if got := p.CheckAdmissionEvidence(ctx, dynamicAdmissionIdentity(op, observation.IntentID)); got.EligibilityState != domain.AdmissionEligibilityUnknown {
		t.Fatal("stale provider snapshot authorized admission")
	}
	// Simulate a process loss after provider confirmation but before Payment
	// accepted the receipt. Restart must replay that exact persisted receipt.
	afterCrash, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatalf("confirmation boundary: %v", err)
	}
	replay, err := afterCrash.dynamicConfirmationReceipt(op.PaymentID)
	if err != nil || !sameFrozenReceipt(receipt, replay) {
		t.Fatal("restart invented a new receipt or changed evidence bytes")
	}
	if !afterCrash.dynamicStatsSnapshot().ReceiptAccepted {
		t.Fatal("original durable receipt not recovered")
	}
	claim := dynamicPurchaseTestClaim(op)
	refund, err := afterCrash.CreateFullRefund(ctx, claim, observation.IntentID)
	if err != nil {
		t.Fatal(err)
	}
	var before, after string
	snapshot := func() *string {
		var s string
		if err := pool.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(p) ORDER BY id)::text FROM payments p`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return &s
	}
	before = *snapshot()
	final, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatalf("refunded restart: %v", err)
	}
	after = *snapshot()
	if before != after {
		t.Fatal("restart reseeded or changed payments")
	}
	refundReplay, err := final.CreateFullRefund(ctx, claim, observation.IntentID)
	if err != nil || refundReplay.RefundID != refund.RefundID || final.dynamicStatsSnapshot().RefundEffects != 1 {
		t.Fatal("restart duplicated a refund effect")
	}
	if evidence := final.CheckAdmissionEvidence(ctx, dynamicAdmissionIdentity(op, observation.IntentID)); evidence.EligibilityState != domain.AdmissionEligibilityDenied {
		t.Fatal("refunded ticket became eligible after restart")
	}
	wrong := cfg
	wrong.RunID = uuid.NewString()
	if _, err = initializeDurableProvider(ctx, pool, repo, wrong); err == nil {
		t.Fatal("foreign run adopted provider state")
	}
}
func TestDurableProviderCancellationCannotBecomeConfirmation(t *testing.T) {
	pool, cfg := durableProviderFixture(t)
	ctx := context.Background()
	repo := repository.NewPostgresPaymentRepository(pool)
	p, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatal(err)
	}
	op := dynamicPurchaseTestOperation(t, uuid.MustParse(cfg.DynamicPurchase.UserID))
	if _, _, err = repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	created, _, err := p.CreateIntent(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.RecordObservation(ctx, *created); err != nil {
		t.Fatal(err)
	}
	claim := domain.FinancialClaim{PaymentID: op.PaymentID, Kind: "cancel", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:cancel", Operation: op}
	if _, err = p.CancelIntent(ctx, claim, created.IntentID); err != nil {
		t.Fatal(err)
	}
	r, err := initializeDurableProvider(ctx, pool, repo, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, secret, err := r.RetrieveIntent(ctx, op.Identity.ProviderAccount, false, created.IntentID)
	if err != nil || got.Status != "canceled" || got.Collected || secret != "" {
		t.Fatal("cancelled intent became payable")
	}
	if _, err = r.dynamicConfirmationReceipt(op.PaymentID); err == nil {
		t.Fatal("cancelled intent confirmed")
	}
}
