//go:build admission_r4

package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/repository"
)

// This test requires a disposable empty PostgreSQL database, never the demo DB.
func TestStaticFixtureRestartPreservesReconciledRows(t *testing.T) {
	dsn := os.Getenv("R4_RESTART_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable restart database not configured")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var tables int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'").Scan(&tables); err != nil || tables != 0 {
		t.Fatal("restart test requires an empty database")
	}
	paths, err := filepath.Glob(filepath.Join(os.Getenv("R4_RESTART_TEST_MIGRATIONS"), "*.up.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatal("test migrations missing")
	}
	sort.Strings(paths)
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(path), err)
		}
	}
	cfg := bootstrapConfig{RuntimeMode: "default_worker"}
	for i := 0; i < 4; i++ {
		id := uuid.New()
		cfg.Fixtures = append(cfg.Fixtures, fixtureConfig{PaymentID: id.String(), BookingID: uuid.NewString(), UserID: uuid.NewString(), Generation: 1, PaymentIntentID: "pi_r4_" + id.String()[:8] + id.String()[9:13] + id.String()[14:18] + id.String()[19:23] + id.String()[24:], ProviderAccount: "acct_r4_local", AmountCents: 100, Currency: "usd", SeedFinancial: true})
	}
	repo := repository.NewPostgresPaymentRepository(pool)
	if err := initializeFinancialFixtures(ctx, pool, repo, cfg); err != nil {
		t.Fatalf("initial seed: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE payments SET booking_disposition='CONFIRMED', reconciliation_state=CASE WHEN id=$1 THEN 'pending' ELSE 'sale_verified' END", cfg.Fixtures[0].PaymentID); err != nil {
		t.Fatal(err)
	}
	var before, after string
	snapshot := func() string {
		var value string
		if err := pool.QueryRow(ctx, "SELECT jsonb_agg(to_jsonb(p) ORDER BY id)::text FROM payments p").Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	before = snapshot()
	if err := initializeFinancialFixtures(ctx, pool, repo, cfg); err != nil {
		t.Fatalf("reconciled restart must succeed: %v", err)
	}
	after = snapshot()
	if before != after {
		t.Fatal("restart modified durable payment rows")
	}
	if err := requireUnusedDynamicBuyer(ctx, pool, cfg.Fixtures[0].UserID); err == nil {
		t.Fatal("dynamic buyer history fence bypassed")
	}
	if err := requireUnusedDynamicBuyer(ctx, pool, uuid.NewString()); err != nil {
		t.Fatal("unused dynamic buyer refused")
	}
	if resumed, err := validateStaticRestart(nil, true, cfg.Fixtures); err == nil || resumed {
		t.Fatal("durable refund audit must refuse recovery")
	}
	for _, sql := range []string{
		"UPDATE payments SET refund_requested_at=clock_timestamp() WHERE id=$1",
		"UPDATE payments SET refund_first_attempt_at=clock_timestamp() WHERE id=$1",
		"UPDATE payments SET refund_provider_id='re_existing' WHERE id=$1",
		"UPDATE payments SET booking_disposition='CANCELLED' WHERE id=$1",
		"UPDATE payments SET payment_intent_id='pi_r4_wrong' WHERE id=$1",
		"UPDATE payments SET frozen_create=jsonb_set(frozen_create,'{receipt_email}','\"changed@example.test\"') WHERE id=$1",
		"DELETE FROM payments WHERE id=$1",
	} {
		// Mutations below are confined to the disposable test database.
		var backup string
		if err := pool.QueryRow(ctx, "SELECT to_jsonb(p)::text FROM payments p WHERE id=$1", cfg.Fixtures[0].PaymentID).Scan(&backup); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, sql, cfg.Fixtures[0].PaymentID); err != nil {
			t.Fatal(err)
		}
		negativeBefore := snapshot()
		if err := initializeFinancialFixtures(ctx, pool, repo, cfg); err == nil {
			t.Fatalf("unsafe restart accepted: %s", sql)
		}
		if negativeBefore != snapshot() {
			t.Fatal("refused restart modified rows")
		}
		if _, err := pool.Exec(ctx, "DELETE FROM payments WHERE id=$1", cfg.Fixtures[0].PaymentID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO payments SELECT * FROM jsonb_populate_record(NULL::payments,$1::jsonb)", backup); err != nil {
			t.Fatal(err)
		}
	}
}
