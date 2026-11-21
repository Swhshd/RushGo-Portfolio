package repository

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func metricsFixture(t *testing.T) *pgxpool.Pool {
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
	name := "rushgo_mvp_metrics_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, `DROP DATABASE "`+name+`"`); admin.Close() })
	dir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("migrate", "-path", ".", "-database", dsn, "up")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migration: %v %s", err, out)
	}
	return pool
}
func TestMoneyMetricsKeepUnknownSeparateIntegration(t *testing.T) {
	pool := metricsFixture(t)
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(pool)
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	err := repo.RecordObservation(ctx, domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, AmountCents: 725, Currency: "usd", Account: op.Identity.ProviderAccount, LiveMode: false, IntentID: "pi_metrics_isolated", Status: "succeeded", Collected: true, ObservedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	unknown := financialFixtureOperation()
	if _, _, err = repo.CreateOrGet(ctx, unknown); err != nil {
		t.Fatal(err)
	}
	// Legacy success lacks managed evidence; it must not increase known revenue.
	_, err = pool.Exec(ctx, `INSERT INTO payments(user_id,status,price,currency)VALUES($1,'success',999,'eur')`, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	v, err := repo.MoneyMetrics(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Groups) != 2 {
		t.Fatalf("currency groups %+v", v.Groups)
	}
	for _, g := range v.Groups {
		switch g.Currency {
		case "usd":
			if g.Payments != 2 || g.UnknownPayments != 1 || g.CollectedKnownMinor != 725 || g.RefundedKnownMinor != 0 {
				t.Fatalf("USD facts %+v", g)
			}
		case "eur":
			if g.UnknownPayments != 1 || g.CollectedKnownMinor != 0 || g.Mode != "unknown" {
				t.Fatalf("legacy facts %+v", g)
			}
		default:
			t.Fatal("unexpected currency")
		}
	}
}
