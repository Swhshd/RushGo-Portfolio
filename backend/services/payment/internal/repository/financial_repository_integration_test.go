package repository

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

type financialPersistence interface {
	CreateOrGet(context.Context, domain.CreateOperation) (*domain.FinancialView, bool, error)
	GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error)
	GetByLogicalIdentity(context.Context, uuid.UUID, int64) (*domain.FinancialView, error)
	RecordObservation(context.Context, domain.ProviderObservation) error
}

// The RED adapter exercises today's real PG writes, not an invented failure stub.
// It disappears from the execution path when the target capability is implemented.
type legacyFinancialAdapter struct{ repo *PostgresPaymentRepository }

func (a legacyFinancialAdapter) CreateOrGet(ctx context.Context, op domain.CreateOperation) (*domain.FinancialView, bool, error) {
	p := &domain.Payment{ID: op.PaymentID, UserId: op.Identity.UserID, BookingId: &op.Identity.BookingID,
		Status: domain.PaymentPending, Price: int32(op.Identity.AmountCents), Currency: op.Identity.Currency}
	if err := a.repo.CreatePayment(ctx, p); err != nil {
		return nil, false, err
	}
	return &domain.FinancialView{Payment: p, Generation: 1, ProviderOutcome: "unknown"}, true, nil
}
func (a legacyFinancialAdapter) GetFinancial(ctx context.Context, id uuid.UUID) (*domain.FinancialView, error) {
	p, err := a.repo.GetPaymentByID(ctx, id)
	return &domain.FinancialView{Payment: p, Generation: 1, ProviderOutcome: "unknown"}, err
}
func (a legacyFinancialAdapter) GetByLogicalIdentity(ctx context.Context, id uuid.UUID, generation int64) (*domain.FinancialView, error) {
	rows, err := a.repo.GetPaymentsByBookingIds(ctx, []uuid.UUID{id})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("logical lifecycle has %d rows, want one", len(rows))
	}
	return &domain.FinancialView{Payment: &rows[0], Generation: generation, ProviderOutcome: "unknown"}, nil
}
func (a legacyFinancialAdapter) RecordObservation(ctx context.Context, observation domain.ProviderObservation) error {
	status := domain.PaymentFail
	if observation.Collected {
		status = domain.PaymentSuccess
	}
	return a.repo.UpdatePaymentStatus(ctx, observation.PaymentID, status)
}

func paymentFinancialFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("PAYMENT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("BLOCKED: PAYMENT_TEST_DATABASE_URL must identify an isolated real PG fixture")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Path != "/ticketbox_payment_test" || parsed.Port() != "25439" {
		t.Fatal("BLOCKED: refusing non-isolated Payment test database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("connect isolated Payment PG")
	}
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "financial_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Errorf("drop disposable schema: %v", err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolatedDSN := parsed.String()
	cmd := exec.Command("migrate", "-path", filepath.ToSlash(filepath.Join("..", "..", "migrations")), "-database", isolatedDSN, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual isolated migrations failed: %s", strings.ReplaceAll(string(output), isolatedDSN, "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(isolatedDSN)
	if err != nil {
		t.Fatal("parse isolated schema config")
	}
	config.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("connect isolated schema")
	}
	t.Cleanup(pool.Close)
	return pool, isolatedDSN
}

func financialFixtureOperation() domain.CreateOperation {
	id := uuid.New()
	op := domain.CreateOperation{
		PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create",
		Identity: domain.FinancialIdentity{BookingID: uuid.New(), UserID: uuid.New(), Generation: 1,
			ProviderAccount: "acct_fixture_test", AmountCents: 725, Currency: "usd"},
		FrozenParams: json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`),
	}
	// Independently hand-written canonical fixture, not the production hash helper.
	op.Identity.Fingerprint = sha256.Sum256([]byte(fmt.Sprintf("v1\n%s\n%s\n1\n725\n\"usd\"\n\"acct_fixture_test\"\ntest\ntrue\n\"always\"\n\"\"\n", op.Identity.BookingID, op.Identity.UserID)))
	return op
}

func financialCapability(repo *PostgresPaymentRepository) financialPersistence {
	if capability, ok := any(repo).(financialPersistence); ok {
		return capability
	}
	return legacyFinancialAdapter{repo}
}

func TestPaymentLogicalIdentityAndNullableLoadersIntegration(t *testing.T) {
	pool, _ := paymentFinancialFixture(t)
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(pool)
	finance := financialCapability(repo)
	t.Run("sixteen_retries_one_payment", func(t *testing.T) {
		op := financialFixtureOperation()
		start := make(chan struct{})
		var wg sync.WaitGroup
		ids := make(chan uuid.UUID, 16)
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				attempt := op
				attempt.PaymentID = uuid.New()
				attempt.Key = "ticketbox:payment:" + attempt.PaymentID.String() + ":g1:create"
				<-start
				view, _, err := finance.CreateOrGet(ctx, attempt)
				if err != nil {
					errs <- err
					return
				}
				ids <- view.Payment.ID
			}()
		}
		close(start)
		wg.Wait()
		close(ids)
		close(errs)
		for err := range errs {
			t.Errorf("identical retry rejected: %v", err)
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments WHERE booking_id=$1`, op.Identity.BookingID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("logical Booking/g1 retry created %d Payment rows, want 1", count)
		}
		unique := map[uuid.UUID]bool{}
		for id := range ids {
			unique[id] = true
		}
		if len(unique) != 1 {
			t.Errorf("retries returned %d distinct Payment IDs, want 1", len(unique))
		}
		if count != 1 {
			return
		}
		view, err := finance.GetByLogicalIdentity(ctx, op.Identity.BookingID, 1)
		if err != nil {
			t.Fatal(err)
		}
		var key string
		if err := pool.QueryRow(ctx, `SELECT create_key FROM payments WHERE id=$1`, view.Payment.ID).Scan(&key); err != nil {
			t.Fatal(err)
		}
		if key != "ticketbox:payment:"+view.Payment.ID.String()+":g1:create" {
			t.Fatal("create key does not belong to persisted Payment")
		}
	})
	t.Run("immutable_conflicts", func(t *testing.T) {
		for _, field := range []string{"amount", "user", "account", "mode", "currency", "frozen_params"} {
			t.Run(field, func(t *testing.T) {
				op := financialFixtureOperation()
				if _, _, err := finance.CreateOrGet(ctx, op); err != nil {
					t.Fatal(err)
				}
				changed := op
				changed.PaymentID = uuid.New()
				changed.Key = "ticketbox:payment:" + changed.PaymentID.String() + ":g1:create"
				switch field {
				case "amount":
					changed.Identity.AmountCents = 726
					changed.FrozenParams = json.RawMessage(`{"version":1,"amount":726,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
				case "user":
					changed.Identity.UserID = uuid.New()
				case "account":
					changed.Identity.ProviderAccount = "acct_other"
				case "mode":
					changed.Identity.LiveMode = true
				case "currency":
					changed.Identity.Currency = "eur"
				case "frozen_params":
					changed.FrozenParams = json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"},"receipt_email":"different@example.test"}`)
				}
				if field != "currency" {
					changed.Identity.Fingerprint, _ = domain.FinancialFingerprint(changed.Identity, changed.FrozenParams)
				}
				_, _, err := finance.CreateOrGet(ctx, changed)
				if err == nil || (field != "currency" && !errors.Is(err, ErrFinancialConflict)) {
					t.Errorf("changed immutable %s did not return identity conflict: %v", field, err)
				}
			})
		}
	})
	t.Run("all_nullable_loaders", func(t *testing.T) {
		booking, user := uuid.New(), uuid.New()
		ids := []uuid.UUID{uuid.New(), uuid.New()}
		if _, err := pool.Exec(ctx, `INSERT INTO payments(id,user_id,booking_id,status,price,currency,payment_intent_id) VALUES
			($1,$3,$4,NULL,725,'usd',NULL),($2,$3,$4,NULL,725,'usd','pi_nullable_fixture')`, ids[0], ids[1], user, booking); err != nil {
			t.Fatal(err)
		}
		assertAbsent := func(p *domain.Payment) {
			t.Helper()
			if p.OrderId != nil || p.Transaction_id != nil || p.PaymentMethod != "" || p.Status != "" {
				t.Error("NULL optional fields fabricated as present")
			}
		}
		for _, id := range ids {
			p, err := repo.GetPaymentByID(ctx, id)
			if err != nil {
				t.Errorf("ID loader cannot read legal NULL: %v", err)
				continue
			}
			assertAbsent(p)
		}
		p, err := repo.GetPaymentByPaymentIntentId(ctx, "pi_nullable_fixture")
		if err != nil {
			t.Errorf("intent loader cannot read legal NULL: %v", err)
		} else {
			assertAbsent(p)
		}
		for _, method := range []string{"booking", "condition"} {
			var rows []domain.Payment
			var err error
			if method == "booking" {
				rows, err = repo.GetPaymentsByBookingIds(ctx, []uuid.UUID{booking})
			} else {
				rows, err = repo.GetListPaymentsByCondition(ctx, &domain.GetPaymentsByCondition{BookingId: &booking})
			}
			if err != nil {
				t.Errorf("%s loader cannot read legal NULL: %v", method, err)
				continue
			}
			if len(rows) != 2 {
				t.Errorf("%s returned %d rows, want 2", method, len(rows))
			}
			for i := range rows {
				assertAbsent(&rows[i])
			}
		}
	})
	t.Run("collected_is_monotonic", func(t *testing.T) {
		op := financialFixtureOperation()
		view, _, err := finance.CreateOrGet(ctx, op)
		if err != nil {
			t.Fatal(err)
		}
		observation := domain.ProviderObservation{PaymentID: view.Payment.ID, BookingID: op.Identity.BookingID, Generation: 1,
			Account: op.Identity.ProviderAccount, IntentID: "pi_collected_fixture", AmountCents: 725, Currency: "usd",
			Collected: true, Status: "succeeded", ObservedAt: time.Now()}
		if err = finance.RecordObservation(ctx, observation); err != nil {
			t.Fatal(err)
		}
		if err = repo.UpdatePaymentStatus(ctx, view.Payment.ID, domain.PaymentTimeout); !errors.Is(err, ErrFinancialStateProtected) {
			t.Fatal("generic status setter did not reject managed financial write", err)
		}
		observation.Collected = false
		observation.Status = "requires_payment_method"
		if err = finance.RecordObservation(ctx, observation); err != nil {
			t.Fatal(err)
		}
		// Even the generic legacy adapter must not erase previously verified collection.
		p := *view.Payment
		p.Status = domain.PaymentFail
		if err = repo.UpdatePayment(ctx, &p); !errors.Is(err, ErrFinancialStateProtected) {
			t.Fatal("generic projection setter did not reject managed financial write", err)
		}
		actual, err := finance.GetFinancial(ctx, view.Payment.ID)
		if err != nil {
			t.Fatal(err)
		}
		if actual.ProviderOutcome != "collected" || actual.Payment.Status != domain.PaymentSuccess {
			t.Errorf("collection regressed: outcome=%q status=%q, want collected/success", actual.ProviderOutcome, actual.Payment.Status)
		}
	})
}

func TestFinancialMigrationDownRetainsManagedFactsIntegration(t *testing.T) {
	pool, dsn := paymentFinancialFixture(t)
	ctx := context.Background()
	// Exercise this migration's down even after later receipt migrations exist.
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", dsn, "goto", "202610110001").CombinedOutput(); err != nil {
		t.Fatalf("select financial spine migration: %s", strings.ReplaceAll(string(output), dsn, "[isolated DSN]"))
	}
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", dsn, "down", "1").CombinedOutput(); err != nil {
		t.Fatalf("empty disposable migration rollback failed: %s", strings.ReplaceAll(string(output), dsn, "[isolated DSN]"))
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='payments' AND column_name='managed')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("empty down retained managed fields")
	}
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", dsn, "up", "1").CombinedOutput(); err != nil {
		t.Fatalf("reapply actual financial migration: %s", strings.ReplaceAll(string(output), dsn, "[isolated DSN]"))
	}
	op := financialFixtureOperation()
	if _, _, err := NewPostgresPaymentRepository(pool).CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("migrate", "-path", "../../migrations", "-database", dsn, "down", "1").CombinedOutput()
	if err == nil || !strings.Contains(string(output), "refusing to remove managed financial identities") {
		t.Fatal("migration down did not refuse loss of durable financial responsibility")
	}
	v, err := NewPostgresPaymentRepository(pool).GetFinancial(ctx, op.PaymentID)
	if err != nil || v.Payment.ID != op.PaymentID {
		t.Fatal("refused down lost managed identity")
	}
}

// Drop the actual PostgreSQL COMMIT CommandComplete and ReadyForQuery messages.
// The server accepted COMMIT; the client gets EOF. No product test hook is used.
func paymentCommitACKProxy(t *testing.T, upstream string) (string, <-chan bool) {
	t.Helper()
	parsed, err := url.Parse(upstream)
	if err != nil || parsed.Host == "" {
		t.Fatal("invalid isolated Payment upstream")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	barrier := make(chan bool, 1)
	go func() {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		server, err := net.Dial("tcp", parsed.Host)
		if err != nil {
			return
		}
		defer server.Close()
		go func() { io.Copy(server, client) }()
		for {
			var header [5]byte
			if _, err := io.ReadFull(server, header[:]); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint32(header[1:])) - 4
			if length < 0 || length > 16*1024*1024 {
				return
			}
			body := make([]byte, length)
			if _, err := io.ReadFull(server, body); err != nil {
				return
			}
			if header[0] == 'C' && string(body) == "COMMIT\x00" {
				barrier <- true
				return
			}
			if _, err := client.Write(append(header[:], body...)); err != nil {
				return
			}
		}
	}()
	return listener.Addr().String(), barrier
}

func TestFinancialCommitACKLossReloadsOriginalIdentityIntegration(t *testing.T) {
	pool, dsn := paymentFinancialFixture(t)
	address, committed := paymentCommitACKProxy(t, dsn)
	parsed, _ := url.Parse(dsn)
	parsed.Host = address
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal("parse commit ACK proxy")
	}
	config.MaxConns = 1
	proxyPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal("connect commit ACK proxy")
	}
	defer proxyPool.Close()
	op := financialFixtureOperation()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := NewPostgresPaymentRepository(proxyPool).CreateOrGet(ctx, op); err == nil {
		t.Fatal("COMMIT ACK was not lost")
	}
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("no actual committed-window barrier")
	}
	replay := op
	replay.PaymentID = uuid.New()
	replay.Key = "ticketbox:payment:" + replay.PaymentID.String() + ":g1:create"
	v, created, err := NewPostgresPaymentRepository(pool).CreateOrGet(context.Background(), replay)
	if err != nil {
		t.Fatal(err)
	}
	if created || v.Payment.ID != op.PaymentID {
		t.Fatal("lost COMMIT ACK created a second identity")
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE booking_id=$1`, op.Identity.BookingID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("COMMIT uncertainty left %d lifecycles", count)
	}
}

func TestFinancialLegacyPreflightReportsWithoutMutation(t *testing.T) {
	cmd := exec.Command("python", "../../../../scripts/reliability/preflight.py", "--self-test")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("legacy preflight fixture failed: %s", output)
	}
}

func TestFinancialRealPreflightReportsLegacyHistoryIntegration(t *testing.T) {
	pool, _ := paymentFinancialFixture(t)
	ctx := context.Background()
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	booking := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO payments(user_id,booking_id,status,price,currency,payment_intent_id) VALUES
		($1,$2,'success',725,'usd','pi_legacy_duplicate'),($1,$2,'timeout',725,'usd','pi_legacy_duplicate'),
		($1,NULL,NULL,725,'usd',NULL)`, uuid.New(), booking); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]any{"version": 1, "compose_project": os.Getenv("RELIABILITY_TEST_PROJECT"),
		"postgres_containers": map[string]any{"payment": map[string]any{
			"container": os.Getenv("RELIABILITY_TEST_PG_CONTAINER"), "database": "ticketbox_payment_test", "user": "reliability_fixture", "schema": schema}}}
	file := filepath.Join(t.TempDir(), "manifest.json")
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("python", "../../../../scripts/reliability/preflight.py", "--manifest", file, "--read-only").CombinedOutput()
	if err == nil {
		t.Fatal("unclassified legacy financial history allowed activation")
	}
	var report struct {
		Status   string `json:"status"`
		Services map[string]struct {
			Counts   map[string]int `json:"counts"`
			Mutation bool           `json:"mutation"`
		} `json:"services"`
	}
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatalf("invalid preflight report: %s", output)
	}
	counts := report.Services["payment"].Counts
	if report.Status != "BLOCKED" || counts["duplicate_logical_groups"] != 1 || counts["duplicate_provider_intent_groups"] != 1 ||
		counts["unknown_intent_rows"] != 1 || counts["legacy_provider_verification_rows"] != 2 || report.Services["payment"].Mutation {
		t.Fatalf("incorrect legacy classification: %s", output)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != 3 || after != 3 {
		t.Fatal("read-only preflight changed legacy history")
	}
}

func TestFinancialIdentityRejectsUnsafeSnapshotsIntegration(t *testing.T) {
	pool, _ := paymentFinancialFixture(t)
	repo := NewPostgresPaymentRepository(pool)
	for _, name := range []string{"zero_amount", "overflow", "generation", "empty_account", "zero_user", "wrong_key", "hash_mismatch", "frozen_amount", "frozen_currency", "frozen_methods"} {
		t.Run(name, func(t *testing.T) {
			op := financialFixtureOperation()
			switch name {
			case "zero_amount":
				op.Identity.AmountCents = 0
			case "overflow":
				op.Identity.AmountCents = 2147483648
			case "generation":
				op.Identity.Generation = 2
			case "empty_account":
				op.Identity.ProviderAccount = ""
			case "zero_user":
				op.Identity.UserID = uuid.Nil
			case "wrong_key":
				op.Key = "random-attempt-key"
			case "hash_mismatch":
				op.Identity.Fingerprint = sha256.Sum256([]byte("wrong snapshot"))
			case "frozen_amount":
				op.FrozenParams = json.RawMessage(`{"version":1,"amount":726,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
			case "frozen_currency":
				op.FrozenParams = json.RawMessage(`{"version":1,"amount":725,"currency":"eur","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
			case "frozen_methods":
				op.FrozenParams = json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":false,"allow_redirects":"never"}}`)
			}
			if _, _, err := repo.CreateOrGet(context.Background(), op); err == nil {
				t.Errorf("unsafe immutable %s snapshot accepted", name)
			}
		})
	}
}
