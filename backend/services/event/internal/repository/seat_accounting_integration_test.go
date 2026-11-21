package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

type accountingPersistence interface {
	ApplySaleAccounting(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error)
}

type legacySaleAccountingAdapter struct{ tiers *PostgresTicketTierRepository }

func (a legacySaleAccountingAdapter) ApplySaleAccounting(ctx context.Context, q domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	for tier, n := range q.TierQuantities {
		if _, err := a.tiers.UpdateAvailabilityPessimistic(ctx, tier, -n); err != nil {
			return nil, err
		}
	}
	return &domain.SeatLifecycleResult{State: "booked", AccountingState: "applied", Applied: true}, nil
}
func accountingCapability(r *PostgresSeatRepository, p *pgxpool.Pool) accountingPersistence {
	if actual, ok := any(r).(accountingPersistence); ok {
		return actual
	}
	return legacySaleAccountingAdapter{NewPostgresTicketTierRepository(p)}
}

func seedAccountingBooking(t *testing.T, p *pgxpool.Pool) (domain.SeatLifecycleRequest, uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	event, first, ids := seedLifecycleSeats(t, p, 3)
	second := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity) VALUES($1,$2,'second',800,4,4)`, second, event); err != nil {
		t.Fatal(err)
	}
	var hasBaseline bool
	if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='ticket_tiers' AND column_name='lifecycle_accounting_baseline')`).Scan(&hasBaseline); err != nil {
		t.Fatal(err)
	}
	if hasBaseline {
		// These are new isolated tiers with independently established counts,
		// never a recalculation or adoption of preserved product inventory.
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=ANY($1)`, []uuid.UUID{first, second}); err != nil {
			t.Fatal(err)
		}
	}
	var other []uuid.UUID
	for range 4 {
		id := uuid.New()
		other = append(other, id)
		if _, err := p.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id) VALUES($1,$2,$3)`, id, event, second); err != nil {
			t.Fatal(err)
		}
	}
	q := lifecycleRequest(event, uuid.New(), []uuid.UUID{ids[1], other[0], ids[0]}, time.Now().Add(time.Hour))
	q.TierQuantities = map[uuid.UUID]int32{first: 2, second: 1}
	r := NewPostgresSeatRepository(p)
	if _, err := r.ApplyLifecycle(ctx, q); err != nil {
		t.Fatal(err)
	}
	q.Action = "book"
	if _, err := r.ApplyLifecycle(ctx, q); err != nil {
		t.Fatal(err)
	}
	q.Action = "account"
	return q, first, second
}
func assertTierCounts(t *testing.T, p *pgxpool.Pool, first, second uuid.UUID, a, b int32) {
	t.Helper()
	for id, want := range map[uuid.UUID]int32{first: a, second: b} {
		var got int32
		if err := p.QueryRow(context.Background(), `SELECT available_quantity FROM ticket_tiers WHERE id=$1`, id).Scan(&got); err != nil || got != want {
			t.Errorf("tier %s got %d want %d err=%v", id, got, want, err)
		}
	}
}

func TestSaleAccountingAndReversalApplyOnceIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	r := NewPostgresSeatRepository(p)
	account := accountingCapability(r, p)
	t.Run("five_debits_five_reversals_restart_replay", func(t *testing.T) {
		q, first, second := seedAccountingBooking(t, p)
		for range 5 {
			if _, err := account.ApplySaleAccounting(ctx, q); err != nil {
				t.Errorf("duplicate debit: %v", err)
			}
		}
		assertTierCounts(t, p, first, second, 1, 3)
		q.Action = "close"
		for range 5 {
			if _, err := NewPostgresSeatRepository(p).ApplyLifecycle(ctx, q); err != nil {
				t.Errorf("close replay: %v", err)
			}
		}
		assertTierCounts(t, p, first, second, 3, 4)
		q.Action = "account"
		if _, err := account.ApplySaleAccounting(ctx, q); err == nil {
			t.Error("closed lifecycle accepted accounting")
		}
		assertTierCounts(t, p, first, second, 3, 4)
	})
	t.Run("second_tier_failure_no_partial_commit", func(t *testing.T) {
		q, first, second := seedAccountingBooking(t, p)
		// Sort physical tier identities independently, then fail whichever is
		// second in the mandated lock/update order.
		fail := second
		if first.String() > second.String() {
			fail = first
		}
		if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers ADD CONSTRAINT fixture_no_second_debit CHECK(id<>'`+fail.String()+`'::uuid OR available_quantity=total_quantity)`); err != nil {
			t.Fatal(err)
		}
		if _, err := account.ApplySaleAccounting(ctx, q); err == nil {
			t.Fatal("second-tier CHECK did not fail")
		}
		assertTierCounts(t, p, first, second, 3, 4)
		if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers DROP CONSTRAINT fixture_no_second_debit`); err != nil {
			t.Fatal(err)
		}
		if _, err := account.ApplySaleAccounting(ctx, q); err != nil {
			t.Fatal(err)
		}
		assertTierCounts(t, p, first, second, 1, 3)
	})
	t.Run("seat_derived_delta_and_legacy_bypass", func(t *testing.T) {
		q, first, second := seedAccountingBooking(t, p)
		wrong := q
		wrong.TierQuantities = map[uuid.UUID]int32{first: 1, second: 1}
		if _, err := account.ApplySaleAccounting(ctx, wrong); err == nil {
			t.Error("untrusted tier delta accepted")
		}
		assertTierCounts(t, p, first, second, 3, 4)
		tiers := NewPostgresTicketTierRepository(p)
		if _, err := tiers.UpdateAvailabilityPessimistic(ctx, first, -1); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("pessimistic managed bypass: %v", err)
		}
		if _, err := tiers.UpdateAvailability(ctx, second, -1, 1); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("optimistic managed bypass: %v", err)
		}
		if err := tiers.UpdateAvailabilityNoLock(ctx, second, -1); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("naive managed bypass: %v", err)
		}
		assertTierCounts(t, p, first, second, 3, 4)
	})
}

func TestAccountingUnknownBaselineAndDriftRemainBlockedIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	r := NewPostgresSeatRepository(p)
	for _, scenario := range []string{"unknown", "drift", "seat_coverage", "unmanaged_booked"} {
		t.Run(scenario, func(t *testing.T) {
			q, first, second := seedAccountingBooking(t, p)
			switch scenario {
			case "unknown":
				if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline=NULL WHERE id=$1`, first); err != nil {
					t.Fatal(err)
				}
			case "drift":
				if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET available_quantity=2 WHERE id=$1`, first); err != nil {
					t.Fatal(err)
				}
			case "seat_coverage":
				if _, err := p.Exec(ctx, `UPDATE seats SET deleted_at=NOW() WHERE ticket_tier_id=$1 AND status='available'`, first); err != nil {
					t.Fatal(err)
				}
			case "unmanaged_booked":
				if _, err := p.Exec(ctx, `UPDATE seats SET status='booked',booking_id=$2 WHERE ticket_tier_id=$1 AND status='available'`, first, uuid.New()); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.ApplySaleAccounting(ctx, q); !errors.Is(err, ErrAccountingBaseline) {
				t.Fatalf("unknown/drift accepted: %v", err)
			}
			var flag string
			if err := p.QueryRow(ctx, `SELECT accounting_state FROM booking_seat_lifecycles WHERE booking_id=$1`, q.BookingID).Scan(&flag); err != nil || flag != "not_applied" {
				t.Fatalf("failure committed flag: %s %v", flag, err)
			}
			want := int32(3)
			if scenario == "drift" {
				want = 2
			}
			assertTierCounts(t, p, first, second, want, 4)
		})
	}
}

func TestAccountingBackendDeathBeforeSecondTierCommitIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q, first, second := seedAccountingBooking(t, p)
	fail := second
	if first.String() > second.String() {
		fail = first
	}
	blocker, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_xact_lock(7946281)`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `CREATE FUNCTION fixture_block_second_tier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='`+fail.String()+`'::uuid THEN PERFORM pg_advisory_xact_lock(7946281); END IF; RETURN NEW; END $$;
	 CREATE TRIGGER fixture_second_tier BEFORE UPDATE ON ticket_tiers FOR EACH ROW EXECUTE FUNCTION fixture_block_second_tier()`); err != nil {
		t.Fatal(err)
	}
	config := p.Config().Copy()
	name := "accounting_kill_" + uuid.NewString()
	config.ConnConfig.RuntimeParams["application_name"] = name
	worker, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	done := make(chan error, 1)
	go func() { _, err := NewPostgresSeatRepository(worker).ApplySaleAccounting(ctx, q); done <- err }()
	var pid int
	for {
		err := p.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event='advisory'`, name).Scan(&pid)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("second-tier trigger barrier not reached")
		case <-time.After(time.Millisecond):
		}
	}
	var killed bool
	if err := p.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&killed); err != nil || !killed {
		t.Fatalf("kill backend: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("terminated accounting reported success")
	}
	assertTierCounts(t, p, first, second, 3, 4)
	var flag string
	if err := p.QueryRow(ctx, `SELECT accounting_state FROM booking_seat_lifecycles WHERE booking_id=$1`, q.BookingID).Scan(&flag); err != nil || flag != "not_applied" {
		t.Fatalf("death committed flag: %s %v", flag, err)
	}
	if _, err := p.Exec(ctx, `DROP TRIGGER fixture_second_tier ON ticket_tiers; DROP FUNCTION fixture_block_second_tier()`); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPostgresSeatRepository(p).ApplySaleAccounting(ctx, q); err != nil {
		t.Fatal(err)
	}
	assertTierCounts(t, p, first, second, 1, 3)
}

func TestSaleAccountingReallocatedBAndMigrationRetentionIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	r := NewPostgresSeatRepository(p)
	q, first, second := seedAccountingBooking(t, p)
	if _, err := r.ApplySaleAccounting(ctx, q); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/000006_lifecycle_sale_accounting.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, string(down)); err == nil {
		t.Fatal("down erased sale facts")
	}
	q.Action = "close"
	if _, err := r.ApplyLifecycle(ctx, q); err != nil {
		t.Fatal(err)
	}
	b := q
	b.BookingID = uuid.New()
	b.OperationID = uuid.New()
	b.Action = "reserve"
	if _, err := r.ApplyLifecycle(ctx, b); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := r.ApplyLifecycle(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	assertTierCounts(t, p, first, second, 3, 4)
	for _, id := range q.SeatIDs {
		var owner uuid.UUID
		if err := p.QueryRow(ctx, `SELECT reserved_by_booking_id FROM seats WHERE id=$1`, id).Scan(&owner); err != nil || owner != b.BookingID {
			t.Fatalf("B changed %s %v", owner, err)
		}
	}
}

func TestEventReadOnlyAccountingPreflightIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	_, first, second := seedAccountingBooking(t, p)
	if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline=NULL WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := p.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]any{"version": 1, "compose_project": os.Getenv("RELIABILITY_TEST_PROJECT"), "postgres_containers": map[string]any{"event": map[string]string{"container": os.Getenv("RELIABILITY_TEST_PG_CONTAINER"), "database": "ticketbox_event_test", "user": "reliability_fixture", "schema": schema}}}
	path := filepath.Join(t.TempDir(), "manifest.json")
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("python", "../../../../scripts/reliability/preflight.py", "--manifest", path, "--read-only").CombinedOutput()
	if err == nil || !strings.Contains(string(output), `"unknown_baseline_tiers": 1`) {
		t.Fatalf("unclassified tier not blocked: %s", output)
	}
	assertTierCounts(t, p, first, second, 3, 4)
	var baseline *string
	if err := p.QueryRow(ctx, `SELECT lifecycle_accounting_baseline FROM ticket_tiers WHERE id=$1`, first).Scan(&baseline); err != nil || baseline != nil {
		t.Fatal("preflight classified or mutated unknown tier")
	}
}

func TestNewSeatedTierGetsProvenBaselineOnlyAfterCompleteGenerationIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	r := NewPostgresSeatRepository(p)
	event, tier := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date)VALUES($1,'new','concert','fixture','fixture',NOW());`, event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity)VALUES($1,$2,'new',100,2,2)`, tier, event); err != nil {
		t.Fatal(err)
	}
	var seats []*domain.Seat
	for range 2 {
		seats = append(seats, &domain.Seat{ID: uuid.New(), EventID: event, TicketTierID: tier, Status: domain.SeatStatusAvailable, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	}
	if err := r.CreateBatch(ctx, seats); err != nil {
		t.Fatal(err)
	}
	var baseline *string
	if err := p.QueryRow(ctx, `SELECT lifecycle_accounting_baseline FROM ticket_tiers WHERE id=$1`, tier).Scan(&baseline); err != nil || baseline == nil || *baseline != "verified" {
		t.Fatalf("new proven seated inventory blocked: %v %v", baseline, err)
	}
	// A partial or historical seeded tier must not be promoted by an additional
	// batch merely because it eventually happens to match a count.
	other := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity)VALUES($1,$2,'unknown',100,2,2)`, other, event); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		seat := &domain.Seat{ID: uuid.New(), EventID: event, TicketTierID: other, Status: domain.SeatStatusAvailable, CreatedAt: time.Now(), UpdatedAt: time.Now()}
		if err := r.CreateBatch(ctx, []*domain.Seat{seat}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.QueryRow(ctx, `SELECT lifecycle_accounting_baseline FROM ticket_tiers WHERE id=$1`, other).Scan(&baseline); err != nil || baseline != nil {
		t.Fatal("incomplete historical generation automatically adopted")
	}
}

func TestAccountingQuantityConstraintValidationAndEmptyDownSafetyIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	down, err := os.ReadFile("../../migrations/000006_lifecycle_sale_accounting.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers VALIDATE CONSTRAINT ticket_tier_quantity_bounds`); err != nil {
		t.Fatal(err)
	}
	event, tier, _ := seedLifecycleSeats(t, p, 3)
	for _, n := range []int{-1, 4} {
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET available_quantity=$2 WHERE id=$1`, tier, n); err == nil {
			t.Errorf("out-of-bounds %d accepted", n)
		}
	}
	// Simulate pre-existing drift before a NOT VALID constraint is installed;
	// validation must refuse it and preserve the actual historical quantity.
	if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers DROP CONSTRAINT ticket_tier_quantity_bounds`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity) VALUES($1,$2,'historical',100,1,2)`, uuid.New(), event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers ADD CONSTRAINT ticket_tier_quantity_bounds CHECK(total_quantity>=0 AND available_quantity>=0 AND available_quantity<=total_quantity) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `ALTER TABLE ticket_tiers VALIDATE CONSTRAINT ticket_tier_quantity_bounds`); err == nil {
		t.Fatal("historical drift silently validated")
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM ticket_tiers WHERE available_quantity>total_quantity`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("history clamped/deleted: %d %v", count, err)
	}
}
