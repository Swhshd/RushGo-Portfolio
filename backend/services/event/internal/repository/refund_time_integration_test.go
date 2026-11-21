package repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/refundtime"
)

func TestRefundTimeAcquirePersistsQualificationAndSameBindingReplay(t *testing.T) {
	p, _, authority := refundTimeIntegrationFixture(t)
	eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
	binding := newRefundTimeTestBinding(eventID)

	first, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	second, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatalf("same-binding acquire replay: %v", err)
	}
	if first.State != "QUALIFIED" || !first.QualifiedAt.Before(first.StartsAt) || first.Version != 1 {
		t.Fatalf("qualified permit=%+v, want qualified before start at date version 1", first)
	}
	if second != first {
		t.Fatalf("same-binding replay changed permit: first=%+v replay=%+v", first, second)
	}
}

func TestRefundTimeReleaseBeforeAcquireCannotResurrect(t *testing.T) {
	p, _, authority := refundTimeIntegrationFixture(t)
	eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
	binding := newRefundTimeTestBinding(eventID)

	released, err := authority.Release(context.Background(), binding, "ABORTED")
	if err != nil {
		t.Fatalf("release before acquire: %v", err)
	}
	acquired, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatalf("acquire after release tombstone: %v", err)
	}
	replayed, err := authority.Release(context.Background(), binding, "ABORTED")
	if err != nil {
		t.Fatalf("release tombstone replay: %v", err)
	}
	if released.State != "RELEASED" || released.Version != 0 || !released.StartsAt.IsZero() || !released.QualifiedAt.IsZero() {
		t.Fatalf("release tombstone=%+v, want RELEASED without qualification evidence", released)
	}
	if acquired != released || replayed != released {
		t.Fatalf("tombstone changed across acquire/release: initial=%+v acquire=%+v replay=%+v", released, acquired, replayed)
	}
}

func TestRefundTimeBindingMismatchCannotChangeExistingPermit(t *testing.T) {
	p, _, authority := refundTimeIntegrationFixture(t)
	eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
	binding := newRefundTimeTestBinding(eventID)
	first, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}

	mismatch := binding
	mismatch.Fingerprint = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if _, err := authority.Acquire(context.Background(), mismatch); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("mismatched replay error=%v, want ErrBindingMismatch", err)
	}
	if _, err := authority.Release(context.Background(), mismatch, "ABORTED"); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("mismatched release error=%v, want ErrBindingMismatch", err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE event_refund_time_permits SET fingerprint=$2 WHERE attempt_id=$1`, binding.AttemptID, mismatch.Fingerprint); err == nil {
		t.Fatal("direct permit update changed the immutable binding fingerprint")
	}
	second, err := authority.Acquire(context.Background(), binding)
	if err != nil || second != first {
		t.Fatalf("original binding was changed: permit=%+v err=%v", second, err)
	}
}

func TestRefundTimeStartedEventRejectionCannotRefreshAfterDateChange(t *testing.T) {
	p, eventRepo, authority := refundTimeIntegrationFixture(t)
	startedAt := databaseTime(t, p).Add(-time.Minute)
	eventID := seedRefundTimeEvent(t, p, startedAt)
	binding := newRefundTimeTestBinding(eventID)

	rejected, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatalf("acquire started event: %v", err)
	}
	if rejected.State != "REJECTED" || rejected.QualifiedAt.Before(rejected.StartsAt) || rejected.Version != 1 {
		t.Fatalf("started event permit=%+v, want persisted time rejection", rejected)
	}
	event, err := eventRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	event.Date = databaseTime(t, p).Add(time.Hour)
	if err := eventRepo.UpdateDate(context.Background(), event, event.DateVersion); err != nil {
		t.Fatalf("move rejected event into future: %v", err)
	}

	replayed, err := authority.Acquire(context.Background(), binding)
	if err != nil {
		t.Fatalf("replay rejected binding: %v", err)
	}
	if replayed != rejected {
		t.Fatalf("rejected permit was refreshed after date change: before=%+v after=%+v", rejected, replayed)
	}
}

func TestRefundTimeDateChangeAndPermitAcquisitionSerializeInBothOrders(t *testing.T) {
	t.Run("date change first", func(t *testing.T) {
		p, eventRepo, authority := refundTimeIntegrationFixture(t)
		eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
		event, err := eventRepo.GetByID(context.Background(), eventID)
		if err != nil {
			t.Fatal(err)
		}
		event.Date = databaseTime(t, p).Add(-time.Minute)
		if err := eventRepo.UpdateDate(context.Background(), event, event.DateVersion); err != nil {
			t.Fatalf("date change before acquire: %v", err)
		}
		permit, err := authority.Acquire(context.Background(), newRefundTimeTestBinding(eventID))
		if err != nil {
			t.Fatalf("acquire after date change: %v", err)
		}
		if permit.State != "REJECTED" || permit.Version != 2 || !permit.StartsAt.Equal(event.Date) {
			t.Fatalf("permit after date change=%+v, want rejection at date version 2", permit)
		}
	})

	t.Run("permit first", func(t *testing.T) {
		p, eventRepo, authority := refundTimeIntegrationFixture(t)
		eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
		permit, err := authority.Acquire(context.Background(), newRefundTimeTestBinding(eventID))
		if err != nil || permit.State != "QUALIFIED" {
			t.Fatalf("acquire before date change: permit=%+v err=%v", permit, err)
		}
		event, err := eventRepo.GetByID(context.Background(), eventID)
		if err != nil {
			t.Fatal(err)
		}
		event.Date = databaseTime(t, p).Add(2 * time.Hour)
		if err := eventRepo.UpdateDate(context.Background(), event, event.DateVersion); !errors.Is(err, ErrActiveRefundTimePermit) {
			t.Fatalf("date change with qualified permit error=%v, want ErrActiveRefundTimePermit", err)
		}
		if err := eventRepo.Delete(context.Background(), eventID); !errors.Is(err, ErrActiveRefundTimePermit) {
			t.Fatalf("delete with qualified permit error=%v, want ErrActiveRefundTimePermit", err)
		}
	})
}

func TestRefundTimeMetadataUpdateCannotWriteStaleDate(t *testing.T) {
	p, eventRepo, _ := refundTimeIntegrationFixture(t)
	eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
	stale, err := eventRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	newDate := databaseTime(t, p).Add(2 * time.Hour)
	current, err := eventRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	current.Date = newDate
	if err := eventRepo.UpdateDate(context.Background(), current, current.DateVersion); err != nil {
		t.Fatal(err)
	}
	stale.Title = "metadata edit with stale date version"
	if err := eventRepo.Update(context.Background(), stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale metadata update error=%v, want ErrConflict", err)
	}
	got, err := eventRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Date.Equal(newDate) || got.DateVersion != 2 {
		t.Fatalf("stale metadata update overwrote date: got=%s version=%d", got.Date, got.DateVersion)
	}
}

func TestRefundTimeDatabaseGuardBlocksLegacyDateMutationAndDelete(t *testing.T) {
	p, _, authority := refundTimeIntegrationFixture(t)
	startedAt := databaseTime(t, p).Add(time.Hour)
	eventID := seedRefundTimeEvent(t, p, startedAt)
	if _, err := authority.Acquire(context.Background(), newRefundTimeTestBinding(eventID)); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Exec(context.Background(), `UPDATE events SET title='legacy metadata', date=$2 WHERE id=$1`, eventID, startedAt.Add(24*time.Hour)); err == nil {
		t.Fatal("legacy direct date write bypassed qualified permit guard")
	}
	if _, err := p.Exec(context.Background(), `DELETE FROM events WHERE id=$1`, eventID); err == nil {
		t.Fatal("direct event delete bypassed qualified permit guard")
	}
	var date time.Time
	if err := p.QueryRow(context.Background(), `SELECT date FROM events WHERE id=$1`, eventID).Scan(&date); err != nil {
		t.Fatal(err)
	}
	if !date.Equal(startedAt) {
		t.Fatalf("blocked legacy writes changed date to %s, want %s", date, startedAt)
	}
}

func TestRefundTimeReleaseTerminalOutcomesAreIdempotent(t *testing.T) {
	p, _, authority := refundTimeIntegrationFixture(t)
	eventID := seedRefundTimeEvent(t, p, databaseTime(t, p).Add(time.Hour))
	for _, outcome := range []string{"APPROVED", "REJECTED", "ABORTED"} {
		t.Run(outcome, func(t *testing.T) {
			binding := newRefundTimeTestBinding(eventID)
			qualified, err := authority.Acquire(context.Background(), binding)
			if err != nil || qualified.State != "QUALIFIED" {
				t.Fatalf("acquire: permit=%+v err=%v", qualified, err)
			}
			released, err := authority.Release(context.Background(), binding, outcome)
			if err != nil {
				t.Fatalf("release %s: %v", outcome, err)
			}
			replayed, err := authority.Release(context.Background(), binding, outcome)
			if err != nil || replayed != released {
				t.Fatalf("same-outcome release replay: permit=%+v err=%v", replayed, err)
			}
			if released.State != "RELEASED" || released.Version != qualified.Version {
				t.Fatalf("release permit=%+v, want RELEASED preserving date version %d", released, qualified.Version)
			}
			for _, conflictingOutcome := range []string{"APPROVED", "REJECTED", "ABORTED"} {
				if conflictingOutcome == outcome {
					continue // The identical outcome was verified as an idempotent replay above.
				}
				if _, err := authority.Release(context.Background(), binding, conflictingOutcome); !errors.Is(err, ErrConflict) {
					t.Fatalf("terminal replay %s -> %s error=%v, want ErrConflict", outcome, conflictingOutcome, err)
				}
			}
			preserved, err := authority.Release(context.Background(), binding, outcome)
			if err != nil || preserved != released {
				t.Fatalf("conflicting requests changed terminal evidence: permit=%+v err=%v", preserved, err)
			}
		})
	}
}

func TestRefundTimeMigrationDownUpAllowsEmptyEvidence(t *testing.T) {
	p, _, _ := refundTimeIntegrationFixture(t)
	if err := runRefundTimeMigration(t, p, "down", "1"); err != nil {
		t.Fatalf("roll back empty Event refund time migration: %v", err)
	}
	var permitTableExists, dateVersionExists bool
	if err := p.QueryRow(context.Background(), `SELECT to_regclass('event_refund_time_permits') IS NOT NULL`).Scan(&permitTableExists); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='events' AND column_name='date_version')`).Scan(&dateVersionExists); err != nil {
		t.Fatal(err)
	}
	if permitTableExists || dateVersionExists {
		t.Fatalf("empty rollback retained migration objects: permit table=%v date_version=%v", permitTableExists, dateVersionExists)
	}

	if err := runRefundTimeMigration(t, p, "up", "1"); err != nil {
		t.Fatalf("reapply Event refund time migration: %v", err)
	}
	if err := p.QueryRow(context.Background(), `SELECT to_regclass('event_refund_time_permits') IS NOT NULL`).Scan(&permitTableExists); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='events' AND column_name='date_version')`).Scan(&dateVersionExists); err != nil {
		t.Fatal(err)
	}
	if !permitTableExists || !dateVersionExists {
		t.Fatalf("reapplied migration missing objects: permit table=%v date_version=%v", permitTableExists, dateVersionExists)
	}
}

func TestRefundTimeMigrationDownRefusesAnyPermitEvidence(t *testing.T) {
	for _, initialState := range []string{"QUALIFIED", "REJECTED", "RELEASED_TOMBSTONE"} {
		t.Run(initialState, func(t *testing.T) {
			p, _, authority := refundTimeIntegrationFixture(t)
			startsAt := databaseTime(t, p).Add(time.Hour)
			if initialState == "REJECTED" {
				startsAt = databaseTime(t, p).Add(-time.Minute)
			}
			eventID := seedRefundTimeEvent(t, p, startsAt)
			binding := newRefundTimeTestBinding(eventID)
			wantState := initialState
			switch initialState {
			case "RELEASED_TOMBSTONE":
				if _, err := authority.Release(context.Background(), binding, "ABORTED"); err != nil {
					t.Fatalf("persist release tombstone: %v", err)
				}
				wantState = "RELEASED"
			default:
				permit, err := authority.Acquire(context.Background(), binding)
				if err != nil {
					t.Fatalf("persist %s permit: %v", initialState, err)
				}
				if permit.State != initialState {
					t.Fatalf("permit state=%s, want %s", permit.State, initialState)
				}
			}

			err := runRefundTimeMigration(t, p, "down", "1")
			if err == nil || !strings.Contains(err.Error(), "permit evidence exists") {
				t.Fatalf("rollback error=%v, want durable-evidence refusal", err)
			}
			var gotState string
			if err := p.QueryRow(context.Background(), `SELECT state FROM event_refund_time_permits WHERE attempt_id=$1`, binding.AttemptID).Scan(&gotState); err != nil {
				t.Fatalf("permit evidence was removed by failed rollback: %v", err)
			}
			if gotState != wantState {
				t.Fatalf("permit state after refused rollback=%s, want %s", gotState, wantState)
			}
			var dateVersionExists bool
			if err := p.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='events' AND column_name='date_version')`).Scan(&dateVersionExists); err != nil {
				t.Fatal(err)
			}
			if !dateVersionExists {
				t.Fatal("refused rollback dropped the Event date version column")
			}
		})
	}
}

func refundTimeIntegrationFixture(t *testing.T) (*pgxpool.Pool, *PostgresEventRepository, *PostgresRefundTimeRepository) {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("EVENT_REFUND_TEST_DATABASE_URL"))
	if rawURL == "" {
		t.Skip("EVENT_REFUND_TEST_DATABASE_URL is not set; isolated PostgreSQL test skipped")
	}
	databaseURL, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal("invalid isolated Event refund database URL")
	}
	address := net.ParseIP(databaseURL.Hostname())
	if address == nil || !address.IsPrivate() || databaseURL.Port() != "25443" || databaseURL.Path != "/ticketbox_event_refund_test" {
		t.Fatal("BLOCKED: Event refund test requires its private-IP :25443 /ticketbox_event_refund_test database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, rawURL)
	if err != nil {
		t.Fatal("connect to isolated Event refund test database")
	}
	var databaseName string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil || databaseName != "ticketbox_event_refund_test" {
		admin.Close()
		t.Fatal("connected database identity does not match isolated Event refund test database")
	}
	var uuidOSSPresent bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='uuid-ossp')`).Scan(&uuidOSSPresent); err != nil || !uuidOSSPresent {
		admin.Close()
		t.Fatal("isolated Event refund test database must already have uuid-ossp installed")
	}

	schema := "event_refund_time_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal("create isolated Event refund test schema")
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Errorf("drop isolated Event refund test schema: %v", err)
		}
	})

	query := databaseURL.Query()
	query.Set("search_path", schema+",public")
	databaseURL.RawQuery = query.Encode()
	migrationCtx, migrationCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer migrationCancel()
	migration := exec.CommandContext(migrationCtx, "migrate", "-path", "../../migrations", "-database", databaseURL.String(), "up")
	output, err := migration.CombinedOutput()
	if err != nil {
		safeOutput := strings.ReplaceAll(string(output), rawURL, "[isolated DSN]")
		safeOutput = strings.ReplaceAll(safeOutput, databaseURL.String(), "[isolated DSN]")
		t.Fatalf("apply actual Event migrations: %s", safeOutput)
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL.String())
	if err != nil {
		t.Fatal("parse isolated Event refund test database URL")
	}
	poolConfig.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("connect to isolated Event refund test schema")
	}
	t.Cleanup(pool.Close)
	return pool, NewPostgresEventRepository(pool), NewPostgresRefundTimeRepository(pool)
}

func runRefundTimeMigration(t *testing.T, pool *pgxpool.Pool, args ...string) error {
	t.Helper()
	var schema string
	if err := pool.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		return fmt.Errorf("read disposable Event schema: %w", err)
	}
	databaseURL, err := url.Parse(strings.TrimSpace(os.Getenv("EVENT_REFUND_TEST_DATABASE_URL")))
	if err != nil {
		return errors.New("invalid isolated Event refund database URL")
	}
	query := databaseURL.Query()
	query.Set("search_path", schema+",public")
	databaseURL.RawQuery = query.Encode()
	commandArgs := append([]string{"-path", "../../migrations", "-database", databaseURL.String()}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "migrate", commandArgs...)
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	safeOutput := strings.ReplaceAll(string(output), strings.TrimSpace(os.Getenv("EVENT_REFUND_TEST_DATABASE_URL")), "[isolated DSN]")
	safeOutput = strings.ReplaceAll(safeOutput, databaseURL.String(), "[isolated DSN]")
	return fmt.Errorf("migrate %s: %w: %s", strings.Join(args, " "), err, safeOutput)
}

func seedRefundTimeEvent(t *testing.T, pool *pgxpool.Pool, startsAt time.Time) uuid.UUID {
	t.Helper()
	eventID := uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO events(id,title,category,venue,location,date) VALUES($1,'refund fixture','concert','venue','location',$2)`, eventID, startsAt); err != nil {
		t.Fatalf("seed Event refund fixture: %v", err)
	}
	return eventID
}

func databaseTime(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func newRefundTimeTestBinding(eventID uuid.UUID) refundtime.Binding {
	return refundtime.Binding{
		AttemptID: uuid.New(), RequestID: uuid.New(), BookingID: uuid.New(), PaymentID: uuid.New(),
		EventID: eventID, ApproverID: uuid.New(), Generation: 1,
		Fingerprint: fmt.Sprintf("%064x", 17),
	}
}
