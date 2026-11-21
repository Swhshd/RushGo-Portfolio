package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func devBaselineSQL(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("../../../../scripts/dev_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func runDevBaseline(t *testing.T, p *pgxpool.Pool, eventIDs []uuid.UUID, setFlags bool) error {
	t.Helper()
	ctx := context.Background()
	conn, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx,
		"SELECT set_config('ticketbox.dev_baseline','',false), set_config('ticketbox.event_ids','',false)"); err != nil {
		t.Fatal(err)
	}
	if setFlags {
		ids := make([]string, len(eventIDs))
		for i, id := range eventIDs {
			ids[i] = id.String()
		}
		if _, err := conn.Exec(ctx,
			"SELECT set_config('ticketbox.dev_baseline','on',false), set_config('ticketbox.event_ids',$1,false)",
			strings.Join(ids, ",")); err != nil {
			t.Fatal(err)
		}
	}

	_, err = conn.Conn().PgConn().Exec(ctx, devBaselineSQL(t)).ReadAll()
	if err != nil {
		_, _ = conn.Conn().PgConn().Exec(ctx, "ROLLBACK").ReadAll()
	}
	return err
}

func baselineOf(t *testing.T, p *pgxpool.Pool, tier uuid.UUID) *string {
	t.Helper()
	var baseline *string
	if err := p.QueryRow(context.Background(), `SELECT lifecycle_accounting_baseline FROM ticket_tiers WHERE id=$1`, tier).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	return baseline
}

func TestDevBaselineIsEventScopedAndIdempotentIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	cleanEvent, cleanTier, _ := seedUnverifiedLifecycleSeats(t, p, 2)
	verifiedEvent, verifiedTier, _ := seedUnverifiedLifecycleSeats(t, p, 2)
	bookedEvent, bookedTier, bookedSeats := seedUnverifiedLifecycleSeats(t, p, 2)
	softEvent, softTier, softSeats := seedUnverifiedLifecycleSeats(t, p, 2)
	shortEvent, shortTier, _ := seedUnverifiedLifecycleSeats(t, p, 2)
	_, otherCleanTier, _ := seedUnverifiedLifecycleSeats(t, p, 2)

	ctx := context.Background()
	if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, verifiedTier); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE seats SET status='booked',booking_id=$2,lifecycle_generation=1 WHERE id=$1`, bookedSeats[0], uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE seats SET deleted_at=clock_timestamp() WHERE id=$1`, softSeats[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET total_quantity=3,available_quantity=3 WHERE id=$1`, shortTier); err != nil {
		t.Fatal(err)
	}

	var beforeClean, beforeBooked, beforeSoft, beforeShort int32
	for tier, dst := range map[uuid.UUID]*int32{cleanTier: &beforeClean, bookedTier: &beforeBooked, softTier: &beforeSoft, shortTier: &beforeShort} {
		if err := p.QueryRow(ctx, `SELECT available_quantity FROM ticket_tiers WHERE id=$1`, tier).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}

	targets := []uuid.UUID{cleanEvent, verifiedEvent}
	if err := runDevBaseline(t, p, targets, true); err != nil {
		t.Fatalf("first baseline run: %v", err)
	}
	if err := runDevBaseline(t, p, targets, true); err != nil {
		t.Fatalf("idempotent baseline run: %v", err)
	}

	if got := baselineOf(t, p, cleanTier); got == nil || *got != "verified" {
		t.Fatalf("clean tier not verified: %v", got)
	}
	if got := baselineOf(t, p, verifiedTier); got == nil || *got != "verified" {
		t.Fatalf("existing verified tier changed: %v", got)
	}
	for _, tier := range []uuid.UUID{bookedTier, softTier, shortTier, otherCleanTier} {
		if got := baselineOf(t, p, tier); got != nil {
			t.Fatalf("non-targeted/non-qualifying tier %s was marked verified: %v", tier, *got)
		}
	}

	var afterClean, afterBooked, afterSoft, afterShort int32
	for tier, dst := range map[uuid.UUID]*int32{cleanTier: &afterClean, bookedTier: &afterBooked, softTier: &afterSoft, shortTier: &afterShort} {
		if err := p.QueryRow(ctx, `SELECT available_quantity FROM ticket_tiers WHERE id=$1`, tier).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	if beforeClean != afterClean || beforeBooked != afterBooked || beforeSoft != afterSoft || beforeShort != afterShort {
		t.Fatal("baseline helper changed availability")
	}

	_ = bookedEvent
	_ = softEvent
	_ = shortEvent
}

func TestDevBaselineRequiresEventScopeIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	conn, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx,
		"SELECT set_config('ticketbox.dev_baseline','on',false), set_config('ticketbox.event_ids','',false)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Conn().PgConn().Exec(ctx, devBaselineSQL(t)).ReadAll(); err == nil {
		t.Fatal("missing event scope did not fail closed")
	}
	_, _ = conn.Conn().PgConn().Exec(ctx, "ROLLBACK").ReadAll()
}

func TestDevBaselineFailsClosedForBrokenTargetsIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	bookedEvent, bookedTier, bookedSeats := seedUnverifiedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(context.Background(), `UPDATE seats SET status='booked',booking_id=$2,lifecycle_generation=1 WHERE id=$1`, bookedSeats[0], uuid.New()); err != nil {
		t.Fatal(err)
	}
	if err := runDevBaseline(t, p, []uuid.UUID{bookedEvent}, true); err == nil {
		t.Fatal("broken target did not fail closed")
	}
	if got := baselineOf(t, p, bookedTier); got != nil {
		t.Fatalf("failed run left baseline: %v", *got)
	}

	softEvent, softTier, softSeats := seedUnverifiedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(context.Background(), `UPDATE seats SET deleted_at=clock_timestamp() WHERE id=$1`, softSeats[0]); err != nil {
		t.Fatal(err)
	}
	if err := runDevBaseline(t, p, []uuid.UUID{softEvent}, true); err == nil {
		t.Fatal("soft-deleted target did not fail closed")
	}
	if got := baselineOf(t, p, softTier); got != nil {
		t.Fatalf("failed run left soft-target baseline: %v", *got)
	}

	debitEvent, debitTier, debitSeats := seedUnverifiedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(context.Background(), `INSERT INTO booking_seat_lifecycles(
		booking_id,generation,event_id,seat_ids,seat_set_hash,reservation_deadline,state,
		accounting_state,last_operation_id,debits,accounted_at)
		VALUES($1,1,$2,$3,decode(repeat('00',32),'hex'),clock_timestamp()+interval '1 hour','booked',
		'applied',$4,$5::jsonb,clock_timestamp())`,
		uuid.New(), debitEvent, debitSeats, uuid.New(), fmt.Sprintf(`{"%s":2}`, debitTier)); err != nil {
		t.Fatal(err)
	}
	if err := runDevBaseline(t, p, []uuid.UUID{debitEvent}, true); err == nil {
		t.Fatal("applied-debit target did not fail closed")
	}
	if got := baselineOf(t, p, debitTier); got != nil {
		t.Fatalf("failed run left applied-debit baseline: %v", *got)
	}

	if err := runDevBaseline(t, p, []uuid.UUID{bookedEvent}, false); err == nil {
		t.Fatal("missing opt-in flag did not fail closed")
	}
	if err := runDevBaseline(t, p, []uuid.UUID{uuid.New()}, true); err == nil {
		t.Fatal("unknown event did not fail closed")
	}
}
