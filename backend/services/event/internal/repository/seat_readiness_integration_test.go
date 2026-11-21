package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAccountingReadinessPredicateIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	tiers := NewPostgresTicketTierRepository(p)

	t.Run("verified_clean", func(t *testing.T) {
		_, tier, _ := seedLifecycleSeats(t, p, 2)
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, tier); err != nil {
			t.Fatal(err)
		}
		ready, err := tiers.AccountingReadiness(ctx, tier)
		if err != nil || !ready {
			t.Fatalf("verified clean tier ready=%v err=%v", ready, err)
		}
	})
	t.Run("baseline_null", func(t *testing.T) {
		_, tier, _ := seedLifecycleSeats(t, p, 2)
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline=NULL WHERE id=$1`, tier); err != nil {
			t.Fatal(err)
		}
		ready, err := tiers.AccountingReadiness(ctx, tier)
		if err != nil || ready {
			t.Fatalf("NULL baseline ready=%v err=%v", ready, err)
		}
	})
	t.Run("coverage_drift", func(t *testing.T) {
		_, tier, ids := seedLifecycleSeats(t, p, 2)
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, tier); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE seats SET deleted_at=NOW() WHERE id=$1`, ids[0]); err != nil {
			t.Fatal(err)
		}
		ready, err := tiers.AccountingReadiness(ctx, tier)
		if err != nil || ready {
			t.Fatalf("coverage drift ready=%v err=%v", ready, err)
		}
	})
	t.Run("unknown_booked", func(t *testing.T) {
		_, tier, ids := seedLifecycleSeats(t, p, 2)
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, tier); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE seats SET status='booked',booking_id=$2 WHERE id=$1`, ids[0], uuid.New()); err != nil {
			t.Fatal(err)
		}
		ready, err := tiers.AccountingReadiness(ctx, tier)
		if err != nil || ready {
			t.Fatalf("unknown booked ready=%v err=%v", ready, err)
		}
	})
	t.Run("projection_drift", func(t *testing.T) {
		_, tier, _ := seedLifecycleSeats(t, p, 2)
		if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified',available_quantity=1 WHERE id=$1`, tier); err != nil {
			t.Fatal(err)
		}
		ready, err := tiers.AccountingReadiness(ctx, tier)
		if err != nil || ready {
			t.Fatalf("projection drift ready=%v err=%v", ready, err)
		}
	})
	t.Run("missing_tier", func(t *testing.T) {
		ready, err := tiers.AccountingReadiness(ctx, uuid.New())
		if !errors.Is(err, ErrNotFound) || ready {
			t.Fatalf("missing tier ready=%v err=%v", ready, err)
		}
	})
	t.Run("verified_sale_remains_ready", func(t *testing.T) {
		q, first, _ := seedAccountingBooking(t, p)
		ready, err := tiers.AccountingReadiness(ctx, first)
		if err != nil || !ready {
			t.Fatalf("verified held/booked tier ready=%v err=%v", ready, err)
		}
		r := NewPostgresSeatRepository(p)
		if _, err := r.ApplySaleAccounting(ctx, q); err != nil {
			t.Fatal(err)
		}
		ready, err = tiers.AccountingReadiness(ctx, first)
		if err != nil || !ready {
			t.Fatalf("verified accounted tier ready=%v err=%v", ready, err)
		}
	})
}

func TestAccountingReadinessForTiersBatchIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	tiers := NewPostgresTicketTierRepository(p)

	_, readyTier, _ := seedLifecycleSeats(t, p, 2)
	_, unreadyTier, _ := seedUnverifiedLifecycleSeats(t, p, 2)
	_, coverageDriftTier, coverageSeats := seedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(ctx, `UPDATE seats SET deleted_at=NOW() WHERE id=$1`, coverageSeats[0]); err != nil {
		t.Fatal(err)
	}
	_, unknownBookedTier, bookedSeats := seedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(ctx, `UPDATE seats SET status='booked',booking_id=$2 WHERE id=$1`, bookedSeats[0], uuid.New()); err != nil {
		t.Fatal(err)
	}
	missing := uuid.New()

	empty, err := tiers.AccountingReadinessForTiers(ctx, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty tier list projection = %v, err=%v", empty, err)
	}

	got, err := tiers.AccountingReadinessForTiers(ctx, []uuid.UUID{readyTier, unreadyTier, coverageDriftTier, unknownBookedTier, missing})
	if err != nil {
		t.Fatal(err)
	}
	if !got[readyTier] {
		t.Fatal("verified clean tier reported not ready")
	}
	if got[unreadyTier] {
		t.Fatal("NULL baseline tier reported ready")
	}
	if got[coverageDriftTier] {
		t.Fatal("incomplete seat coverage reported ready")
	}
	if got[unknownBookedTier] {
		t.Fatal("booked seat without lifecycle authority reported ready")
	}
	if _, present := got[missing]; present {
		t.Fatal("missing tier must be absent from the projection, not ready")
	}
}

func TestBatchReadinessMatchesScalarAcrossAppliedAndReversedDebitsIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	tiers := NewPostgresTicketTierRepository(p)
	q, first, second := seedAccountingBooking(t, p)
	missing := uuid.New()
	ids := []uuid.UUID{first, second, missing}
	seatRepo := NewPostgresSeatRepository(p)

	assertMatchesScalar := func(stage string) {
		t.Helper()
		batch, err := tiers.AccountingReadinessForTiers(ctx, ids)
		if err != nil {
			t.Fatalf("%s batch readiness: %v", stage, err)
		}
		for _, id := range ids[:2] {
			want, err := tiers.AccountingReadiness(ctx, id)
			if err != nil {
				t.Fatalf("%s scalar readiness for %s: %v", stage, id, err)
			}
			if got, present := batch[id]; !present || got != want {
				t.Errorf("%s batch readiness for %s = %v (present=%v), scalar = %v", stage, id, got, present, want)
			}
		}
		if _, present := batch[missing]; present {
			t.Errorf("%s missing tier must be absent from batch projection", stage)
		}
	}

	assertMatchesScalar("booked without applied debit")
	if _, err := seatRepo.ApplySaleAccounting(ctx, q); err != nil {
		t.Fatal(err)
	}
	assertMatchesScalar("applied debit")

	q.Action = "close"
	if _, err := seatRepo.ApplyLifecycle(ctx, q); err != nil {
		t.Fatal(err)
	}
	assertMatchesScalar("reversed debit after close")
}

func TestAppliedDebitReadinessIndexIsPartialJSONBGINIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	var method, operatorClass, predicate string
	var ready, valid bool
	err := p.QueryRow(context.Background(), `SELECT am.amname, opc.opcname, pg_get_expr(i.indpred, i.indrelid), i.indisready, i.indisvalid
		FROM pg_index i
		JOIN pg_class idx ON idx.oid=i.indexrelid
		JOIN pg_am am ON am.oid=idx.relam
		JOIN pg_opclass opc ON opc.oid=i.indclass[0]
		WHERE idx.relname='booking_seat_lifecycles_applied_debits_gin'`).
		Scan(&method, &operatorClass, &predicate, &ready, &valid)
	if err != nil {
		t.Fatalf("find applied-debit index installed by Event migrations: %v", err)
	}
	if method != "gin" || operatorClass != "jsonb_ops" || !ready || !valid || !strings.Contains(predicate, "accounting_state = 'applied'") {
		t.Fatalf("unexpected applied-debit index: method=%q opclass=%q predicate=%q ready=%v valid=%v", method, operatorClass, predicate, ready, valid)
	}
}

// A tier whose accounting baseline has not been verified must not be allowed
// to reserve seats. This is the pre-payment boundary; the final sale
// accounting check in ApplySaleAccounting remains authoritative.
func TestReserveRejectsUnreadyTierBeforeMutationIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	event, tier, ids := seedLifecycleSeats(t, p, 2)
	if _, err := p.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline=NULL,available_quantity=total_quantity WHERE id=$1`, tier); err != nil {
		t.Fatal(err)
	}

	var beforeAvailable, beforeVersion int32
	if err := p.QueryRow(ctx, `SELECT available_quantity,version FROM ticket_tiers WHERE id=$1`, tier).Scan(&beforeAvailable, &beforeVersion); err != nil {
		t.Fatal(err)
	}
	beforeRevision := seatRevision(t, p, ids[0])

	req := lifecycleRequest(event, uuid.New(), []uuid.UUID{ids[0]}, time.Now().Add(time.Hour))
	if _, err := NewPostgresSeatRepository(p).ApplyLifecycle(ctx, req); !errors.Is(err, ErrAccountingBaseline) {
		t.Fatalf("reserve on unready tier returned %v, want ErrAccountingBaseline", err)
	}

	var status string
	var revision int64
	if err := p.QueryRow(ctx, `SELECT status,seat_revision FROM seats WHERE id=$1`, ids[0]).Scan(&status, &revision); err != nil {
		t.Fatal(err)
	}
	if status != "available" || revision != beforeRevision {
		t.Fatalf("reserve mutated seat: status=%s revision=%d want available/%d", status, revision, beforeRevision)
	}
	var lifecycles int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM booking_seat_lifecycles WHERE booking_id=$1`, req.BookingID).Scan(&lifecycles); err != nil {
		t.Fatal(err)
	}
	if lifecycles != 0 {
		t.Fatalf("reserve created %d lifecycle rows", lifecycles)
	}
	var afterAvailable, afterVersion int32
	if err := p.QueryRow(ctx, `SELECT available_quantity,version FROM ticket_tiers WHERE id=$1`, tier).Scan(&afterAvailable, &afterVersion); err != nil {
		t.Fatal(err)
	}
	if afterAvailable != beforeAvailable || afterVersion != beforeVersion {
		t.Fatalf("reserve mutated tier: available=%d version=%d want %d/%d", afterAvailable, afterVersion, beforeAvailable, beforeVersion)
	}
}
