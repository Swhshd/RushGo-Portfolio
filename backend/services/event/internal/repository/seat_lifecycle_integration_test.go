package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

type lifecyclePersistence interface {
	ApplyLifecycle(context.Context, domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error)
}

// Before the repair, exercise the real existing effects rather than treating
// an absent additive API or migration as the behavioral failure.
type legacySeatLifecycleAdapter struct{ repo *PostgresSeatRepository }

func (a legacySeatLifecycleAdapter) ApplyLifecycle(ctx context.Context, req domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	var m SeatMutationResult
	var err error
	if req.Action == "book" {
		m, err = a.repo.UpdateStatusBatch(ctx, req.SeatIDs, domain.SeatStatusBooked, &req.BookingID)
	} else {
		action := domain.RESERVED_SEAT
		if req.Action == "close" {
			action = domain.COMPENSATE_SEAT
		}
		m, err = a.repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{SeatIds: req.SeatIDs, ReservedByBookingId: req.BookingID, Action: action})
	}
	state := map[string]string{"reserve": "held", "book": "booked", "close": "closed"}[req.Action]
	return &domain.SeatLifecycleResult{State: state, AccountingState: "not_applied", ChangedSeats: m.ChangedSeats, Applied: len(m.ChangedSeats) > 0, ReservationDeadline: &req.ReservationDeadline}, err
}

func eventLifecycleCapability(repo *PostgresSeatRepository) lifecyclePersistence {
	if actual, ok := any(repo).(lifecyclePersistence); ok {
		return actual
	}
	return legacySeatLifecycleAdapter{repo}
}

func eventLifecycleFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("EVENT_TEST_DATABASE_URL"))
	if err != nil || parsed.Port() != "25439" || parsed.Path != "/ticketbox_event_test" {
		t.Fatal("BLOCKED: isolated Event PG required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal("connect isolated Event PG")
	}
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "lifecycle_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := parsed.Query()
	q.Set("search_path", schema+",public")
	parsed.RawQuery = q.Encode()
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", parsed.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual Event migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal("parse isolated Event schema")
	}
	config.MaxConns = 12
	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("connect isolated Event schema")
	}
	t.Cleanup(p.Close)
	return p
}

func seedLifecycleSeats(t *testing.T, p *pgxpool.Pool, count int) (uuid.UUID, uuid.UUID, []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	event, tier := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date,publication_status,sales_status)
		VALUES($1,'fixture','concert','fixture','fixture',NOW()+INTERVAL '1 day','published','selling')`, event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,lifecycle_accounting_baseline) VALUES($1,$2,'fixture',725,$3,$3,'verified')`, tier, event, count); err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, count)
	for i := range ids {
		ids[i] = uuid.New()
		if _, err := p.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id) VALUES($1,$2,$3)`, ids[i], event, tier); err != nil {
			t.Fatal(err)
		}
	}
	return event, tier, ids
}

// seedUnverifiedLifecycleSeats preserves the pre-baseline fixture shape for
// tests that exercise baseline repair or the legacy/unverified state.
func seedUnverifiedLifecycleSeats(t *testing.T, p *pgxpool.Pool, count int) (uuid.UUID, uuid.UUID, []uuid.UUID) {
	t.Helper()
	event, tier, ids := seedLifecycleSeats(t, p, count)
	if _, err := p.Exec(context.Background(), `UPDATE ticket_tiers SET lifecycle_accounting_baseline=NULL WHERE id=$1`, tier); err != nil {
		t.Fatal(err)
	}
	return event, tier, ids
}

func lifecycleRequest(event, booking uuid.UUID, ids []uuid.UUID, deadline time.Time) domain.SeatLifecycleRequest {
	seats := make([]string, len(ids))
	for i, id := range ids {
		seats[i] = id.String()
	}
	sort.Strings(seats)
	return domain.SeatLifecycleRequest{BookingID: booking, EventID: event, Generation: 1, SeatIDs: ids,
		Fingerprint: sha256.Sum256([]byte("v1\n" + event.String() + "\n" + strings.Join(seats, ",") + "\n")),
		OperationID: uuid.New(), Action: "reserve", ReservationDeadline: deadline.UTC().Truncate(time.Microsecond)}
}

func TestClosedGenerationCannotReopenAfterReallocationIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	repo := NewPostgresSeatRepository(p)
	life := eventLifecycleCapability(repo)
	ctx := context.Background()
	t.Run("close_before_delayed_reserve", func(t *testing.T) {
		event, _, ids := seedLifecycleSeats(t, p, 2)
		a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(time.Hour))
		a.Action = "close"
		if r, err := life.ApplyLifecycle(ctx, a); err != nil || r.State != "closed" {
			t.Fatalf("close-before-reserve: %+v %v", r, err)
		}
		for _, action := range []string{"reserve", "book"} {
			a.Action = action
			if r, err := life.ApplyLifecycle(ctx, a); err == nil {
				t.Errorf("closed generation reopened through %s: %+v", action, r)
			}
		}
		for _, id := range ids {
			if got := seatRevision(t, p, id); got != 1 {
				t.Errorf("tombstone changed seat revision to %d", got)
			}
		}
	})
	t.Run("A_close_B_acquires_A_replays", func(t *testing.T) {
		event, _, ids := seedLifecycleSeats(t, p, 2)
		a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(time.Hour))
		if _, err := life.ApplyLifecycle(ctx, a); err != nil {
			t.Fatal(err)
		}
		a.Action = "close"
		if _, err := life.ApplyLifecycle(ctx, a); err != nil {
			t.Fatal(err)
		}
		b := lifecycleRequest(event, uuid.New(), []uuid.UUID{ids[1], ids[0]}, a.ReservationDeadline)
		if _, err := life.ApplyLifecycle(ctx, b); err != nil {
			t.Fatal(err)
		}
		for _, action := range []string{"close", "reserve", "book"} {
			a.Action = action
			r, err := life.ApplyLifecycle(ctx, a)
			if action == "close" && (err != nil || len(r.ChangedSeats) != 0) {
				t.Errorf("closed replay should retain tombstone and skip B: %+v %v", r, err)
			} else if action != "close" && err == nil {
				t.Errorf("A %s reopened: %+v", action, r)
			}
		}
		for _, id := range ids {
			var owner uuid.UUID
			if err := p.QueryRow(ctx, `SELECT reserved_by_booking_id FROM seats WHERE id=$1`, id).Scan(&owner); err != nil || owner != b.BookingID || seatRevision(t, p, id) != 4 {
				t.Fatalf("B changed by A: owner=%s err=%v", owner, err)
			}
		}
	})
	t.Run("original_deadline_mixed_conflict_null_owner", func(t *testing.T) {
		event, _, ids := seedLifecycleSeats(t, p, 3)
		a := lifecycleRequest(event, uuid.New(), ids[:2], time.Now().Add(time.Hour))
		if _, err := life.ApplyLifecycle(ctx, a); err != nil {
			t.Fatal(err)
		}
		a.SeatIDs = []uuid.UUID{ids[1], ids[0]}
		if r, err := life.ApplyLifecycle(ctx, a); err != nil || len(r.ChangedSeats) != 0 || !r.ReservationDeadline.Equal(a.ReservationDeadline) {
			t.Fatalf("held replay: %+v %v", r, err)
		}
		var expiry time.Time
		if err := p.QueryRow(ctx, `SELECT reservation_expired_at FROM seats WHERE id=$1`, ids[0]).Scan(&expiry); err != nil || !expiry.Equal(a.ReservationDeadline) {
			t.Errorf("not original immutable deadline: %s %v", expiry, err)
		}
		other := lifecycleRequest(event, uuid.New(), []uuid.UUID{ids[2], ids[0]}, a.ReservationDeadline)
		if _, err := life.ApplyLifecycle(ctx, other); err == nil || seatRevision(t, p, ids[2]) != 1 {
			t.Fatalf("mixed conflict partially applied: %v", err)
		}
		if seat, err := repo.GetByID(ctx, ids[2]); err != nil || seat.BookingID != nil {
			t.Errorf("legal NULL-owner row unreadable: %+v %v", seat, err)
		}
	})
}

func TestManagedLifecycleLegacyBypassesAndImmutableIdentityIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	r := NewPostgresSeatRepository(p)
	ctx := context.Background()
	event, _, ids := seedLifecycleSeats(t, p, 2)
	a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(time.Hour))
	if _, err := r.ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	for _, status := range []domain.SeatStatus{domain.SeatStatusReserved, domain.SeatStatusBooked, domain.SeatStatusAvailable} {
		if _, err := r.UpdateStatus(ctx, ids[0], status, &a.BookingID); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("single bypass %s: %v", status, err)
		}
		if _, err := r.UpdateStatusBatch(ctx, ids, status, &a.BookingID); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("batch bypass %s: %v", status, err)
		}
	}
	for _, action := range []domain.ReserveSeatAction{domain.RESERVED_SEAT, domain.COMPENSATE_SEAT} {
		if _, err := r.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{SeatIds: ids, ReservedByBookingId: a.BookingID, Action: action}); !errors.Is(err, ErrManagedSeat) {
			t.Errorf("compensation/reserve bypass: %v", err)
		}
	}
	mutations := []func(*domain.SeatLifecycleRequest){
		func(q *domain.SeatLifecycleRequest) { q.ReservationDeadline = q.ReservationDeadline.Add(time.Minute) },
		func(q *domain.SeatLifecycleRequest) { q.Generation = 2 },
		func(q *domain.SeatLifecycleRequest) { q.EventID = uuid.New() },
		func(q *domain.SeatLifecycleRequest) {
			q.SeatIDs = []uuid.UUID{ids[0]}
			q.Fingerprint = sha256.Sum256([]byte("v1\n" + event.String() + "\n" + ids[0].String() + "\n"))
		},
	}
	for _, mutate := range mutations {
		q := a
		mutate(&q)
		if _, err := r.ApplyLifecycle(ctx, q); err == nil {
			t.Error("changed immutable identity accepted")
		}
	}
	a.Action = "book"
	if _, err := r.ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	if got, err := r.ApplyLifecycle(ctx, a); err != nil || got.Applied || len(got.ChangedSeats) != 0 {
		t.Fatalf("book replay %+v %v", got, err)
	}
	authorities, err := r.GetSeatAuthorities(ctx, ids)
	if err != nil || len(authorities) != 2 {
		t.Fatalf("authorities: %+v %v", authorities, err)
	}
	for _, x := range authorities {
		if x.Status != domain.SeatStatusBooked || x.BookingID == nil || *x.BookingID != a.BookingID || x.Generation == nil || *x.Generation != 1 || x.LifecycleState == nil || *x.LifecycleState != "booked" {
			t.Errorf("invalid booked authority: %+v", x)
		}
	}
	a.Action = "close"
	if _, err := r.ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{SeatIds: ids, ReservedByBookingId: a.BookingID, Action: domain.RESERVED_SEAT}); !errors.Is(err, ErrManagedSeat) {
		t.Fatalf("closed owner bypass: %v", err)
	}
	for _, id := range ids {
		if seatRevision(t, p, id) != 4 {
			t.Error("bypass/replay advanced revision")
		}
	}
}

func TestLifecycleSortedLocksAndCloseSerializationIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	event, _, ids := seedLifecycleSeats(t, p, 2)
	a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(time.Hour))
	r := NewPostgresSeatRepository(p)
	if _, err := r.ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	blocker, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT booking_id FROM booking_seat_lifecycles WHERE booking_id=$1 FOR UPDATE`, a.BookingID); err != nil {
		t.Fatal(err)
	}
	name := "fence_test_" + uuid.NewString()
	config := p.Config().Copy()
	config.ConnConfig.RuntimeParams["application_name"] = name
	worker, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	closeReq := a
	closeReq.Action = "close"
	closeReq.SeatIDs = []uuid.UUID{ids[1], ids[0]}
	closed := make(chan error, 1)
	go func() { _, err := NewPostgresSeatRepository(worker).ApplyLifecycle(ctx, closeReq); closed <- err }()
	// Observe the actual PostgreSQL lock wait before queueing the delayed book;
	// the first transaction owns the absent-identity fence and will close first.
	for {
		var waiting bool
		if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, name).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("close never reached PG barrier")
		case <-time.After(time.Millisecond):
		}
	}
	booked := make(chan error, 1)
	a.Action = "book"
	go func() { _, err := r.ApplyLifecycle(ctx, a); booked <- err }()
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := <-booked; !errors.Is(err, ErrLifecycleClosed) {
		t.Fatalf("delayed book beat winning close: %v", err)
	}
	// Different Booking identities and reversed input must still lock the same
	// seat batch in sorted order; only one entire reservation wins.
	requests := []domain.SeatLifecycleRequest{lifecycleRequest(event, uuid.New(), ids, a.ReservationDeadline), lifecycleRequest(event, uuid.New(), []uuid.UUID{ids[1], ids[0]}, a.ReservationDeadline)}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, q := range requests {
		wg.Add(1)
		go func(q domain.SeatLifecycleRequest) {
			defer wg.Done()
			<-start
			_, err := r.ApplyLifecycle(ctx, q)
			results <- err
		}(q)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrSeatNotAvailable) {
			t.Errorf("unexpected sorted-lock loser: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("whole-batch winners=%d", wins)
	}
}

func TestLegacyExpiryCannotReleaseManagedConfirmationWinnerIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	r := NewPostgresSeatRepository(p)
	ctx := context.Background()
	event, _, ids := seedLifecycleSeats(t, p, 1)
	a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(-time.Minute))
	a.Action = "close"
	if _, err := r.ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	// A genuine expired managed hold is seeded as a preserved database fact.
	// The legacy cleaner must leave it for Booking's authoritative decision.
	if _, err := p.Exec(ctx, `UPDATE booking_seat_lifecycles SET state='held' WHERE booking_id=$1`, a.BookingID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE seats SET status='reserved',reserved_by_booking_id=$2,reservation_expired_at=$3,lifecycle_generation=1 WHERE id=$1`, ids[0], a.BookingID, a.ReservationDeadline); err != nil {
		t.Fatal(err)
	}
	discovery, err := r.GetReservedExpiredSeats(ctx)
	if err != nil || len(discovery.BookingIdSeatIdsMap[a.BookingID]) != 1 {
		t.Fatalf("discovery: %+v %v", discovery, err)
	}
	if got, err := r.UndoReservedExpiredSeats(ctx); err != nil || len(got.ChangedSeats) != 0 {
		t.Fatalf("expiry bypass: %+v %v", got, err)
	}
	// Reproduce a stale discovery without sleeping: the fixture establishes
	// the booked winner before the previously discovered cleanup runs.
	if _, err := p.Exec(ctx, `UPDATE seats SET status='booked',booking_id=$2,reserved_by_booking_id=NULL,reservation_expired_at=NULL WHERE id=$1`, ids[0], a.BookingID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE booking_seat_lifecycles SET state='booked' WHERE booking_id=$1`, a.BookingID); err != nil {
		t.Fatal(err)
	}
	if got, err := r.UndoReservedExpiredSeats(ctx); err != nil || len(got.ChangedSeats) != 0 {
		t.Fatalf("stale cleaner released winner: %+v %v", got, err)
	}
	var status string
	if err := p.QueryRow(ctx, `SELECT status FROM seats WHERE id=$1`, ids[0]).Scan(&status); err != nil || status != "booked" {
		t.Fatalf("winner status=%s %v", status, err)
	}
}

func TestSeatLifecycleMigrationDownRetainsTombstonesIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	ctx := context.Background()
	down, err := os.ReadFile("../../migrations/000005_booking_seat_lifecycles.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	event, _, ids := seedLifecycleSeats(t, p, 1)
	a := lifecycleRequest(event, uuid.New(), ids, time.Now().Add(time.Hour))
	a.Action = "close"
	if _, err := NewPostgresSeatRepository(p).ApplyLifecycle(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, string(down)); err == nil {
		t.Fatal("down erased closed tombstone")
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM booking_seat_lifecycles WHERE booking_id=$1 AND state='closed'`, a.BookingID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("tombstone lost %d %v", count, err)
	}
}
