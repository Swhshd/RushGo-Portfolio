package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

func dispatchCommand() domain.CreateBookingCommand {
	b := &domain.Booking{ID: uuid.New(), UserID: uuid.New(), EventID: uuid.New(), Version: 1, Status: domain.StatusPending, CreatedAt: time.Now(), TotalAmountCents: 725, Items: []domain.BookingItem{{ID: uuid.New(), TicketTierID: uuid.New(), Quantity: 1, UnitPriceCents: 725, SeatIDs: []uuid.UUID{uuid.New()}}}}
	hash, _, _ := domain.CanonicalBookingRequest(b.EventID, b.Items, "pessimistic")
	return domain.CreateBookingCommand{Booking: b, Identity: domain.BookingRequestIdentity{UserID: b.UserID, RequestID: "original-key", Hash: hash}, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().Add(15 * time.Minute)}
}
func TestBookingDispatchClaimsRejectStaleAndExpiredResultsIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	r := NewPostgresBookingRepository(p)
	ctx := context.Background()
	command := dispatchCommand()
	v, _, err := r.CreateOrGetRequest(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.ClaimBookingDispatch(ctx, v.Booking.ID, time.Second)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if c.Snapshot.Booking == nil || c.Snapshot.Booking.RequestID != command.Identity.RequestID {
		t.Fatal("dispatch claim lost the persisted request identity required by its reliability boundary")
	}
	if _, err := p.Exec(ctx, `UPDATE bookings SET dispatch_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, v.Booking.ID); err != nil {
		t.Fatal(err)
	}
	fresh, err := r.ClaimBookingDispatch(ctx, v.Booking.ID, 30*time.Second)
	if err != nil || fresh == nil || fresh.Token == c.Token || fresh.Fence <= c.Fence {
		t.Fatal("expired owner not fenced", err)
	}
	if ok, err := r.FinishDispatch(ctx, *c, uuid.New(), true, "", time.Time{}); err != nil || ok {
		t.Fatal("stale result accepted", err)
	}
	if ok, err := r.FinishDispatch(ctx, *fresh, uuid.New(), true, "", time.Time{}); err != nil || !ok {
		t.Fatal("fresh result rejected", err)
	}
	got, err := r.GetByRequest(ctx, command.Identity)
	if err != nil || got.Booking.ID != v.Booking.ID || got.DispatchState != "accepted" {
		t.Fatal("original identity lost", err)
	}
	// Finish must evaluate lease time after lock acquisition, not transaction NOW.
	command = dispatchCommand()
	command.Identity.RequestID = "lock-wait"
	v, _, err = r.CreateOrGetRequest(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	c, err = r.ClaimBookingDispatch(ctx, v.Booking.ID, time.Second)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	lock, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, v.Booking.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	fail := make(chan error, 1)
	go func() {
		ok, err := r.FinishDispatch(ctx, *c, uuid.New(), true, "", time.Time{})
		done <- ok
		fail <- err
	}()
	barrier, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := p.QueryRow(barrier, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%SELECT id FROM bookings WHERE id=$1 FOR UPDATE%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if barrier.Err() != nil {
			t.Fatal("completion did not wait at row-lock barrier")
		}
	}
	if _, err := lock.Exec(ctx, `UPDATE bookings SET dispatch_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, v.Booking.ID); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, err := <-done, <-fail; ok || err != nil {
		t.Fatal("expired lock-wait completion accepted", err)
	}
}

func TestBookingDispatchMigrationRetainsResponsibilitiesIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	ctx := context.Background()
	down, err := os.ReadFile("../../migrations/000004_booking_request_dispatch.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, err := os.ReadFile("../../migrations/000004_booking_request_dispatch.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, string(down)); err != nil {
		t.Fatal("empty dispatch down", err)
	}
	if _, err := p.Exec(ctx, string(up)); err != nil {
		t.Fatal("dispatch up", err)
	}
	r := NewPostgresBookingRepository(p)
	command := dispatchCommand()
	if _, _, err := r.CreateOrGetRequest(ctx, command); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, string(down)); err == nil {
		t.Fatal("down deleted retained request responsibility")
	}
	if got, err := r.GetByRequest(ctx, command.Identity); err != nil || got.Booking.ID != command.Booking.ID {
		t.Fatal("guard damaged retained request", err)
	}
}

func TestBookingLifecycleHeldRowRespectsDatabaseBudgetIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	r := NewPostgresBookingRepository(p)
	ctx := context.Background()
	ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
	lock, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, ref.BookingID); err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	_, _, err = r.DecideLifecycle(caller, fixtureDecision(ref, "cancel", 1), lifecycleFixtureEvent(ref, "cancel"))
	if err == nil || time.Since(started) > 4*time.Second {
		t.Fatal("lifecycle row lock exceeded database budget", err, time.Since(started))
	}
}

func TestBookingRequestAndItemsCommitAtomicallyIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	r := NewPostgresBookingRepository(p)
	ctx := context.Background()
	command := dispatchCommand()
	if _, err := p.Exec(ctx, `ALTER TABLE booking_items ADD CONSTRAINT fixture_reject_item CHECK(quantity<>1)`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.CreateOrGetRequest(ctx, command); err == nil {
		t.Fatal("item failure hidden")
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM bookings WHERE user_id=$1 AND request_id=$2`, command.Identity.UserID, command.Identity.RequestID).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial keyed Booking committed", count, err)
	}
}
