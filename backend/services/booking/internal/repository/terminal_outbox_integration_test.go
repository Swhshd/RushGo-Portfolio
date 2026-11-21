package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTerminalDecisionAndOutboxAreAtomicIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	r := NewPostgresBookingRepository(p)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, kind := range []string{"cancel", "fail", "expire", "abort_confirmation"} {
		t.Run(kind, func(t *testing.T) {
			ref := seedLifecycleBooking(t, p, time.Now().Add(-time.Hour))
			d := fixtureDecision(ref, kind, 1)
			if kind == "abort_confirmation" {
				if _, err := p.Exec(ctx, `UPDATE bookings SET reservation_deadline=$2 WHERE id=$1`, ref.BookingID, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				claim := fixtureDecision(ref, "claim_confirmation", 1)
				v, changed, err := r.DecideLifecycle(ctx, claim, nil)
				if err != nil || !changed {
					t.Fatal("confirmation claim", err)
				}
				d.ExpectedVersion = v.Booking.Version
				d.ConfirmationDecisionID = &claim.DecisionID
			}
			event := lifecycleFixtureEvent(ref, kind)
			stable := event.ID
			event.ID = uuid.New()
			if _, changed, err := r.DecideLifecycle(ctx, d, event); err == nil || changed {
				t.Fatal("random terminal event identity accepted")
			}
			event.ID = stable
			if _, err := p.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload) VALUES($1,$2,$3,'{}')`, stable, event.EventType, event.EventKey); err != nil {
				t.Fatal(err)
			}
			if _, changed, err := r.DecideLifecycle(ctx, d, event); err == nil || changed {
				t.Fatal("outbox insert failure committed decision")
			}
			v, err := r.GetLifecycle(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if v.Booking.Version != d.ExpectedVersion || v.DecisionID != nil && *v.DecisionID == d.DecisionID {
				t.Fatal("decision persisted without its required intent")
			}
			if _, err := p.Exec(ctx, `DELETE FROM outbox WHERE id=$1`, stable); err != nil {
				t.Fatal(err)
			}
			for replay := 0; replay < 5; replay++ {
				v, changed, err := r.DecideLifecycle(ctx, d, event)
				if err != nil {
					t.Fatal(err)
				}
				if changed != (replay == 0) || v.Booking.Version != d.ExpectedVersion+1 || v.Phase != "TERMINAL" {
					t.Fatal("repeat decision lost identity", replay, changed, v)
				}
			}
			var count int
			var actual uuid.UUID
			if err := p.QueryRow(ctx, `SELECT count(*),min(id::text)::uuid FROM outbox WHERE event_key=$1`, ref.BookingID.String()).Scan(&count, &actual); err != nil {
				t.Fatal(err)
			}
			if count != 1 || actual != stable {
				t.Fatal("semantic intent changed", count, actual, stable)
			}
			published := 0
			if n, err := r.ProcessPendingOutbox(ctx, 1, func(context.Context, OutboxEvent) error { published++; return context.DeadlineExceeded }); n != 0 || err == nil {
				t.Fatal("publish ack loss not retained")
			}
			if n, err := r.ProcessPendingOutbox(ctx, 1, func(_ context.Context, e OutboxEvent) error {
				published++
				if e.ID != stable {
					t.Fatal("relay changed envelope identity")
				}
				return nil
			}); n != 1 || err != nil {
				t.Fatal("republish", n, err)
			}
			if published != 2 {
				t.Fatal("expected technical replay", published)
			}
		})
	}
}
