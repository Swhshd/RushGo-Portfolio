package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestVerifyBookingTicketEligibilityRequiresAppliedExactLifecycleIntegration(t *testing.T) {
	p := eventLifecycleFixture(t)
	repo := NewPostgresSeatRepository(p)
	ctx := context.Background()
	eventID, tierID, seatIDs := seedLifecycleSeats(t, p, 2)
	bookingID := uuid.New()
	request := lifecycleRequest(eventID, bookingID, seatIDs, time.Now().Add(time.Hour))
	request.Action = "reserve"
	if _, err := repo.ApplyLifecycle(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.Action = "book"
	if _, err := repo.ApplyLifecycle(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.Action = "account"
	request.TierQuantities = map[uuid.UUID]int32{tierID: int32(len(seatIDs))}
	if _, err := repo.ApplySaleAccounting(ctx, request); err != nil {
		t.Fatal(err)
	}
	eligible, reason, err := repo.VerifyBookingTicketEligibility(ctx, bookingID, eventID, 1, seatIDs)
	if err != nil || !eligible || reason != "event_booking_seats_accounted" {
		t.Fatalf("fully eligible Event booking rejected: eligible=%v reason=%q err=%v", eligible, reason, err)
	}
	if _, err := p.Exec(ctx, `UPDATE booking_seat_lifecycles SET accounting_state='not_applied',debits=NULL,accounted_at=NULL WHERE booking_id=$1 AND generation=1`, bookingID); err != nil {
		t.Fatal(err)
	}
	eligible, _, err = repo.VerifyBookingTicketEligibility(ctx, bookingID, eventID, 1, seatIDs)
	if err != nil || eligible {
		t.Fatalf("unaccounted lifecycle was accepted: eligible=%v err=%v", eligible, err)
	}
	debits, err := json.Marshal(request.TierQuantities)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE booking_seat_lifecycles SET accounting_state='applied',debits=$2::jsonb,accounted_at=clock_timestamp() WHERE booking_id=$1 AND generation=1`, bookingID, debits); err != nil {
		t.Fatal(err)
	}
	otherBooking := uuid.New()
	if _, err := p.Exec(ctx, `UPDATE seats SET booking_id=$2 WHERE id=$1`, seatIDs[0], otherBooking); err != nil {
		t.Fatal(err)
	}
	eligible, _, err = repo.VerifyBookingTicketEligibility(ctx, bookingID, eventID, 1, seatIDs)
	if err != nil || eligible {
		t.Fatalf("mismatched seat owner was accepted: eligible=%v err=%v", eligible, err)
	}
}
