package repository

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestReadPaidRecoveryTicketSnapshotCountsAllBookingLinkedRowsIntegration(t *testing.T) {
	requireAdminAfterSalesPostgresOrSkip(t)
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	booking := seedAdminAfterSalesBooking(t, p, uuid.New(), 1)
	if err := issueAdminAfterSalesTicket(t, p, booking); err != nil {
		t.Fatal(err)
	}

	// Deliberately seed rows whose Saga binding conflicts with the Booking.
	// Recovery must see every row for the Booking instead of hiding such facts
	// behind a current Saga or lifecycle-generation filter.
	otherSaga, otherPayment := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,ticket_count)
		VALUES($1,$2,$3,$4,$5,1,'denied','payment_evidence_unknown','unknown','unknown',0)`, uuid.New(), otherSaga, booking.bookingID, otherPayment, booking.eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'REVOKED')`, uuid.New(), booking.bookingID, booking.itemID, otherSaga, otherPayment, booking.eventID, booking.tierID, uuid.New()); err != nil {
		t.Fatal(err)
	}

	snapshot, err := repo.ReadPaidRecoveryTicketSnapshot(ctx, booking.bookingID, 1)
	if err != nil {
		t.Fatalf("ReadPaidRecoveryTicketSnapshot() error = %v", err)
	}
	if snapshot.BookingID != booking.bookingID || snapshot.SagaID != booking.sagaID || snapshot.Generation != 1 {
		t.Fatalf("snapshot identity = %+v", snapshot)
	}
	if snapshot.TicketCount != 2 || snapshot.IssuanceReceiptCount != 2 || snapshot.ObservedAt.IsZero() {
		t.Fatalf("snapshot omitted conflicting linked facts or observation time: %+v", snapshot)
	}
}
