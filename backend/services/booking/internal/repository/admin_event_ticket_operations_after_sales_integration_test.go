package repository

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReadAdminEventTicketOperationsReturnsPersistedDeliveryExceptionsIntegration(t *testing.T) {
	requireAdminAfterSalesPostgresOrSkip(t)
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	eventID := uuid.New()

	missingReceipt := seedAdminAfterSalesBooking(t, p, eventID, 2)
	deniedReceipt := seedAdminAfterSalesBooking(t, p, eventID, 1)
	complete := seedAdminAfterSalesBooking(t, p, eventID, 1)
	otherEvent := seedAdminAfterSalesBooking(t, p, uuid.New(), 1)

	if _, err := p.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,ticket_count)
		VALUES($1,$2,$3,$4,$5,1,'denied','payment_evidence_unknown','unknown','unknown',0)`, uuid.New(), deniedReceipt.sagaID, deniedReceipt.bookingID, deniedReceipt.paymentID, eventID); err != nil {
		t.Fatal(err)
	}
	if err := issueAdminAfterSalesTicket(t, p, complete); err != nil {
		t.Fatal(err)
	}
	if err := issueAdminAfterSalesTicket(t, p, otherEvent); err != nil {
		t.Fatal(err)
	}

	snapshot, err := repo.ReadAdminEventTicketOperations(ctx, eventID)
	if err != nil {
		t.Fatalf("ReadAdminEventTicketOperations() error = %v", err)
	}
	if snapshot.EventID != eventID || len(snapshot.DeliveryExceptions) != 2 {
		t.Fatalf("event-scoped delivery exceptions = %d, want 2: %+v", len(snapshot.DeliveryExceptions), snapshot.DeliveryExceptions)
	}
	byBooking := make(map[uuid.UUID]AdminEventAfterSalesException, len(snapshot.DeliveryExceptions))
	for _, exception := range snapshot.DeliveryExceptions {
		byBooking[exception.BookingID] = exception
		if exception.BookingStatus != "CONFIRMED" || exception.State != "CONFIRMED_TICKET_DELIVERY_PENDING" || exception.ExpectedTicketCount == exception.RecordedTicketCount {
			t.Fatalf("exception overstated delivery facts: %+v", exception)
		}
	}
	if _, found := byBooking[complete.bookingID]; found {
		t.Fatal("complete Ticket set was reported as a delivery exception")
	}
	if _, found := byBooking[otherEvent.bookingID]; found {
		t.Fatal("another Event's Booking was included in the snapshot")
	}
	missing := byBooking[missingReceipt.bookingID]
	if missing.IssuanceReceiptOutcome != "NOT_RECORDED" || missing.IssuanceReasonCode != "" || missing.ExpectedTicketCount != 2 || missing.RecordedTicketCount != 0 {
		t.Fatalf("missing receipt projection = %+v", missing)
	}
	denied := byBooking[deniedReceipt.bookingID]
	if denied.IssuanceReceiptOutcome != "denied" || denied.IssuanceReasonCode != "payment_evidence_unknown" || denied.ExpectedTicketCount != 1 || denied.RecordedTicketCount != 0 {
		t.Fatalf("denied receipt projection = %+v", denied)
	}
}

type adminAfterSalesBooking struct {
	bookingID, eventID, sagaID, paymentID, itemID, tierID uuid.UUID
	seatIDs                                               []uuid.UUID
}

func seedAdminAfterSalesBooking(t *testing.T, p *pgxpool.Pool, eventID uuid.UUID, quantity int) adminAfterSalesBooking {
	t.Helper()
	ctx := context.Background()
	booking := adminAfterSalesBooking{
		bookingID: uuid.New(), eventID: eventID, sagaID: uuid.New(), paymentID: uuid.New(), itemID: uuid.New(), tierID: uuid.New(),
		seatIDs: make([]uuid.UUID, quantity),
	}
	for i := range booking.seatIDs {
		booking.seatIDs[i] = uuid.New()
	}
	if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',725,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+interval '1 hour',$5)`,
		booking.bookingID, uuid.New(), booking.eventID, uuid.New(), booking.sagaID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,$4,725,$5)`,
		booking.itemID, booking.bookingID, booking.tierID, quantity, booking.seatIDs); err != nil {
		t.Fatal(err)
	}
	return booking
}

func issueAdminAfterSalesTicket(t *testing.T, p *pgxpool.Pool, booking adminAfterSalesBooking) error {
	t.Helper()
	if len(booking.seatIDs) != 1 {
		t.Fatalf("fixture requires one seat, got %d", len(booking.seatIDs))
	}
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count)
		VALUES($1,$2,$3,$4,$5,1,'issued','ticket_issuance_eligible','provider_observed_no_dispute','provider_observed_no_refund','stripe_test_api',clock_timestamp(),1)`,
		uuid.New(), booking.sagaID, booking.bookingID, booking.paymentID, booking.eventID); err != nil {
		return err
	}
	_, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'ISSUED')`,
		uuid.New(), booking.bookingID, booking.itemID, booking.sagaID, booking.paymentID, booking.eventID, booking.tierID, booking.seatIDs[0])
	return err
}

func requireAdminAfterSalesPostgresOrSkip(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("BOOKING_TEST_DATABASE_URL")
	project := os.Getenv("TICKET_E2E_PROJECT")
	containerID := os.Getenv("BOOKING_TEST_DATABASE_CONTAINER_ID")
	if !isIsolatedBookingTestDatabaseURL(dsn) || project == "" || containerID == "" {
		t.Skip("SKIP: verified isolated Booking PostgreSQL test target is unavailable")
	}
	containerJSON, err := exec.Command("docker", "inspect", containerID).Output()
	if err != nil {
		t.Skip("SKIP: isolated Booking PostgreSQL container cannot be inspected")
	}
	networkJSON, err := exec.Command("docker", "network", "inspect", project+"_ticket-e2e").Output()
	if err != nil || !isVerifiedTicketE2EPostgresTarget(dsn, project, containerID, containerJSON, networkJSON) {
		t.Skip("SKIP: Booking PostgreSQL target does not match the verified isolated test container")
	}
}
