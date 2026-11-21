package repository

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
)

func TestUnusedRefundSuccessfulRefundWithTicketSetMismatchStaysRefundedAndNeedsAttentionIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	approved := approvedUnusedRefund(t, r, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var itemID, sagaID, paymentID, eventID, tierID uuid.UUID
	if err := p.QueryRow(ctx, `SELECT booking_item_id,saga_id,payment_id,event_id,ticket_tier_id FROM tickets WHERE id=$1`, f.ticketID).
		Scan(&itemID, &sagaID, &paymentID, &eventID, &tierID); err != nil {
		t.Fatal(err)
	}
	additionalSeat, additionalTicket := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `UPDATE booking_items SET quantity=quantity+1,seat_ids=array_append(seat_ids,$2) WHERE id=$1`, itemID, additionalSeat); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status,checked_in_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'CHECKED_IN',clock_timestamp())`, additionalTicket, f.bookingID, itemID, sagaID, paymentID, eventID, tierID, additionalSeat); err != nil {
		t.Fatal(err)
	}

	snapshot := f.decision.Payment
	snapshot.RefundDecisionID = approved.DecisionID
	snapshot.RefundState = "succeeded"
	snapshot.RefundAmountCents = approved.AmountCents
	snapshot.RefundStatus = "succeeded"
	result, err := r.ApplyUnusedRefundPaymentSnapshot(ctx, approved, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != domain.RefundRequestRefunded || result.PaymentRefundState != "succeeded" ||
		result.TicketRevocationState != "needs_attention" {
		t.Fatalf("refund/ticket mismatch projection=%+v, want REFUNDED with needs_attention", result)
	}
	var issued, checkedIn int
	if err = p.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='ISSUED'),count(*) FILTER (WHERE status='CHECKED_IN')
		FROM tickets WHERE booking_id=$1`, f.bookingID).Scan(&issued, &checkedIn); err != nil {
		t.Fatal(err)
	}
	if issued != 0 || checkedIn != 1 {
		t.Fatalf("ticket revocation changed a non-issued Ticket: issued=%d checked_in=%d", issued, checkedIn)
	}
}

func TestUnusedRefundEmptyHistoryMigrationDownUpUpPreservesBookingIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t, "9")
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	dsn := unusedRefundMigrationDSN(t, p)

	// Start a fresh schema at the historical migration boundary (9). Remove its empty coordination schema first,
	// then exercise migration 8's original empty-history rollback contract.
	if err := runUnusedRefundMigration(dsn, "down", "2"); err != nil {
		t.Fatalf("migration 8 down on empty refund history failed: %v", err)
	}
	var requestTableMissing, appendOnlyFunctionMissing, refundColumnMissing bool
	if err := p.QueryRow(context.Background(), `SELECT to_regclass('booking_refund_requests') IS NULL,
		to_regprocedure('booking_refund_append_only()') IS NULL,
		NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema()
			AND table_name='bookings' AND column_name='refund_decision_id')`).
		Scan(&requestTableMissing, &appendOnlyFunctionMissing, &refundColumnMissing); err != nil {
		t.Fatal(err)
	}
	if !requestTableMissing || !appendOnlyFunctionMissing || !refundColumnMissing {
		t.Fatalf("migration 8 down left refund objects: table_missing=%v function_missing=%v column_missing=%v",
			requestTableMissing, appendOnlyFunctionMissing, refundColumnMissing)
	}

	for range 2 {
		if err := runUnusedRefundMigration(dsn, "up"); err != nil {
			t.Fatalf("migration up failed: %v", err)
		}
	}
	var status string
	if err := p.QueryRow(context.Background(), `SELECT status FROM bookings WHERE id=$1`, f.bookingID).Scan(&status); err != nil || status != "CONFIRMED" {
		t.Fatalf("legacy Booking after migration down/up/up status=%q error=%v", status, err)
	}
	var requestTablePresent, appendOnlyFunctionPresent, refundColumnPresent bool
	if err := p.QueryRow(context.Background(), `SELECT to_regclass('booking_refund_requests') IS NOT NULL,
		to_regprocedure('booking_refund_append_only()') IS NOT NULL,
		EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema()
			AND table_name='bookings' AND column_name='refund_decision_id')`).
		Scan(&requestTablePresent, &appendOnlyFunctionPresent, &refundColumnPresent); err != nil {
		t.Fatal(err)
	}
	if !requestTablePresent || !appendOnlyFunctionPresent || !refundColumnPresent {
		t.Fatalf("migration 8 up did not restore refund objects: table=%v function=%v column=%v",
			requestTablePresent, appendOnlyFunctionPresent, refundColumnPresent)
	}
}

func TestUnusedRefundMigrationDownRefusesExistingRefundHistoryIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	approved := approvedUnusedRefund(t, r, f)
	dsn := unusedRefundMigrationDSN(t, p)

	if err := runUnusedRefundMigration(dsn, "down", "2"); err == nil {
		t.Fatal("migration 8 down succeeded with an approved refund history")
	}
	var decisionID *uuid.UUID
	if err := p.QueryRow(context.Background(), `SELECT refund_decision_id FROM bookings WHERE id=$1`, f.bookingID).Scan(&decisionID); err != nil {
		t.Fatal(err)
	}
	if decisionID == nil || *decisionID != approved.DecisionID {
		t.Fatalf("failed down migration damaged refund binding: got %v want %s", decisionID, approved.DecisionID)
	}
	var requests, decisions int
	if err := p.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM booking_refund_requests),
		(SELECT count(*) FROM booking_refund_decisions)`).Scan(&requests, &decisions); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || decisions != 1 {
		t.Fatalf("failed down migration changed durable history: requests=%d decisions=%d", requests, decisions)
	}
}

func unusedRefundMigrationDSN(t *testing.T, p *pgxpool.Pool) string {
	t.Helper()
	var schema string
	if err := p.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(os.Getenv("BOOKING_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("parse isolated Booking test DSN")
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func runUnusedRefundMigration(dsn string, args ...string) error {
	commandArgs := []string{"-path", "../../migrations", "-database", dsn}
	commandArgs = append(commandArgs, args...)
	return exec.Command("migrate", commandArgs...).Run()
}
