package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
)

func validOriginalCandidate(c domain.TicketIssuanceCandidate) bool {
	return c.BookingID == originalEvent.BookingID && c.EventID == originalEvent.EventID &&
		c.SagaID == originalEvent.SagaID && c.PaymentID == originalEvent.PaymentID &&
		c.Managed && c.Generation == 1 && c.Status == domain.BookingStatus("CONFIRMED") &&
		c.TotalAmountCents == 100 && c.Currency == "usd" &&
		len(c.Items) == 1 && c.Items[0].Quantity == 1 && len(c.Items[0].SeatIDs) == 1
}

// A fresh statement observes committed rows. Failure is inconclusive, never
// permission to repeat a transaction whose commit response was lost.
func verifyIssued(ctx context.Context, pool *pgxpool.Pool, c domain.TicketIssuanceCandidate, out map[string]any) error {
	if !validOriginalCandidate(c) {
		return errors.New("original_order_shape_mismatch")
	}
	var count, valid, receiptCount, issued, refunds int
	err := pool.QueryRow(ctx, `SELECT
    (SELECT count(*) FROM tickets WHERE booking_id=$1),
    (SELECT count(*) FROM tickets WHERE booking_id=$1 AND booking_item_id=$2 AND seat_id=$3 AND saga_id=$4 AND payment_id=$5 AND event_id=$6 AND lifecycle_generation=1 AND status='ISSUED' AND ticket_tier_id=$8),
    (SELECT count(*) FROM ticket_issuance_receipts WHERE booking_id=$1),
    (SELECT count(*) FROM ticket_issuance_receipts WHERE booking_id=$1 AND event_id=$7 AND saga_id=$4 AND payment_id=$5 AND event_ref_id=$6 AND lifecycle_generation=1 AND outcome='issued' AND ticket_count=1 AND evidence_source='stripe_test_api'),
    (SELECT count(*) FROM booking_refund_requests WHERE booking_id=$1)`,
		originalEvent.BookingID, c.Items[0].ID, c.Items[0].SeatIDs[0], originalEvent.SagaID,
		originalEvent.PaymentID, originalEvent.EventID, originalEvent.ID, c.Items[0].TicketTierID).
		Scan(&count, &valid, &receiptCount, &issued, &refunds)
	if err != nil {
		return err
	}
	if count != 1 || valid != 1 || receiptCount != 1 || issued != 1 || refunds != 0 {
		return errors.New("persisted_ticket_or_receipt_mismatch")
	}
	out["tickets"], out["receipts"], out["seatAndItemMatch"] = count, receiptCount, true
	return nil
}
