package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminBookingDetailProjectionKeepsCurrencyAndFacts(t *testing.T) {
	bookingID, eventID, userID, tierID, seatID, ticketID, requestID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC().Add(-time.Minute)
	detail := &domain.AdminBookingDetail{
		Booking: &domain.Booking{
			ID: bookingID, UserID: userID, EventID: eventID, Status: domain.StatusConfirmed,
			TotalAmountCents: 1000, Currency: "usd", BookingMode: "pessimistic",
			LifecycleGeneration: 1, LifecyclePhase: "TERMINAL", CreatedAt: now,
			Items: []domain.BookingItem{{ID: uuid.New(), TicketTierID: tierID, TierName: "Floor", Quantity: 1, UnitPriceCents: 1000, SeatIDs: []uuid.UUID{seatID}}},
		},
		Tickets:    []domain.AdminBookingTicket{{TicketID: ticketID, Status: "ISSUED", SeatID: seatID, TicketTierID: tierID, IssuedAt: now}},
		Receipt:    &domain.AdminBookingReceipt{Outcome: "issued", ReasonCode: "eligible", TicketCount: 1, ReceiptCount: 1, CreatedAt: &now, ObservedAt: &now},
		Refund:     &domain.BookingRefundRequest{RequestID: requestID, BookingID: bookingID, EventID: eventID, Status: domain.RefundRequestRequested, AmountCents: 1000, Currency: "usd", RequestedAt: &now, PaymentRefundState: "not_required", TicketRevocationState: "not_started"},
		ObservedAt: now.Add(2 * time.Minute),
	}
	projection := (&BookingServer{}).adminBookingDetailProjection(detail)
	if projection.GetBooking().GetCurrency() != "usd" || projection.GetCurrency() != "usd" || projection.GetLifecyclePhase() != "TERMINAL" {
		t.Fatalf("unexpected booking projection: %+v", projection)
	}
	if len(projection.GetTickets()) != 1 || !projection.GetReceiptPresent() || projection.GetReceipt().GetTicketCount() != 1 ||
		!projection.GetRefundPresent() || projection.GetRefund().GetRequestId() != requestID.String() {
		t.Fatalf("unexpected detail projection: %+v", projection)
	}
}

func TestGetAdminBookingDetailFailsClosedWithoutAdminWiring(t *testing.T) {
	server := &BookingServer{}
	_, err := server.GetAdminBookingDetail(context.Background(), &bookingv1.GetAdminBookingDetailRequest{BookingId: uuid.NewString()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("disabled secure wiring error = %v", err)
	}
	server.SetAdminBookingDetailSecure(true)
	_, err = server.GetAdminBookingDetail(context.Background(), &bookingv1.GetAdminBookingDetailRequest{BookingId: uuid.NewString()})
	if err == nil {
		t.Fatal("secure admin detail accepted a request without a verified gateway principal")
	}
}
