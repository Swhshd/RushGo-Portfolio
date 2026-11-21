//go:build !ticket_e2e && !aftersales_acceptance

package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestProductionBuildRejectsSimulatedProviderAuthority(t *testing.T) {
	bookingID, eventID, paymentID, sagaID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	event := TicketIssuanceEvent{ID: uuid.New(), SagaID: sagaID, BookingID: bookingID, PaymentID: paymentID, EventID: eventID, Generation: 1}
	candidate := TicketIssuanceCandidate{
		BookingID: bookingID, UserID: uuid.New(), EventID: eventID, PaymentID: paymentID, SagaID: sagaID,
		Status: StatusConfirmed, Managed: true, Generation: 1, TotalAmountCents: 2000, Currency: "usd",
		Items: []BookingItem{{ID: uuid.New(), BookingID: bookingID, TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}},
	}
	evidence := TicketAuthorityEvidence{
		SagaEligible: true, PaymentEligible: true, PaymentDecisionKnown: true, EventEligible: true,
		DisputeState: DisputeNoDispute, RefundState: RefundNoRefund,
		Source: "SIMULATED_PROVIDER_AUTHORITY", ObservedAt: time.Now().UTC(),
	}
	if got := ValidateTicketIssuance(event, candidate, evidence); got.Eligible {
		t.Fatal("production build accepted simulated provider evidence")
	}
}
