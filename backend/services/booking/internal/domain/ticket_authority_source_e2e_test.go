//go:build ticket_e2e

package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTicketE2EBuildAcceptsDeclaredSimulatedAuthorityOnlyWithCompleteEvidence(t *testing.T) {
	bookingID, eventID, paymentID, sagaID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seatID := uuid.New()
	event := TicketIssuanceEvent{ID: uuid.New(), SagaID: sagaID, BookingID: bookingID, PaymentID: paymentID, EventID: eventID, Generation: 1}
	candidate := TicketIssuanceCandidate{
		BookingID: bookingID, UserID: uuid.New(), EventID: eventID, PaymentID: paymentID, SagaID: sagaID,
		Status: StatusConfirmed, Managed: true, Generation: 1, TotalAmountCents: 2000, Currency: "usd",
		Items: []BookingItem{{ID: uuid.New(), BookingID: bookingID, TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{seatID}}},
	}
	evidence := TicketAuthorityEvidence{
		SagaEligible: true, PaymentEligible: true, PaymentDecisionKnown: true, EventEligible: true,
		DisputeState: DisputeNoDispute, RefundState: RefundNoRefund,
		Source: "SIMULATED_PROVIDER_AUTHORITY", ObservedAt: time.Now().UTC(),
	}
	if got := ValidateTicketIssuance(event, candidate, evidence); !got.Eligible {
		t.Fatalf("complete simulated evidence denied in the ticket_e2e build: %s", got.ReasonCode)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*TicketIssuanceEvent, *TicketIssuanceCandidate, *TicketAuthorityEvidence)
	}{
		{name: "undeclared source", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.Source = "synthetic"
		}},
		{name: "unknown dispute", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.DisputeState = EvidenceUnknown
		}},
		{name: "unknown refund", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.RefundState = RefundUnknown
		}},
		{name: "missing observation", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.ObservedAt = time.Time{}
		}},
		{name: "payment unknown", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.PaymentDecisionKnown = false
		}},
		{name: "booking identity mismatch", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.PaymentID = uuid.New()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changedEvent, changedCandidate, changedEvidence := event, candidate, evidence
			changedCandidate.Items = append([]BookingItem(nil), candidate.Items...)
			tc.mutate(&changedEvent, &changedCandidate, &changedEvidence)
			if got := ValidateTicketIssuance(changedEvent, changedCandidate, changedEvidence); got.Eligible {
				t.Fatal("incomplete simulated evidence was eligible")
			}
		})
	}
}
