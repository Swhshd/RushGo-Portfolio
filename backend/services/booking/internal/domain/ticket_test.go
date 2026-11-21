package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestValidateTicketIssuanceRequiresAllFreshAuthorities(t *testing.T) {
	bookingID, eventID, paymentID, sagaID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seat1, seat2 := uuid.New(), uuid.New()
	candidate := TicketIssuanceCandidate{
		BookingID: bookingID, UserID: uuid.New(), EventID: eventID, PaymentID: paymentID, SagaID: sagaID,
		Status: StatusConfirmed, Managed: true, Generation: 1, TotalAmountCents: 2000, Currency: "usd",
		Items: []BookingItem{{ID: uuid.New(), BookingID: bookingID, TicketTierID: uuid.New(), Quantity: 2,
			SeatIDs: []uuid.UUID{seat1, seat2}}},
	}
	event := TicketIssuanceEvent{ID: uuid.New(), SagaID: sagaID, BookingID: bookingID, PaymentID: paymentID, EventID: eventID, Generation: 1}
	good := TicketAuthorityEvidence{SagaEligible: true, PaymentEligible: true, PaymentDecisionKnown: true, EventEligible: true,
		DisputeState: DisputeNoDispute, RefundState: RefundNoRefund, Source: "stripe_test_api", ObservedAt: time.Now().UTC()}

	if got := ValidateTicketIssuance(event, candidate, good); !got.Eligible {
		t.Fatalf("eligible candidate denied: %s", got.ReasonCode)
	}

	cases := []struct {
		name   string
		mutate func(*TicketIssuanceEvent, *TicketIssuanceCandidate, *TicketAuthorityEvidence)
	}{
		{name: "unknown dispute", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.DisputeState = EvidenceUnknown
		}},
		{name: "historical dispute", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.DisputeState = DisputePresent
		}},
		{name: "unknown refund", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.RefundState = RefundUnknown
		}},
		{name: "no current observation", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.ObservedAt = time.Time{}
		}},
		{name: "no provider source", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) { e.Source = "" }},
		{name: "saga missing", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.SagaEligible = false
		}},
		{name: "event unaccounted", mutate: func(_ *TicketIssuanceEvent, _ *TicketIssuanceCandidate, e *TicketAuthorityEvidence) {
			e.EventEligible = false
		}},
		{name: "booking identity mismatch", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.BookingID = uuid.New()
		}},
		{name: "no seat GA", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.Items[0].SeatIDs = nil
		}},
		{name: "quantity mismatch", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.Items[0].Quantity = 1
		}},
		{name: "duplicate seat", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.Items[0].SeatIDs[1] = b.Items[0].SeatIDs[0]
		}},
		{name: "unmanaged historical booking", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.Managed = false
		}},
		{name: "not confirmed", mutate: func(_ *TicketIssuanceEvent, b *TicketIssuanceCandidate, _ *TicketAuthorityEvidence) {
			b.Status = StatusCancelled
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, booking, evidence := event, candidate, good
			booking.Items = append([]BookingItem(nil), candidate.Items...)
			booking.Items[0].SeatIDs = append([]uuid.UUID(nil), candidate.Items[0].SeatIDs...)
			tc.mutate(&ev, &booking, &evidence)
			if got := ValidateTicketIssuance(ev, booking, evidence); got.Eligible {
				t.Fatal("unsafe ticket issuance was eligible")
			}
		})
	}
}

func TestTicketIssuanceEventRequiresExactVersionedIdentity(t *testing.T) {
	event := TicketIssuanceEvent{ID: uuid.New(), SagaID: uuid.New(), BookingID: uuid.New(), PaymentID: uuid.New(), EventID: uuid.New(), Generation: 1}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	event.Generation = 2
	if err := event.Validate(); err == nil {
		t.Fatal("unsupported generation accepted")
	}
}
