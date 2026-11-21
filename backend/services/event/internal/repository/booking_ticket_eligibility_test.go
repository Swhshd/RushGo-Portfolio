package repository

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestValidateBookingTicketEligibilityRequiresExactBookedAccountedSeats(t *testing.T) {
	bookingID, eventID, tierID := uuid.New(), uuid.New(), uuid.New()
	seatA, seatB := uuid.New(), uuid.New()
	generation := int64(1)
	snapshot := bookingSeatEligibilitySnapshot{
		Lifecycle: bookingSeatLifecycleEvidence{BookingID: bookingID, EventID: eventID, Generation: generation, State: "booked", AccountingState: "applied", SeatIDs: []uuid.UUID{seatA, seatB}, Debits: mustMarshalDebits(t, map[uuid.UUID]int32{tierID: 2})},
		Seats: []bookingSeatEvidenceRow{
			{ID: seatA, EventID: eventID, TicketTierID: tierID, BookingID: &bookingID, LifecycleGeneration: &generation, Status: "booked"},
			{ID: seatB, EventID: eventID, TicketTierID: tierID, BookingID: &bookingID, LifecycleGeneration: &generation, Status: "booked"},
		},
	}
	eligible, reason := validateBookingTicketEligibility(bookingID, eventID, generation, []uuid.UUID{seatA, seatB}, snapshot)
	if !eligible || reason != "event_booking_seats_accounted" {
		t.Fatalf("valid booked/accounted seat set rejected: eligible=%v reason=%q", eligible, reason)
	}

	tests := []struct {
		name    string
		mutate  func(*bookingSeatEligibilitySnapshot)
		request []uuid.UUID
	}{
		{name: "accounting not applied", mutate: func(s *bookingSeatEligibilitySnapshot) { s.Lifecycle.AccountingState = "not_applied" }, request: []uuid.UUID{seatA, seatB}},
		{name: "ownership mismatch", mutate: func(s *bookingSeatEligibilitySnapshot) { owner := uuid.New(); s.Seats[0].BookingID = &owner }, request: []uuid.UUID{seatA, seatB}},
		{name: "incomplete seat set", mutate: func(s *bookingSeatEligibilitySnapshot) { s.Lifecycle.SeatIDs = []uuid.UUID{seatA} }, request: []uuid.UUID{seatA, seatB}},
		{name: "debit mismatch", mutate: func(s *bookingSeatEligibilitySnapshot) {
			s.Lifecycle.Debits = mustMarshalDebits(t, map[uuid.UUID]int32{tierID: 1})
		}, request: []uuid.UUID{seatA, seatB}},
		{name: "duplicate request", mutate: func(*bookingSeatEligibilitySnapshot) {}, request: []uuid.UUID{seatA, seatA}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copy := snapshot
			copy.Lifecycle.SeatIDs = append([]uuid.UUID(nil), snapshot.Lifecycle.SeatIDs...)
			copy.Seats = append([]bookingSeatEvidenceRow(nil), snapshot.Seats...)
			test.mutate(&copy)
			eligible, reason := validateBookingTicketEligibility(bookingID, eventID, generation, test.request, copy)
			if eligible || reason == "" {
				t.Fatalf("invalid seat evidence was admitted: eligible=%v reason=%q", eligible, reason)
			}
		})
	}
}

func mustMarshalDebits(t *testing.T, debits map[uuid.UUID]int32) json.RawMessage {
	t.Helper()
	value, err := json.Marshal(debits)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
