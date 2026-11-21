package domain

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
)

func TestBookingRequestCanonicalIdentity(t *testing.T) {
	event := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	tier := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	a := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	items := []BookingItem{{TicketTierID: tier, Quantity: 2, SeatIDs: []uuid.UUID{b, a}, UnitPriceCents: 900}}
	want := sha256.Sum256([]byte("v1\n00000000-0000-0000-0000-000000000001\npessimistic\n00000000-0000-0000-0000-000000000002:2:00000000-0000-0000-0000-000000000003,00000000-0000-0000-0000-000000000004\n"))
	hash, mode, err := CanonicalBookingRequest(event, items, "")
	if err != nil || hash != want || mode != "pessimistic" {
		t.Fatal("request did not use locked canonical encoding", err)
	}
	items[0].SeatIDs = []uuid.UUID{a, b}
	items[0].UnitPriceCents = 1
	if got, _, err := CanonicalBookingRequest(event, items, "pessimistic"); err != nil || got != want {
		t.Fatal("seat order or price altered request identity", err)
	}
	for _, invalid := range [][]BookingItem{nil, {{TicketTierID: tier, Quantity: 0}}, {{TicketTierID: tier, Quantity: 1, SeatIDs: []uuid.UUID{a, b}}}, {{TicketTierID: tier, Quantity: 2, SeatIDs: []uuid.UUID{a, a}}}, {items[0], items[0]}, {{TicketTierID: uuid.Nil, Quantity: 1, SeatIDs: []uuid.UUID{a}}}} {
		if _, _, err := CanonicalBookingRequest(event, invalid, "pessimistic"); err == nil {
			t.Fatal("invalid or duplicate request accepted")
		}
	}
	if _, _, err := CanonicalBookingRequest(event, items, "unknown"); err == nil {
		t.Fatal("unknown mode accepted")
	}
	for _, key := range []string{"a", "UUID_key-1"} {
		if !ValidBookingRequestID(key) {
			t.Fatal("valid key rejected")
		}
	}
	for _, key := range []string{"", "a b", "a\nb", "é"} {
		if ValidBookingRequestID(key) {
			t.Fatal("invalid key accepted")
		}
	}
}
