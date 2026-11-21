package adminticketlist

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorBindsEventFilterAndDescendingKey(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	ticketID := uuid.MustParse("61000000-0000-4000-8000-000000000002")
	issuedAt := time.Date(2026, 11, 1, 12, 34, 56, 789123000, time.UTC)
	want := Cursor{EventID: eventID, StatusFilter: StatusCheckedIn, IssuedAt: issuedAt, TicketID: ticketID}

	encoded, err := EncodeCursor(want)
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	if len(encoded) > MaxCursorLength || strings.Contains(encoded, "=") {
		t.Fatalf("cursor is not bounded raw base64url: %q", encoded)
	}
	got, err := DecodeCursor(encoded, eventID, StatusCheckedIn)
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if got.EventID != want.EventID || got.StatusFilter != want.StatusFilter || !got.IssuedAt.Equal(want.IssuedAt) || got.TicketID != want.TicketID {
		t.Fatalf("decoded cursor = %#v, want %#v", got, want)
	}
	if _, err := DecodeCursor(encoded, uuid.MustParse("60000000-0000-4000-8000-000000000002"), StatusCheckedIn); err == nil {
		t.Fatal("cursor bound to another event was accepted")
	}
	if _, err := DecodeCursor(encoded, eventID, StatusIssued); err == nil {
		t.Fatal("cursor bound to another filter was accepted")
	}
}

func TestCursorRejectsMalformedUnboundedAndUnsupportedValues(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	valid, err := EncodeCursor(Cursor{
		EventID: eventID, StatusFilter: StatusAll,
		IssuedAt: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),
		TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"!", valid + "=", strings.Repeat("A", MaxCursorLength+1), valid[:len(valid)-1] + "!"} {
		t.Run(value, func(t *testing.T) {
			if _, err := DecodeCursor(value, eventID, StatusAll); err == nil {
				t.Fatalf("DecodeCursor(%q) accepted malformed cursor", value)
			}
		})
	}
	if _, err := NormalizeStatusFilter("checked_in"); err == nil {
		t.Fatal("lowercase status filter was accepted")
	}
	if _, err := NormalizeStatusFilter("CHECK_IN_PENDING"); err == nil {
		t.Fatal("unknown status filter was accepted")
	}
}
