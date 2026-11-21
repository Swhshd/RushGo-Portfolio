package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
)

func TestInboxRecordDetailPathSeparatesEventRemindersFromOrders(t *testing.T) {
	bookingID := uuid.New()
	eventID := uuid.New()
	if got, want := InboxRecordDetailPath(domain.BusinessInboxRecord{Type: "tickets_issued", BookingID: bookingID}), "/my-tickets/orders/"+bookingID.String(); got != want {
		t.Fatalf("booking detail path=%q want %q", got, want)
	}
	if got, want := InboxRecordDetailPath(domain.BusinessInboxRecord{Type: "event_onsale", EventID: eventID}), "/events/"+eventID.String(); got != want {
		t.Fatalf("event detail path=%q want %q", got, want)
	}
	if got, want := InboxRecordDetailPath(domain.BusinessInboxRecord{Type: "event_restock"}), "/events"; got != want {
		t.Fatalf("missing event identity detail path=%q want %q", got, want)
	}
}
