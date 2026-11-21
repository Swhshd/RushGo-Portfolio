package domain

import (
	"time"

	"github.com/google/uuid"
)

// AdminBookingDetail is a read-only, Booking-owned projection for an exact
// booking lookup. It does not assert provider collection or live admission.
type AdminBookingDetail struct {
	Booking    *Booking
	Tickets    []AdminBookingTicket
	Receipt    *AdminBookingReceipt
	Refund     *BookingRefundRequest
	ObservedAt time.Time
}

type AdminBookingTicket struct {
	TicketID     uuid.UUID
	Status       string
	SeatID       uuid.UUID
	TicketTierID uuid.UUID
	IssuedAt     time.Time
	CheckedInAt  *time.Time
	RevokedAt    *time.Time
}

type AdminBookingReceipt struct {
	Outcome      string
	ReasonCode   string
	TicketCount  int32
	ReceiptCount int32
	ObservedAt   *time.Time
	CreatedAt    *time.Time
}
