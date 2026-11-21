package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TicketIssuanceRequestedV1 struct {
	Version          int       `json:"version"`
	TicketIssuanceID uuid.UUID `json:"ticket_issuance_id"`
	SagaID           uuid.UUID `json:"saga_id"`
	BookingID        uuid.UUID `json:"booking_id"`
	PaymentID        uuid.UUID `json:"payment_id"`
	EventID          uuid.UUID `json:"event_id"`
	Generation       int64     `json:"generation"`
}

type TicketIssuanceOutboxEvent struct {
	ID, SagaID, BookingID uuid.UUID
	EventKey, EventType   string
	Payload               json.RawMessage
	CreatedAt             time.Time
	Attempts              int
	ClaimToken            uuid.UUID
}
