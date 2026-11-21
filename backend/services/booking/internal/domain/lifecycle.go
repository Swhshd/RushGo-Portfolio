package domain

import (
	"github.com/google/uuid"
	"time"
)

type LifecycleRef struct {
	BookingID  uuid.UUID
	Generation int64
}
type LifecycleDecision struct {
	Ref                    LifecycleRef
	DecisionID             uuid.UUID
	ConfirmationDecisionID *uuid.UUID
	Kind, Reason           string
	ExpectedVersion        int32
}
type LifecycleSnapshot struct {
	Booking                            *Booking
	Generation                         int64
	Phase                              string
	DecisionID, ConfirmationDecisionID *uuid.UUID
	Reason, Currency, BookingMode      string
	ReservationDeadline                *time.Time
	DispatchState                      string
	DispatchAttempts                   int
}

func BookingDecisionID(bookingID uuid.UUID, kind string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:"+kind))
}

// BookingOutboxID preserves the generation-one confirmation identity and names
// terminal intents by the winning lifecycle decision, not by delivery attempt.
func BookingOutboxID(bookingID uuid.UUID, kind string) uuid.UUID {
	if kind == "confirm" {
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking_confirmation:"+bookingID.String()))
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking-event:"+bookingID.String()+":g1:"+kind))
}
