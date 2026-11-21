package domain

import (
	"crypto/sha256"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

func SeatSetFingerprint(eventID uuid.UUID, seatIDs []uuid.UUID) ([32]byte, error) {
	if eventID == uuid.Nil || len(seatIDs) == 0 {
		return [32]byte{}, errors.New("event and seats are required")
	}
	ids := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		if id == uuid.Nil {
			return [32]byte{}, errors.New("empty seat ID")
		}
		ids[i] = id.String()
	}
	sort.Strings(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			return [32]byte{}, errors.New("duplicate seat ID")
		}
	}
	return sha256.Sum256([]byte("v1\n" + eventID.String() + "\n" + strings.Join(ids, ",") + "\n")), nil
}

type SeatLifecycleRequest struct {
	BookingID, EventID  uuid.UUID
	Generation          int64
	SeatIDs             []uuid.UUID
	Fingerprint         [32]byte
	OperationID         uuid.UUID
	Action              string
	ReservationDeadline time.Time
	TierQuantities      map[uuid.UUID]int32
}

type SeatLifecycleResult struct {
	State, AccountingState string
	ChangedSeats           []*Seat
	Applied                bool
	ReservationDeadline    *time.Time
}

type SeatAuthority struct {
	SeatID, EventID     uuid.UUID
	Status              SeatStatus
	BookingID           *uuid.UUID
	ReservationDeadline *time.Time
	Generation          *int64
	LifecycleState      *string
	Coherent            bool
}
