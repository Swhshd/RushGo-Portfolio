package admission

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ticketbox/event/internal/domain"
)

const keyPrefix = "rushgo:seat-admission"

type Mode string

const (
	ModePostgresOnly   Mode = "POSTGRES_ONLY"
	ModeRedisAdmission Mode = "REDIS_ADMISSION"
)

var (
	ErrConflict       = errors.New("seat admission conflict")
	ErrInfrastructure = errors.New("seat admission infrastructure error")
	ErrInvalidRequest = errors.New("invalid seat admission request")
)

type AcquireResult struct {
	Created  int
	Existing int
}

type ReleaseResult struct {
	Deleted       int
	Missing       int
	OwnerMismatch int
}

type Admission interface {
	Acquire(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, owner uuid.UUID, reservationMinutes int32) (AcquireResult, error)
	Release(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, owner uuid.UUID) (ReleaseResult, error)
}

func Key(eventID, seatID uuid.UUID) string {
	return fmt.Sprintf("%s:%s:%s", keyPrefix, eventID, seatID)
}

func ReservationTTL(minutes int32) (time.Duration, error) {
	minutes = domain.NormalizeReservationTimeInMinutes(minutes)
	return time.Duration(minutes) * time.Minute, nil
}

func validateRequest(eventID, owner uuid.UUID, seatIDs []uuid.UUID) error {
	if eventID == uuid.Nil || owner == uuid.Nil || len(seatIDs) == 0 {
		return ErrInvalidRequest
	}
	seen := make(map[uuid.UUID]struct{}, len(seatIDs))
	for _, seatID := range seatIDs {
		if seatID == uuid.Nil {
			return ErrInvalidRequest
		}
		if _, exists := seen[seatID]; exists {
			return fmt.Errorf("duplicate seat %s: %w", seatID, ErrInvalidRequest)
		}
		seen[seatID] = struct{}{}
	}
	return nil
}
