package domain

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
)

type SagaInput struct {
	BookingID, UserID, EventID uuid.UUID
	Generation                 int64
	SeatIDs                    []uuid.UUID
	AmountCents                int64
	Currency, BookingMode      string
	ReservationDeadline        time.Time
	Fingerprint                [32]byte
}
type SagaClaim struct {
	SagaID, Token uuid.UUID
	Fence         int64
	Direction     string
	LeaseUntil    time.Time
	Saga          *Saga
	Input         SagaInput
	Reclaimed     bool
}
type StepResult struct {
	StepID                          uuid.UUID
	Direction, State, ErrorCategory string
	Evidence                        json.RawMessage
	NextAttemptAt                   time.Time
}

// CanonicalInput includes no transport, credential or runtime claim data.
func CanonicalInput(input SagaInput) (SagaInput, []byte, error) {
	if input.BookingID == uuid.Nil || input.UserID == uuid.Nil || input.EventID == uuid.Nil || input.Generation != 1 || input.AmountCents < 1 || input.AmountCents > 2147483647 || input.Currency != "usd" || (input.BookingMode != "pessimistic" && input.BookingMode != "optimistic") || input.ReservationDeadline.IsZero() || len(input.SeatIDs) == 0 {
		return input, nil, errors.New("invalid saga input")
	}
	input.SeatIDs = append([]uuid.UUID(nil), input.SeatIDs...)
	sort.Slice(input.SeatIDs, func(i, j int) bool { return input.SeatIDs[i].String() < input.SeatIDs[j].String() })
	for i, id := range input.SeatIDs {
		if id == uuid.Nil || (i > 0 && id == input.SeatIDs[i-1]) {
			return input, nil, errors.New("invalid saga seat set")
		}
	}
	input.ReservationDeadline = input.ReservationDeadline.UTC().Truncate(time.Microsecond)
	wire := struct {
		Version                    int
		BookingID, UserID, EventID uuid.UUID
		Generation                 int64
		SeatIDs                    []uuid.UUID
		AmountCents                int64
		Currency, BookingMode      string
		ReservationDeadline        time.Time
	}{1, input.BookingID, input.UserID, input.EventID, input.Generation, input.SeatIDs, input.AmountCents, input.Currency, input.BookingMode, input.ReservationDeadline}
	raw, err := json.Marshal(wire)
	if err != nil {
		return input, nil, err
	}
	input.Fingerprint = sha256.Sum256(raw)
	return input, raw, nil
}

func DecodeInput(raw []byte) (SagaInput, error) {
	var wire struct {
		Version                    int
		BookingID, UserID, EventID uuid.UUID
		Generation                 int64
		SeatIDs                    []uuid.UUID
		AmountCents                int64
		Currency, BookingMode      string
		ReservationDeadline        time.Time
	}
	if err := json.Unmarshal(raw, &wire); err != nil || wire.Version != 1 {
		return SagaInput{}, errors.New("invalid persisted saga input")
	}
	input, _, err := CanonicalInput(SagaInput{BookingID: wire.BookingID, UserID: wire.UserID, EventID: wire.EventID, Generation: wire.Generation, SeatIDs: wire.SeatIDs, AmountCents: wire.AmountCents, Currency: wire.Currency, BookingMode: wire.BookingMode, ReservationDeadline: wire.ReservationDeadline})
	return input, err
}
