package adminticketlist

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultPageSize  int32 = 20
	MaxPageSize      int32 = 100
	SourceRecordCap  int32 = 5000
	MaxCursorLength        = 62
	cursorVersion          = byte(1)
	cursorByteLength       = 46
)

const (
	StatusAll       = "ALL"
	StatusIssued    = "ISSUED"
	StatusCheckedIn = "CHECKED_IN"
	StatusRevoked   = "REVOKED"
)

var (
	ErrInvalidStatusFilter = errors.New("invalid Admin event ticket status filter")
	ErrInvalidPageSize     = errors.New("invalid Admin event ticket page size")
	ErrInvalidCursor       = errors.New("invalid Admin event ticket cursor")
)

// Cursor binds a keyset position to one Event and one exact status filter.
// The binary representation contains no authority or business data.
type Cursor struct {
	EventID      uuid.UUID
	StatusFilter string
	IssuedAt     time.Time
	TicketID     uuid.UUID
}

func NormalizeStatusFilter(value string) (string, error) {
	if value == "" {
		return StatusAll, nil
	}
	switch value {
	case StatusAll, StatusIssued, StatusCheckedIn, StatusRevoked:
		return value, nil
	default:
		return "", ErrInvalidStatusFilter
	}
}

func NormalizePageSize(value int32) (int32, error) {
	if value == 0 {
		return DefaultPageSize, nil
	}
	if value < 1 || value > MaxPageSize {
		return 0, ErrInvalidPageSize
	}
	return value, nil
}

func EncodeCursor(cursor Cursor) (string, error) {
	status, err := NormalizeStatusFilter(cursor.StatusFilter)
	if err != nil || cursor.EventID == uuid.Nil || cursor.TicketID == uuid.Nil || !validCursorTime(cursor.IssuedAt) {
		return "", ErrInvalidCursor
	}
	statusCode, ok := statusCode(status)
	if !ok {
		return "", ErrInvalidCursor
	}
	buffer := make([]byte, cursorByteLength)
	buffer[0] = cursorVersion
	copy(buffer[1:17], cursor.EventID[:])
	buffer[17] = statusCode
	binary.BigEndian.PutUint64(buffer[18:26], uint64(cursor.IssuedAt.Unix()))
	binary.BigEndian.PutUint32(buffer[26:30], uint32(cursor.IssuedAt.Nanosecond()))
	copy(buffer[30:46], cursor.TicketID[:])
	encoded := base64.RawURLEncoding.EncodeToString(buffer)
	if len(encoded) != MaxCursorLength {
		return "", ErrInvalidCursor
	}
	return encoded, nil
}

func DecodeCursor(encoded string, expectedEventID uuid.UUID, expectedStatusFilter string) (Cursor, error) {
	if len(encoded) != MaxCursorLength || expectedEventID == uuid.Nil {
		return Cursor{}, ErrInvalidCursor
	}
	filter, err := NormalizeStatusFilter(expectedStatusFilter)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	buffer, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(buffer) != cursorByteLength || buffer[0] != cursorVersion {
		return Cursor{}, ErrInvalidCursor
	}
	var eventID, ticketID uuid.UUID
	copy(eventID[:], buffer[1:17])
	copy(ticketID[:], buffer[30:46])
	filterCode, ok := statusCode(filter)
	if !ok || buffer[17] != filterCode || eventID != expectedEventID || ticketID == uuid.Nil {
		return Cursor{}, ErrInvalidCursor
	}
	seconds := int64(binary.BigEndian.Uint64(buffer[18:26]))
	nanos := binary.BigEndian.Uint32(buffer[26:30])
	if nanos > 999999999 {
		return Cursor{}, ErrInvalidCursor
	}
	issuedAt := time.Unix(seconds, int64(nanos)).UTC()
	if !validCursorTime(issuedAt) {
		return Cursor{}, ErrInvalidCursor
	}
	if base64.RawURLEncoding.EncodeToString(buffer) != encoded {
		return Cursor{}, ErrInvalidCursor
	}
	return Cursor{EventID: eventID, StatusFilter: filter, IssuedAt: issuedAt, TicketID: ticketID}, nil
}

func statusCode(status string) (byte, bool) {
	switch status {
	case StatusAll:
		return 0, true
	case StatusIssued:
		return 1, true
	case StatusCheckedIn:
		return 2, true
	case StatusRevoked:
		return 3, true
	default:
		return 0, false
	}
}

func validCursorTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999 && value.Nanosecond() >= 0 && value.Nanosecond() <= 999999999
}
