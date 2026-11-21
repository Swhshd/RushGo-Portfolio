package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type BookingRequestIdentity struct {
	UserID    uuid.UUID
	RequestID string
	Hash      [32]byte
}
type CreateBookingCommand struct {
	Booking               *Booking
	Identity              BookingRequestIdentity
	Currency, BookingMode string
	ReservationDeadline   time.Time
}
type DispatchClaim struct {
	Ref        LifecycleRef
	Token      uuid.UUID
	Fence      int64
	Version    int32
	Kind       string
	Snapshot   LifecycleSnapshot
	LeaseUntil time.Time
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func ValidBookingRequestID(key string) bool { return requestIDPattern.MatchString(key) }

func CanonicalBookingRequest(event uuid.UUID, items []BookingItem, mode string) ([32]byte, string, error) {
	var empty [32]byte
	if mode == "" {
		mode = "pessimistic"
	}
	if event == uuid.Nil || len(items) == 0 || (mode != "pessimistic" && mode != "optimistic") {
		return empty, mode, errors.New("invalid booking request")
	}
	tiers, seats := make(map[uuid.UUID]bool), make(map[uuid.UUID]bool)
	var records []string
	for _, item := range items {
		if item.TicketTierID == uuid.Nil || tiers[item.TicketTierID] || item.Quantity <= 0 || int(item.Quantity) != len(item.SeatIDs) {
			return empty, mode, errors.New("invalid booking item")
		}
		tiers[item.TicketTierID] = true
		ids := make([]string, 0, len(item.SeatIDs))
		for _, id := range item.SeatIDs {
			if id == uuid.Nil || seats[id] {
				return empty, mode, errors.New("duplicate or invalid seat")
			}
			seats[id] = true
			ids = append(ids, id.String())
		}
		sort.Strings(ids)
		records = append(records, fmt.Sprintf("%s:%d:%s\n", item.TicketTierID, item.Quantity, strings.Join(ids, ",")))
	}
	sort.Strings(records)
	return sha256.Sum256([]byte("v1\n" + event.String() + "\n" + mode + "\n" + strings.Join(records, ""))), mode, nil
}
