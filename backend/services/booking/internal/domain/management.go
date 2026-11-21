package domain

import (
	"errors"
	"github.com/google/uuid"
	"time"
)

var ErrManagementBounds = errors.New("management query exceeds bounded scope")
var ErrFeatureConflict = errors.New("feature flag version changed")
var ErrUnknownFeature = errors.New("feature flag is not allowlisted")

// These flags affect optional presentation only. Never add financial,
// authorization, admission, migration or transport-security decisions here.
func FeatureDescription(key string) (string, bool) {
	if key == "discovery_advanced_filters" {
		return "Show the optional date, location, price and sort controls in event discovery.", true
	}
	return "", false
}

type FeatureFlag struct {
	Key, Description string
	Default, Enabled bool
	Version          int64
	UpdatedAt        *time.Time
	ActorID          *uuid.UUID
}
type FeatureChange struct {
	Key           string
	Before, After bool
	Version       int64
	ActorID       uuid.UUID
	ChangedAt     time.Time
}
type BookingMetrics struct {
	Orders, IssuedTickets, CheckedInTickets, RefundRequests, ConfirmedRefunds int64
	ObservedAt                                                                time.Time
}
