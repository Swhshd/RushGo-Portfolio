package domain

import (
	"time"

	"github.com/google/uuid"
)

type DeliveryClaim struct {
	Notification *Notification
	Token        uuid.UUID
	Fence        int64
	Attempts     int64
	LeaseUntil   time.Time
}
type DeliveryResult struct {
	State, ErrorCategory, ProviderID string
	NextAttemptAt                    time.Time
}
