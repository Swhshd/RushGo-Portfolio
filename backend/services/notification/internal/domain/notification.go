package domain

import (
	"time"

	"github.com/google/uuid"
)

type Notification struct {
	ID         uuid.UUID
	Type       string
	Recipient  string
	Channel    string
	Locale     string
	Payload    map[string]interface{}
	Status     string
	ProviderID string
	SentAt     *time.Time
	CreatedAt  time.Time
	Managed    bool
}
