package eventreminder

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	EnvelopeType         = "EventReminderV1"
	ConsentVersion       = "event-alerts-v1"
	ActionTrigger        = "trigger"
	ActionWithdrawn      = "withdrawn"
	ActionConsentUpdated = "consent_updated"
	ActionResubscribed   = "resubscribed"
	TypeOnSale           = "event_onsale"
	TypeRestock          = "event_restock"
)

var ErrInvalidEnvelope = errors.New("invalid EventReminderV1")

// Envelope is the versioned Event-to-Notification contract. Event reminder
// messages contain only public event fields and identity needed for an inbox
// item; they never contain an email address or payment data.
type Envelope struct {
	Version             int32      `json:"version"`
	NotificationID      uuid.UUID  `json:"notification_id"`
	SubscriptionID      uuid.UUID  `json:"subscription_id"`
	SubscriptionVersion int64      `json:"subscription_version"`
	ActiveSinceVersion  int64      `json:"active_since_version"`
	Action              string     `json:"action"`
	Type                string     `json:"type"`
	UserID              uuid.UUID  `json:"user_id"`
	EventID             uuid.UUID  `json:"event_id"`
	EventTitle          string     `json:"event_title"`
	EventDate           time.Time  `json:"event_date"`
	EmailOptIn          bool       `json:"email_opt_in"`
	EmailConsentVersion string     `json:"email_consent_version"`
	EmailConsentID      *uuid.UUID `json:"email_consent_id,omitempty"`
}

func Parse(raw []byte) (*Envelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, ErrInvalidEnvelope
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, ErrInvalidEnvelope
	}
	// Older producers omitted this additive field. Keep those messages readable
	// while new Event producers send the authoritative active generation floor.
	if envelope.ActiveSinceVersion == 0 {
		if envelope.Action == ActionResubscribed {
			envelope.ActiveSinceVersion = envelope.SubscriptionVersion
		} else {
			envelope.ActiveSinceVersion = 1
		}
	}
	if envelope.Version != 1 || envelope.NotificationID == uuid.Nil || envelope.SubscriptionID == uuid.Nil ||
		envelope.SubscriptionVersion < 1 || envelope.ActiveSinceVersion < 1 || envelope.ActiveSinceVersion > envelope.SubscriptionVersion || envelope.UserID == uuid.Nil || envelope.EventID == uuid.Nil ||
		strings.TrimSpace(envelope.EventTitle) == "" || len(envelope.EventTitle) > 500 || envelope.EventDate.IsZero() {
		return nil, ErrInvalidEnvelope
	}
	if envelope.Action != ActionTrigger && envelope.Action != ActionWithdrawn && envelope.Action != ActionConsentUpdated && envelope.Action != ActionResubscribed {
		return nil, ErrInvalidEnvelope
	}
	if envelope.Type != TypeOnSale && envelope.Type != TypeRestock {
		return nil, ErrInvalidEnvelope
	}
	if envelope.EmailOptIn {
		if envelope.EmailConsentVersion != ConsentVersion || envelope.EmailConsentID == nil || *envelope.EmailConsentID == uuid.Nil {
			return nil, ErrInvalidEnvelope
		}
	} else if envelope.EmailConsentVersion != "" || envelope.EmailConsentID != nil {
		return nil, ErrInvalidEnvelope
	}
	if envelope.Type == TypeOnSale && envelope.Action == ActionTrigger && envelope.EmailOptIn && envelope.EmailConsentVersion != ConsentVersion {
		return nil, ErrInvalidEnvelope
	}
	return &envelope, nil
}
