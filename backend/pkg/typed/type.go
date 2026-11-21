package typed

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/stripe/stripe-go/v84"
)

type PaymentEvent struct {
	Id      string
	Data    *stripe.EventData
	Request *stripe.EventRequest
}

// PaymentIntentWakeID validates correlation only; it creates no money fact.
func PaymentIntentWakeID(raw json.RawMessage) (string, error) {
	var event PaymentEvent
	if len(raw) == 0 || json.Unmarshal(raw, &event) != nil || event.Data == nil {
		return "", errors.New("invalid payment wake")
	}
	object := event.Data.Raw
	if len(object) == 0 && event.Data.Object != nil {
		var err error
		object, err = json.Marshal(event.Data.Object)
		if err != nil {
			return "", err
		}
	}
	var normalized struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	if json.Unmarshal(object, &normalized) != nil || !strings.HasPrefix(normalized.ID, "pi_") || len(normalized.ID) < 4 || len(normalized.ID) > 255 || (normalized.Object != "" && normalized.Object != "payment_intent") {
		return "", errors.New("invalid payment wake")
	}
	return normalized.ID, nil
}

// VerifiedPaymentEvent carries only the provider adapter's normalized allowlist.
// Consumers use this as a wakeup and reload trusted Payment facts.
func VerifiedPaymentEvent(eventID string, normalized json.RawMessage) PaymentEvent {
	return PaymentEvent{Id: eventID, Data: &stripe.EventData{Raw: append(json.RawMessage(nil), normalized...)}}
}
