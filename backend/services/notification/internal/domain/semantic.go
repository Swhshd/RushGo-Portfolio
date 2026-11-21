package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

type SemanticKey struct {
	Type       string
	EntityID   uuid.UUID
	Generation int64
	Channel    string
}

var ErrInvalidIntent = errors.New("invalid notification intent")

func NotificationID(key SemanticKey) uuid.UUID {
	prefix := "ticketbox:" + key.Type + ":"
	if key.Type == "welcome_email" {
		prefix = "ticketbox:user_welcome:"
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(prefix+key.EntityID.String()))
}

// PrepareIntent retains only immutable business fields. Envelope metadata never
// becomes a second semantic delivery, and unknown fields cannot affect its hash.
func PrepareIntent(typ string, raw []byte) (SemanticKey, *Notification, [32]byte, error) {
	var zero [32]byte
	var incoming map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&incoming) != nil || incoming == nil {
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	entityField := "booking_id"
	fields := []string{"booking_id", "user_id", "event_id"}
	switch typ {
	case "booking_confirmation":
		fields = append(fields, "total", "currency")
	case "booking_failure":
		fields = append(fields, "reason")
	case "booking_cancellation":
	case "welcome_email":
		entityField = "user_id"
		fields = []string{"user_id", "name"}
	default:
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	text, ok := incoming[entityField].(string)
	if !ok {
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	id, err := uuid.Parse(text)
	if err != nil || id == uuid.Nil {
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	payload := map[string]any{entityField: id.String()}
	for _, field := range fields {
		value, present := incoming[field]
		if !present || field == entityField {
			continue
		}
		if field == "total" {
			number, ok := value.(json.Number)
			if !ok {
				return SemanticKey{}, nil, zero, ErrInvalidIntent
			}
			total, err := number.Int64()
			if err != nil || total < 0 {
				return SemanticKey{}, nil, zero, ErrInvalidIntent
			}
			payload[field] = total
			continue
		}
		if field == "currency" {
			currency, ok := value.(string)
			if !ok || !validCurrency(currency) {
				return SemanticKey{}, nil, zero, ErrInvalidIntent
			}
			payload[field] = currency
			continue
		}
		text, ok := value.(string)
		if !ok {
			return SemanticKey{}, nil, zero, ErrInvalidIntent
		}
		if field == "user_id" || field == "event_id" {
			other, err := uuid.Parse(text)
			if err != nil || other == uuid.Nil {
				return SemanticKey{}, nil, zero, ErrInvalidIntent
			}
			text = other.String()
		}
		payload[field] = text
	}
	email, _ := incoming["email"].(string)
	if email == "" || len(email) > 255 {
		return SemanticKey{}, nil, zero, ErrInvalidIntent
	}
	key := SemanticKey{Type: typ, EntityID: id, Generation: 1, Channel: "email"}
	n := &Notification{ID: NotificationID(key), Type: typ, Recipient: email, Channel: key.Channel, Locale: "en", Payload: payload, Status: "PENDING", CreatedAt: time.Now(), Managed: true}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return SemanticKey{}, nil, zero, fmt.Errorf("%w: encoding", ErrInvalidIntent)
	}
	return key, n, sha256.Sum256(encoded), nil
}
