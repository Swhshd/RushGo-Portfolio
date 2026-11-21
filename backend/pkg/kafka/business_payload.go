package kafka

import (
	"encoding/json"

	"github.com/google/uuid"
)

// RequireEntityPayload validates the known producer shape before any effect.
// Optional envelope/payload fields remain forwards compatible.
func RequireEntityPayload(raw json.RawMessage, field string) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return ErrPermanent
	}
	var value string
	if json.Unmarshal(fields[field], &value) != nil {
		return ErrPermanent
	}
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return ErrPermanent
	}
	for _, name := range []string{"email", "name", "reason"} {
		if rawValue, ok := fields[name]; ok {
			var text string
			if string(rawValue) == "null" || json.Unmarshal(rawValue, &text) != nil {
				return ErrPermanent
			}
		}
	}
	return nil
}
