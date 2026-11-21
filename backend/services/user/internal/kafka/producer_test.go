package kafka

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/ticketbox/user/internal/domain"
	"testing"
)

func TestPrivacyUserRegisteredPayloadOnlyBusinessFields(t *testing.T) {
	raw := userRegisteredData(&domain.User{ID: uuid.New(), Email: "synthetic@example.com", Name: "M6", PasswordHash: "synthetic-secret-canary", EmailCanonical: "synthetic@example.com"})
	var fields map[string]interface{}
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 3 || fields["user_id"] == nil || fields["email"] == nil || fields["name"] == nil {
		t.Fatal("business event credential boundary changed")
	}
}
