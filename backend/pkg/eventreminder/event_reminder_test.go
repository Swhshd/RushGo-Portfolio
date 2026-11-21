package eventreminder

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParseRejectsUntrustedOrIncompleteReminder(t *testing.T) {
	valid := Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 1,
		Action: ActionTrigger, Type: TypeOnSale, UserID: uuid.New(), EventID: uuid.New(),
		EventTitle: "Launch show", EventDate: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(raw); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}

	for name, mutate := range map[string]func(*Envelope){
		"missing identity":                 func(e *Envelope) { e.UserID = uuid.Nil },
		"negative version":                 func(e *Envelope) { e.SubscriptionVersion = 0 },
		"activation floor exceeds version": func(e *Envelope) { e.ActiveSinceVersion = 2 },
		"unknown type":                     func(e *Envelope) { e.Type = "welcome_email" },
		"unknown action":                   func(e *Envelope) { e.Action = "send_email" },
		"empty title":                      func(e *Envelope) { e.EventTitle = " " },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			data, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Parse(data); err == nil {
				t.Fatal("invalid reminder accepted")
			}
		})
	}

	legacyResubscribe := valid
	legacyResubscribe.SubscriptionVersion = 4
	legacyResubscribe.Action = ActionResubscribed
	legacyResubscribe.ActiveSinceVersion = 0
	legacyRaw, err := json.Marshal(legacyResubscribe)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(legacyRaw)
	if err != nil || parsed.ActiveSinceVersion != 4 {
		t.Fatalf("legacy resubscribe activation floor=%+v err=%v, want 4", parsed, err)
	}

	withEmail := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"email":"buyer@example.test"}`)...)
	if _, err := Parse(withEmail); err == nil {
		t.Fatal("email-bearing payload accepted")
	}
	if _, err := Parse(append(raw, []byte(` {}`)...)); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
