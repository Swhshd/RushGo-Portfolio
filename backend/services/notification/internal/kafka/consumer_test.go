package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
)

func TestNotificationConsumerRejectsUnknownAndMalformedBeforeEffects(t *testing.T) {
	c := &NotificationConsumer{}
	for _, typ := range []string{"future", topics.TypeBookingConfirmed, topics.TypeBookingFailed, topics.TypeBookingCancelled} {
		for _, data := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"booking_id":null}`), json.RawMessage(`{"booking_id":"11111111-1111-4111-8111-111111111111","email":{}}`)} {
			if err := c.handleBookingEvent(context.Background(), pkgkafka.Event{Type: typ, Data: data}); !errors.Is(err, pkgkafka.ErrPermanent) {
				t.Fatalf("booking schema accepted type=%s data=%s err=%v", typ, data, err)
			}
		}
	}
	for _, typ := range []string{topics.TypeUserRegistered, "future"} {
		for _, data := range []json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{"user_id":"11111111-1111-4111-8111-111111111111","email":{}}`)} {
			if err := c.handleUserEvent(context.Background(), pkgkafka.Event{Type: typ, Data: data}); !errors.Is(err, pkgkafka.ErrPermanent) {
				t.Fatal("user schema accepted", typ, err)
			}
		}
	}
}
