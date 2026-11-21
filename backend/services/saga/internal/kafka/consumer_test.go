package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
)

func TestSagaConsumerRejectsUnknownAndMalformedBeforeWake(t *testing.T) {
	c := &SagaOrchestratorConsumer{}
	for _, typ := range []string{"future", topics.TypePaymentSucceed, topics.TypePaymentFail} {
		for _, data := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"Data":null}`), json.RawMessage(`{"Data":{"object":{"id":"not_intent"}}}`)} {
			if err := c.ConsumerHandler(context.Background(), pkgkafka.Event{Type: typ, Data: data}); !errors.Is(err, pkgkafka.ErrPermanent) {
				t.Fatalf("schema accepted type=%s data=%s err=%v", typ, data, err)
			}
		}
	}
}
