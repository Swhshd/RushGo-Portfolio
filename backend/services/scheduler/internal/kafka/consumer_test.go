package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

func TestSchedulerConsumerRejectsUnknownAndMalformedBeforeApply(t *testing.T) {
	c := &SchedulerConfigConsumer{logger: zap.NewNop()}
	for _, typ := range []string{"future", topics.TypeSchedulerConfigChanged} {
		for _, data := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"Id":null}`), json.RawMessage(`{"Id":"11111111-1111-4111-8111-111111111111","Name":"job","IsEnabled":true,"IntervalExpression":"invalid","Version":2,"Timeout":1000000000}`)} {
			if err := c.ConsumerHandler(context.Background(), pkgkafka.Event{Type: typ, Data: data}); !errors.Is(err, pkgkafka.ErrPermanent) {
				t.Fatalf("schema accepted type=%s data=%s err=%v", typ, data, err)
			}
		}
	}
}
