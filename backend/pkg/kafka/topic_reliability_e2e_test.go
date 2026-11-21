//go:build reliability_e2e

package kafka

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

func TestAcceptanceModeUsesTheSameCampaignTopicForConsumerAndProducer(t *testing.T) {
	dir := t.TempDir()
	suffix := "0123456789abcdef01234567"
	t.Setenv("RELIABILITY_ACCEPTANCE_MODE", "true")
	t.Setenv("RELIABILITY_FAULT_TOKEN", "t23-private-token-0123456789")
	t.Setenv("RELIABILITY_CONTROL_DIR", dir)
	t.Setenv("RELIABILITY_KAFKA_TOPIC_SUFFIX", suffix)
	consumer := NewConsumer([]string{"127.0.0.1:1"}, "booking.events",
		"notification-booking-group", func(context.Context, Event) error { return nil }, zap.NewNop())
	producer := NewProducer([]string{"127.0.0.1:1"}, []string{"booking.events"}, zap.NewNop())
	if consumer.reliabilityErr != nil || producer.configErr != nil {
		t.Fatalf("tagged acceptance Kafka clients were not enabled: consumer=%v producer=%v", consumer.reliabilityErr, producer.configErr)
	}
	want := "booking.events.t23-" + suffix
	if consumer.topic != want {
		t.Fatalf("consumer campaign topic = %q, want %q", consumer.topic, want)
	}
	if got := producer.writers["booking.events"].Topic; got != want {
		t.Fatalf("producer campaign topic = %q, want %q", got, want)
	}
}
