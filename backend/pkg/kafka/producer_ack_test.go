package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func TestProducerAcknowledgmentPolicyCanRequireAllInSyncReplicas(t *testing.T) {
	clearReliabilityEnv(t)
	defaultProducer := NewProducer([]string{"127.0.0.1:1"}, []string{"ordinary.events"}, zap.NewNop())
	defer defaultProducer.Close()
	if got := defaultProducer.writers["ordinary.events"].RequiredAcks; got != kafkago.RequireOne {
		t.Fatalf("existing producer acknowledgment policy changed: got %v, want RequireOne", got)
	}

	durableProducer := NewProducerWithAllAcks([]string{"127.0.0.1:1"}, []string{"ticket.issuance.events"}, zap.NewNop())
	defer durableProducer.Close()
	if got := durableProducer.writers["ticket.issuance.events"].RequiredAcks; got != kafkago.RequireAll {
		t.Fatalf("Ticket outbox producer acknowledgment policy = %v, want RequireAll", got)
	}
}
