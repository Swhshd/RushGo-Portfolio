package kafka

import (
	"context"

	"github.com/ticketbox/pkg/grpcsecurity"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/saga/internal/domain"
	"go.uber.org/zap"
)

type SagaOrchestratorProducer struct {
	logger                 *zap.Logger
	producer               *pkgkafka.Producer
	ticketIssuanceProducer *pkgkafka.Producer
	ticketIssuanceTopic    string
}

func NewSagaOrchestratorProducer(brokes []string, logger *zap.Logger) *SagaOrchestratorProducer {
	namespace, valid := grpcsecurity.TicketE2EKafkaIdentity()
	ticketTopic := ticketIssuanceTopic(topics.TopicTicketIssuanceEvents, namespace, valid)
	return &SagaOrchestratorProducer{
		logger:                 logger,
		ticketIssuanceTopic:    ticketTopic,
		producer:               pkgkafka.NewProducer(brokes, []string{topics.TopicOrderEvents, topics.TopicBookingEvents, topics.TopicNotificationEvents}, logger),
		ticketIssuanceProducer: pkgkafka.NewProducerWithAllAcks(brokes, []string{ticketTopic}, logger),
	}
}

func ticketIssuanceTopic(topic, namespace string, valid bool) string {
	if !valid || namespace == "" {
		return topic
	}
	return topic + "." + namespace
}

func (s *SagaOrchestratorProducer) PublishEvent(ctx context.Context, topic string, key string, event pkgkafka.Event) error {
	return s.producer.Publish(ctx, topic, key, event)
}

func (s *SagaOrchestratorProducer) PublishTicketIssuanceRequested(ctx context.Context, event domain.TicketIssuanceOutboxEvent) error {
	return s.ticketIssuanceProducer.Publish(ctx, s.ticketIssuanceTopic, event.EventKey, pkgkafka.Event{
		ID: event.ID.String(), Type: event.EventType, Timestamp: event.CreatedAt, Data: event.Payload,
	})
}

func (s *SagaOrchestratorProducer) Close() error {
	if err := s.producer.Close(); err != nil {
		return err
	}
	return s.ticketIssuanceProducer.Close()
}
