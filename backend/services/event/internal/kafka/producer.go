package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/pkg/eventreminder"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type EventEventProducer struct {
	producer *pkgkafka.Producer
	logger   *zap.Logger
}

type EventReminderProducer struct {
	producer *pkgkafka.Producer
}

func NewEventReminderProducer(brokers []string, logger *zap.Logger) *EventReminderProducer {
	return &EventReminderProducer{producer: pkgkafka.NewProducerWithAllAcks(brokers, []string{topics.TopicEventEvents}, logger)}
}

func (p *EventReminderProducer) Publish(ctx context.Context, reminder repository.ReminderOutboxEvent) error {
	return p.producer.Publish(ctx, topics.TopicEventEvents, reminder.SubscriptionID.String(), pkgkafka.Event{
		ID: reminder.ID.String(), Type: eventreminder.EnvelopeType, Timestamp: time.Now().UTC(), Data: reminder.Payload,
	})
}

func (p *EventReminderProducer) Close() error { return p.producer.Close() }

func NewEventEventProducer(brokers []string, logger *zap.Logger) *EventEventProducer {
	producer := pkgkafka.NewProducer(brokers, []string{topics.TopicEventEvents}, logger)
	return &EventEventProducer{producer: producer, logger: logger}
}

func (p *EventEventProducer) PublishEventCreated(ctx context.Context, event *domain.Event) error {
	data, err := json.Marshal(map[string]interface{}{
		"event_id": event.ID.String(),
		"title":    event.Title,
		"category": event.Category,
	})
	if err != nil {
		return err
	}

	evt := pkgkafka.Event{
		Type:      topics.TypeEventCreated,
		Timestamp: time.Now(),
		Data:      data,
	}

	return p.producer.Publish(ctx, topics.TopicEventEvents, event.ID.String(), evt)
}

func (p *EventEventProducer) Close() error {
	return p.producer.Close()
}
