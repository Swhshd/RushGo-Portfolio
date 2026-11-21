package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type BookingEventProducer struct {
	producer *pkgkafka.Producer
	logger   *zap.Logger
}

func NewBookingEventProducer(brokers []string, logger *zap.Logger) *BookingEventProducer {
	producer := pkgkafka.NewProducer(brokers, []string{topics.TopicBookingEvents}, logger)
	return &BookingEventProducer{producer: producer, logger: logger}
}

func (p *BookingEventProducer) PublishBookingConfirmed(ctx context.Context, booking *domain.Booking, email string) error {
	data, err := json.Marshal(map[string]interface{}{
		"booking_id": booking.ID.String(),
		"user_id":    booking.UserID.String(),
		"event_id":   booking.EventID.String(),
		"email":      email,
		"total":      booking.TotalAmountCents,
	})
	if err != nil {
		return err
	}

	event := pkgkafka.Event{
		Type:      topics.TypeBookingConfirmed,
		Timestamp: time.Now(),
		Data:      data,
	}
	return p.producer.Publish(ctx, topics.TopicBookingEvents, booking.ID.String(), event)
}

func (p *BookingEventProducer) PublishOutboxEvent(ctx context.Context, event repository.OutboxEvent) error {
	key := event.EventKey
	if event.EventType == topics.TypeBookingFailed || event.EventType == topics.TypeBookingCancelled {
		key = event.ID.String()
	}
	return p.producer.Publish(ctx, topics.TopicBookingEvents, key, pkgkafka.Event{
		ID:        event.ID.String(),
		Type:      event.EventType,
		Timestamp: event.CreatedAt,
		Data:      event.Payload,
	})
}

func (p *BookingEventProducer) Close() error {
	return p.producer.Close()
}
