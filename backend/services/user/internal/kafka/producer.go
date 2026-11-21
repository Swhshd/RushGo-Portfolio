package kafka

import (
	"context"
	"encoding/json"
	"time"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/user/internal/domain"
	"go.uber.org/zap"
)

type UserEventProducer struct {
	producer *pkgkafka.Producer
	logger   *zap.Logger
}

func userRegisteredData(user *domain.User) []byte {
	data, _ := json.Marshal(map[string]interface{}{"user_id": user.ID.String(), "email": user.Email, "name": user.Name})
	return data
}

func NewUserEventProducer(brokers []string, logger *zap.Logger) *UserEventProducer {
	producer := pkgkafka.NewProducer(brokers, []string{topics.TopicUserEvents}, logger)
	return &UserEventProducer{producer: producer, logger: logger}
}

func (p *UserEventProducer) PublishUserRegistered(ctx context.Context, user *domain.User) error {
	data := userRegisteredData(user)

	event := pkgkafka.Event{
		Type:      topics.TypeUserRegistered,
		Timestamp: time.Now(),
		Data:      data,
	}

	return p.producer.Publish(ctx, topics.TopicUserEvents, user.ID.String(), event)
}

func (p *UserEventProducer) Close() error {
	return p.producer.Close()
}
