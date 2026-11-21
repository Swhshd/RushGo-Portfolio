package kafka

import (
	"context"
	"fmt"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/pkg/typed"
	"github.com/ticketbox/saga/internal/service"
	"go.uber.org/zap"
)

type SagaOrchestratorConsumer struct {
	service            *service.SagaService
	logger             *zap.Logger
	sagaOrchesConsumer *pkgkafka.Consumer
}

func NewSagaOrchestratorConsumer(brokers []string, service *service.SagaService, logger *zap.Logger) *SagaOrchestratorConsumer {
	sagaOrchestratorConsumer := &SagaOrchestratorConsumer{
		service: service,
		logger:  logger,
	}
	sagaOrchestratorConsumer.sagaOrchesConsumer = pkgkafka.NewConsumer(
		brokers,
		topics.TopicPaymentEvents,
		"saga-payment-group",
		sagaOrchestratorConsumer.ConsumerHandler,
		logger)
	return sagaOrchestratorConsumer
}

func (s *SagaOrchestratorConsumer) Start(ctx context.Context) error {
	errChan := make(chan error)
	go func() {
		errChan <- s.sagaOrchesConsumer.Start(ctx)
	}()
	return <-errChan
}

func (s *SagaOrchestratorConsumer) ConsumerHandler(ctx context.Context, event pkgkafka.Event) error {
	if event.Type != topics.TypePaymentSucceed && event.Type != topics.TypePaymentFail {
		return fmt.Errorf("%w: unknown payment event", pkgkafka.ErrPermanent)
	}
	if _, err := typed.PaymentIntentWakeID(event.Data); err != nil {
		return fmt.Errorf("%w: invalid payment wake", pkgkafka.ErrPermanent)
	}
	switch event.Type {
	case topics.TypePaymentSucceed:
		return s.service.HandleSagaAferPaymentSuccess(ctx, event.Data)
	case topics.TypePaymentFail:
		return s.service.HandleSagaAfterPaymentFailure(ctx, event.Data)
	default:
		return pkgkafka.ErrPermanent
	}
}
