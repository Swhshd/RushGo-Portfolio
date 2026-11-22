package kafka

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	cronv3 "github.com/robfig/cron/v3"

	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
	service_scheduler "github.com/ticketbox/scheduler/internal/service/scheduler"
	"go.uber.org/zap"
)

type SchedulerConfigConsumer struct {
	service                      *service_scheduler.SchedulerService
	logger                       *zap.Logger
	schedulerConfigEventConsumer *pkgkafka.Consumer
}

func NewSchedulerConfigConsumer(broker []string, service *service_scheduler.SchedulerService, logger *zap.Logger) *SchedulerConfigConsumer {
	result := &SchedulerConfigConsumer{
		service: service,
		logger:  logger,
	}
	result.schedulerConfigEventConsumer = pkgkafka.NewConsumer(
		broker,
		topics.TopicSchedulerConfigEvent,
		"scheduler-config-event-group",
		result.ConsumerHandler,
		logger,
	)
	return result
}

func (o *SchedulerConfigConsumer) ConsumerHandler(ctx context.Context, event pkgkafka.Event) error {
	switch event.Type {
	case topics.TypeSchedulerConfigChanged:
		var payload domain.SchedulerConfig
		err := json.Unmarshal(event.Data, &payload)
		if err != nil || payload.Id == uuid.Nil || payload.Name == "" || payload.Version < 1 || payload.Timeout <= 0 {
			return pkgkafka.ErrPermanent
		}
		// Parse with the runtime's seconds field before applying the new
		// configuration version.
		parser := cronv3.NewParser(cronv3.Second | cronv3.Minute | cronv3.Hour | cronv3.Dom | cronv3.Month | cronv3.Dow | cronv3.Descriptor)
		if _, err := parser.Parse(payload.IntervalExpression); err != nil {
			return pkgkafka.ErrPermanent
		}
		o.logger.Info("[SchedulerConfigConsumer][SchedulerConfigChanged] Received scheduler config changed event", zap.String("job-name", payload.Name), zap.Duration("timeout", payload.Timeout))
		return o.service.HandleSchedulerConfigChanged(ctx, payload)
	default:
		return pkgkafka.ErrPermanent
	}
}

func (o *SchedulerConfigConsumer) Start(ctx context.Context) error {
	return o.schedulerConfigEventConsumer.Start(ctx)
}
