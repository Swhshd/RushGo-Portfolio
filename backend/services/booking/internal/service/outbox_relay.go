package service

import (
	"context"
	"time"

	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type OutboxRelayPublisher interface {
	PublishOutboxEvent(context.Context, repository.OutboxEvent) error
}

type OutboxRelay struct {
	repo      repository.BookingRepository
	publisher OutboxRelayPublisher
	logger    *zap.Logger
	metrics   *observability.Metrics
}

func NewOutboxRelay(repo repository.BookingRepository, publisher OutboxRelayPublisher, logger *zap.Logger, metrics ...*observability.Metrics) *OutboxRelay {
	var domainMetrics *observability.Metrics
	if len(metrics) > 0 {
		domainMetrics = metrics[0]
	}
	return &OutboxRelay{repo: repo, publisher: publisher, logger: logger, metrics: domainMetrics}
}

func (r *OutboxRelay) RelayBatch(ctx context.Context, limit int) (int, error) {
	ctx, relaySpan := otel.Tracer("github.com/ticketbox/booking").Start(ctx, "outbox.relay")
	defer relaySpan.End()
	count, err := r.repo.ProcessPendingOutbox(ctx, limit, func(ctx context.Context, event repository.OutboxEvent) error {
		publishCtx, publishSpan := otel.Tracer("github.com/ticketbox/booking").Start(ctx, "outbox.publish")
		publishSpan.SetAttributes(attribute.String("outbox.event_id", event.ID.String()), attribute.String("event_type", event.EventType))
		publishErr := r.publisher.PublishOutboxEvent(publishCtx, event)
		if publishErr != nil {
			publishSpan.SetStatus(codes.Error, "outbox publish failed")
			if r.metrics != nil {
				r.metrics.OutboxPublish.Add(publishCtx, 1, metric.WithAttributes(attribute.String("event_type", event.EventType), attribute.String("result", "failed")))
				r.metrics.OutboxRetries.Add(publishCtx, 1, metric.WithAttributes(attribute.String("event_type", event.EventType)))
			}
		} else if r.metrics != nil {
			r.metrics.OutboxPublish.Add(publishCtx, 1, metric.WithAttributes(attribute.String("event_type", event.EventType), attribute.String("result", "success")))
			r.metrics.OutboxDeliveryDelay.Record(publishCtx, time.Since(event.CreatedAt).Seconds(), metric.WithAttributes(attribute.String("event_type", event.EventType)))
		}
		publishSpan.End()
		return publishErr
	})
	if err != nil {
		relaySpan.SetStatus(codes.Error, "outbox relay failed")
		if r.metrics != nil {
			r.metrics.OutboxRelayAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "failed")))
		}
	} else if r.metrics != nil {
		r.metrics.OutboxRelayAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "success")))
	}
	if counter, ok := r.repo.(repository.PendingOutboxCounter); ok && r.metrics != nil {
		if pending, countErr := counter.CountPendingOutbox(ctx); countErr == nil {
			r.metrics.OutboxPending.Record(ctx, pending)
		} else if r.logger != nil {
			r.logger.Warn("failed to observe outbox pending count", zap.Error(countErr))
		}
	}
	return count, err
}

func (r *OutboxRelay) Run(ctx context.Context, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := r.RelayBatch(ctx, batchSize)
			if err != nil {
				r.logger.Error("outbox relay failed", zap.Error(err))
				continue
			}
			if count > 0 {
				r.logger.Info("outbox events published", zap.Int("count", count))
			}
		}
	}
}
