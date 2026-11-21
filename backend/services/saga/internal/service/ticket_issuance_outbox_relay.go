package service

import (
	"context"
	"errors"
	"time"

	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
)

type ticketIssuanceOutboxPublisher interface {
	PublishTicketIssuanceRequested(context.Context, domain.TicketIssuanceOutboxEvent) error
}

type TicketIssuanceOutboxRelay struct {
	repo      repository.TicketIssuanceOutboxRepository
	publisher ticketIssuanceOutboxPublisher
	logger    *zap.Logger
}

func NewTicketIssuanceOutboxRelay(repo repository.TicketIssuanceOutboxRepository, publisher ticketIssuanceOutboxPublisher, logger *zap.Logger) *TicketIssuanceOutboxRelay {
	return &TicketIssuanceOutboxRelay{repo: repo, publisher: publisher, logger: logger}
}

func (r *TicketIssuanceOutboxRelay) RelayBatch(ctx context.Context, limit int) (int, error) {
	events, err := r.repo.ClaimTicketIssuanceOutbox(ctx, limit, 30*time.Second)
	if err != nil {
		return 0, err
	}
	for _, event := range events {
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		publishErr := r.publisher.PublishTicketIssuanceRequested(publishCtx, event)
		cancel()
		if publishErr != nil {
			retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, retryErr := r.repo.RetryTicketIssuanceOutbox(retryCtx, event.ID, event.ClaimToken, time.Now().Add(ticketOutboxBackoff(event.Attempts)))
			retryCancel()
			if retryErr != nil {
				return 0, errors.Join(publishErr, retryErr)
			}
			if r.logger != nil {
				r.logger.Warn("ticket issuance event publish deferred", zap.String("event_id", event.ID.String()))
			}
			continue
		}
		ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
		completed, ackErr := r.repo.CompleteTicketIssuanceOutbox(ackCtx, event.ID, event.ClaimToken)
		ackCancel()
		if ackErr != nil || !completed {
			if ackErr == nil {
				ackErr = repository.ErrRecoveryConflict
			}
			return 0, ackErr
		}
	}
	return len(events), nil
}

func ticketOutboxBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := min(attempt-1, 6)
	return min(time.Minute, time.Second*time.Duration(1<<shift))
}

func (r *TicketIssuanceOutboxRelay) Run(ctx context.Context, interval time.Duration, batchSize int) {
	if interval <= 0 {
		interval = time.Second
	}
	if batchSize < 1 || batchSize > 100 {
		batchSize = 25
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RelayBatch(ctx, batchSize); err != nil && r.logger != nil {
				r.logger.Warn("ticket issuance outbox relay failed", zap.Error(err))
			}
		}
	}
}
