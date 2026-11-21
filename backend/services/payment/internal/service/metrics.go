package service

import (
	"context"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"time"
)

func (p *PaymentService) MoneyMetrics(ctx context.Context, from, to time.Time) (*domain.MoneyMetrics, error) {
	repo, ok := p.repo.(repository.MetricsRepository)
	if !ok {
		return nil, repository.ErrFinancialConflict
	}
	return repo.MoneyMetrics(ctx, from, to)
}
