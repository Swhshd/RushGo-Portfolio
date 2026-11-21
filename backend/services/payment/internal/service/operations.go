package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

func (p *PaymentService) OperationsPayment(ctx context.Context, booking uuid.UUID, generation int64) (*domain.FinancialView, bool, error) {
	view, err := p.GetFinancialPayment(ctx, uuid.Nil, booking, generation)
	if err != nil {
		return nil, false, err
	}
	repo, ok := p.repo.(repository.OperationsRepository)
	if !ok || view == nil || view.Payment == nil {
		return nil, false, repository.ErrFinancialConflict
	}
	stale, err := repo.RefundPreparedStale(ctx, view.Payment.ID)
	return view, stale, err
}
