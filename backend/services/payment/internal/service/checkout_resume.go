package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

// ResumePayment only retrieves a previously persisted provider intent. A missing
// or uncertain intent is left to the existing dispatch/reconciliation workers.
func (p *PaymentService) ResumePayment(ctx context.Context, booking uuid.UUID, generation int64) (*domain.CreatePaymentResponse, error) {
	if p.provider == nil || p.booking == nil || p.liveMode || booking == uuid.Nil || generation != 1 {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	f, err := p.GetFinancialPayment(ctx, uuid.Nil, booking, generation)
	if err != nil {
		return nil, err
	}
	if f == nil || f.Payment == nil || !f.Managed || f.LiveMode == nil || *f.LiveMode || f.ProviderAccount != p.account {
		return nil, repository.ErrFinancialConflict
	}
	response := &domain.CreatePaymentResponse{PaymentID: f.Payment.ID, Status: string(f.Payment.Status), CreateState: "processing", RetryAfterSeconds: 1, Currency: f.Payment.Currency}
	if f.CreateState != "create_ready" || f.Payment.PaymentIntentId == "" || f.RefundState != "not_required" || f.ErrorCategory != "" {
		return response, nil
	}
	view, err := p.readEligibleBooking(ctx, booking, generation)
	if err != nil {
		return nil, err
	}
	if !bookingPayable(view) || view.Phase != "OPEN" {
		return response, nil
	}
	if view.Booking.UserId != f.Payment.UserId.String() || view.Booking.TotalAmountCents != int64(f.Payment.Price) || view.Currency != f.Payment.Currency {
		return nil, repository.ErrFinancialConflict
	}
	remote, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	observation, secret, err := p.provider.RetrieveIntent(remote, p.account, false, f.Payment.PaymentIntentId)
	if err != nil {
		return nil, err
	}
	if observation == nil || observation.PaymentID != f.Payment.ID || observation.BookingID != booking || observation.Generation != generation || observation.Account != p.account || observation.LiveMode || observation.IntentID != f.Payment.PaymentIntentId || observation.AmountCents != int64(f.Payment.Price) || observation.Currency != f.Payment.Currency {
		return nil, repository.ErrProviderEvidenceConflict
	}
	if err := p.repo.RecordObservation(ctx, *observation); err != nil {
		return nil, err
	}
	response.Status = observation.Status
	latest, err := p.readEligibleBooking(ctx, booking, generation)
	if err != nil {
		return nil, err
	}
	if !bookingPayable(latest) || latest.Phase != "OPEN" {
		return response, nil
	}
	switch observation.Status {
	case "requires_payment_method", "requires_confirmation", "requires_action":
		if !observation.Collected && observation.SucceededRefundCents == 0 && secret != "" {
			response.PaymentItentId = observation.IntentID
			response.PaymentIntentClientSecret = secret
			response.CreateState = "ready"
			response.RetryAfterSeconds = 0
		}
	}
	return response, nil
}
