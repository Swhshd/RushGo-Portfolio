package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"time"
)

type unusedRefundPaymentClient struct {
	client paymentv1.PaymentServiceClient
}

func (a unusedRefundPaymentClient) GetUnusedRefundPaymentSnapshot(ctx context.Context, c domain.RefundApprovalCandidate) (domain.UnusedRefundPaymentSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := a.client.GetUnusedRefundPaymentSnapshot(ctx, &paymentv1.UnusedRefundPaymentSnapshotRequest{BookingId: c.BookingID.String(), PaymentId: c.PaymentID.String(), Generation: c.Generation, ForApproval: c.ForApproval})
	if err != nil {
		return domain.UnusedRefundPaymentSnapshot{}, err
	}
	if v == nil {
		return domain.UnusedRefundPaymentSnapshot{}, errors.New("missing Payment snapshot")
	}
	booking, err := uuid.Parse(v.BookingId)
	if err != nil {
		return domain.UnusedRefundPaymentSnapshot{}, err
	}
	payment, err := uuid.Parse(v.PaymentId)
	if err != nil {
		return domain.UnusedRefundPaymentSnapshot{}, err
	}
	buyer, err := uuid.Parse(v.BuyerId)
	if err != nil {
		return domain.UnusedRefundPaymentSnapshot{}, err
	}
	decision := uuid.Nil
	if v.RefundDecisionId != "" {
		decision, err = uuid.Parse(v.RefundDecisionId)
		if err != nil {
			return domain.UnusedRefundPaymentSnapshot{}, err
		}
	}
	return domain.UnusedRefundPaymentSnapshot{BookingID: booking, BuyerID: buyer, PaymentID: payment, PaymentIntentID: v.PaymentIntentId, ProviderAccount: v.ProviderAccount, Livemode: v.Livemode, Generation: v.Generation, AmountCents: v.AmountCents, Currency: v.Currency, RefundDecisionID: decision, RefundState: v.RefundState, RefundAmountCents: v.RefundAmountCents, RefundStatus: v.RefundStatus, Collected: v.Collected, EligibleForApproval: v.EligibleForApproval}, nil
}

// The existing relay owns retries and marks published only after this returns.
// Payment independently re-reads the immutable Booking decision; payload is a selector.
type unusedRefundPublisher struct {
	fallback service.OutboxRelayPublisher
	payment  paymentv1.PaymentServiceClient
}

func (p unusedRefundPublisher) PublishOutboxEvent(ctx context.Context, event repository.OutboxEvent) error {
	if event.EventType != domain.TypeUnusedTicketRefundApprovedV1 {
		return p.fallback.PublishOutboxEvent(ctx, event)
	}
	if p.payment == nil {
		return errors.New("secure refund delivery unavailable")
	}
	var payload struct {
		BookingID  string `json:"booking_id"`
		DecisionID string `json:"refund_decision_id"`
		PaymentID  string `json:"payment_id"`
		Generation int64  `json:"generation"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil {
		return errors.New("invalid refund outbox selector")
	}
	for _, id := range []string{payload.BookingID, payload.DecisionID, payload.PaymentID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil {
			return errors.New("invalid refund outbox identity")
		}
	}
	if payload.Generation != 1 || event.EventKey != payload.BookingID || event.ID.String() != payload.DecisionID {
		return errors.New("invalid refund generation")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := p.payment.AcceptApprovedUnusedRefundDecision(ctx, &paymentv1.AcceptApprovedUnusedRefundDecisionRequest{BookingId: payload.BookingID, RefundDecisionId: payload.DecisionID, PaymentId: payload.PaymentID, Generation: payload.Generation})
	if err != nil {
		return err
	}
	if result == nil || result.Id != payload.PaymentID || result.BookingId != payload.BookingID || result.PaymentGeneration != payload.Generation || result.RefundAmountCents <= 0 || result.RefundState == "" || result.RefundState == "not_required" {
		return errors.New("refund receipt identity mismatch")
	}
	return nil
}
