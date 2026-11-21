package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc"
	"testing"
)

type refundDeliveryClient struct {
	paymentv1.PaymentServiceClient
	calls    int
	response *paymentv1.PaymentEntry
	err      error
}

func (c *refundDeliveryClient) AcceptApprovedUnusedRefundDecision(_ context.Context, _ *paymentv1.AcceptApprovedUnusedRefundDecisionRequest, _ ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	c.calls++
	return c.response, c.err
}

type refundFallback struct{ calls int }

func (p *refundFallback) PublishOutboxEvent(context.Context, repository.OutboxEvent) error {
	p.calls++
	return nil
}
func TestUnusedRefundOutboxAcknowledgesOnlyAuthoritativeReceipt(t *testing.T) {
	booking, decision, payment := uuid.New(), uuid.New(), uuid.New()
	payload, _ := json.Marshal(map[string]any{"booking_id": booking.String(), "refund_decision_id": decision.String(), "payment_id": payment.String(), "generation": 1})
	event := repository.OutboxEvent{ID: decision, EventType: domain.TypeUnusedTicketRefundApprovedV1, EventKey: booking.String(), Payload: payload}
	client := &refundDeliveryClient{}
	fallback := &refundFallback{}
	publisher := unusedRefundPublisher{fallback: fallback, payment: client}
	if err := publisher.PublishOutboxEvent(context.Background(), event); err == nil {
		t.Fatal("empty receipt acknowledged")
	}
	client.err = errors.New("response lost")
	if err := publisher.PublishOutboxEvent(context.Background(), event); err == nil {
		t.Fatal("timeout acknowledged")
	}
	client.err = nil
	client.response = &paymentv1.PaymentEntry{Id: payment.String(), BookingId: booking.String(), PaymentGeneration: 1, RefundAmountCents: 100, RefundState: "pending"}
	if err := publisher.PublishOutboxEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if fallback.calls != 0 {
		t.Fatal("refund selector leaked to Kafka fallback")
	}
	if err := publisher.PublishOutboxEvent(context.Background(), repository.OutboxEvent{EventType: "BookingConfirmed"}); err != nil || fallback.calls != 1 {
		t.Fatal("existing outbox routing changed")
	}
	event.EventKey = uuid.NewString()
	before := client.calls
	if err := publisher.PublishOutboxEvent(context.Background(), event); err == nil || client.calls != before {
		t.Fatal("mismatched outbox binding sent")
	}
}
