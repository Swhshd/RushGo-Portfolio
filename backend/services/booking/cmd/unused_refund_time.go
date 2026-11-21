package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/pkg/refundtime"
)

type unusedRefundEventClient struct{ client eventv1.EventServiceClient }

func timeBindingProto(b refundtime.Binding) *eventv1.RefundTimeBinding {
	return &eventv1.RefundTimeBinding{AttemptId: b.AttemptID.String(), RequestId: b.RequestID.String(), BookingId: b.BookingID.String(), PaymentId: b.PaymentID.String(), EventId: b.EventID.String(), ApproverId: b.ApproverID.String(), Generation: b.Generation, Fingerprint: b.Fingerprint}
}
func timePermitResult(b refundtime.Binding, p *eventv1.RefundTimePermit) (refundtime.Permit, error) {
	if p == nil || p.Binding == nil {
		return refundtime.Permit{}, fmt.Errorf("Event time permit missing")
	}
	raw := p.Binding
	ids := []string{raw.AttemptId, raw.RequestId, raw.BookingId, raw.PaymentId, raw.EventId, raw.ApproverId}
	expected := []uuid.UUID{b.AttemptID, b.RequestID, b.BookingID, b.PaymentID, b.EventID, b.ApproverID}
	for i, id := range ids {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed != expected[i] {
			return refundtime.Permit{}, fmt.Errorf("Event time permit identity mismatch")
		}
	}
	if raw.Generation != b.Generation || raw.Fingerprint != b.Fingerprint {
		return refundtime.Permit{}, fmt.Errorf("Event time permit binding mismatch")
	}
	result := refundtime.Permit{Binding: b, State: p.State, Version: p.Version}
	if p.StartsAt != nil {
		if p.StartsAt.CheckValid() != nil {
			return result, fmt.Errorf("invalid Event time")
		}
		result.StartsAt = p.StartsAt.AsTime()
	}
	if p.QualifiedAt != nil {
		if p.QualifiedAt.CheckValid() != nil {
			return result, fmt.Errorf("invalid Event qualification time")
		}
		result.QualifiedAt = p.QualifiedAt.AsTime()
	}
	if result.State == "QUALIFIED" && !result.QualifiedFor(b) {
		return refundtime.Permit{}, fmt.Errorf("invalid Event qualification")
	}
	if result.State != "QUALIFIED" && result.State != "REJECTED" && result.State != "RELEASED" {
		return refundtime.Permit{}, fmt.Errorf("unknown Event permit state")
	}
	return result, nil
}
func (c unusedRefundEventClient) Acquire(ctx context.Context, b refundtime.Binding) (refundtime.Permit, error) {
	if c.client == nil || !b.Valid() {
		return refundtime.Permit{}, fmt.Errorf("Event time authority unavailable")
	}
	p, err := c.client.AcquireUnusedRefundTimePermit(ctx, timeBindingProto(b))
	if err != nil {
		return refundtime.Permit{}, err
	}
	return timePermitResult(b, p)
}
func (c unusedRefundEventClient) Release(ctx context.Context, b refundtime.Binding, outcome string) (refundtime.Permit, error) {
	if c.client == nil || !b.Valid() {
		return refundtime.Permit{}, fmt.Errorf("Event time authority unavailable")
	}
	if outcome != "APPROVED" && outcome != "REJECTED" && outcome != "ABORTED" {
		return refundtime.Permit{}, fmt.Errorf("invalid terminal outcome")
	}
	p, err := c.client.ReleaseUnusedRefundTimePermit(ctx, &eventv1.ReleaseUnusedRefundTimePermitRequest{Binding: timeBindingProto(b), Outcome: outcome})
	if err != nil {
		return refundtime.Permit{}, err
	}
	result, err := timePermitResult(b, p)
	if err == nil && result.State != "RELEASED" {
		return refundtime.Permit{}, fmt.Errorf("Event permit not released")
	}
	return result, err
}
