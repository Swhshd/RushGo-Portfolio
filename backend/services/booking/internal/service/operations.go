package service

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
)

var ErrOperationsUnavailable = errors.New("operations source unavailable")

func (s *BookingService) ListOperationsBookings(ctx context.Context, q domain.OperationsQuery) ([]domain.OperationsBooking, bool, error) {
	repo, ok := s.bookingRepo.(repository.OperationsRepository)
	if !ok {
		return nil, false, ErrOperationsUnavailable
	}
	items, more, err := repo.ListOperationsBookings(ctx, q)
	if err != nil {
		return nil, false, err
	}
	slots := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i := range items {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			v := &items[i]
			now := time.Now().UTC()
			add := func(kind, source, state, next string, at time.Time) {
				v.Issues = append(v.Issues, domain.OperationIssue{Kind: kind, Source: source, State: state, ObservedAt: at, NextAction: next})
			}
			if v.IssuanceDenied {
				add("issuance_exception", "booking.ticket_issuance_receipts", "denied", "Inspect the issuance receipt and existing recovery diagnostics.", now)
			}
			if v.RefundStatus == "REFUND_PROCESSING" || v.RefundStatus == "APPROVED" || v.RefundStatus == "NEEDS_ATTENTION" {
				if v.RefundDecidedAt != nil && now.Sub(*v.RefundDecidedAt) > 15*time.Minute {
					add("refund_stale", "booking.booking_refund_requests", v.RefundStatus, "Open refund details and refund-explain; preserve the original decision.", now)
				}
			}
			if v.RefundStatus == "REFUNDED" && v.RevocationState != "revoked" {
				add("refunded_unrevoked", "booking.booking_refund_requests", v.RevocationState, "Inspect ticket revocation recovery; do not re-refund.", now)
			}
			if v.OutboxStale {
				add("outbox_stale", "booking.outbox", "unpublished", "Inspect outbox relay and Kafka availability.", now)
			}
			if v.AdmissionStale {
				add("admission_pending", "booking.ticket_admission_intents", "PENDING", "Use admission-fence-inspect; do not remove claims.", now)
			}
			v.PaymentSourceState = "unavailable"
			if s.checkoutPayment == nil || v.Generation != 1 {
				return
			}
			call, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			response, err := s.checkoutPayment.GetOperationsPayment(call, &paymentv1.FinancialPaymentRequest{BookingId: v.BookingID.String(), Generation: v.Generation})
			if status.Code(err) == codes.NotFound {
				v.PaymentSourceState = "not_recorded"
				return
			}
			if err != nil || response == nil || response.Payment == nil || response.Payment.BookingId != v.BookingID.String() || response.Payment.PaymentGeneration != v.Generation {
				return
			}
			p := response.Payment
			v.PaymentSourceState = "available"
			observed := response.ObservedAt.AsTime()
			if p.ProviderOutcome == "collected" && v.TicketCount < v.ExpectedTickets {
				add("paid_unissued", "payment.payments + booking.tickets", p.ProviderOutcome, "Inspect paid-recovery and issuance receipt; no manual issue or refund.", observed)
			}
			if p.ProviderOutcome == "unknown" || p.RefundState == "unknown" {
				add("provider_unknown", "payment.payments", p.ProviderOutcome+"/"+p.RefundState, "Use refund-explain and provider reconciliation evidence.", observed)
			}
			if response.RefundPreparedStale && p.RefundState != "succeeded" {
				add("refund_prepared_stale", "payment.payment_refund_audit", "PREPARED", "Inspect the original attempt; unknown transport must not be retried blindly.", observed)
			}
		}(i)
	}
	wg.Wait()
	return items, more, nil
}
func (s *BookingService) ListStaffWork(ctx context.Context, user uuid.UUID, admin bool, limit, offset int) (*bookingv1.StaffWorkList, error) {
	repo, ok := s.bookingRepo.(repository.OperationsRepository)
	if !ok {
		return nil, ErrOperationsUnavailable
	}
	rows, more, err := repo.ListStaffWork(ctx, user, admin, limit, offset)
	if err != nil {
		return nil, err
	}
	result := &bookingv1.StaffWorkList{HasMore: more, ObservedAt: timestamppb.New(time.Now().UTC())}
	for _, r := range rows {
		v := &bookingv1.StaffWorkItem{EventId: r.EventID.String(), Access: r.Access, PendingRefunds: r.PendingRefunds, EventSourceState: "unavailable"}
		if s.eventClient != nil {
			call, cancel := context.WithTimeout(ctx, time.Second)
			event, err := s.eventClient.GetAdminEvent(call, &eventv1.GetEventRequest{EventId: r.EventID.String()})
			cancel()
			if err == nil && event != nil && event.Id == r.EventID.String() {
				v.Title = event.Title
				v.StartsAt = event.Date
				v.EventState = event.SalesStatus
				v.EventSourceState = "available"
			}
		}
		result.Items = append(result.Items, v)
	}
	return result, nil
}
