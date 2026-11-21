package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrAccountingNotReady means the authoritative tier evidence is not ready
	// for a new sale or a new PaymentIntent.
	ErrAccountingNotReady = errors.New("accounting_not_ready")
	// ErrAccountingUnavailable means the authority could not be consulted. It
	// is deliberately distinct from "not ready" so callers can retry safely.
	ErrAccountingUnavailable = errors.New("accounting_readiness_unavailable")
)

type AccountingReadinessChecker interface {
	Check(context.Context, []uuid.UUID) error
}

type eventAccountingReadiness struct {
	client eventv1.EventServiceClient
}

func NewEventAccountingReadiness(client eventv1.EventServiceClient) AccountingReadinessChecker {
	return &eventAccountingReadiness{client: client}
}

func (c *eventAccountingReadiness) Check(ctx context.Context, tierIDs []uuid.UUID) error {
	if c == nil || c.client == nil || len(tierIDs) == 0 {
		return ErrAccountingUnavailable
	}
	seen := make(map[uuid.UUID]struct{}, len(tierIDs))
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, id := range tierIDs {
		if id == uuid.Nil {
			return ErrAccountingNotReady
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		res, err := c.client.GetTicketAvailability(checkCtx, &eventv1.GetTicketAvailabilityRequest{TierId: id.String()})
		if err != nil {
			switch status.Code(err) {
			case codes.NotFound, codes.InvalidArgument:
				return ErrAccountingNotReady
			default:
				return ErrAccountingUnavailable
			}
		}
		if res == nil || res.TierId != id.String() || !res.AccountingReady {
			return ErrAccountingNotReady
		}
	}
	return nil
}

func bookingTierIDs(view *bookingv1.LifecycleDisposition) ([]uuid.UUID, error) {
	if view == nil || view.Booking == nil || len(view.Booking.Items) == 0 {
		return nil, ErrAccountingNotReady
	}
	ids := make([]uuid.UUID, 0, len(view.Booking.Items))
	for _, item := range view.Booking.Items {
		if item == nil {
			return nil, ErrAccountingNotReady
		}
		id, err := uuid.Parse(item.TicketTierId)
		if err != nil || id == uuid.Nil {
			return nil, ErrAccountingNotReady
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func checkAccountingReadiness(ctx context.Context, checker AccountingReadinessChecker, view *bookingv1.LifecycleDisposition) error {
	if checker == nil {
		return ErrAccountingUnavailable
	}
	ids, err := bookingTierIDs(view)
	if err != nil {
		return err
	}
	return checker.Check(ctx, ids)
}
