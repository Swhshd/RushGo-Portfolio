package service

import (
	"context"
	"errors"
	"github.com/google/uuid"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"time"
)

var ErrEventNotOnSale = errors.New("event is not on sale")
var ErrEventSaleUnknown = errors.New("event sale authority unavailable")

func (s *BookingService) checkNewBookingSale(ctx context.Context, id uuid.UUID) error {
	if s.eventClient == nil {
		return ErrEventSaleUnknown
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.eventClient.CheckEventSaleEligibility(bounded, &eventv1.CheckEventSaleEligibilityRequest{EventId: id.String()})
	if err != nil || result == nil {
		return ErrEventSaleUnknown
	}
	switch result.State {
	case eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_ELIGIBLE:
		return nil
	case eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_DENIED:
		return ErrEventNotOnSale
	default:
		return ErrEventSaleUnknown
	}
}
