package service

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"testing"
)

type saleGateEvent struct {
	eventv1.EventServiceClient
	response *eventv1.CheckEventSaleEligibilityResponse
	err      error
	calls    int
}

func (e *saleGateEvent) CheckEventSaleEligibility(context.Context, *eventv1.CheckEventSaleEligibilityRequest, ...grpc.CallOption) (*eventv1.CheckEventSaleEligibilityResponse, error) {
	e.calls++
	return e.response, e.err
}

type saleReplayRepo struct {
	repository.BookingRepository
	existing *domain.LifecycleSnapshot
}

func (r *saleReplayRepo) GetByRequest(context.Context, domain.BookingRequestIdentity) (*domain.LifecycleSnapshot, error) {
	if r.existing != nil {
		return r.existing, nil
	}
	return nil, repository.ErrNotFound
}
func TestSaleGateRejectsBeforeBookingSideEffectsAndPreservesReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  eventv1.EventSaleEligibilityState
		err    error
		replay bool
		want   error
	}{
		{name: "paused", state: eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_DENIED, want: ErrEventNotOnSale},
		{name: "unknown", want: ErrEventSaleUnknown}, {name: "dependency unavailable", err: errors.New("offline"), want: ErrEventSaleUnknown},
		{name: "original terminal replay while paused", state: eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_DENIED, replay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &saleGateEvent{response: &eventv1.CheckEventSaleEligibilityResponse{State: tc.state}, err: tc.err}
			r := &saleReplayRepo{}
			id, buyer, event := uuid.New(), uuid.New(), uuid.New()
			if tc.replay {
				r.existing = &domain.LifecycleSnapshot{Booking: &domain.Booking{ID: id, UserID: buyer, EventID: event}, Phase: "TERMINAL"}
			}
			s := NewBookingService(r, e, nil, nil)
			result, err := s.CreateBooking(context.Background(), buyer, event, []domain.BookingItem{{TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}}, "pessimistic", "original-request")
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want %v", err, tc.want)
			}
			if tc.replay && (result.Booking.ID != id || e.calls != 0) {
				t.Fatal("replay entered new sale path")
			}
			// Any persistence or Saga call would panic: embedded implementations are nil.
		})
	}
}
