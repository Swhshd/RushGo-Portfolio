package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"go.uber.org/zap"
)

func TestValidateEventCreateRequiresPositiveBoundedPaidTierPrice(t *testing.T) {
	base := []domain.TicketTier{{Name: "General", PriceCents: 1, TotalQuantity: 1}}
	for _, tc := range []struct {
		name  string
		price int64
		valid bool
	}{
		{name: "minimum positive price", price: 1, valid: true},
		{name: "free tier is not supported", price: 0},
		{name: "negative price", price: -1},
		{name: "price exceeds configured bound", price: MaxTicketPriceCents + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tiers := append([]domain.TicketTier(nil), base...)
			tiers[0].PriceCents = tc.price
			err := validateEventCreate("Event", "Music", "Hall", "City", tiers,
				domain.PublicationDraft, domain.SalesScheduled, nil, nil)
			if tc.valid && err != nil {
				t.Fatalf("valid price rejected: %v", err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidEventRequest) {
				t.Fatalf("invalid price error = %v, want ErrInvalidEventRequest", err)
			}
		})
	}
}

func TestValidateEventCreateRejectsUnboundedSeatGenerationAndBadWindow(t *testing.T) {
	validTier := []domain.TicketTier{{Name: "General", PriceCents: 725, TotalQuantity: 1}}
	tooMany := []domain.TicketTier{{Name: "General", PriceCents: 725, TotalQuantity: MaxSeatsPerTicketTier + 1}}
	start := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	end := start
	if err := validateEventCreate("Event", "Music", "Hall", "City", tooMany,
		domain.PublicationDraft, domain.SalesScheduled, nil, nil); !errors.Is(err, ErrInvalidEventRequest) {
		t.Fatalf("unbounded quantity error = %v, want ErrInvalidEventRequest", err)
	}
	if err := validateEventCreate("Event", "Music", "Hall", "City", validTier,
		domain.PublicationDraft, domain.SalesScheduled, &start, &end); !errors.Is(err, ErrInvalidEventRequest) {
		t.Fatalf("invalid sales window error = %v, want ErrInvalidEventRequest", err)
	}
}

func TestCreateEventPassesCompleteInventoryThroughOneAtomicRepositoryCall(t *testing.T) {
	repo := &eventCreationRepositoryStub{}
	svc := NewEventService(repo, nil, nil, zap.NewNop())
	actorID := uuid.New()
	date := time.Date(2027, 1, 1, 20, 0, 0, 0, time.UTC)
	event, err := svc.CreateEventWithLifecycle(context.Background(), "Event", "Description", "Music", "Hall", "City", "", date,
		[]domain.TicketTier{{Name: "General", PriceCents: 725, TotalQuantity: 21}},
		domain.PublicationDraft, domain.SalesScheduled, nil, nil, actorID, "create-request-1")
	if err != nil {
		t.Fatalf("CreateEventWithLifecycle() error = %v", err)
	}
	if repo.calls != 1 || repo.event != event {
		t.Fatalf("atomic repository calls=%d event pointer retained=%v, want one call with returned event", repo.calls, repo.event == event)
	}
	if len(repo.event.Seats) != 21 {
		t.Fatalf("inventory passed to atomic create has %d seats, want 21", len(repo.event.Seats))
	}
	for _, seat := range repo.event.Seats {
		if seat.EventID != event.ID || seat.TicketTierID != event.Tiers[0].ID || seat.Status != domain.SeatStatusAvailable {
			t.Fatalf("generated seat does not belong to the new event/tier: %+v", seat)
		}
	}
	if repo.event.CreationRequest == nil || repo.event.CreationRequest.ActorID != actorID || repo.event.CreationRequest.RequestID != "create-request-1" || len(repo.event.CreationRequest.RequestHash) != 32 {
		t.Fatalf("creation idempotency identity = %+v", repo.event.CreationRequest)
	}
}

func TestCreateEventRejectsIdempotencyKeyWithoutTrustedActor(t *testing.T) {
	repo := &eventCreationRepositoryStub{}
	svc := NewEventService(repo, nil, nil, zap.NewNop())
	_, err := svc.CreateEventWithLifecycle(context.Background(), "Event", "", "Music", "Hall", "City", "", time.Now().Add(time.Hour),
		[]domain.TicketTier{{Name: "General", PriceCents: 725, TotalQuantity: 1}},
		domain.PublicationDraft, domain.SalesScheduled, nil, nil, uuid.Nil, "create-request-1")
	if !errors.Is(err, ErrInvalidEventRequest) {
		t.Fatalf("missing actor id error = %v, want ErrInvalidEventRequest", err)
	}
	if repo.calls != 0 {
		t.Fatalf("invalid request reached repository %d times, want 0", repo.calls)
	}
}

type eventCreationRepositoryStub struct {
	repository.EventRepository
	event *domain.Event
	calls int
	err   error
}

func (r *eventCreationRepositoryStub) Create(_ context.Context, event *domain.Event) error {
	r.calls++
	r.event = event
	return r.err
}
