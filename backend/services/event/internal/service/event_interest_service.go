package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
)

type EventInterestService struct {
	events *EventService
	repo   repository.EventInterestRepository
}

type EventInterestState struct {
	EventID         uuid.UUID
	Favorite        bool
	Subscriptions   []repository.EventInterest
	SaleStatus      string
	OnSaleAvailable bool
	RestockStatus   string
}

func NewEventInterestService(events *EventService, repo repository.EventInterestRepository) *EventInterestService {
	return &EventInterestService{events: events, repo: repo}
}

func (s *EventInterestService) List(ctx context.Context, userID uuid.UUID, kind string, limit, offset int32) (repository.EventInterestPage, error) {
	return s.repo.ListMyEventInterests(ctx, userID, kind, limit, offset)
}

func (s *EventInterestService) Get(ctx context.Context, userID, eventID uuid.UUID) (EventInterestState, error) {
	event, err := s.events.GetPublicEvent(ctx, eventID)
	if err != nil {
		return EventInterestState{}, err
	}
	items, err := s.repo.GetMyEventInterests(ctx, userID, eventID)
	if err != nil {
		return EventInterestState{}, err
	}
	status, err := s.repo.RestockCollectionStatus(ctx, eventID)
	if err != nil {
		return EventInterestState{}, err
	}
	now := time.Now().UTC()
	available, err := s.repo.CanCreateOnSaleInterest(ctx, eventID)
	if err != nil {
		return EventInterestState{}, err
	}
	result := EventInterestState{
		EventID: eventID, Subscriptions: make([]repository.EventInterest, 0, len(items)),
		SaleStatus: domain.EffectiveSalesStatus(event, now), OnSaleAvailable: available, RestockStatus: status,
	}
	for _, item := range items {
		if item.Kind == repository.InterestFavorite {
			result.Favorite = item.Status == "active"
			continue
		}
		result.Subscriptions = append(result.Subscriptions, item)
	}
	return result, nil
}

func (s *EventInterestService) Upsert(ctx context.Context, userID, eventID uuid.UUID, kind string, emailOptIn bool, consentVersion string) (repository.EventInterest, error) {
	return s.repo.UpsertMyEventInterest(ctx, userID, eventID, kind, emailOptIn, consentVersion)
}

func (s *EventInterestService) Withdraw(ctx context.Context, userID, eventID uuid.UUID, kind string) (repository.EventInterest, error) {
	return s.repo.WithdrawMyEventInterest(ctx, userID, eventID, kind)
}

func (s *EventInterestService) DisableAlertEmails(ctx context.Context, userID uuid.UUID, kind string) (int32, error) {
	return s.repo.DisableMyEventAlertEmails(ctx, userID, kind)
}

func (s *EventInterestService) Counts(ctx context.Context, eventID uuid.UUID) (repository.EventInterestCounts, error) {
	if _, err := s.events.GetEvent(ctx, eventID); err != nil {
		return repository.EventInterestCounts{}, err
	}
	return s.repo.ReadEventInterestCounts(ctx, eventID)
}
