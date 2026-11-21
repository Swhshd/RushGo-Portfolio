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

type refundTimeEventUpdateRepository struct {
	repository.EventRepository
	event                domain.Event
	metadataUpdateCount  int
	dateUpdateCount      int
	dateUpdateExpected   int64
	conflictBeforeUpdate bool
}

func (r *refundTimeEventUpdateRepository) GetByID(context.Context, uuid.UUID) (*domain.Event, error) {
	event := r.event
	return &event, nil
}

func (r *refundTimeEventUpdateRepository) Update(_ context.Context, event *domain.Event) error {
	r.metadataUpdateCount++
	if r.conflictBeforeUpdate {
		r.event.Date = r.event.Date.Add(24 * time.Hour)
		r.event.DateVersion++
		return repository.ErrConflict
	}
	r.event.Title = event.Title
	r.event.Description = event.Description
	r.event.Category = event.Category
	r.event.Venue = event.Venue
	r.event.Location = event.Location
	r.event.ImageURL = event.ImageURL
	return nil
}

func (r *refundTimeEventUpdateRepository) UpdateDate(_ context.Context, event *domain.Event, expectedVersion int64) error {
	r.dateUpdateCount++
	r.dateUpdateExpected = expectedVersion
	if expectedVersion != r.event.DateVersion {
		return repository.ErrConflict
	}
	r.event.Date = event.Date
	r.event.DateVersion++
	return nil
}

func TestRefundTimeMetadataUpdateDoesNotWriteDate(t *testing.T) {
	date := time.Date(2031, 4, 5, 12, 30, 0, 0, time.UTC)
	repo := &refundTimeEventUpdateRepository{event: domain.Event{ID: uuid.New(), Title: "before", Date: date, DateVersion: 4}}
	svc := NewEventService(repo, nil, nil, zap.NewNop())

	updated, err := svc.UpdateEventVersioned(context.Background(), repo.event.ID, "after", "", "", "", "", "", time.Time{}, 0)
	if err != nil {
		t.Fatalf("metadata update: %v", err)
	}
	if repo.metadataUpdateCount != 1 || repo.dateUpdateCount != 0 {
		t.Fatalf("metadata update called metadata=%d date=%d, want metadata=1 date=0", repo.metadataUpdateCount, repo.dateUpdateCount)
	}
	if !updated.Date.Equal(date) || updated.DateVersion != 4 {
		t.Fatalf("metadata update changed date/version: got %s/%d, want %s/4", updated.Date, updated.DateVersion, date)
	}
}

func TestRefundTimeDateUpdateRequiresExpectedVersion(t *testing.T) {
	date := time.Date(2031, 4, 5, 12, 30, 0, 0, time.UTC)
	repo := &refundTimeEventUpdateRepository{event: domain.Event{ID: uuid.New(), Date: date, DateVersion: 4}}
	svc := NewEventService(repo, nil, nil, zap.NewNop())

	_, err := svc.UpdateEventVersioned(context.Background(), repo.event.ID, "", "", "", "", "", "", date.Add(24*time.Hour), 0)
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("date change without expected version returned %v, want ErrConflict", err)
	}
	if repo.metadataUpdateCount != 0 || repo.dateUpdateCount != 0 {
		t.Fatalf("unversioned date change wrote metadata=%d date=%d times", repo.metadataUpdateCount, repo.dateUpdateCount)
	}
}

func TestRefundTimeDateUpdateUsesExpectedVersion(t *testing.T) {
	date := time.Date(2031, 4, 5, 12, 30, 0, 0, time.UTC)
	repo := &refundTimeEventUpdateRepository{event: domain.Event{ID: uuid.New(), Date: date, DateVersion: 4}}
	svc := NewEventService(repo, nil, nil, zap.NewNop())

	updated, err := svc.UpdateEventVersioned(context.Background(), repo.event.ID, "", "", "", "", "", "", date.Add(24*time.Hour), 4)
	if err != nil {
		t.Fatalf("versioned date update: %v", err)
	}
	if repo.dateUpdateCount != 1 || repo.dateUpdateExpected != 4 || repo.metadataUpdateCount != 0 {
		t.Fatalf("date update calls metadata=%d date=%d expected=%d, want 0/1/4", repo.metadataUpdateCount, repo.dateUpdateCount, repo.dateUpdateExpected)
	}
	if !updated.Date.Equal(date.Add(24*time.Hour)) || updated.DateVersion != 5 {
		t.Fatalf("date update returned date/version %s/%d, want %s/5", updated.Date, updated.DateVersion, date.Add(24*time.Hour))
	}
}

func TestRefundTimeMetadataUpdateReturnsDateVersionConflict(t *testing.T) {
	date := time.Date(2031, 4, 5, 12, 30, 0, 0, time.UTC)
	repo := &refundTimeEventUpdateRepository{event: domain.Event{ID: uuid.New(), Date: date, DateVersion: 4}, conflictBeforeUpdate: true}
	svc := NewEventService(repo, nil, nil, zap.NewNop())

	_, err := svc.UpdateEventVersioned(context.Background(), repo.event.ID, "after", "", "", "", "", "", time.Time{}, 0)
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("metadata update race returned %v, want ErrConflict", err)
	}
}
