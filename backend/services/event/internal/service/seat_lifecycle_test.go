package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"go.uber.org/zap"
)

func TestLifecycleAdmissionAndCommittedRealtime(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		redisErr, pgErr          error
		wantCalls, wantPublishes int
	}{
		{"redis_conflict", admission.ErrConflict, nil, 0, 0},
		{"redis_unavailable", errors.New("unavailable"), nil, 1, 1},
		{"PG_rejected", nil, repository.ErrLifecycleClosed, 1, 0},
		{"committed", nil, nil, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := uuid.New()
			seat := changedSeat(event, "reserved", 2)
			repo := &seatRepositoryStub{reservationErr: tc.pgErr, lifecycleResult: &domain.SeatLifecycleResult{State: "held", ChangedSeats: []*domain.Seat{seat}}}
			gate := &admissionStub{acquireErr: tc.redisErr}
			publisher := &seatRealtimePublisherStub{}
			s := NewEventServiceWithAdmissionAndRealtimePublisher(nil, nil, repo, zap.NewNop(), gate, admission.ModeRedisAdmission, publisher)
			_, err := s.ApplyLifecycle(context.Background(), domain.SeatLifecycleRequest{BookingID: uuid.New(), EventID: event, Generation: 1, Action: "reserve", SeatIDs: []uuid.UUID{seat.ID}, ReservationDeadline: time.Now().Add(time.Minute)})
			if tc.redisErr == admission.ErrConflict || tc.pgErr != nil {
				if err == nil {
					t.Fatal("rejection reported success")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if repo.lifecycleCalls != tc.wantCalls || publisher.calls != tc.wantPublishes {
				t.Fatalf("PG calls=%d publishes=%d", repo.lifecycleCalls, publisher.calls)
			}
		})
	}
}
