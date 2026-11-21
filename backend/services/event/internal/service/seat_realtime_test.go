package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
)

func TestSeatRealtimePublisherPayloadIsSanitized(t *testing.T) {
	eventID := uuid.New()
	seat := changedSeat(eventID, "reserved", 17)
	seat.BookingID = uuidPtr(uuid.New())
	seat.ReservedByBookingID = uuidPtr(uuid.New())
	seat.OrderID = uuidPtr(uuid.New())

	batch := buildSeatUpdateBatch(eventID, []*domain.Seat{seat}, time.Unix(10, 0))
	encoded, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"booking_id", "reserved_by_booking_id", "order_id", "user_id", "email", "payment", "saga"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("public realtime payload contains forbidden field %q: %s", forbidden, text)
		}
	}
	var decoded struct {
		EventID     string `json:"event_id"`
		MessageID   string `json:"message_id"`
		SeatUpdates []struct {
			SeatID       string `json:"seat_id"`
			Status       string `json:"status"`
			SeatRevision string `json:"seat_revision"`
		} `json:"seat_updates"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EventID != eventID.String() || decoded.MessageID == "" || len(decoded.SeatUpdates) != 1 {
		t.Fatalf("batch identity = %+v", decoded)
	}
	update := decoded.SeatUpdates[0]
	if update.SeatID != seat.ID.String() || update.Status != "reserved" || update.SeatRevision != "17" {
		t.Fatalf("seat update = %+v", update)
	}
}

func TestReservationPublishesCommittedChangedRowsOnce(t *testing.T) {
	eventID := uuid.New()
	seat := changedSeat(eventID, "reserved", 2)
	repo := &seatRepositoryStub{reservationResult: repository.SeatMutationResult{
		Success: true, ChangedSeats: []*domain.Seat{seat},
	}}
	publisher := &seatRealtimePublisherStub{}
	svc := NewEventServiceWithAdmissionAndRealtimePublisher(nil, nil, repo, zap.NewNop(), nil, admission.ModePostgresOnly, publisher)

	outcome, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID: eventID, SeatIds: []uuid.UUID{seat.ID}, ReservedByBookingId: uuid.New(), Action: domain.RESERVED_SEAT,
	})
	if err != nil || !outcome.Success {
		t.Fatalf("reserve = %+v, %v", outcome, err)
	}
	if publisher.calls != 1 || len(publisher.batches[0].SeatUpdates) != 1 || publisher.batches[0].SeatUpdates[0].Status != "reserved" {
		t.Fatalf("published batches = %+v", publisher.batches)
	}
}

func TestRealtimePublishFailureDoesNotFailCommittedReservation(t *testing.T) {
	eventID := uuid.New()
	seat := changedSeat(eventID, "reserved", 2)
	repo := &seatRepositoryStub{reservationResult: repository.SeatMutationResult{
		Success: true, ChangedSeats: []*domain.Seat{seat},
	}}
	publisher := &seatRealtimePublisherStub{err: errors.New("redis publish failed")}
	svc := NewEventServiceWithAdmissionAndRealtimePublisher(nil, nil, repo, zap.NewNop(), nil, admission.ModePostgresOnly, publisher)

	outcome, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID: eventID, SeatIds: []uuid.UUID{seat.ID}, ReservedByBookingId: uuid.New(), Action: domain.RESERVED_SEAT,
	})
	if err != nil || !outcome.Success {
		t.Fatalf("committed reservation changed result after publish failure: outcome=%+v err=%v", outcome, err)
	}
}

func TestRealtimeNoOpDoesNotPublish(t *testing.T) {
	repo := &seatRepositoryStub{reservationResult: repository.SeatMutationResult{Success: true}}
	publisher := &seatRealtimePublisherStub{}
	svc := NewEventServiceWithAdmissionAndRealtimePublisher(nil, nil, repo, zap.NewNop(), nil, admission.ModePostgresOnly, publisher)

	_, err := svc.ReservedOrCompensateBatchSeats(context.Background(), &ReservedOrCompensateBatchSeatsReq{
		EventID: uuid.New(), SeatIds: []uuid.UUID{uuid.New()}, ReservedByBookingId: uuid.New(), Action: domain.RESERVED_SEAT,
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 0 {
		t.Fatalf("publish calls=%d, want no publish for no-op result", publisher.calls)
	}
}

type seatRealtimePublisherStub struct {
	calls   int
	batches []SeatUpdateBatch
	err     error
}

func (p *seatRealtimePublisherStub) Publish(_ context.Context, batch SeatUpdateBatch) error {
	p.calls++
	p.batches = append(p.batches, batch)
	return p.err
}

func changedSeat(eventID uuid.UUID, status string, revision int64) *domain.Seat {
	return &domain.Seat{
		ID:           uuid.New(),
		EventID:      eventID,
		TicketTierID: uuid.New(),
		Status:       domain.SeatStatus(status),
		SeatRevision: revision,
		CreatedAt:    time.Unix(1, 0).UTC(),
		UpdatedAt:    time.Unix(2, 0).UTC(),
	}
}

func uuidPtr(id uuid.UUID) *uuid.UUID { return &id }
