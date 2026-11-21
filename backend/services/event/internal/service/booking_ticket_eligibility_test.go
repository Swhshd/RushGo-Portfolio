package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/repository"
	"go.uber.org/zap"
)

type ticketEligibilityReaderStub struct {
	eligible   bool
	reason     string
	err        error
	booking    uuid.UUID
	event      uuid.UUID
	generation int64
	seats      []uuid.UUID
}

func (s *ticketEligibilityReaderStub) VerifyBookingTicketEligibility(_ context.Context, booking, event uuid.UUID, generation int64, seats []uuid.UUID) (bool, string, error) {
	s.booking, s.event, s.generation, s.seats = booking, event, generation, append([]uuid.UUID(nil), seats...)
	return s.eligible, s.reason, s.err
}

func TestEventServiceDelegatesTicketEligibilityToAuthoritativeReader(t *testing.T) {
	booking, event, seat := uuid.New(), uuid.New(), uuid.New()
	reader := &ticketEligibilityReaderStub{eligible: true, reason: "event_booking_seats_accounted"}
	seats := &seatRepositoryStub{}
	// Keep the ordinary SeatRepository surface while adding the optional,
	// stronger ticket-issuance read capability.
	combined := ticketEligibilitySeatRepository{seatRepositoryStub: seats, ticketEligibilityReaderStub: reader}
	svc := NewEventService(eventRepositoryStub{}, tierRepositoryStub{}, combined, zap.NewNop())
	eligible, reason, err := svc.VerifyBookingTicketEligibility(context.Background(), booking, event, 1, []uuid.UUID{seat})
	if err != nil || !eligible || reason != "event_booking_seats_accounted" || reader.booking != booking || reader.event != event || reader.generation != 1 || len(reader.seats) != 1 || reader.seats[0] != seat {
		t.Fatalf("ticket eligibility read mismatch: eligible=%v reason=%q err=%v reader=%+v", eligible, reason, err, reader)
	}
	reader.err = errors.New("read unavailable")
	if _, _, err := svc.VerifyBookingTicketEligibility(context.Background(), booking, event, 1, []uuid.UUID{seat}); err == nil {
		t.Fatal("Event service discarded read-authority error")
	}
}

type ticketEligibilitySeatRepository struct {
	*seatRepositoryStub
	*ticketEligibilityReaderStub
}

type eventRepositoryStub struct{ repository.EventRepository }
type tierRepositoryStub struct {
	repository.TicketTierRepository
}

var _ repository.SeatRepository = ticketEligibilitySeatRepository{}
