package service

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
)

func (s *BookingService) GetPaidRecoveryTicketSnapshot(ctx context.Context, bookingID uuid.UUID, generation int64) (repository.PaidRecoveryTicketSnapshot, error) {
	if s == nil || s.bookingRepo == nil {
		return repository.PaidRecoveryTicketSnapshot{}, errors.New("paid recovery ticket snapshot authority unavailable")
	}
	if bookingID == uuid.Nil || generation != 1 {
		return repository.PaidRecoveryTicketSnapshot{}, repository.ErrInvalidLifecycleDecision
	}
	reader, ok := s.bookingRepo.(repository.PaidRecoveryTicketSnapshotReader)
	if !ok {
		return repository.PaidRecoveryTicketSnapshot{}, errors.New("paid recovery ticket snapshot reader unavailable")
	}
	return reader.ReadPaidRecoveryTicketSnapshot(ctx, bookingID, generation)
}
