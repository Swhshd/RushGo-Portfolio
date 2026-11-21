package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

var ErrNotFound = errors.New("not found")

type OutboxEvent struct {
	ID        uuid.UUID
	EventType string
	EventKey  string
	Payload   json.RawMessage
	CreatedAt time.Time
}

type OutboxPublisher func(context.Context, OutboxEvent) error

type BookingRepository interface {
	LifecycleRepository
	Create(ctx context.Context, booking *domain.Booking) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Booking, error)
	ListByUserID(ctx context.Context, userID uuid.UUID, page, pageSize int) ([]*domain.Booking, int, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.BookingStatus) error
	CompareAndSetStatus(ctx context.Context, id uuid.UUID, expected, status domain.BookingStatus) (bool, error)
	ConfirmBookingWithOutbox(ctx context.Context, id uuid.UUID, event OutboxEvent) (bool, error)
	ProcessPendingOutbox(ctx context.Context, limit int, publish OutboxPublisher) (int, error)
	BatchUpdateStatus(ctx context.Context, ids []uuid.UUID, status domain.BookingStatus) error
}

// PendingOutboxCounter is an optional relay observation capability. Keeping it
// separate preserves existing repository test doubles and constructor APIs.
type PendingOutboxCounter interface {
	CountPendingOutbox(ctx context.Context) (int64, error)
}
