package repository

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
)

var (
	ErrNotFound                 = errors.New("not found")
	ErrConflict                 = errors.New("version conflict")
	ErrInsufficientTickets      = errors.New("insufficient tickets")
	ErrSeatNotAvailable         = errors.New("seat not available")
	ErrActiveRefundTimePermit   = errors.New("active refund time permit")
	ErrInvalidRefundTimeBinding = errors.New("invalid refund time binding")
	ErrBindingMismatch          = errors.New("refund time binding mismatch")
	ErrInvalidRefundTimeOutcome = errors.New("invalid refund time outcome")
	ErrInvalidEventLifecycle    = errors.New("invalid event lifecycle")
	ErrSaleNotAllowed           = errors.New("event is not accepting new reservations")
	ErrIdempotencyConflict      = errors.New("event creation idempotency key conflict")
	ErrNotImplemented           = errors.New("operation is not implemented")
	ErrInvalidEventListQuery    = errors.New("invalid event list query")
)

const (
	DefaultEventListPageSize = 20
	MaxEventListPage         = 1000
	MaxEventListPageSize     = 50
	MaxEventFilterPriceCents = int64(2147483647)
)

type EventListFilter struct {
	Category        string
	Search          string
	DateFrom        *time.Time
	DateToExclusive *time.Time
	Location        string
	MinPriceCents   *int64
	MaxPriceCents   *int64
	Sort            string
}

type FilteredEventRepository interface {
	ListFiltered(context.Context, EventListFilter, int, int) ([]*domain.Event, int, error)
	ListAdminFiltered(context.Context, EventListFilter, int, int) ([]*domain.Event, int, error)
}

func NormalizeEventListQuery(filter EventListFilter, page, pageSize int) (EventListFilter, int, int, error) {
	if page == 0 {
		page = 1
	}
	if pageSize == 0 {
		pageSize = DefaultEventListPageSize
	}
	if page < 1 || page > MaxEventListPage || pageSize < 1 || pageSize > MaxEventListPageSize {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.Sort == "" {
		filter.Sort = "date_asc"
	}
	switch filter.Sort {
	case "date_asc", "date_desc", "price_asc", "price_desc":
	default:
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if len(filter.Category) > 100 || len(filter.Search) > 500 {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	filter.Location = strings.TrimSpace(filter.Location)
	if len(filter.Location) > 500 {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.DateFrom != nil && filter.DateFrom.IsZero() || filter.DateToExclusive != nil && filter.DateToExclusive.IsZero() {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.DateFrom != nil && filter.DateToExclusive != nil && !filter.DateFrom.Before(*filter.DateToExclusive) {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.MinPriceCents != nil && (*filter.MinPriceCents < 0 || *filter.MinPriceCents > MaxEventFilterPriceCents) {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.MaxPriceCents != nil && (*filter.MaxPriceCents < 0 || *filter.MaxPriceCents > MaxEventFilterPriceCents) {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	if filter.MinPriceCents != nil && filter.MaxPriceCents != nil && *filter.MinPriceCents > *filter.MaxPriceCents {
		return EventListFilter{}, 0, 0, ErrInvalidEventListQuery
	}
	return filter, page, pageSize, nil
}

type EventRepository interface {
	Create(ctx context.Context, event *domain.Event) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Event, error)
	List(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error)
	ListAdmin(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error)
	Update(ctx context.Context, event *domain.Event) error
	UpdateDate(ctx context.Context, event *domain.Event, expectedVersion int64) error
	UpdateSales(ctx context.Context, event *domain.Event) error
	Delete(ctx context.Context, id uuid.UUID) error
}

type UpdateTicketAvailabilityRequest struct {
	tierId        uuid.UUID
	deltaQuantity int
}
type UpdateBatchTicketAvailabilityRequest struct {
	Data []UpdateBatchTicketAvailabilityRequest
}

type TicketTierRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.TicketTier, error)
	GetByEventID(ctx context.Context, eventID uuid.UUID) ([]domain.TicketTier, error)
	AccountingReadiness(ctx context.Context, tierID uuid.UUID) (bool, error)
	AccountingReadinessForTiers(ctx context.Context, tierIDs []uuid.UUID) (map[uuid.UUID]bool, error)
	UpdateAvailability(ctx context.Context, tierID uuid.UUID, delta int32, expectedVersion int32) (*domain.TicketTier, error)
	UpdateAvailabilityNoLock(ctx context.Context, tierID uuid.UUID, delta int32) error
	UpdateAvailabilityPessimistic(ctx context.Context, tierID uuid.UUID, delta int32) (*domain.TicketTier, error)
	UpdateBatchTicketAvailability(ctx context.Context, req *UpdateBatchTicketAvailabilityRequest) error
}

type ReservedOrCompensateBatchSeats struct {
	EventID               uuid.UUID
	SeatIds               []uuid.UUID
	ReservedByBookingId   uuid.UUID
	Action                domain.ReserveSeatAction
	ReservedTimeInMinutes int32
}

type SeatMutationResult struct {
	Success      bool
	Seat         *domain.Seat
	ChangedSeats []*domain.Seat
}

type ReservedExpiredSeatsResult struct {
	BookingIdSeatIdsMap map[uuid.UUID][]uuid.UUID
	EventBookingSeatIDs map[uuid.UUID]map[uuid.UUID][]uuid.UUID
	ChangedSeats        []*domain.Seat
}

type SeatRepository interface {
	SeatLifecycleRepository
	// Create inserts a new seat into the database
	Create(ctx context.Context, seat *domain.Seat) error

	// CreateBatch inserts multiple seats in a single transaction
	CreateBatch(ctx context.Context, seats []*domain.Seat) error

	// GetByID retrieves a seat by its ID
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Seat, error)

	// GetByEventID retrieves all seats for an event, optionally filtered by tier ID
	GetByEventID(ctx context.Context, eventID uuid.UUID, tierID *uuid.UUID) ([]*domain.Seat, error)

	// GetByTierID retrieves all seats for a ticket tier
	GetByTierID(ctx context.Context, tierID uuid.UUID) ([]*domain.Seat, error)

	// UpdateStatus updates the status of a seat, optionally associating it with a booking
	UpdateStatus(ctx context.Context, seatID uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) (SeatMutationResult, error)

	// UpdateStatusBatch updates multiple seats' statuses in a single transaction
	UpdateStatusBatch(ctx context.Context, seatIDs []uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) (SeatMutationResult, error)

	// GetAvailableSeats retrieves all available seats for an event
	GetAvailableSeats(ctx context.Context, eventID uuid.UUID) ([]*domain.Seat, error)

	// GetAvailableSeatsByTier retrieves available seats for a specific ticket tier
	GetAvailableSeatsByTier(ctx context.Context, tierID uuid.UUID) ([]*domain.Seat, error)

	GetSeatsBySeatIds(ctx context.Context, seatIds []uuid.UUID) ([]domain.Seat, error)

	ReservedOrCompensateBatchSeats(ctx context.Context, req *ReservedOrCompensateBatchSeats) (SeatMutationResult, error)

	UndoReservedExpiredSeats(ctx context.Context) (*ReservedExpiredSeatsResult, error)

	GetReservedExpiredSeats(ctx context.Context) (*ReservedExpiredSeatsResult, error)
}
