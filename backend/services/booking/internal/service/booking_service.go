package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/pkg/topics"
)

type BookingService struct {
	bookingRepo      repository.BookingRepository
	eventClient      eventv1.EventServiceClient
	sagaClient       sagav1.SagaOrchestratorServiceClient
	checkoutPayment  paymentv1.PaymentServiceClient
	logger           *zap.Logger
	RequireRequestID bool
	dispatchSlots    chan struct{}
	dispatchConfig   DispatchConfig
	reliabilityHook  *reliabilityhook.Controller
}

func NewBookingService(
	bookingRepo repository.BookingRepository,
	eventClient eventv1.EventServiceClient,
	sagaClient sagav1.SagaOrchestratorServiceClient,
	logger *zap.Logger,
) *BookingService {
	return &BookingService{
		bookingRepo:    bookingRepo,
		eventClient:    eventClient,
		logger:         logger,
		sagaClient:     sagaClient,
		dispatchSlots:  make(chan struct{}, 4),
		dispatchConfig: DefaultDispatchConfig(),
	}
}

type CreateBookingResponse struct {
	Booking             *domain.Booking
	PaymentClientSecret string
	CreationState       string
	RetryAfterSeconds   int32
	CheckoutState       string
	ReservationDeadline *time.Time
	CanCancel           bool
}

func (s *BookingService) CreateBooking(ctx context.Context, userID, eventID uuid.UUID, items []domain.BookingItem, mode, requestID string) (*CreateBookingResponse, error) {
	hash, normalized, err := domain.CanonicalBookingRequest(eventID, items, mode)
	if err != nil || userID == uuid.Nil || (requestID != "" && !domain.ValidBookingRequestID(requestID)) || (s.RequireRequestID && requestID == "") {
		return nil, repository.ErrInvalidBookingRequest
	}
	mode = normalized
	identity := domain.BookingRequestIdentity{UserID: userID, RequestID: requestID, Hash: hash}
	if requestID != "" {
		v, err := s.bookingRepo.GetByRequest(ctx, identity)
		if err == nil {
			return s.recoverBookingResponse(ctx, v)
		}
		if !errors.Is(err, repository.ErrNotFound) {
			return nil, err
		}
	}
	// Snapshot pricing on a private copy; concurrent calls never mutate input.
	if err := s.checkNewBookingSale(ctx, eventID); err != nil {
		return nil, err
	}
	items = append([]domain.BookingItem(nil), items...)
	// Check if evet exists and seats are available
	event, err := s.eventClient.GetEvent(ctx, &eventv1.GetEventRequest{
		EventId: eventID.String(),
	})
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, fmt.Errorf("[Create Booking]: event not found")
	}
	seatIds := []string{}
	for _, bookingItem := range items {
		for _, seatId := range bookingItem.SeatIDs {
			seatIdStr := seatId.String()
			seatIds = append(seatIds, seatIdStr)
		}
	}

	seatsRes, err := s.eventClient.GetSeatsBySeatIds(ctx, &eventv1.GetSeatsBySeatIdsRequest{
		SeatIds: seatIds,
	})

	if err != nil {
		return nil, err
	}
	if seatsRes == nil || len(seatsRes.Seats) == 0 {
		return nil, fmt.Errorf("get seats for create booking method not found")
	}
	requested := make(map[string]uuid.UUID)
	for _, item := range items {
		for _, id := range item.SeatIDs {
			requested[id.String()] = item.TicketTierID
		}
	}
	seen := make(map[string]bool)
	for _, seat := range seatsRes.Seats {
		if seat == nil || seen[seat.Id] || requested[seat.Id] == uuid.Nil || seat.EventId != eventID.String() || seat.TicketTierId != requested[seat.Id].String() || seat.Status != "available" {
			// A concurrent same-key winner may already have reserved these seats.
			if requestID != "" {
				if v, err := s.bookingRepo.GetByRequest(ctx, identity); err == nil {
					return s.recoverBookingResponse(ctx, v)
				} else if !errors.Is(err, repository.ErrNotFound) {
					return nil, err
				}
			}
			return nil, fmt.Errorf("seat unavailable or inconsistent")
		}
		seen[seat.Id] = true
	}
	if len(seen) != len(requested) {
		return nil, repository.ErrInvalidBookingRequest
	}

	now := time.Now()
	booking := &domain.Booking{
		ID:        uuid.New(),
		UserID:    userID,
		EventID:   eventID,
		Status:    domain.StatusPending,
		Version:   1,
		Items:     items,
		CreatedAt: now,
	}

	var totalCents int64
	for i, item := range items {
		items[i].ID = uuid.New()
		items[i].BookingID = booking.ID

		// Validate that seat IDs count matches quantity
		if len(item.SeatIDs) != int(item.Quantity) {
			return nil, fmt.Errorf("seat IDs count (%d) must match quantity (%d)", len(item.SeatIDs), item.Quantity)
		}

		tier, err := s.eventClient.GetTicketAvailability(ctx, &eventv1.GetTicketAvailabilityRequest{
			TierId: item.TicketTierID.String(),
		})
		if err != nil {
			return nil, fmt.Errorf("get tier availability: %w", err)
		}
		if tier == nil || tier.TierId != item.TicketTierID.String() {
			return nil, repository.ErrInvalidBookingRequest
		}
		if !tier.AccountingReady {
			return nil, ErrAccountingNotReady
		}
		if tier.PriceCents < 1 || tier.PriceCents > math.MaxInt32 || int64(item.Quantity) > (math.MaxInt32-totalCents)/tier.PriceCents {
			return nil, repository.ErrInvalidBookingRequest
		}
		items[i].UnitPriceCents = tier.PriceCents
		totalCents += tier.PriceCents * int64(item.Quantity)
	}

	booking.TotalAmountCents = totalCents
	booking.Status = domain.StatusPending
	booking.Items = items

	v, _, err := s.bookingRepo.CreateOrGetRequest(ctx, domain.CreateBookingCommand{Booking: booking, Identity: identity, Currency: "usd", BookingMode: mode, ReservationDeadline: now.Add(15 * time.Minute)})
	if err != nil {
		return nil, fmt.Errorf("save Booking request: %w", err)
	}
	return s.recoverBookingResponse(ctx, v)
}

// ConfigureReliabilityHook installs a private process-startup hook for isolated
// recovery runs. A nil or disabled controller leaves the product path unchanged.
func (s *BookingService) ConfigureReliabilityHook(hook *reliabilityhook.Controller) error {
	if s == nil || hook == nil {
		return fmt.Errorf("missing reliability hook")
	}
	s.reliabilityHook = hook
	return nil
}

func (s *BookingService) CancelBooking(ctx context.Context, bookingID, userID uuid.UUID) (*domain.Booking, error) {
	booking, err := s.bookingRepo.GetByID(ctx, bookingID)
	if err != nil {
		return nil, err
	}

	if booking.UserID != userID {
		return nil, repository.ErrNotFound
	}
	// Already-cancelled retries return the same terminal result. Other terminal
	// states and uncertain/collected payments never enter the cancellation path.
	if booking.Status == domain.StatusCancelled {
		return booking, nil
	}
	checkout, err := s.ResumeBooking(ctx, bookingID, userID)
	if err != nil {
		return nil, err
	}
	if !checkout.CanCancel {
		return nil, repository.ErrLifecycleConflict
	}

	decision := domain.LifecycleDecision{Ref: domain.LifecycleRef{BookingID: bookingID, Generation: 1},
		DecisionID: domain.BookingDecisionID(bookingID, "cancel"), Kind: "cancel", Reason: "user_cancelled", ExpectedVersion: booking.Version}
	snapshot, err := s.DecideLifecycle(ctx, decision)
	if err != nil {
		return nil, err
	}
	return snapshot.Booking, nil
}

func (s *BookingService) GetLifecycle(ctx context.Context, ref domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	return s.bookingRepo.GetLifecycle(ctx, ref)
}

func (s *BookingService) DecideLifecycle(ctx context.Context, decision domain.LifecycleDecision) (*domain.LifecycleSnapshot, error) {
	current, err := s.bookingRepo.GetLifecycle(ctx, decision.Ref)
	if err != nil {
		return nil, err
	}
	var event *repository.OutboxEvent
	if decision.Kind != "claim_confirmation" {
		eventType := topics.TypeBookingFailed
		if decision.Kind == "cancel" {
			eventType = topics.TypeBookingCancelled
		}
		if decision.Kind == "confirm" {
			eventType = topics.TypeBookingConfirmed
		}
		data := map[string]any{"booking_id": current.Booking.ID.String(), "user_id": current.Booking.UserID.String(),
			"event_id": current.Booking.EventID.String(), "email": ""}
		if eventType == topics.TypeBookingFailed {
			data["reason"] = decision.Reason
		}
		if eventType == topics.TypeBookingConfirmed {
			data["total"] = current.Booking.TotalAmountCents
		}
		payload, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		event = &repository.OutboxEvent{ID: domain.BookingOutboxID(current.Booking.ID, decision.Kind),
			EventType: eventType, EventKey: current.Booking.ID.String(), Payload: payload, CreatedAt: time.Now()}
	}
	actual, changed, err := s.bookingRepo.DecideLifecycle(ctx, decision, event)
	if err != nil {
		return nil, err
	}
	if !changed && (actual.DecisionID == nil || *actual.DecisionID != decision.DecisionID) {
		return actual, repository.ErrLifecycleConflict
	}
	return actual, nil
}

func (s *BookingService) ListExpiryCandidates(ctx context.Context, limit int, after uuid.UUID) ([]domain.LifecycleSnapshot, error) {
	return s.bookingRepo.ListExpiryCandidates(ctx, limit, after)
}

func (s *BookingService) GetBooking(ctx context.Context, bookingID uuid.UUID) (*domain.Booking, error) {
	return s.bookingRepo.GetByID(ctx, bookingID)
}

var ErrAdminBookingDetailUnavailable = errors.New("admin booking detail is unavailable")

func (s *BookingService) GetAdminBookingDetail(ctx context.Context, bookingID uuid.UUID) (*domain.AdminBookingDetail, error) {
	repo, ok := s.bookingRepo.(repository.AdminBookingDetailRepository)
	if !ok || s.bookingRepo == nil {
		return nil, ErrAdminBookingDetailUnavailable
	}
	return repo.GetAdminBookingDetail(ctx, bookingID)
}

func (s *BookingService) ListUserBookings(ctx context.Context, userID uuid.UUID, page, pageSize int) ([]*domain.Booking, int, error) {
	return s.bookingRepo.ListByUserID(ctx, userID, page, pageSize)
}

func (s *BookingService) UpdateBookingStatusById(ctx context.Context, bookingId uuid.UUID, status domain.BookingStatus) error {
	booking, err := s.bookingRepo.GetByID(ctx, bookingId)
	if err != nil {
		return err
	}
	// Terminal adapters use the same authoritative decision and durable intent as
	// cancellation and recovery. Unclassified legacy records require attention.
	kind := ""
	switch status {
	case domain.StatusFailed:
		kind = "fail"
	case domain.StatusCancelled:
		kind = "cancel"
	case domain.StatusExpired:
		kind = "expire"
	}
	if kind != "" {
		ref := domain.LifecycleRef{BookingID: bookingId, Generation: 1}
		current, readErr := s.bookingRepo.GetLifecycle(ctx, ref)
		if readErr != nil {
			return readErr
		}
		if !current.Booking.Managed {
			return repository.ErrLegacyLifecycleAttention
		}
		_, err = s.DecideLifecycle(ctx, domain.LifecycleDecision{Ref: ref,
			DecisionID: domain.BookingDecisionID(bookingId, kind), Kind: kind, Reason: "status_update", ExpectedVersion: current.Booking.Version})
		return err
	}

	if booking.Status != domain.StatusPending {
		if status == booking.Status {
			return nil
		}
		return fmt.Errorf("booking %s cannot transition from %s to %s", bookingId, booking.Status, status)
	}
	var changed bool
	if status == domain.StatusConfirmed {
		payload, err := json.Marshal(map[string]interface{}{
			"booking_id": booking.ID.String(),
			"user_id":    booking.UserID.String(),
			"event_id":   booking.EventID.String(),
			"email":      "",
			"total":      booking.TotalAmountCents,
		})
		if err != nil {
			return fmt.Errorf("marshal booking confirmation event: %w", err)
		}
		changed, err = s.bookingRepo.ConfirmBookingWithOutbox(ctx, bookingId, repository.OutboxEvent{
			ID:        uuid.New(),
			EventType: topics.TypeBookingConfirmed,
			EventKey:  bookingId.String(),
			Payload:   payload,
			CreatedAt: time.Now(),
		})
	} else {
		changed, err = s.bookingRepo.CompareAndSetStatus(ctx, bookingId, domain.StatusPending, status)
	}
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("booking %s status changed concurrently", bookingId)
	}
	booking.Status = status
	return nil
}

func (s *BookingService) UpdateBookingStatusByIds(ctx context.Context, bookingIds []uuid.UUID, status domain.BookingStatus) error {
	if status == domain.StatusConfirmed || status == domain.StatusFailed || status == domain.StatusCancelled || status == domain.StatusExpired {
		for _, bookingID := range bookingIds {
			if err := s.UpdateBookingStatusById(ctx, bookingID, status); err != nil {
				return err
			}
		}
		return nil
	}
	return s.bookingRepo.BatchUpdateStatus(ctx, bookingIds, status)
}
