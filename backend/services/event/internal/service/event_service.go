package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/pkg/observability"
)

const (
	MaxEventTicketTiers         = 100
	MaxSeatsPerTicketTier int32 = 5000
	MaxSeatsPerEvent      int32 = 10000
	MaxTicketPriceCents   int64 = 2_147_483_647
)

var ErrInvalidEventRequest = errors.New("invalid event request")
var ErrFilteredEventListUnavailable = errors.New("filtered Event catalog unavailable")

type EventService struct {
	eventRepo         repository.EventRepository
	tierRepo          repository.TicketTierRepository
	seatRepo          repository.SeatRepository
	logger            *zap.Logger
	admission         admission.Admission
	admissionMode     admission.Mode
	realtimePublisher SeatRealtimePublisher
	telemetry         *observability.Metrics
}

func NewEventService(eventRepo repository.EventRepository, tierRepo repository.TicketTierRepository, seatRepo repository.SeatRepository, logger *zap.Logger) *EventService {
	return NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(eventRepo, tierRepo, seatRepo, logger, nil, admission.ModePostgresOnly, nil, nil)
}

func NewEventServiceWithAdmission(
	eventRepo repository.EventRepository,
	tierRepo repository.TicketTierRepository,
	seatRepo repository.SeatRepository,
	logger *zap.Logger,
	seatAdmission admission.Admission,
	mode admission.Mode,
) *EventService {
	return NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(eventRepo, tierRepo, seatRepo, logger, seatAdmission, mode, nil, nil)
}

func NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(
	eventRepo repository.EventRepository,
	tierRepo repository.TicketTierRepository,
	seatRepo repository.SeatRepository,
	logger *zap.Logger,
	seatAdmission admission.Admission,
	mode admission.Mode,
	publisher SeatRealtimePublisher,
	metrics *observability.Metrics,
) *EventService {
	if mode == "" {
		mode = admission.ModePostgresOnly
	}
	if publisher == nil {
		publisher = noOpSeatRealtimePublisher{}
	}
	return &EventService{
		eventRepo:         eventRepo,
		tierRepo:          tierRepo,
		seatRepo:          seatRepo,
		logger:            logger,
		admission:         seatAdmission,
		admissionMode:     mode,
		realtimePublisher: publisher,
		telemetry:         metrics,
	}
}

func NewEventServiceWithAdmissionAndRealtimePublisher(
	eventRepo repository.EventRepository,
	tierRepo repository.TicketTierRepository,
	seatRepo repository.SeatRepository,
	logger *zap.Logger,
	seatAdmission admission.Admission,
	mode admission.Mode,
	publisher SeatRealtimePublisher,
) *EventService {
	return NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(eventRepo, tierRepo, seatRepo, logger, seatAdmission, mode, publisher, nil)
}

func (s *EventService) CreateEvent(ctx context.Context, title, description, category, venue, location, imageURL string, date time.Time, tiers []domain.TicketTier) (*domain.Event, error) {
	return s.CreateEventWithLifecycle(ctx, title, description, category, venue, location, imageURL, date, tiers,
		domain.PublicationDraft, domain.SalesScheduled, nil, nil, uuid.Nil, "")
}

func (s *EventService) CreateEventWithLifecycle(ctx context.Context, title, description, category, venue, location, imageURL string, date time.Time, tiers []domain.TicketTier, publicationStatus, salesStatus string, salesStartAt, salesEndAt *time.Time, actorID uuid.UUID, requestID string) (*domain.Event, error) {
	if publicationStatus == "" {
		publicationStatus = domain.PublicationDraft
	}
	if salesStatus == "" {
		salesStatus = domain.SalesScheduled
	}
	if date.IsZero() {
		return nil, fmt.Errorf("event date is required: %w", ErrInvalidEventRequest)
	}
	if err := validateEventCreate(title, category, venue, location, tiers, publicationStatus, salesStatus, salesStartAt, salesEndAt); err != nil {
		return nil, err
	}
	var creationRequest *domain.EventCreationRequest
	if requestID != "" {
		if actorID == uuid.Nil || len(requestID) > 128 || strings.TrimSpace(requestID) != requestID {
			return nil, fmt.Errorf("actor ID and a 1-128 character idempotency key are required: %w", ErrInvalidEventRequest)
		}
		for _, char := range requestID {
			if char < 0x21 || char > 0x7e {
				return nil, fmt.Errorf("idempotency key must contain printable non-space ASCII: %w", ErrInvalidEventRequest)
			}
		}
		requestHash, err := eventCreateRequestHash(title, description, category, venue, location, imageURL, date, tiers, publicationStatus, salesStatus, salesStartAt, salesEndAt)
		if err != nil {
			return nil, fmt.Errorf("fingerprint event creation request: %w", err)
		}
		creationRequest = &domain.EventCreationRequest{ActorID: actorID, RequestID: requestID, RequestHash: requestHash}
	}
	now := time.Now()
	event := &domain.Event{
		ID:                uuid.New(),
		Title:             title,
		Description:       description,
		Category:          category,
		Venue:             venue,
		Location:          location,
		Date:              date,
		DateVersion:       1,
		ImageURL:          imageURL,
		Status:            "active",
		PublicationStatus: publicationStatus,
		SalesStatus:       salesStatus,
		SalesStartAt:      salesStartAt,
		SalesEndAt:        salesEndAt,
		CreationRequest:   creationRequest,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	for i := range tiers {
		tiers[i].ID = uuid.New()
		tiers[i].EventID = event.ID
		tiers[i].AvailableQuantity = tiers[i].TotalQuantity
		tiers[i].CreatedAt = now
	}
	event.Tiers = tiers
	var totalSeats int32
	for _, tier := range tiers {
		totalSeats += tier.TotalQuantity
	}
	if totalSeats > MaxSeatsPerEvent {
		return nil, fmt.Errorf("event exceeds maximum seat count of %d", MaxSeatsPerEvent)
	}
	event.Seats = s.generateSeatsForEvent(event, tiers)
	if int32(len(event.Seats)) != totalSeats {
		return nil, fmt.Errorf("generated seat count does not match ticket tier inventory: %w", ErrInvalidEventRequest)
	}
	if err := s.eventRepo.Create(ctx, event); err != nil {
		return nil, fmt.Errorf("create event and inventory atomically: %w", err)
	}

	return event, nil
}

func eventCreateRequestHash(title, description, category, venue, location, imageURL string, date time.Time, tiers []domain.TicketTier, publicationStatus, salesStatus string, salesStartAt, salesEndAt *time.Time) ([]byte, error) {
	type tierInput struct {
		Name          string `json:"name"`
		PriceCents    int64  `json:"price_cents"`
		TotalQuantity int32  `json:"total_quantity"`
	}
	type input struct {
		Title             string      `json:"title"`
		Description       string      `json:"description"`
		Category          string      `json:"category"`
		Venue             string      `json:"venue"`
		Location          string      `json:"location"`
		ImageURL          string      `json:"image_url"`
		Date              string      `json:"date"`
		PublicationStatus string      `json:"publication_status"`
		SalesStatus       string      `json:"sales_status"`
		SalesStartAt      string      `json:"sales_start_at"`
		SalesEndAt        string      `json:"sales_end_at"`
		Tiers             []tierInput `json:"tiers"`
	}
	canonical := input{
		Title: title, Description: description, Category: category, Venue: venue, Location: location, ImageURL: imageURL,
		Date: date.UTC().Format(time.RFC3339Nano), PublicationStatus: publicationStatus, SalesStatus: salesStatus,
	}
	if salesStartAt != nil {
		canonical.SalesStartAt = salesStartAt.UTC().Format(time.RFC3339Nano)
	}
	if salesEndAt != nil {
		canonical.SalesEndAt = salesEndAt.UTC().Format(time.RFC3339Nano)
	}
	for _, tier := range tiers {
		canonical.Tiers = append(canonical.Tiers, tierInput{Name: tier.Name, PriceCents: tier.PriceCents, TotalQuantity: tier.TotalQuantity})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(encoded)
	return fingerprint[:], nil
}

func validateEventCreate(title, category, venue, location string, tiers []domain.TicketTier, publicationStatus, salesStatus string, salesStartAt, salesEndAt *time.Time) error {
	if strings.TrimSpace(title) == "" || len(title) > 500 || strings.TrimSpace(category) == "" || len(category) > 100 || strings.TrimSpace(venue) == "" || len(venue) > 500 || strings.TrimSpace(location) == "" || len(location) > 500 {
		return fmt.Errorf("valid title, category, venue, and location are required: %w", ErrInvalidEventRequest)
	}
	if len(tiers) == 0 || len(tiers) > MaxEventTicketTiers {
		return fmt.Errorf("event must have between 1 and %d ticket tiers: %w", MaxEventTicketTiers, ErrInvalidEventRequest)
	}
	if publicationStatus == "" {
		publicationStatus = domain.PublicationDraft
	}
	if publicationStatus != domain.PublicationDraft && publicationStatus != domain.PublicationPublished {
		return fmt.Errorf("invalid publication status: %w", ErrInvalidEventRequest)
	}
	if salesStatus == "" {
		salesStatus = domain.SalesScheduled
	}
	switch salesStatus {
	case domain.SalesScheduled, domain.SalesSelling, domain.SalesPaused, domain.SalesEnded, domain.SalesCancelled:
	default:
		return fmt.Errorf("invalid sales status: %w", ErrInvalidEventRequest)
	}
	if salesStartAt != nil && salesEndAt != nil && !salesStartAt.Before(*salesEndAt) {
		return fmt.Errorf("sales start must be before sales end: %w", ErrInvalidEventRequest)
	}
	var total int32
	for _, tier := range tiers {
		if strings.TrimSpace(tier.Name) == "" || len(tier.Name) > 255 || tier.PriceCents < 1 || tier.PriceCents > MaxTicketPriceCents {
			return fmt.Errorf("ticket tier name or price is outside allowed bounds (1-%d cents): %w", MaxTicketPriceCents, ErrInvalidEventRequest)
		}
		if tier.TotalQuantity < 1 || tier.TotalQuantity > MaxSeatsPerTicketTier {
			return fmt.Errorf("ticket tier quantity must be between 1 and %d: %w", MaxSeatsPerTicketTier, ErrInvalidEventRequest)
		}
		if total > MaxSeatsPerEvent-tier.TotalQuantity {
			return fmt.Errorf("event exceeds maximum seat count of %d: %w", MaxSeatsPerEvent, ErrInvalidEventRequest)
		}
		total += tier.TotalQuantity
	}
	return nil
}

func (s *EventService) generateSeatsForEvent(event *domain.Event, tiers []domain.TicketTier) []*domain.Seat {
	var allSeats []*domain.Seat
	now := time.Now()

	for _, tier := range tiers {
		// Generate seats based on tier quantity
		seats := s.generateSeatsForTier(event.ID, tier.ID, tier.Name, tier.TotalQuantity, now)
		allSeats = append(allSeats, seats...)
	}
	return allSeats
}

func (s *EventService) generateSeatsForTier(eventID, tierID uuid.UUID, tierName string, quantity int32, now time.Time) []*domain.Seat {
	seats := make([]*domain.Seat, 0, quantity)

	// Calculate rows and seats per row for layout
	// For simplicity, we'll use a grid layout
	rows := int((quantity + 19) / 20) // Max 20 seats per row
	if rows < 1 {
		rows = 1
	}

	seatNum := 0
	for row := 0; row < rows && seatNum < int(quantity); row++ {
		rowLabel := seatRowLabel(row)
		seatsInRow := int(quantity) - seatNum
		if seatsInRow > 20 {
			seatsInRow = 20
		}

		for i := 0; i < seatsInRow; i++ {
			seats = append(seats, &domain.Seat{
				ID:           uuid.New(),
				EventID:      eventID,
				TicketTierID: tierID,
				Status:       domain.SeatStatusAvailable,
				BookingID:    nil,
				OrderID:      nil,
				Position: domain.Position{
					SectionID: tierName,
					Row:       rowLabel,
					Seat:      i + 1,
					X:         i * 50,
					Y:         row * 50,
				},
				CreatedAt: now,
				UpdatedAt: now,
			})
			seatNum++
		}
	}

	return seats
}

func seatRowLabel(row int) string {
	if row < 0 {
		return ""
	}
	var label []byte
	for row >= 0 {
		label = append([]byte{byte('A' + row%26)}, label...)
		row = row/26 - 1
	}
	return string(label)
}

func (s *EventService) GetEvent(ctx context.Context, eventID uuid.UUID) (*domain.Event, error) {
	return s.eventRepo.GetByID(ctx, eventID)
}

func (s *EventService) GetPublicEvent(ctx context.Context, eventID uuid.UUID) (*domain.Event, error) {
	event, err := s.eventRepo.GetByID(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if !domain.EventIsPublished(event) {
		return nil, repository.ErrNotFound
	}
	return event, nil
}

func (s *EventService) ensurePublished(ctx context.Context, eventID uuid.UUID) error {
	event, err := s.eventRepo.GetByID(ctx, eventID)
	if err != nil {
		return err
	}
	if !domain.EventIsPublished(event) {
		return repository.ErrNotFound
	}
	return nil
}

func (s *EventService) ListEvents(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error) {
	return s.eventRepo.List(ctx, category, search, page, pageSize)
}

func (s *EventService) ListEventsFiltered(ctx context.Context, filter repository.EventListFilter, page, pageSize int) ([]*domain.Event, int, int, int, error) {
	filter, page, pageSize, err := repository.NormalizeEventListQuery(filter, page, pageSize)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	if filtered, ok := s.eventRepo.(repository.FilteredEventRepository); ok {
		events, total, err := filtered.ListFiltered(ctx, filter, page, pageSize)
		return events, total, page, pageSize, err
	}
	if hasAdvancedEventListFilter(filter) {
		return nil, 0, 0, 0, ErrFilteredEventListUnavailable
	}
	events, total, err := s.eventRepo.List(ctx, filter.Category, filter.Search, page, pageSize)
	return events, total, page, pageSize, err
}

func (s *EventService) ListAdminEvents(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error) {
	return s.eventRepo.ListAdmin(ctx, category, search, page, pageSize)
}

func (s *EventService) ListAdminEventsFiltered(ctx context.Context, filter repository.EventListFilter, page, pageSize int) ([]*domain.Event, int, int, int, error) {
	filter, page, pageSize, err := repository.NormalizeEventListQuery(filter, page, pageSize)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	if filtered, ok := s.eventRepo.(repository.FilteredEventRepository); ok {
		events, total, err := filtered.ListAdminFiltered(ctx, filter, page, pageSize)
		return events, total, page, pageSize, err
	}
	if hasAdvancedEventListFilter(filter) {
		return nil, 0, 0, 0, ErrFilteredEventListUnavailable
	}
	events, total, err := s.eventRepo.ListAdmin(ctx, filter.Category, filter.Search, page, pageSize)
	return events, total, page, pageSize, err
}

func hasAdvancedEventListFilter(filter repository.EventListFilter) bool {
	return filter.DateFrom != nil || filter.DateToExclusive != nil || filter.Location != "" ||
		filter.MinPriceCents != nil || filter.MaxPriceCents != nil || filter.Sort != "date_asc"
}

func (s *EventService) UpdateEvent(ctx context.Context, eventID uuid.UUID, title, description, category, venue, location, imageURL string, date time.Time) (*domain.Event, error) {
	return s.UpdateEventVersioned(ctx, eventID, title, description, category, venue, location, imageURL, date, 0)
}

func (s *EventService) UpdateEventVersioned(ctx context.Context, eventID uuid.UUID, title, description, category, venue, location, imageURL string, date time.Time, expectedDateVersion int64) (*domain.Event, error) {
	event, err := s.eventRepo.GetByID(ctx, eventID)
	if err != nil {
		return nil, err
	}

	if title != "" {
		event.Title = title
	}
	if description != "" {
		event.Description = description
	}
	if category != "" {
		event.Category = category
	}
	if venue != "" {
		event.Venue = venue
	}
	if location != "" {
		event.Location = location
	}
	if imageURL != "" {
		event.ImageURL = imageURL
	}
	dateChanged := !date.IsZero() && !date.Equal(event.Date)
	if expectedDateVersion > 0 && expectedDateVersion != event.DateVersion {
		return nil, repository.ErrConflict
	}
	if dateChanged {
		if expectedDateVersion <= 0 {
			return nil, repository.ErrConflict
		}
		event.Date = date
		if err := s.eventRepo.UpdateDate(ctx, event, expectedDateVersion); err != nil {
			return nil, err
		}
	} else if err := s.eventRepo.Update(ctx, event); err != nil {
		return nil, err
	}

	return s.eventRepo.GetByID(ctx, eventID)
}

func (s *EventService) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	return s.eventRepo.Delete(ctx, eventID)
}

func (s *EventService) UpdateEventSales(ctx context.Context, eventID uuid.UUID, publicationStatus, salesStatus string, salesStartAt, salesEndAt *time.Time, clearSalesWindow bool) (*domain.Event, error) {
	if publicationStatus != domain.PublicationDraft && publicationStatus != domain.PublicationPublished {
		return nil, repository.ErrInvalidEventLifecycle
	}
	switch salesStatus {
	case domain.SalesScheduled, domain.SalesSelling, domain.SalesPaused, domain.SalesEnded, domain.SalesCancelled:
	default:
		return nil, repository.ErrInvalidEventLifecycle
	}
	if clearSalesWindow && (salesStartAt != nil || salesEndAt != nil) {
		return nil, repository.ErrInvalidEventLifecycle
	}
	if salesStartAt != nil && salesEndAt != nil && !salesStartAt.Before(*salesEndAt) {
		return nil, repository.ErrInvalidEventLifecycle
	}
	event := &domain.Event{
		ID: eventID, PublicationStatus: publicationStatus, SalesStatus: salesStatus,
		SalesStartAt: salesStartAt, SalesEndAt: salesEndAt,
		UpdateSalesWindow: clearSalesWindow || salesStartAt != nil || salesEndAt != nil,
	}
	if err := s.eventRepo.UpdateSales(ctx, event); err != nil {
		return nil, err
	}
	return s.eventRepo.GetByID(ctx, eventID)
}

func (s *EventService) CheckEventSaleEligibility(ctx context.Context, eventID uuid.UUID) (bool, string, error) {
	event, err := s.eventRepo.GetByID(ctx, eventID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return false, "event_not_found", nil
		}
		return false, "sale_state_unknown", err
	}
	eligible, reason := domain.EventSaleEligibility(event, time.Now())
	return eligible, reason, nil
}

func (s *EventService) GetTicketAvailability(ctx context.Context, tierID uuid.UUID) (*domain.TicketTier, error) {
	tier, err := s.tierRepo.GetByID(ctx, tierID)
	if err != nil {
		return nil, err
	}
	if err := s.ensurePublished(ctx, tier.EventID); err != nil {
		return nil, err
	}
	return tier, nil
}

func (s *EventService) CheckAccountingReadiness(ctx context.Context, tierID uuid.UUID) (bool, error) {
	return s.tierRepo.AccountingReadiness(ctx, tierID)
}

// CheckAccountingReadinessBatch projects readiness for a catalog response. It
// never invents readiness: tiers missing from the result are not ready.
func (s *EventService) CheckAccountingReadinessBatch(ctx context.Context, tierIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	return s.tierRepo.AccountingReadinessForTiers(ctx, tierIDs)
}

func (s *EventService) UpdateTicketAvailability(ctx context.Context, tierID uuid.UUID, delta int32, expectedVersion int32, mode string) (*domain.TicketTier, error) {
	switch mode {
	case "naive":
		if err := s.tierRepo.UpdateAvailabilityNoLock(ctx, tierID, delta); err != nil {
			return nil, err
		}
		return s.tierRepo.GetByID(ctx, tierID)
	case "pessimistic":
		return s.tierRepo.UpdateAvailabilityPessimistic(ctx, tierID, delta)
	case "optimistic":
		return s.tierRepo.UpdateAvailability(ctx, tierID, delta, expectedVersion)
	default:
		return s.tierRepo.UpdateAvailabilityPessimistic(ctx, tierID, delta)
	}
}

type UpdateTicketAvailabilityRequest struct {
	tierId        uuid.UUID
	deltaQuantity int
}
type UpdateBatchTicketAvailabilityRequest struct {
	Data []UpdateBatchTicketAvailabilityRequest
}

func (s *EventService) UpdateBatchTicketAvailability(ctx context.Context, req *UpdateBatchTicketAvailabilityRequest) error {
	return repository.ErrNotImplemented
}

func (s *EventService) GetSeats(ctx context.Context, eventID uuid.UUID, tierID *uuid.UUID) ([]*domain.Seat, error) {
	if err := s.ensurePublished(ctx, eventID); err != nil {
		return nil, err
	}
	return s.seatRepo.GetByEventID(ctx, eventID, tierID)
}

func (s *EventService) ApplyLifecycle(ctx context.Context, req domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	if req.Action == "reserve" && s.admissionMode == admission.ModeRedisAdmission && s.admission != nil {
		remaining := time.Until(req.ReservationDeadline)
		if remaining <= 0 {
			return nil, repository.ErrSeatNotAvailable
		}
		// Redis retains the existing minute TTL representation; PostgreSQL uses
		// the exact immutable deadline and remains the final reservation owner.
		minutes := int32((remaining + time.Minute - 1) / time.Minute)
		_, err := s.admission.Acquire(ctx, req.EventID, req.SeatIDs, req.BookingID, minutes)
		if errors.Is(err, admission.ErrConflict) {
			return nil, repository.ErrSeatNotAvailable
		}
		if errors.Is(err, admission.ErrInvalidRequest) {
			return nil, err
		}
		if err != nil {
			s.logAdmissionFallback(err)
		}
	}
	result, err := s.seatRepo.ApplyLifecycle(ctx, req)
	if err != nil {
		if req.Action == "reserve" {
			s.cleanupAdmission(ctx, req.EventID, req.SeatIDs, &req.BookingID)
		}
		return nil, err
	}
	if req.Action == "book" || req.Action == "close" {
		s.cleanupAdmission(ctx, req.EventID, req.SeatIDs, &req.BookingID)
	}
	s.publishChangedSeatRows(ctx, result.ChangedSeats)
	return result, nil
}

func (s *EventService) ApplySaleAccounting(ctx context.Context, req domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	return s.seatRepo.ApplySaleAccounting(ctx, req)
}

func (s *EventService) UpdateSeatStatus(ctx context.Context, seatID uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) (*domain.Seat, error) {
	result, err := s.seatRepo.UpdateStatus(ctx, seatID, status, bookingID)
	if err != nil {
		return nil, err
	}
	s.publishChangedSeatRows(ctx, result.ChangedSeats)
	return result.Seat, nil
}

func (s *EventService) UpdateBatchSeatStatus(ctx context.Context, eventID uuid.UUID, seatIds []uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) error {
	result, err := s.seatRepo.UpdateStatusBatch(ctx, seatIds, status, bookingID)
	if err != nil {
		return err
	}
	if len(result.ChangedSeats) > 0 && (status == domain.SeatStatusBooked || status == domain.SeatStatusAvailable) {
		s.cleanupAdmission(ctx, eventID, seatIds, bookingID)
	}
	s.publishChangedSeatRows(ctx, result.ChangedSeats)
	return nil
}

func (s *EventService) GetSeatsBySeatIds(ctx context.Context, seatIds []uuid.UUID) ([]domain.Seat, error) {
	seats, err := s.seatRepo.GetSeatsBySeatIds(ctx, seatIds)
	if err != nil {
		return nil, err
	}
	seen := make(map[uuid.UUID]struct{}, len(seats))
	for _, seat := range seats {
		if _, ok := seen[seat.EventID]; ok {
			continue
		}
		if err := s.ensurePublished(ctx, seat.EventID); err != nil {
			return nil, err
		}
		seen[seat.EventID] = struct{}{}
	}
	return seats, nil
}

type bookingTicketEligibilityReader interface {
	VerifyBookingTicketEligibility(context.Context, uuid.UUID, uuid.UUID, int64, []uuid.UUID) (bool, string, error)
}

func (s *EventService) VerifyBookingTicketEligibility(ctx context.Context, bookingID, eventID uuid.UUID, generation int64, seatIDs []uuid.UUID) (bool, string, error) {
	reader, ok := s.seatRepo.(bookingTicketEligibilityReader)
	if !ok {
		return false, "event_authority_unavailable", errors.New("event booking seat authority unavailable")
	}
	return reader.VerifyBookingTicketEligibility(ctx, bookingID, eventID, generation, seatIDs)
}

type ReservedOrCompensateBatchSeatsReq struct {
	EventID               uuid.UUID
	SeatIds               []uuid.UUID
	ReservedByBookingId   uuid.UUID
	Action                domain.ReserveSeatAction
	ReservedTimeInMinutes int32
}

type ReservationOutcome struct {
	Success           bool
	RedisStatus       string
	RedisFallback     bool
	PostgresAttempted bool
	PostgresWinner    bool
	PostgresConflict  bool
	PostgresError     bool
	RedisLatencyNs    int64
}

const (
	RedisStatusNotApplicable     = "NOT_APPLICABLE"
	RedisStatusSuccess           = "SUCCESS"
	RedisStatusConflict          = "CONFLICT"
	RedisStatusInfrastructureErr = "INFRASTRUCTURE_ERROR"
)

func (s *EventService) ReservedOrCompensateBatchSeats(ctx context.Context, req *ReservedOrCompensateBatchSeatsReq) (ReservationOutcome, error) {
	outcome := ReservationOutcome{RedisStatus: RedisStatusNotApplicable}
	operationCtx, operationSpan := otel.Tracer("github.com/ticketbox/event").Start(ctx, "seat.admission")
	defer operationSpan.End()
	defer func() {
		if outcome.Success {
			operationSpan.SetAttributes(attribute.String("rushgo.result", "success"))
		} else if outcome.RedisStatus == RedisStatusConflict || outcome.PostgresConflict {
			operationSpan.SetAttributes(attribute.String("rushgo.result", "conflict"), attribute.String("rushgo.error.category", "business_conflict"))
		} else if outcome.RedisFallback {
			operationSpan.SetAttributes(attribute.String("rushgo.result", "fallback"))
		}
	}()
	ctx = operationCtx
	if req.Action == domain.RESERVED_SEAT && s.admissionMode == admission.ModeRedisAdmission && s.admission != nil {
		admissionStarted := time.Now()
		_, err := s.admission.Acquire(ctx, req.EventID, req.SeatIds, req.ReservedByBookingId, req.ReservedTimeInMinutes)
		outcome.RedisLatencyNs = time.Since(admissionStarted).Nanoseconds()
		if err != nil {
			if errors.Is(err, admission.ErrConflict) {
				outcome.RedisStatus = RedisStatusConflict
				s.recordAdmission("conflict", outcome.RedisLatencyNs)
				return outcome, repository.ErrSeatNotAvailable
			}
			if errors.Is(err, admission.ErrInvalidRequest) {
				return outcome, err
			}
			outcome.RedisStatus = RedisStatusInfrastructureErr
			outcome.RedisFallback = true
			s.recordAdmission("infrastructure_error", outcome.RedisLatencyNs)
			s.recordAdmissionFallback()
			s.logAdmissionFallback(err)
		} else {
			outcome.RedisStatus = RedisStatusSuccess
			s.recordAdmission("success", outcome.RedisLatencyNs)
			outcome.PostgresAttempted = true
			mutation, dbErr := s.reserveSeatsInPostgres(ctx, req)
			if dbErr == nil {
				outcome.Success = mutation.Success
				outcome.PostgresWinner = len(mutation.ChangedSeats) > 0
				if outcome.PostgresWinner {
					s.recordPostgresReservation("winner")
				} else {
					s.recordPostgresReservation("success")
				}
				s.publishChangedSeatRows(ctx, mutation.ChangedSeats)
				return outcome, nil
			}
			outcome.PostgresConflict = errors.Is(dbErr, repository.ErrSeatNotAvailable)
			outcome.PostgresError = !outcome.PostgresConflict
			if outcome.PostgresConflict {
				s.recordPostgresReservation("business_conflict")
			} else {
				s.recordPostgresReservation("infrastructure_error")
				operationSpan.SetStatus(codes.Error, "postgres reservation failed")
			}
			s.cleanupAdmission(ctx, req.EventID, req.SeatIds, &req.ReservedByBookingId)
			return outcome, dbErr
		}
	}

	outcome.PostgresAttempted = true
	mutation, err := s.reserveSeatsInPostgres(ctx, req)
	if err != nil {
		outcome.PostgresConflict = errors.Is(err, repository.ErrSeatNotAvailable)
		outcome.PostgresError = !outcome.PostgresConflict
		if outcome.PostgresConflict {
			s.recordPostgresReservation("business_conflict")
		} else {
			s.recordPostgresReservation("infrastructure_error")
		}
		return outcome, err
	}
	outcome.Success = mutation.Success
	outcome.PostgresWinner = len(mutation.ChangedSeats) > 0
	if outcome.PostgresWinner {
		s.recordPostgresReservation("winner")
	}
	if req.Action == domain.COMPENSATE_SEAT {
		s.cleanupAdmission(ctx, req.EventID, req.SeatIds, &req.ReservedByBookingId)
	}
	s.publishChangedSeatRows(ctx, mutation.ChangedSeats)
	return outcome, nil
}

func (s *EventService) recordAdmission(result string, latencyNs int64) {
	if s.telemetry == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("mode", string(s.admissionMode)), attribute.String("result", result)}
	s.telemetry.SeatAdmissionAttempts.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	s.telemetry.SeatAdmissionDuration.Record(context.Background(), float64(latencyNs)/float64(time.Second), metric.WithAttributes(attribute.String("mode", string(s.admissionMode))))
}

func (s *EventService) recordAdmissionFallback() {
	if s.telemetry != nil {
		s.telemetry.SeatAdmissionFallbacks.Add(context.Background(), 1, metric.WithAttributes(attribute.String("mode", string(s.admissionMode))))
	}
}

func (s *EventService) recordPostgresReservation(result string) {
	if s.telemetry != nil {
		s.telemetry.SeatPostgresAttempts.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", result)))
	}
}

func (s *EventService) reserveSeatsInPostgres(ctx context.Context, req *ReservedOrCompensateBatchSeatsReq) (repository.SeatMutationResult, error) {
	return s.seatRepo.ReservedOrCompensateBatchSeats(ctx, &repository.ReservedOrCompensateBatchSeats{
		EventID:               req.EventID,
		SeatIds:               req.SeatIds,
		ReservedByBookingId:   req.ReservedByBookingId,
		Action:                req.Action,
		ReservedTimeInMinutes: req.ReservedTimeInMinutes,
	})
}

func (s *EventService) publishChangedSeatRows(ctx context.Context, seats []*domain.Seat) {
	if len(seats) == 0 {
		return
	}
	for eventID, batch := range buildSeatUpdateBatchesByEvent(seats, time.Now()) {
		if len(batch.SeatUpdates) == 0 {
			continue
		}
		publishCtx, publishSpan := otel.Tracer("github.com/ticketbox/event").Start(ctx, "seat_realtime.publish", trace.WithAttributes(attribute.String("event_id", eventID.String())))
		err := s.realtimePublisher.Publish(publishCtx, batch)
		if s.telemetry != nil {
			result := "success"
			if err != nil {
				result = "failed"
			}
			s.telemetry.RealtimePublish.Add(publishCtx, 1, metric.WithAttributes(attribute.String("result", result)))
		}
		if err != nil {
			publishSpan.SetStatus(codes.Error, "realtime publish failed")
			if s.logger != nil {
				s.logger.Warn("seat_realtime_publish failed",
					zap.String("event_id", eventID.String()),
					zap.String("message_id", batch.MessageID),
					zap.Error(err))
			}
		}
		publishSpan.End()
	}
}

func (s *EventService) cleanupAdmission(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, owner *uuid.UUID) {
	if s.admissionMode != admission.ModeRedisAdmission || s.admission == nil || owner == nil || eventID == uuid.Nil {
		return
	}
	if _, err := s.admission.Release(ctx, eventID, seatIDs, *owner); err != nil {
		s.logAdmissionCleanupFailure(err)
	}
}

func (s *EventService) logAdmissionFallback(err error) {
	if s.logger != nil {
		s.logger.Warn("seat admission unavailable; falling back to PostgreSQL", zap.Error(err))
	}
}

func (s *EventService) logAdmissionCleanupFailure(err error) {
	if s.logger != nil {
		s.logger.Warn("seat admission cleanup failed after PostgreSQL mutation", zap.Error(err))
	}
}

func (s *EventService) UndoReservedExpiredSeats(ctx context.Context) (*repository.ReservedExpiredSeatsResult, error) {
	result, err := s.seatRepo.UndoReservedExpiredSeats(ctx)
	if err != nil {
		return nil, fmt.Errorf("[UndoReservedExpiredSeats]: %w", err)
	}

	s.logger.Info("Released expired reserved seats",
		zap.Int("booking_count", len(result.BookingIdSeatIdsMap)))
	s.publishChangedSeatRows(ctx, result.ChangedSeats)
	for eventID, bookings := range result.EventBookingSeatIDs {
		for bookingID, seatIDs := range bookings {
			s.cleanupAdmission(ctx, eventID, seatIDs, &bookingID)
		}
	}

	return result, nil
}

func (s *EventService) GetReservedExpiredSeats(ctx context.Context) (*repository.ReservedExpiredSeatsResult, error) {
	result, err := s.seatRepo.GetReservedExpiredSeats(ctx)
	if err != nil {
		return nil, fmt.Errorf("[GetReservedExpiredSeats]: %w", err)
	}
	return result, nil
}
