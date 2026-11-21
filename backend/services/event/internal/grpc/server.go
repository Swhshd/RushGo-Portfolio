package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/pkg/refundtime"
)

type EventServer struct {
	eventv1.UnimplementedEventServiceServer
	service             *service.EventService
	interestService     *service.EventInterestService
	logger              *zap.Logger
	refundTimeAuthority refundtime.Authority
}

func (s *EventServer) SetEventInterestService(interestService *service.EventInterestService) {
	s.interestService = interestService
}

func NewEventServer(svc *service.EventService, logger *zap.Logger) *EventServer {
	return &EventServer{service: svc, logger: logger}
}

func (s *EventServer) SetRefundTimeAuthority(authority refundtime.Authority) {
	s.refundTimeAuthority = authority
}

func (s *EventServer) CreateEvent(ctx context.Context, req *eventv1.CreateEventRequest) (*eventv1.EventDetail, error) {
	if req == nil || req.Date == nil || !req.Date.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "valid event date required")
	}
	var tiers []domain.TicketTier
	for _, t := range req.Tiers {
		if t == nil {
			return nil, status.Error(codes.InvalidArgument, "ticket tier is required")
		}
		tiers = append(tiers, domain.TicketTier{
			Name:          t.Name,
			PriceCents:    t.PriceCents,
			TotalQuantity: t.TotalQuantity,
		})
	}

	salesStartAt, err := optionalTimestamp(req.SalesStartAt)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid sales start time")
	}
	salesEndAt, err := optionalTimestamp(req.SalesEndAt)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid sales end time")
	}
	actorID := uuid.Nil
	if req.ActorId != "" {
		actorID, err = uuid.Parse(req.ActorId)
		if err != nil || actorID == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid event creator identity")
		}
	}
	event, err := s.service.CreateEventWithLifecycle(ctx, req.Title, req.Description, req.Category, req.Venue, req.Location, req.ImageUrl, req.Date.AsTime(), tiers,
		req.PublicationStatus, req.SalesStatus, salesStartAt, salesEndAt, actorID, req.RequestId)
	if err != nil {
		if errors.Is(err, repository.ErrIdempotencyConflict) {
			return nil, status.Error(codes.AlreadyExists, "idempotency key was already used with different event data")
		}
		if errors.Is(err, service.ErrInvalidEventRequest) {
			return nil, status.Error(codes.InvalidArgument, "invalid event data or ticket inventory")
		}
		s.logger.Error("CreateEvent failed", zap.Error(err))
		return nil, status.Error(codes.Unavailable, "failed to create event and ticket inventory")
	}

	return toEventDetail(event, s.catalogReadiness(ctx, event)), nil
}

func (s *EventServer) GetEvent(ctx context.Context, req *eventv1.GetEventRequest) (*eventv1.EventDetail, error) {
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}

	event, err := s.service.GetPublicEvent(ctx, eventID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		return nil, status.Error(codes.Internal, "failed to get event")
	}

	return toEventDetail(event, s.catalogReadiness(ctx, event)), nil
}

func (s *EventServer) GetAdminEvent(ctx context.Context, req *eventv1.GetEventRequest) (*eventv1.EventDetail, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "event ID required")
	}
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}
	event, err := s.service.GetEvent(ctx, eventID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		return nil, status.Error(codes.Unavailable, "failed to get event")
	}
	return toEventDetail(event, s.catalogReadiness(ctx, event)), nil
}

func (s *EventServer) ListEvents(ctx context.Context, req *eventv1.ListEventsRequest) (*eventv1.ListEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "list request required")
	}
	filter, err := eventListFilterFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event list query")
	}
	events, total, page, pageSize, err := s.service.ListEventsFiltered(ctx, filter, int(req.Page), int(req.PageSize))
	if err != nil {
		if errors.Is(err, repository.ErrInvalidEventListQuery) {
			return nil, status.Error(codes.InvalidArgument, "invalid event list query")
		}
		return nil, status.Error(codes.Internal, "failed to list events")
	}

	readiness := s.catalogReadiness(ctx, events...)
	var details []*eventv1.EventDetail
	for _, e := range events {
		details = append(details, toEventDetail(e, readiness))
	}

	return &eventv1.ListEventsResponse{
		Events:   details,
		Total:    int32(total),
		Page:     int32(page),
		PageSize: int32(pageSize),
	}, nil
}

func (s *EventServer) ListAdminEvents(ctx context.Context, req *eventv1.ListEventsRequest) (*eventv1.ListEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "list request required")
	}
	filter, err := eventListFilterFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event list query")
	}
	events, total, page, pageSize, err := s.service.ListAdminEventsFiltered(ctx, filter, int(req.Page), int(req.PageSize))
	if err != nil {
		if errors.Is(err, repository.ErrInvalidEventListQuery) {
			return nil, status.Error(codes.InvalidArgument, "invalid event list query")
		}
		return nil, status.Error(codes.Unavailable, "failed to list events")
	}
	readiness := s.catalogReadiness(ctx, events...)
	details := make([]*eventv1.EventDetail, 0, len(events))
	for _, event := range events {
		details = append(details, toEventDetail(event, readiness))
	}
	return &eventv1.ListEventsResponse{Events: details, Total: int32(total), Page: int32(page), PageSize: int32(pageSize)}, nil
}

func eventListFilterFromProto(req *eventv1.ListEventsRequest) (repository.EventListFilter, error) {
	if req == nil {
		return repository.EventListFilter{}, repository.ErrInvalidEventListQuery
	}
	filter := repository.EventListFilter{
		Category: req.GetCategory(), Search: req.GetSearch(), Location: req.GetLocation(), Sort: req.GetSort(),
	}
	if req.GetDateFrom() != nil {
		if !req.GetDateFrom().IsValid() {
			return repository.EventListFilter{}, repository.ErrInvalidEventListQuery
		}
		value := req.GetDateFrom().AsTime().UTC()
		filter.DateFrom = &value
	}
	if req.GetDateToExclusive() != nil {
		if !req.GetDateToExclusive().IsValid() {
			return repository.EventListFilter{}, repository.ErrInvalidEventListQuery
		}
		value := req.GetDateToExclusive().AsTime().UTC()
		filter.DateToExclusive = &value
	}
	if req.MinPriceCents != nil {
		value := req.GetMinPriceCents()
		filter.MinPriceCents = &value
	}
	if req.MaxPriceCents != nil {
		value := req.GetMaxPriceCents()
		filter.MaxPriceCents = &value
	}
	return filter, nil
}

func (s *EventServer) UpdateEvent(ctx context.Context, req *eventv1.UpdateEventRequest) (*eventv1.EventDetail, error) {
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}

	var date time.Time
	if req.Date != nil {
		date = req.Date.AsTime()
	}
	event, err := s.service.UpdateEventVersioned(ctx, eventID, req.Title, req.Description, req.Category, req.Venue, req.Location, req.ImageUrl, date, req.ExpectedDateVersion)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		if errors.Is(err, repository.ErrConflict) {
			return nil, status.Error(codes.Aborted, "event date version conflict - retry")
		}
		if errors.Is(err, repository.ErrActiveRefundTimePermit) {
			return nil, status.Error(codes.FailedPrecondition, "event date is held by a pending refund review")
		}
		return nil, status.Error(codes.Internal, "failed to update event")
	}

	return toEventDetail(event, s.catalogReadiness(ctx, event)), nil
}

func (s *EventServer) UpdateEventSales(ctx context.Context, req *eventv1.UpdateEventSalesRequest) (*eventv1.EventDetail, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "event sales update required")
	}
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}
	salesStartAt, err := optionalTimestamp(req.SalesStartAt)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid sales start time")
	}
	salesEndAt, err := optionalTimestamp(req.SalesEndAt)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid sales end time")
	}
	event, err := s.service.UpdateEventSales(ctx, eventID, req.PublicationStatus, req.SalesStatus, salesStartAt, salesEndAt, req.ClearSalesWindow)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			return nil, status.Error(codes.NotFound, "event not found")
		case errors.Is(err, repository.ErrInvalidEventLifecycle):
			return nil, status.Error(codes.InvalidArgument, "invalid event sale lifecycle or window")
		default:
			return nil, status.Error(codes.Unavailable, "failed to update event sales")
		}
	}
	return toEventDetail(event, s.catalogReadiness(ctx, event)), nil
}

func (s *EventServer) CheckEventSaleEligibility(ctx context.Context, req *eventv1.CheckEventSaleEligibilityRequest) (*eventv1.CheckEventSaleEligibilityResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "event ID required")
	}
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}
	eligible, reason, err := s.service.CheckEventSaleEligibility(ctx, eventID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "event sale state unavailable")
	}
	state := eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_DENIED
	if reason == "sale_state_unknown" {
		state = eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_UNKNOWN
	} else if eligible {
		state = eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_ELIGIBLE
	}
	return &eventv1.CheckEventSaleEligibilityResponse{State: state, ReasonCode: reason}, nil
}

func optionalTimestamp(value *timestamppb.Timestamp) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	if !value.IsValid() {
		return nil, errors.New("invalid timestamp")
	}
	result := value.AsTime()
	return &result, nil
}

func (s *EventServer) DeleteEvent(ctx context.Context, req *eventv1.DeleteEventRequest) (*eventv1.DeleteEventResponse, error) {
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}

	if err := s.service.DeleteEvent(ctx, eventID); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		if errors.Is(err, repository.ErrActiveRefundTimePermit) {
			return nil, status.Error(codes.FailedPrecondition, "event is held by a pending refund review")
		}
		return nil, status.Error(codes.Internal, "failed to delete event")
	}

	return &eventv1.DeleteEventResponse{}, nil
}

func (s *EventServer) GetTicketAvailability(ctx context.Context, req *eventv1.GetTicketAvailabilityRequest) (*eventv1.TicketAvailabilityResponse, error) {
	tierID, err := uuid.Parse(req.TierId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tier ID")
	}

	tier, err := s.service.GetTicketAvailability(ctx, tierID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "tier not found")
	}
	ready, err := s.service.CheckAccountingReadiness(ctx, tierID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "accounting readiness unavailable")
	}

	return &eventv1.TicketAvailabilityResponse{
		TierId:            tier.ID.String(),
		AvailableQuantity: tier.AvailableQuantity,
		Version:           tier.Version,
		PriceCents:        tier.PriceCents,
		AccountingReady:   ready,
	}, nil
}

func (s *EventServer) UpdateTicketAvailability(ctx context.Context, req *eventv1.UpdateTicketAvailabilityRequest) (*eventv1.TicketTier, error) {
	tierID, err := uuid.Parse(req.TierId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tier ID")
	}

	tier, err := s.service.UpdateTicketAvailability(ctx, tierID, req.QuantityDelta, req.ExpectedVersion, req.Mode)
	if err != nil {
		if errors.Is(err, repository.ErrConflict) {
			return nil, status.Error(codes.Aborted, "version conflict - retry")
		}
		if errors.Is(err, repository.ErrInsufficientTickets) {
			return nil, status.Error(codes.FailedPrecondition, "insufficient tickets")
		}
		return nil, status.Error(codes.Internal, "failed to update availability")
	}

	ready, err := s.service.CheckAccountingReadiness(ctx, tierID)
	if err != nil {
		s.logger.Warn("accounting readiness unavailable for tier update", zap.Error(err))
	}
	return toTicketTier(tier, map[uuid.UUID]bool{tierID: err == nil && ready}), nil
}

// catalogReadiness resolves the authoritative readiness projection for every
// tier of the given events in one query. A readiness failure is conservative:
// the catalog reports those tiers as not ready instead of guessing, and the
// sale-side guard stays authoritative.
func (s *EventServer) catalogReadiness(ctx context.Context, events ...*domain.Event) map[uuid.UUID]bool {
	ids := make([]uuid.UUID, 0)
	for _, e := range events {
		if e == nil {
			continue
		}
		for _, t := range e.Tiers {
			ids = append(ids, t.ID)
		}
	}
	if len(ids) == 0 {
		return map[uuid.UUID]bool{}
	}
	ready, err := s.service.CheckAccountingReadinessBatch(ctx, ids)
	if err != nil {
		s.logger.Warn("accounting readiness unavailable for catalog", zap.Error(err))
		return map[uuid.UUID]bool{}
	}
	return ready
}

func toEventDetail(e *domain.Event, readiness map[uuid.UUID]bool) *eventv1.EventDetail {
	publicationStatus := e.PublicationStatus
	if publicationStatus == "" && e.Status == "active" {
		publicationStatus = domain.PublicationPublished
	}
	detail := &eventv1.EventDetail{
		Id:                e.ID.String(),
		Title:             e.Title,
		Description:       e.Description,
		Category:          e.Category,
		Venue:             e.Venue,
		Location:          e.Location,
		Date:              timestamppb.New(e.Date),
		ImageUrl:          e.ImageURL,
		Status:            e.Status,
		CreatedAt:         timestamppb.New(e.CreatedAt),
		DateVersion:       e.DateVersion,
		PublicationStatus: publicationStatus,
		SalesStatus:       domain.EffectiveSalesStatus(e, time.Now()),
	}
	if e.SalesStartAt != nil {
		detail.SalesStartAt = timestamppb.New(*e.SalesStartAt)
	}
	if e.SalesEndAt != nil {
		detail.SalesEndAt = timestamppb.New(*e.SalesEndAt)
	}
	for _, t := range e.Tiers {
		detail.Tiers = append(detail.Tiers, toTicketTier(&t, readiness))
	}
	return detail
}

func toTicketTier(t *domain.TicketTier, readiness map[uuid.UUID]bool) *eventv1.TicketTier {
	return &eventv1.TicketTier{
		Id:                t.ID.String(),
		EventId:           t.EventID.String(),
		Name:              t.Name,
		PriceCents:        t.PriceCents,
		TotalQuantity:     t.TotalQuantity,
		AvailableQuantity: t.AvailableQuantity,
		Version:           t.Version,
		AccountingReady:   readiness[t.ID],
	}
}

func (s *EventServer) GetSeatsOfEventByTierId(ctx context.Context, req *eventv1.GetSeatsRequest) (*eventv1.GetSeatsResponse, error) {
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}

	var tierID *uuid.UUID
	if req.TicketTierId != "" {
		id, err := uuid.Parse(req.TicketTierId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid tier ID")
		}
		tierID = &id
	}

	seats, err := s.service.GetSeats(ctx, eventID, tierID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		return nil, status.Error(codes.Unavailable, "failed to get seats")
	}

	var protoSeats []*eventv1.Seat
	for _, seat := range seats {
		protoSeats = append(protoSeats, toSeat(seat))
	}

	return &eventv1.GetSeatsResponse{
		Seats: protoSeats,
	}, nil
}

func (s *EventServer) UpdateSeatStatus(ctx context.Context, req *eventv1.UpdateSeatStatusRequest) (*eventv1.UpdateSeatStatusResponse, error) {
	seatID, err := uuid.Parse(req.SeatId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid seat ID")
	}

	// Validate status
	seatStatus := domain.SeatStatus(req.Status)
	validStatus := []domain.SeatStatus{
		domain.SeatStatusAvailable,
		domain.SeatStatusBooked,
		domain.SeatStatusReserved,
	}
	if !slices.Contains(validStatus, seatStatus) {
		return nil, status.Error(codes.InvalidArgument, "invalid status")
	}

	var bookingID *uuid.UUID
	if req.BookingId != "" {
		id, err := uuid.Parse(req.BookingId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid booking ID")
		}
		bookingID = &id
	}

	seat, err := s.service.UpdateSeatStatus(ctx, seatID, seatStatus, bookingID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "seat not found")
		}
		if errors.Is(err, repository.ErrSeatNotAvailable) {
			return nil, status.Error(codes.FailedPrecondition, "seat not available")
		}
		return nil, status.Error(codes.Internal, "failed to update seat status")
	}

	return &eventv1.UpdateSeatStatusResponse{
		Seat: toSeat(seat),
	}, nil
}

func (s EventServer) UpdateBatchSeatStatus(ctx context.Context, req *eventv1.UpdateBatchSeatStatusRequest) (*emptypb.Empty, error) {
	seatIds := []uuid.UUID{}
	for _, val := range req.SeatIds {
		seatId, err := uuid.Parse(val)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid seat ID")
		}
		seatIds = append(seatIds, seatId)
	}

	// Validate status
	seatStatus := domain.SeatStatus(req.Status)
	validStatus := []domain.SeatStatus{
		domain.SeatStatusAvailable,
		domain.SeatStatusBooked,
		domain.SeatStatusReserved,
	}
	if !slices.Contains(validStatus, seatStatus) {
		return nil, status.Error(codes.InvalidArgument, "invalid status")
	}

	var bookingID *uuid.UUID
	if req.BookingId != "" {
		id, err := uuid.Parse(req.BookingId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid booking ID")
		}
		bookingID = &id
	}

	eventID := uuid.Nil
	if req.EventId != "" {
		parsedEventID, err := uuid.Parse(req.EventId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid event ID")
		}
		eventID = parsedEventID
	}

	err := s.service.UpdateBatchSeatStatus(ctx, eventID, seatIds, seatStatus, bookingID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "seat not found")
		}
		if errors.Is(err, repository.ErrSeatNotAvailable) {
			return nil, status.Error(codes.FailedPrecondition, "seat not available")
		}
		return nil, status.Error(codes.Internal, "failed to update seat status "+err.Error())
	}

	return nil, nil
}

func toSeat(s *domain.Seat) *eventv1.Seat {
	positionJSON, _ := json.Marshal(s.Position)

	seat := &eventv1.Seat{
		Id:           s.ID.String(),
		EventId:      s.EventID.String(),
		TicketTierId: s.TicketTierID.String(),
		Status:       string(s.Status),
		Position:     string(positionJSON),
		CreatedAt:    timestamppb.New(s.CreatedAt),
		UpdatedAt:    timestamppb.New(s.UpdatedAt),
		SeatRevision: s.SeatRevision,
	}

	if s.BookingID != nil {
		seat.BookingId = s.BookingID.String()
	}
	if s.OrderID != nil {
		seat.OrderId = s.OrderID.String()
	}

	return seat
}

func (s *EventServer) UpdateBatchTicketAvailability(ctx context.Context, req *eventv1.UpdateTicketAvailabilityRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "batch ticket availability is not supported")
}

func (s *EventServer) GetSeatsBySeatIds(ctx context.Context, req *eventv1.GetSeatsBySeatIdsRequest) (*eventv1.GetSeatsResponse, error) {
	seatUUIDs := []uuid.UUID{}
	for _, seatId := range req.SeatIds {
		uid, err := uuid.Parse(seatId)
		if err != nil {
			s.logger.Sugar().Errorf("[Event][GetSeatsBySeatIds]: Existing invalid UUID seat in request", zap.Error(err))
			return nil, fmt.Errorf("Existing invalid UUID seat in request: %w", err)
		}
		seatUUIDs = append(seatUUIDs, uid)
	}

	domainSeats, err := s.service.GetSeatsBySeatIds(ctx, seatUUIDs)
	if err != nil {
		return nil, err
	}
	if len(domainSeats) == 0 {
		return nil, fmt.Errorf("Get seats by seatIds fail: seat not found")
	}
	seats := []*eventv1.Seat{}
	for _, seat := range domainSeats {
		resSeat := toSeat(&seat)
		seats = append(seats, resSeat)
	}
	return &eventv1.GetSeatsResponse{
		Seats: seats,
	}, nil
}

func (s *EventServer) VerifyBookingTicketEligibility(ctx context.Context, req *eventv1.VerifyBookingTicketEligibilityRequest) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	if req == nil || req.Generation != 1 || len(req.SeatIds) == 0 || len(req.SeatIds) > 512 {
		return nil, status.Error(codes.InvalidArgument, "valid booking lifecycle and seat set required")
	}
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	eventID, err := uuid.Parse(req.EventId)
	if err != nil || eventID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event id")
	}
	seatIDs := make([]uuid.UUID, 0, len(req.SeatIds))
	seen := make(map[uuid.UUID]struct{}, len(req.SeatIds))
	for _, raw := range req.SeatIds {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid seat id")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "duplicate seat id")
		}
		seen[id] = struct{}{}
		seatIDs = append(seatIDs, id)
	}
	eligible, reason, err := s.service.VerifyBookingTicketEligibility(ctx, bookingID, eventID, req.Generation, seatIDs)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "event seat eligibility unavailable")
	}
	state := eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_DENIED
	if eligible {
		state = eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE
	}
	return &eventv1.VerifyBookingTicketEligibilityResponse{State: state, ReasonCode: reason}, nil
}

func (s *EventServer) AcquireUnusedRefundTimePermit(ctx context.Context, req *eventv1.RefundTimeBinding) (*eventv1.RefundTimePermit, error) {
	binding, err := refundTimeBindingFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid refund time binding required")
	}
	if s.refundTimeAuthority == nil {
		return nil, status.Error(codes.Unavailable, "Event refund time authority unavailable")
	}
	permit, err := s.refundTimeAuthority.Acquire(ctx, binding)
	if err != nil {
		return nil, refundTimeAuthorityStatus(err)
	}
	return refundTimePermitToProto(binding, permit)
}

func (s *EventServer) ReleaseUnusedRefundTimePermit(ctx context.Context, req *eventv1.ReleaseUnusedRefundTimePermitRequest) (*eventv1.RefundTimePermit, error) {
	if req == nil || !validRefundTimeOutcome(req.GetOutcome()) {
		return nil, status.Error(codes.InvalidArgument, "valid refund time binding and terminal outcome required")
	}
	binding, err := refundTimeBindingFromProto(req.GetBinding())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid refund time binding required")
	}
	if s.refundTimeAuthority == nil {
		return nil, status.Error(codes.Unavailable, "Event refund time authority unavailable")
	}
	permit, err := s.refundTimeAuthority.Release(ctx, binding, req.GetOutcome())
	if err != nil {
		return nil, refundTimeAuthorityStatus(err)
	}
	return refundTimePermitToProto(binding, permit)
}

func refundTimeBindingFromProto(req *eventv1.RefundTimeBinding) (refundtime.Binding, error) {
	if req == nil {
		return refundtime.Binding{}, repository.ErrInvalidRefundTimeBinding
	}
	ids := []*uuid.UUID{}
	for _, raw := range []string{req.GetAttemptId(), req.GetRequestId(), req.GetBookingId(), req.GetPaymentId(), req.GetEventId(), req.GetApproverId()} {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil {
			return refundtime.Binding{}, repository.ErrInvalidRefundTimeBinding
		}
		ids = append(ids, &id)
	}
	binding := refundtime.Binding{
		AttemptID: *ids[0], RequestID: *ids[1], BookingID: *ids[2], PaymentID: *ids[3],
		EventID: *ids[4], ApproverID: *ids[5], Generation: req.GetGeneration(), Fingerprint: req.GetFingerprint(),
	}
	if !binding.Valid() {
		return refundtime.Binding{}, repository.ErrInvalidRefundTimeBinding
	}
	return binding, nil
}

func refundTimePermitToProto(binding refundtime.Binding, permit refundtime.Permit) (*eventv1.RefundTimePermit, error) {
	if permit.Binding != binding {
		return nil, status.Error(codes.Unavailable, "Event returned a mismatched refund time binding")
	}
	if permit.State == "QUALIFIED" && !permit.QualifiedFor(binding) {
		return nil, status.Error(codes.Unavailable, "Event returned invalid refund time qualification evidence")
	}
	if permit.State == "REJECTED" && (permit.StartsAt.IsZero() || permit.QualifiedAt.IsZero() || permit.QualifiedAt.Before(permit.StartsAt) || permit.Version <= 0) {
		return nil, status.Error(codes.Unavailable, "Event returned invalid refund time rejection evidence")
	}
	if permit.State != "QUALIFIED" && permit.State != "REJECTED" && permit.State != "RELEASED" {
		return nil, status.Error(codes.Unavailable, "Event returned an unknown refund time permit state")
	}
	result := &eventv1.RefundTimePermit{
		Binding: refundTimeBindingToProto(permit.Binding),
		State:   permit.State,
		Version: permit.Version,
	}
	if !permit.StartsAt.IsZero() {
		result.StartsAt = timestamppb.New(permit.StartsAt)
	}
	if !permit.QualifiedAt.IsZero() {
		result.QualifiedAt = timestamppb.New(permit.QualifiedAt)
	}
	return result, nil
}

func refundTimeBindingToProto(binding refundtime.Binding) *eventv1.RefundTimeBinding {
	return &eventv1.RefundTimeBinding{
		AttemptId: binding.AttemptID.String(), RequestId: binding.RequestID.String(), BookingId: binding.BookingID.String(),
		PaymentId: binding.PaymentID.String(), EventId: binding.EventID.String(), ApproverId: binding.ApproverID.String(),
		Generation: binding.Generation, Fingerprint: binding.Fingerprint,
	}
}

func validRefundTimeOutcome(outcome string) bool {
	return outcome == "APPROVED" || outcome == "REJECTED" || outcome == "ABORTED"
}

func refundTimeAuthorityStatus(err error) error {
	switch {
	case errors.Is(err, repository.ErrInvalidRefundTimeBinding), errors.Is(err, repository.ErrInvalidRefundTimeOutcome):
		return status.Error(codes.InvalidArgument, "valid refund time binding and terminal outcome required")
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, "event not found")
	case errors.Is(err, repository.ErrBindingMismatch), errors.Is(err, repository.ErrConflict):
		return status.Error(codes.Aborted, "refund time binding or terminal outcome conflict")
	default:
		return status.Error(codes.Unavailable, "Event refund time authority unavailable")
	}
}

func (s *EventServer) ReservedOrCompensateBatchSeats(ctx context.Context, req *eventv1.ReservedOrCompensateBatchSeatsReq) (*eventv1.ReservedOrCompensateBatchSeatsRes, error) {
	seatUUIDs := []uuid.UUID{}
	if len(req.SeatIds) == 0 {
		return nil, fmt.Errorf("[ReservedOrCompensateBatchSeats]: seatIds is required")
	}

	for _, seatId := range req.SeatIds {
		sUUID, err := uuid.Parse(seatId)
		if err != nil {
			return nil, fmt.Errorf("[ReservedOrCompensateBatchSeats]: seat id is invalid: %w", err)
		}
		seatUUIDs = append(seatUUIDs, sUUID)
	}
	if domain.ReserveSeatAction(req.Action) != domain.RESERVED_SEAT && domain.ReserveSeatAction(req.Action) != domain.COMPENSATE_SEAT {
		return nil, fmt.Errorf("[ReservedOrCompensateBatchSeats]: invalid action")
	}

	if req.ReservedByBookingId == "" {
		return nil, status.Error(codes.InvalidArgument, "reserved_by_booking_id is required for owner-safe seat transition")
	}
	reservedByBookingUUID, err := uuid.Parse(req.ReservedByBookingId)
	if err != nil {
		return nil, fmt.Errorf("[ReservedOrCompensateBatchSeats]: reserved_by_booking_id is invalid: %w", err)
	}
	eventID := uuid.Nil
	if req.EventId != "" {
		eventID, err = uuid.Parse(req.EventId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid event ID")
		}
	}

	result, err := s.service.ReservedOrCompensateBatchSeats(ctx, &service.ReservedOrCompensateBatchSeatsReq{
		EventID:               eventID,
		SeatIds:               seatUUIDs,
		Action:                domain.ReserveSeatAction(req.Action),
		ReservedByBookingId:   reservedByBookingUUID,
		ReservedTimeInMinutes: req.ReservationTimeInMinutes,
	})
	setReservationOutcomeTrailer(ctx, result)
	if err != nil {
		if errors.Is(err, admission.ErrInvalidRequest) {
			return nil, status.Error(codes.InvalidArgument, "invalid seat admission request")
		}
		if errors.Is(err, repository.ErrSaleNotAllowed) {
			return nil, status.Error(codes.FailedPrecondition, "event is not accepting new reservations")
		}
		if errors.Is(err, repository.ErrSeatNotAvailable) {
			return nil, status.Error(codes.FailedPrecondition, "seat reservation conflict")
		}
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "seat not found")
		}
		return nil, fmt.Errorf("[ReservedOrCompensateBatchSeats]: fail to %s: %w", req.Action, err)
	}
	return &eventv1.ReservedOrCompensateBatchSeatsRes{
		Success:           result.Success,
		RedisStatus:       result.RedisStatus,
		RedisFallback:     result.RedisFallback,
		PostgresAttempted: result.PostgresAttempted,
		PostgresWinner:    result.PostgresWinner,
		PostgresConflict:  result.PostgresConflict,
		PostgresError:     result.PostgresError,
		RedisLatencyNs:    result.RedisLatencyNs,
	}, nil

}

func setReservationOutcomeTrailer(ctx context.Context, outcome service.ReservationOutcome) {
	_ = grpc.SetTrailer(ctx, metadata.Pairs(
		"redis_status", outcome.RedisStatus,
		"redis_fallback", strconv.FormatBool(outcome.RedisFallback),
		"postgres_attempted", strconv.FormatBool(outcome.PostgresAttempted),
		"postgres_winner", strconv.FormatBool(outcome.PostgresWinner),
		"postgres_conflict", strconv.FormatBool(outcome.PostgresConflict),
		"postgres_error", strconv.FormatBool(outcome.PostgresError),
		"redis_latency_ns", strconv.FormatInt(outcome.RedisLatencyNs, 10),
	))
}

func (s *EventServer) UndoReservedExpiredSeats(ctx context.Context, req *eventv1.UndoReservedExpiredSeatsReq) (*eventv1.UndoReservedExpiredSeatsRes, error) {
	result, err := s.service.UndoReservedExpiredSeats(ctx)
	if err != nil {
		return nil, fmt.Errorf("[UndoReservedExpiredSeats]: fail to release expired seats: %w", err)
	}

	bookingSeatIdsMap := make(map[string]*eventv1.SeatIds, len(result.BookingIdSeatIdsMap))
	for bookingID, seatIDs := range result.BookingIdSeatIdsMap {
		ids := make([]string, len(seatIDs))
		for i, seatID := range seatIDs {
			ids[i] = seatID.String()
		}
		bookingSeatIdsMap[bookingID.String()] = &eventv1.SeatIds{SeatIds: ids}
	}

	return &eventv1.UndoReservedExpiredSeatsRes{
		BookingSeatIdsMap: bookingSeatIdsMap,
	}, nil
}

func (s *EventServer) GetReservedExpiredSeats(ctx context.Context, req *eventv1.GetReservedExpiredSeatsReq) (*eventv1.GetReservedExpiredSeatsRes, error) {
	result, err := s.service.GetReservedExpiredSeats(ctx)
	if err != nil {
		return nil, fmt.Errorf("[GetReservedExpiredSeats]: fail to get reserved expired seats: %w", err)
	}

	bookingSeatIdsMap := make(map[string]*eventv1.SeatIds, len(result.BookingIdSeatIdsMap))
	for bookingID, seatIDs := range result.BookingIdSeatIdsMap {
		ids := make([]string, len(seatIDs))
		for i, seatID := range seatIDs {
			ids[i] = seatID.String()
		}
		bookingSeatIdsMap[bookingID.String()] = &eventv1.SeatIds{SeatIds: ids}
	}

	return &eventv1.GetReservedExpiredSeatsRes{
		BookingSeatIdsMap: bookingSeatIdsMap,
	}, nil
}
