package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
)

type BookingServer struct {
	bookingv1.UnimplementedBookingServiceServer
	service                          *service.BookingService
	tickets                          *service.TicketManager
	refunds                          *service.UnusedTicketRefundService
	refundsSecure                    bool
	ticketAuthenticator              TicketPrincipalAuthenticator
	adminOutboxAuth                  TicketPrincipalAuthenticator
	adminOutbox                      *service.AdminOutboxService
	adminOutboxSecure                bool
	adminEventTicketOperations       *service.AdminEventTicketOperationsService
	adminEventTicketOperationsSecure bool
	adminBookingDetailSecure         bool
	adminEventTicketList             *service.AdminEventTicketListService
	adminEventTicketListSecure       bool
	logger                           *zap.Logger
}

func NewBookingServer(svc *service.BookingService, logger *zap.Logger, tickets ...*service.TicketManager) *BookingServer {
	server := &BookingServer{service: svc, logger: logger}
	if len(tickets) > 0 {
		server.tickets = tickets[0]
	}
	return server
}

func (s *BookingServer) SetTicketPrincipalAuthenticator(authenticator TicketPrincipalAuthenticator) {
	s.ticketAuthenticator = authenticator
}

func (s *BookingServer) SetAdminOutboxPrincipalAuthenticator(authenticator TicketPrincipalAuthenticator) {
	s.adminOutboxAuth = authenticator
}

func (s *BookingServer) SetAdminOutboxService(adminOutbox *service.AdminOutboxService) {
	s.adminOutbox = adminOutbox
}

func (s *BookingServer) SetAdminOutboxSecure(enabled bool) {
	s.adminOutboxSecure = enabled
}

func (s *BookingServer) SetAdminEventTicketOperationsService(operations *service.AdminEventTicketOperationsService) {
	s.adminEventTicketOperations = operations
}

func (s *BookingServer) SetAdminEventTicketOperationsSecure(enabled bool) {
	s.adminEventTicketOperationsSecure = enabled
}

func (s *BookingServer) SetAdminBookingDetailSecure(enabled bool) {
	s.adminBookingDetailSecure = enabled
}

func (s *BookingServer) SetAdminEventTicketListService(ticketList *service.AdminEventTicketListService) {
	s.adminEventTicketList = ticketList
}

func (s *BookingServer) SetAdminEventTicketListSecure(enabled bool) {
	s.adminEventTicketListSecure = enabled
}

func (s *BookingServer) authenticateTicketPrincipal(ctx context.Context) (TicketPrincipal, error) {
	if s == nil || s.ticketAuthenticator == nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "ticket authentication unavailable")
	}
	return s.ticketAuthenticator.Authenticate(ctx)
}

func (s *BookingServer) authenticateAdminOutboxPrincipal(ctx context.Context) (TicketPrincipal, error) {
	if s == nil || s.adminOutboxAuth == nil {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "Admin outbox authorization unavailable")
	}
	return s.adminOutboxAuth.Authenticate(ctx)
}

func (s *BookingServer) ListOwnedTickets(ctx context.Context, req *bookingv1.ListOwnedTicketsRequest) (*bookingv1.ListOwnedTicketsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	principal, err := s.authenticateTicketPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	userID, err := parseTicketAPIUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user identity")
	}
	if userID != principal.UserID {
		return nil, status.Error(codes.PermissionDenied, "ticket owner authorization denied")
	}
	if s.tickets == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	tickets, err := s.tickets.ListOwnedTickets(ctx, userID)
	if err != nil {
		return nil, status.Error(codes.Internal, "ticket list unavailable")
	}
	response := &bookingv1.ListOwnedTicketsResponse{Tickets: make([]*bookingv1.TicketDetail, 0, len(tickets))}
	for _, ticket := range tickets {
		response.Tickets = append(response.Tickets, ticketDetail(ticket))
	}
	return response, nil
}

func (s *BookingServer) RotateTicketCredential(ctx context.Context, req *bookingv1.RotateTicketCredentialRequest) (*bookingv1.TicketCredentialResponse, error) {
	if req == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	principal, err := s.authenticateTicketPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	userID, err := parseTicketAPIUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user identity")
	}
	if userID != principal.UserID {
		return nil, status.Error(codes.PermissionDenied, "ticket owner authorization denied")
	}
	if s.tickets == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	ticketID, err := parseTicketAPIUUID(req.TicketId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid ticket identity")
	}
	key := req.GetIdempotencyKey()
	if key == "" {
		// Older Gateways omit this additive field. Preserve their one-shot
		// calls; response-loss recovery requires an explicit stable key.
		key = uuid.NewString()
	}
	credential, err := s.tickets.RotateCredential(ctx, userID, ticketID, key)
	if errors.Is(err, repository.ErrTicketOwnerDenied) {
		return nil, status.Error(codes.PermissionDenied, "ticket owner authorization denied")
	}
	if errors.Is(err, repository.ErrTicketCredentialMissing) {
		return nil, status.Error(codes.NotFound, "ticket unavailable")
	}
	if errors.Is(err, repository.ErrTicketNotEligible) {
		return nil, status.Error(codes.FailedPrecondition, "ticket is not eligible for a credential")
	}
	if errors.Is(err, repository.ErrTicketIdempotencyKeyInvalid) {
		return nil, status.Error(codes.InvalidArgument, "invalid credential rotation idempotency key")
	}
	if errors.Is(err, repository.ErrTicketIdempotencyConflict) {
		return nil, status.Error(codes.AlreadyExists, "idempotency key conflicts with another ticket operation")
	}
	if errors.Is(err, repository.ErrTicketIdempotencyInProgress) {
		return nil, status.Error(codes.Aborted, "credential rotation is still in progress")
	}
	if errors.Is(err, service.ErrTicketAuthorityUnknown) {
		return nil, status.Error(codes.Unavailable, "ticket authority unavailable")
	}
	if errors.Is(err, service.ErrTicketNotEligible) {
		return nil, status.Error(codes.FailedPrecondition, "ticket is not eligible for a credential")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "ticket credential service unavailable")
	}
	return &bookingv1.TicketCredentialResponse{Credential: credential.Token, ExpiresAt: timestamppb.New(credential.ExpiresAt), Outcome: string(credential.Outcome), CredentialVersion: credential.CredentialVersion}, nil
}

func (s *BookingServer) SetEventScannerAssignment(ctx context.Context, req *bookingv1.SetEventScannerAssignmentRequest) (*bookingv1.EventScannerAssignmentResponse, error) {
	if req == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "scanner assignment authorization denied")
	}
	eventID, err := parseTicketAPIUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	staffID, err := parseTicketAPIUUID(req.StaffUserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid staff identity")
	}
	actorID, err := parseTicketAPIUUID(req.ActorUserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid actor identity")
	}
	if actorID != principal.UserID {
		return nil, status.Error(codes.PermissionDenied, "scanner assignment authorization denied")
	}
	if s.tickets == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	assignment, err := s.tickets.SetScannerAssignment(ctx, eventID, staffID, actorID, req.Active)
	if err != nil {
		return nil, status.Error(codes.Internal, "scanner assignment update failed")
	}
	return scannerAssignment(assignment), nil
}

func (s *BookingServer) ListEventScannerAssignments(ctx context.Context, req *bookingv1.ListEventScannerAssignmentsRequest) (*bookingv1.ListEventScannerAssignmentsResponse, error) {
	if req == nil || s.tickets == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "scanner assignment authorization denied")
	}
	eventID, err := parseTicketAPIUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	assignments, err := s.tickets.ListScannerAssignments(ctx, eventID)
	if err != nil {
		return nil, status.Error(codes.Internal, "scanner assignments unavailable")
	}
	response := &bookingv1.ListEventScannerAssignmentsResponse{Assignments: make([]*bookingv1.EventScannerAssignmentResponse, 0, len(assignments))}
	for _, assignment := range assignments {
		response.Assignments = append(response.Assignments, scannerAssignment(assignment))
	}
	return response, nil
}

func (s *BookingServer) CheckInTicket(ctx context.Context, req *bookingv1.CheckInTicketRequest) (*bookingv1.CheckInTicketResponse, error) {
	if req == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	principal, err := s.authenticateTicketPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	eventID, err := parseTicketAPIUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event identity")
	}
	staffID, err := parseTicketAPIUUID(req.StaffUserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid staff identity")
	}
	if staffID != principal.UserID {
		return nil, status.Error(codes.PermissionDenied, "scanner identity authorization denied")
	}
	if s.tickets == nil {
		return nil, status.Error(codes.Unavailable, "ticket service unavailable")
	}
	if len(req.Credential) == 0 || len(req.Credential) > 128 || len(req.IdempotencyKey) == 0 || len(req.IdempotencyKey) > 128 {
		return nil, status.Error(codes.InvalidArgument, "invalid check-in request")
	}
	result, err := s.tickets.CheckIn(ctx, eventID, staffID, req.Credential, req.IdempotencyKey)
	if errors.Is(err, repository.ErrScannerNotAssigned) {
		return nil, status.Error(codes.PermissionDenied, "scanner is not assigned to this event")
	}
	if errors.Is(err, repository.ErrTicketCredentialMissing) {
		return nil, status.Error(codes.NotFound, "ticket credential invalid")
	}
	if errors.Is(err, repository.ErrTicketIdempotencyConflict) {
		return nil, status.Error(codes.AlreadyExists, "idempotency key conflicts with another scan")
	}
	if errors.Is(err, repository.ErrTicketIdempotencyInProgress) {
		return nil, status.Error(codes.Aborted, "ticket operation is still in progress")
	}
	if err != nil {
		return nil, ticketCheckInError(err)
	}
	response := &bookingv1.CheckInTicketResponse{ResultCode: result.ResultCode, ReasonCode: result.ReasonCode, Replayed: result.Replayed, EvidenceSource: result.EvidenceSource}
	if result.CheckedInAt != nil {
		response.CheckedInAt = timestamppb.New(*result.CheckedInAt)
	}
	if result.ObservedAt != nil {
		response.ObservedAt = timestamppb.New(*result.ObservedAt)
	}
	return response, nil
}

func parseTicketAPIUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid identity")
	}
	return id, nil
}

func ticketDetail(ticket domain.Ticket) *bookingv1.TicketDetail {
	detail := &bookingv1.TicketDetail{Id: ticket.ID.String(), BookingId: ticket.BookingID.String(), EventId: ticket.EventID.String(), SeatId: ticket.SeatID.String(), TicketTierId: ticket.TicketTierID.String(), Status: string(ticket.Status), BookingStatus: string(ticket.BookingStatus)}
	if !ticket.IssuedAt.IsZero() {
		detail.IssuedAt = timestamppb.New(ticket.IssuedAt)
	}
	if ticket.CheckedInAt != nil {
		detail.CheckedInAt = timestamppb.New(*ticket.CheckedInAt)
	}
	return detail
}

func scannerAssignment(value domain.EventScannerAssignment) *bookingv1.EventScannerAssignmentResponse {
	return &bookingv1.EventScannerAssignmentResponse{EventId: value.EventID.String(), StaffUserId: value.StaffUserID.String(), Active: value.Active, ChangedAt: timestamppb.New(value.ChangedAt)}
}

func (s *BookingServer) CreateBooking(ctx context.Context, req *bookingv1.CreateBookingRequest) (*bookingv1.BookingDetail, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking request")
	}
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user ID")
	}
	eventID, err := uuid.Parse(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event ID")
	}

	var items []domain.BookingItem
	for _, item := range req.Items {
		if item == nil {
			return nil, status.Error(codes.InvalidArgument, "invalid booking item")
		}
		tierID, err := uuid.Parse(item.TicketTierId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid tier ID")
		}

		var seatIDs []uuid.UUID
		for _, seatID := range item.SeatIds {
			id, err := uuid.Parse(seatID)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid seat ID")
			}
			seatIDs = append(seatIDs, id)
		}

		items = append(items, domain.BookingItem{
			TicketTierID: tierID,
			Quantity:     item.Quantity,
			SeatIDs:      seatIDs,
		})
	}

	mode := req.BookingMode
	if mode == "" {
		mode = "pessimistic"
	}

	res, err := s.service.CreateBooking(ctx, userID, eventID, items, mode, req.RequestId)
	if err != nil {
		if errors.Is(err, service.ErrEventNotOnSale) {
			return nil, status.Error(codes.FailedPrecondition, "event_not_on_sale")
		}
		if errors.Is(err, service.ErrEventSaleUnknown) {
			return nil, status.Error(codes.Unavailable, "event_sale_unavailable")
		}
		if errors.Is(err, repository.ErrRequestConflict) {
			return nil, status.Error(codes.AlreadyExists, "booking_request_conflict")
		}
		if errors.Is(err, repository.ErrInvalidBookingRequest) {
			return nil, status.Error(codes.InvalidArgument, "invalid_booking_request")
		}
		if errors.Is(err, service.ErrAccountingNotReady) {
			return nil, status.Error(codes.FailedPrecondition, "accounting_not_ready")
		}
		s.logger.Error("CreateBooking failed", zap.Error(err))
		return nil, status.Error(codes.Internal, "booking failed: "+err.Error())
	}
	return toBookingDetailV2(res), nil
}

func (s *BookingServer) GetBooking(ctx context.Context, req *bookingv1.GetBookingRequest) (*bookingv1.BookingDetail, error) {
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking ID")
	}

	booking, err := s.service.GetBooking(ctx, bookingID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "booking not found")
		}
		return nil, status.Error(codes.Internal, "failed to get booking")
	}

	return toBookingDetail(booking), nil
}

func (s *BookingServer) ListUserBookings(ctx context.Context, req *bookingv1.ListUserBookingsRequest) (*bookingv1.ListUserBookingsResponse, error) {
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user ID")
	}

	bookings, total, err := s.service.ListUserBookings(ctx, userID, int(req.Page), int(req.PageSize))
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list bookings")
	}

	var details []*bookingv1.BookingDetail
	for _, b := range bookings {
		details = append(details, toBookingDetail(b))
	}

	return &bookingv1.ListUserBookingsResponse{
		Bookings: details,
		Total:    int32(total),
	}, nil
}

func (s *BookingServer) CancelBooking(ctx context.Context, req *bookingv1.CancelBookingRequest) (*bookingv1.BookingDetail, error) {
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking ID")
	}
	userID, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user ID")
	}

	booking, err := s.service.CancelBooking(ctx, bookingID, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "booking not found")
		}
		if status.Code(err) == codes.Unavailable {
			return nil, err
		}
		if errors.Is(err, repository.ErrLifecycleConflict) {
			return nil, status.Error(codes.FailedPrecondition, "Booking cannot be cancelled in its current lifecycle")
		}
		return nil, status.Error(codes.Internal, "cancel failed")
	}

	return toBookingDetail(booking), nil
}

func toLifecycleDisposition(v *domain.LifecycleSnapshot) *bookingv1.LifecycleDisposition {
	result := &bookingv1.LifecycleDisposition{Booking: toBookingDetail(v.Booking), Generation: v.Generation, Phase: v.Phase,
		Reason: v.Reason, Version: v.Booking.Version, Currency: v.Currency, BookingMode: v.BookingMode, DispatchState: v.DispatchState}
	if v.DecisionID != nil {
		result.DecisionId = v.DecisionID.String()
	}
	if v.ConfirmationDecisionID != nil {
		result.ConfirmationDecisionId = v.ConfirmationDecisionID.String()
	}
	if v.ReservationDeadline != nil {
		result.ReservationDeadline = timestamppb.New(*v.ReservationDeadline)
	}
	return result
}
func lifecycleRPCError(err error) error {
	if errors.Is(err, repository.ErrNotFound) {
		return status.Error(codes.NotFound, "Booking not found")
	}
	if errors.Is(err, repository.ErrLifecycleConflict) || errors.Is(err, repository.ErrLegacyLifecycleAttention) {
		return status.Error(codes.FailedPrecondition, "Booking lifecycle does not permit this decision")
	}
	if errors.Is(err, repository.ErrInvalidLifecycleDecision) {
		return status.Error(codes.InvalidArgument, "invalid lifecycle decision")
	}
	return status.Error(codes.Internal, "Booking lifecycle persistence unavailable")
}
func (s *BookingServer) GetLifecycleDisposition(ctx context.Context, req *bookingv1.LifecycleRequest) (*bookingv1.LifecycleDisposition, error) {
	id, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Booking ID")
	}
	v, err := s.service.GetLifecycle(ctx, domain.LifecycleRef{BookingID: id, Generation: req.Generation})
	if err != nil {
		return nil, lifecycleRPCError(err)
	}
	return toLifecycleDisposition(v), nil
}
func (s *BookingServer) DecideLifecycle(ctx context.Context, req *bookingv1.LifecycleDecisionRequest) (*bookingv1.LifecycleDisposition, error) {
	id, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Booking ID")
	}
	decisionID, err := uuid.Parse(req.DecisionId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid decision ID")
	}
	d := domain.LifecycleDecision{Ref: domain.LifecycleRef{BookingID: id, Generation: req.Generation}, DecisionID: decisionID, Kind: req.Kind, Reason: req.Reason, ExpectedVersion: req.ExpectedVersion}
	if req.ConfirmationDecisionId != "" {
		claim, err := uuid.Parse(req.ConfirmationDecisionId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid confirmation decision ID")
		}
		d.ConfirmationDecisionID = &claim
	}
	v, err := s.service.DecideLifecycle(ctx, d)
	if err != nil {
		return nil, lifecycleRPCError(err)
	}
	return toLifecycleDisposition(v), nil
}
func (s *BookingServer) ListExpiryCandidates(ctx context.Context, req *bookingv1.ListExpiryCandidatesRequest) (*bookingv1.ListExpiryCandidatesResponse, error) {
	after := uuid.Nil
	if req.AfterId != "" {
		id, err := uuid.Parse(req.AfterId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid expiry cursor")
		}
		after = id
	}
	rows, err := s.service.ListExpiryCandidates(ctx, int(req.Limit), after)
	if err != nil {
		return nil, lifecycleRPCError(err)
	}
	result := &bookingv1.ListExpiryCandidatesResponse{}
	for i := range rows {
		result.Bookings = append(result.Bookings, toLifecycleDisposition(&rows[i]))
	}
	return result, nil
}

func toBookingDetail(b *domain.Booking) *bookingv1.BookingDetail {
	detail := &bookingv1.BookingDetail{
		Id:               b.ID.String(),
		UserId:           b.UserID.String(),
		EventId:          b.EventID.String(),
		Status:           string(b.Status),
		TotalAmountCents: b.TotalAmountCents,
		CreatedAt:        timestamppb.New(b.CreatedAt),
		Currency:         b.Currency,
	}
	for _, item := range b.Items {
		var seatIDs []string
		for _, id := range item.SeatIDs {
			seatIDs = append(seatIDs, id.String())
		}
		detail.Items = append(detail.Items, &bookingv1.BookingItem{
			Id:             item.ID.String(),
			TicketTierId:   item.TicketTierID.String(),
			TierName:       item.TierName,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
			SeatIds:        seatIDs,
		})
	}
	return detail
}

func toBookingDetailV2(b *service.CreateBookingResponse) *bookingv1.BookingDetail {
	detail := &bookingv1.BookingDetail{
		Id:                        b.Booking.ID.String(),
		UserId:                    b.Booking.UserID.String(),
		EventId:                   b.Booking.EventID.String(),
		Status:                    string(b.Booking.Status),
		TotalAmountCents:          b.Booking.TotalAmountCents,
		CreatedAt:                 timestamppb.New(b.Booking.CreatedAt),
		Currency:                  b.Booking.Currency,
		PaymentIntentClientSecret: b.PaymentClientSecret,
		CreationState:             b.CreationState,
		RetryAfterSeconds:         b.RetryAfterSeconds,
		LifecycleGeneration:       b.Booking.LifecycleGeneration,
		CheckoutState:             b.CheckoutState,
		CanCancel:                 b.CanCancel,
	}
	if b.ReservationDeadline != nil {
		detail.ReservationDeadline = timestamppb.New(*b.ReservationDeadline)
	}
	for _, item := range b.Booking.Items {
		var seatIDs []string
		for _, id := range item.SeatIDs {
			seatIDs = append(seatIDs, id.String())
		}
		detail.Items = append(detail.Items, &bookingv1.BookingItem{
			Id:             item.ID.String(),
			TicketTierId:   item.TicketTierID.String(),
			TierName:       item.TierName,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
			SeatIds:        seatIDs,
		})
	}
	return detail
}

func (s *BookingServer) UpdateBookingStatusById(ctx context.Context, req *bookingv1.UpdateBookingStatusByIdReq) (*emptypb.Empty, error) {
	bookingId, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("booking id invalid: %w", err)
	}
	err = s.service.UpdateBookingStatusById(ctx, bookingId, domain.BookingStatus(req.Status))
	return nil, err
}

func (s *BookingServer) UpdateBookingStatusByIds(ctx context.Context, req *bookingv1.UpdateBookingStatusByIdsReq) (*emptypb.Empty, error) {
	bookingStatus := domain.BookingStatus(req.Status)
	allowBookingStatus := map[domain.BookingStatus]bool{
		domain.StatusCancelled: true,
		domain.StatusConfirmed: true,
		domain.StatusExpired:   true,
		domain.StatusFailed:    true,
		domain.StatusPending:   true,
	}

	isAllow, ok := allowBookingStatus[bookingStatus]

	if !ok || !isAllow {
		return nil, fmt.Errorf("The booking status is invalid")
	}

	if len(req.BookingIds) == 0 {
		s.logger.Info("[BookingServer][UpdateBookingStatusByIds] Booking ids are empty, return")
		return nil, nil
	}

	bookingUUIDs := make([]uuid.UUID, 0, len(req.BookingIds))
	for _, bookingId := range req.BookingIds {
		bookingUUID, err := uuid.Parse(bookingId)
		if err != nil {
			s.logger.Error("[BookingServer] [UpdateBookingStatusByIds] The bookingId is not valid", zap.Error(err))
			return nil, err
		}
		bookingUUIDs = append(bookingUUIDs, bookingUUID)
	}
	err := s.service.UpdateBookingStatusByIds(ctx, bookingUUIDs, bookingStatus)
	return nil, err
}
