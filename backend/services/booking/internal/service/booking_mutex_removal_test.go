package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
)

type mutexRemovalEventClient struct {
	eventv1.EventServiceClient
	eventID uuid.UUID
	seatID  uuid.UUID
	tierID  uuid.UUID
}

func (c *mutexRemovalEventClient) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	return &eventv1.EventDetail{Id: c.eventID.String()}, nil
}

func (c *mutexRemovalEventClient) CheckEventSaleEligibility(context.Context, *eventv1.CheckEventSaleEligibilityRequest, ...grpc.CallOption) (*eventv1.CheckEventSaleEligibilityResponse, error) {
	return &eventv1.CheckEventSaleEligibilityResponse{State: eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_ELIGIBLE}, nil
}

func (c *mutexRemovalEventClient) GetSeatsBySeatIds(context.Context, *eventv1.GetSeatsBySeatIdsRequest, ...grpc.CallOption) (*eventv1.GetSeatsResponse, error) {
	return &eventv1.GetSeatsResponse{Seats: []*eventv1.Seat{{
		Id: c.seatID.String(), EventId: c.eventID.String(), TicketTierId: c.tierID.String(), Status: "available",
	}}}, nil
}

func (c *mutexRemovalEventClient) GetTicketAvailability(context.Context, *eventv1.GetTicketAvailabilityRequest, ...grpc.CallOption) (*eventv1.TicketAvailabilityResponse, error) {
	return &eventv1.TicketAvailabilityResponse{TierId: c.tierID.String(), PriceCents: 100, AccountingReady: true}, nil
}

type mutexRemovalSagaClient struct {
	sagav1.SagaOrchestratorServiceClient
}

func (mutexRemovalSagaClient) StartOrderSaga(context.Context, *sagav1.StartOrderSagaRequest, ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	return &sagav1.OrderSagaResponse{Status: "success", PaymentIntentClientSecret: "test_secret"}, nil
}

type mutexRemovalBookingRepo struct {
	repository.LifecycleRepository
	created *domain.Booking
}

func (r *mutexRemovalBookingRepo) Create(_ context.Context, booking *domain.Booking) error {
	r.created = booking
	return nil
}
func (r *mutexRemovalBookingRepo) GetByID(context.Context, uuid.UUID) (*domain.Booking, error) {
	return nil, repository.ErrNotFound
}
func (r *mutexRemovalBookingRepo) ListByUserID(context.Context, uuid.UUID, int, int) ([]*domain.Booking, int, error) {
	return nil, 0, nil
}
func (r *mutexRemovalBookingRepo) UpdateStatus(context.Context, uuid.UUID, domain.BookingStatus) error {
	return nil
}
func (r *mutexRemovalBookingRepo) CompareAndSetStatus(context.Context, uuid.UUID, domain.BookingStatus, domain.BookingStatus) (bool, error) {
	return true, nil
}
func (r *mutexRemovalBookingRepo) ConfirmBookingWithOutbox(context.Context, uuid.UUID, repository.OutboxEvent) (bool, error) {
	return true, nil
}
func (r *mutexRemovalBookingRepo) ProcessPendingOutbox(context.Context, int, repository.OutboxPublisher) (int, error) {
	return 0, nil
}
func (r *mutexRemovalBookingRepo) BatchUpdateStatus(context.Context, []uuid.UUID, domain.BookingStatus) error {
	return nil
}
func (r *mutexRemovalBookingRepo) GetLifecycle(context.Context, domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	if r.created == nil {
		return nil, repository.ErrNotFound
	}
	return &domain.LifecycleSnapshot{Booking: r.created, Generation: 1, Phase: "OPEN", Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: r.created.ReservationDeadline, DispatchState: "pending"}, nil
}

func (r *mutexRemovalBookingRepo) CreateOrGetRequest(ctx context.Context, c domain.CreateBookingCommand) (*domain.LifecycleSnapshot, bool, error) {
	r.created = c.Booking
	r.created.ReservationDeadline = &c.ReservationDeadline
	v, err := r.GetLifecycle(ctx, domain.LifecycleRef{BookingID: c.Booking.ID, Generation: 1})
	return v, true, err
}
func (r *mutexRemovalBookingRepo) DecideLifecycle(context.Context, domain.LifecycleDecision, *repository.OutboxEvent) (*domain.LifecycleSnapshot, bool, error) {
	return nil, false, repository.ErrNotFound
}
func (r *mutexRemovalBookingRepo) ListExpiryCandidates(context.Context, int, uuid.UUID) ([]domain.LifecycleSnapshot, error) {
	return nil, nil
}

func TestCreateBookingDoesNotRequireLegacyBookingRedisMutex(t *testing.T) {
	eventID := uuid.New()
	tierID := uuid.New()
	seatID := uuid.New()
	repo := &mutexRemovalBookingRepo{}
	eventClient := &mutexRemovalEventClient{eventID: eventID, tierID: tierID, seatID: seatID}
	sagaClient := mutexRemovalSagaClient{}
	svc := NewBookingService(repo, eventClient, sagaClient, zap.NewNop())

	result, err := svc.CreateBooking(context.Background(), uuid.New(), eventID, []domain.BookingItem{{
		TicketTierID: tierID,
		Quantity:     1,
		SeatIDs:      []uuid.UUID{seatID},
	}}, "pessimistic", "")
	if err != nil {
		t.Fatalf("CreateBooking returned error without legacy mutex: %v", err)
	}
	if result == nil || repo.created == nil {
		t.Fatal("CreateBooking did not persist a booking")
	}
}
