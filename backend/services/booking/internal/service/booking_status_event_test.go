package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc"
)

// fakeBookingRepo is an in-memory BookingRepository used to exercise the
// status-transition logic without a live database.
type fakeBookingRepo struct {
	repository.LifecycleRepository
	bookings        map[uuid.UUID]*domain.Booking
	confirmedEvents []repository.OutboxEvent
}

func newFakeBookingRepo() *fakeBookingRepo {
	return &fakeBookingRepo{bookings: map[uuid.UUID]*domain.Booking{}}
}

func (f *fakeBookingRepo) Create(_ context.Context, booking *domain.Booking) error {
	f.bookings[booking.ID] = booking
	return nil
}

func (f *fakeBookingRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Booking, error) {
	b, ok := f.bookings[id]
	if !ok {
		return nil, repository.ErrNotFound
	}
	return b, nil
}

func (f *fakeBookingRepo) ListByUserID(_ context.Context, _ uuid.UUID, _, _ int) ([]*domain.Booking, int, error) {
	return nil, 0, nil
}

func (f *fakeBookingRepo) UpdateStatus(_ context.Context, id uuid.UUID, status domain.BookingStatus) error {
	if b, ok := f.bookings[id]; ok {
		b.Status = status
	}
	return nil
}

func (f *fakeBookingRepo) CompareAndSetStatus(_ context.Context, id uuid.UUID, expected, status domain.BookingStatus) (bool, error) {
	b, ok := f.bookings[id]
	if !ok || b.Status != expected {
		return false, nil
	}
	b.Status = status
	return true, nil
}

func (f *fakeBookingRepo) BatchUpdateStatus(_ context.Context, ids []uuid.UUID, status domain.BookingStatus) error {
	for _, id := range ids {
		if b, ok := f.bookings[id]; ok {
			b.Status = status
		}
	}
	return nil
}

func (f *fakeBookingRepo) ConfirmBookingWithOutbox(_ context.Context, id uuid.UUID, event repository.OutboxEvent) (bool, error) {
	b, ok := f.bookings[id]
	if !ok || b.Status != domain.StatusPending {
		return false, nil
	}
	b.Status = domain.StatusConfirmed
	f.confirmedEvents = append(f.confirmedEvents, event)
	return true, nil
}

func (f *fakeBookingRepo) ProcessPendingOutbox(_ context.Context, _ int, _ repository.OutboxPublisher) (int, error) {
	return 0, nil
}

func (f *fakeBookingRepo) GetLifecycle(ctx context.Context, ref domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	b, err := f.GetByID(ctx, ref.BookingID)
	if err != nil {
		return nil, err
	}
	return &domain.LifecycleSnapshot{Booking: b, Generation: b.LifecycleGeneration, Phase: b.LifecyclePhase, Currency: b.Currency, BookingMode: b.BookingMode, ReservationDeadline: b.ReservationDeadline}, nil
}
func (f *fakeBookingRepo) DecideLifecycle(ctx context.Context, d domain.LifecycleDecision, event *repository.OutboxEvent) (*domain.LifecycleSnapshot, bool, error) {
	v, err := f.GetLifecycle(ctx, d.Ref)
	if err != nil {
		return nil, false, err
	}
	if d.Kind != "cancel" || event == nil {
		return v, false, repository.ErrInvalidLifecycleDecision
	}
	if v.Booking.Status == domain.StatusCancelled {
		v.DecisionID = &d.DecisionID
		return v, false, nil
	}
	if v.Booking.LifecyclePhase != "OPEN" || v.Booking.Version != d.ExpectedVersion {
		return v, false, nil
	}
	v.Booking.Status = domain.StatusCancelled
	v.Booking.LifecyclePhase = "TERMINAL"
	v.Booking.Version++
	v.Phase = "TERMINAL"
	v.DecisionID = &d.DecisionID
	f.confirmedEvents = append(f.confirmedEvents, *event)
	return v, true, nil
}
func (f *fakeBookingRepo) ListExpiryCandidates(context.Context, int, uuid.UUID) ([]domain.LifecycleSnapshot, error) {
	return nil, nil
}

type cancellationInventoryClient struct {
	eventv1.EventServiceClient
	restorationCalls int
}

func (c *cancellationInventoryClient) UpdateTicketAvailability(context.Context, *eventv1.UpdateTicketAvailabilityRequest, ...grpc.CallOption) (*eventv1.TicketTier, error) {
	c.restorationCalls++
	return &eventv1.TicketTier{}, nil
}

func TestCancellationDecisionDoesNotRestoreInventoryAndReplays(t *testing.T) {
	repo := newFakeBookingRepo()
	id, user := uuid.New(), uuid.New()
	deadline := time.Now().Add(time.Hour)
	repo.bookings[id] = &domain.Booking{ID: id, UserID: user, Status: domain.StatusPending, Version: 1, Managed: true,
		LifecycleGeneration: 1, LifecyclePhase: "OPEN", Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: &deadline,
		Items: []domain.BookingItem{{TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}}}
	eventClient := &cancellationInventoryClient{}
	svc := NewBookingService(repo, eventClient, nil, zap.NewNop())
	svc.SetCheckoutPaymentClient(&checkoutPayment{result: &paymentv1.CreatePaymentResponse{CreateState: "ready", PaymentIntentClientSecret: "original-unpaid-intent"}})
	for i := 0; i < 5; i++ {
		b, err := svc.CancelBooking(context.Background(), id, user)
		if err != nil {
			t.Fatal(err)
		}
		if b.Status != domain.StatusCancelled || b.Version != 2 {
			t.Errorf("cancel replay changed decision: status=%s version=%d", b.Status, b.Version)
		}
	}
	if eventClient.restorationCalls != 0 {
		t.Errorf("cancellation performed %d additive inventory restorations, want 0", eventClient.restorationCalls)
	}
	if len(repo.confirmedEvents) != 1 {
		t.Fatal("cancel did not retain one semantic terminal intent")
	}
}

func TestUpdateBookingStatusConfirmedCreatesOneOutboxIntent(t *testing.T) {
	repo := newFakeBookingRepo()
	bookingID := uuid.New()
	repo.bookings[bookingID] = &domain.Booking{ID: bookingID, Status: domain.StatusPending}
	svc := NewBookingService(repo, nil, nil, zap.NewNop())

	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusConfirmed); err != nil {
		t.Fatalf("first confirmation: %v", err)
	}
	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusConfirmed); err != nil {
		t.Fatalf("duplicate confirmation: %v", err)
	}
	if got := len(repo.confirmedEvents); got != 1 {
		t.Fatalf("expected one durable confirmation intent, got %d", got)
	}
}

func TestUpdateBookingStatusRetainsOneConfirmedSemanticIntent(t *testing.T) {
	repo := newFakeBookingRepo()
	svc := NewBookingService(repo, nil, nil, zap.NewNop())

	bookingID := uuid.New()
	booking := &domain.Booking{ID: bookingID, Status: domain.StatusPending}
	repo.bookings[bookingID] = booking

	// First transition to CONFIRMED must publish the event.
	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusConfirmed); err != nil {
		t.Fatalf("update status: %v", err)
	}
	if got := len(repo.confirmedEvents); got != 1 {
		t.Fatalf("expected exactly one confirmed outbox event after first transition, got %d", got)
	}
	if repo.confirmedEvents[0].EventKey != bookingID.String() {
		t.Fatalf("confirmed outbox event has wrong key: %s", repo.confirmedEvents[0].EventKey)
	}

	// Duplicate CONFIRMED (e.g. a re-delivered webhook) must NOT republish.
	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusConfirmed); err != nil {
		t.Fatalf("duplicate update status: %v", err)
	}
	if got := len(repo.confirmedEvents); got != 1 {
		t.Fatalf("expected confirmed outbox event to stay at one after duplicate transition, got %d", got)
	}
}

func TestUpdateBookingStatusFailureDoesNotPublishConfirmedEvent(t *testing.T) {
	repo := newFakeBookingRepo()
	svc := NewBookingService(repo, nil, nil, zap.NewNop())

	bookingID := uuid.New()
	repo.bookings[bookingID] = &domain.Booking{ID: bookingID, Status: domain.StatusPending}

	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusFailed); !errors.Is(err, repository.ErrLegacyLifecycleAttention) {
		t.Fatalf("unclassified terminal status must require attention: %v", err)
	}
	if got := len(repo.confirmedEvents); got != 0 {
		t.Fatalf("expected no confirmed outbox event on FAILED transition, got %d", got)
	}
}

func TestExpiredBookingCannotBeConfirmedByLatePaymentCallback(t *testing.T) {
	repo := newFakeBookingRepo()
	bookingID := uuid.New()
	repo.bookings[bookingID] = &domain.Booking{ID: bookingID, Status: domain.StatusExpired}
	svc := NewBookingService(repo, nil, nil, zap.NewNop())

	if err := svc.UpdateBookingStatusById(context.Background(), bookingID, domain.StatusConfirmed); err == nil {
		t.Fatal("late payment callback must not confirm an expired booking")
	}
	if repo.bookings[bookingID].Status != domain.StatusExpired {
		t.Fatalf("booking status=%s, want EXPIRED", repo.bookings[bookingID].Status)
	}
	if len(repo.confirmedEvents) != 0 {
		t.Fatal("late payment callback must not create a confirmation outbox event")
	}
}

func TestBatchConfirmedStatusCreatesOutboxIntent(t *testing.T) {
	repo := newFakeBookingRepo()
	firstID := uuid.New()
	secondID := uuid.New()
	repo.bookings[firstID] = &domain.Booking{ID: firstID, Status: domain.StatusPending}
	repo.bookings[secondID] = &domain.Booking{ID: secondID, Status: domain.StatusPending}
	svc := NewBookingService(repo, nil, nil, zap.NewNop())

	if err := svc.UpdateBookingStatusByIds(context.Background(), []uuid.UUID{firstID, secondID}, domain.StatusConfirmed); err != nil {
		t.Fatalf("batch confirmation: %v", err)
	}
	if got := len(repo.confirmedEvents); got != 2 {
		t.Fatalf("expected two confirmation outbox intents, got %d", got)
	}
}
