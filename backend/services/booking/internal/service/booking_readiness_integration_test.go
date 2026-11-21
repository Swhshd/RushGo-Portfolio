package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"go.uber.org/zap"
)

func TestCreateBookingRejectsUnreadyTierBeforePersistenceIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	ready := false
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725, accountingReady: &ready}
	saga := &requestSaga{e: e}
	s := NewBookingService(r, e, saga, zap.NewNop())
	user := uuid.New()
	items := []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}

	if _, err := createKeyed(t, s, user, e.event, items, "unready-floor"); !errors.Is(err, ErrAccountingNotReady) {
		t.Fatalf("unready tier booking returned %v, want ErrAccountingNotReady", err)
	}

	ctx := context.Background()
	var bookings, itemsCount, outbox int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM bookings`).Scan(&bookings); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM booking_items`).Scan(&itemsCount); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if bookings != 0 || itemsCount != 0 || outbox != 0 {
		t.Fatalf("unready tier persisted side effects: bookings=%d items=%d outbox=%d", bookings, itemsCount, outbox)
	}
	saga.mu.Lock()
	calls := saga.calls
	saga.mu.Unlock()
	if calls != 0 {
		t.Fatalf("unready tier dispatched %d saga calls", calls)
	}
}
