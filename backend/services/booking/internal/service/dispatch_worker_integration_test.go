package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"go.uber.org/zap"
)

func TestBookingDispatchRevokedClaimCannotCallSagaIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	saga := &requestSaga{e: e, err: errors.New("unknown")}
	s := NewBookingService(r, e, saga, zap.NewNop())
	first, err := s.CreateBooking(context.Background(), uuid.New(), e.event, []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}, "", "revoked")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE bookings SET dispatch_due_at=clock_timestamp() WHERE id=$1`, first.Booking.ID); err != nil {
		t.Fatal(err)
	}
	c, err := r.ClaimBookingDispatch(context.Background(), first.Booking.ID, 30*time.Second)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE bookings SET dispatch_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, first.Booking.ID); err != nil {
		t.Fatal(err)
	}
	saga.mu.Lock()
	calls := saga.calls
	saga.mu.Unlock()
	w, err := NewDispatchWorker(r, s, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.execute(context.Background(), *c); !errors.Is(err, repository.ErrLifecycleConflict) {
		t.Fatal("revoked ownership not reported", err)
	}
	saga.mu.Lock()
	defer saga.mu.Unlock()
	if saga.calls != calls {
		t.Fatal("revoked dispatch issued remote effect")
	}
}

func TestBookingDispatchStartupPeriodicAndRetryRetentionIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	saga := &requestSaga{e: e, err: errors.New("unknown")}
	s := NewBookingService(r, e, saga, zap.NewNop())
	config := DefaultDispatchConfig()
	config.MaxAttempts = 2
	if err := s.ConfigureDispatch(config); err != nil {
		t.Fatal(err)
	}
	first, err := s.CreateBooking(context.Background(), uuid.New(), e.event, []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}, "", "startup")
	if err != nil {
		t.Fatal(err)
	}
	makeDue := func() {
		t.Helper()
		if _, err := p.Exec(context.Background(), `UPDATE bookings SET dispatch_due_at=clock_timestamp() WHERE id=$1`, first.Booking.ID); err != nil {
			t.Fatal(err)
		}
	}
	makeDue()
	w, err := NewDispatchWorker(r, s, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time)
	scans := make(chan struct{}, 4)
	w.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	w.jitter = func(d time.Duration) time.Duration { scans <- struct{}{}; return d }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	await := func() {
		t.Helper()
		select {
		case <-scans:
		case <-time.After(2 * time.Second):
			t.Fatal("dispatch scan absent")
		}
	}
	await()
	// The scheduling barrier fires before FinishDispatch. Lock the row until
	// that completion releases its claim, rather than sleeping for ordering.
	settled := func() {
		t.Helper()
		bounded, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		for {
			var state string
			var category string
			if err := p.QueryRow(bounded, `SELECT dispatch_state,COALESCE(dispatch_error,'') FROM bookings WHERE id=$1`, first.Booking.ID).Scan(&state, &category); err != nil {
				t.Fatal(err)
			}
			if state == "pending" && category == "retry_age_attention" {
				return
			}
			if bounded.Err() != nil {
				t.Fatal(bounded.Err())
			}
		}
	}
	settled()
	makeDue()
	ticks <- time.Now()
	await()
	settled()
	var attempts int
	if err := p.QueryRow(context.Background(), `SELECT dispatch_attempts FROM bookings WHERE id=$1`, first.Booking.ID).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatal("threshold dropped responsibility or scan missing", attempts, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker shutdown blocked")
	}
}
