package repository

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/topics"
)

func TestBookingConfirmationAndOutboxAreAtomicIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	bookingID := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO bookings (id, user_id, event_id, status) VALUES ($1, $2, $3, 'PENDING')`, bookingID, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	defer p.Exec(ctx, `DELETE FROM bookings WHERE id = $1`, bookingID)

	eventID := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO outbox (id, event_type, event_key, payload) VALUES ($1, $2, $3, '{}'::jsonb)`, eventID, topics.TypeBookingConfirmed, bookingID.String()); err != nil {
		t.Fatal(err)
	}
	defer p.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, eventID)

	changed, err := repo.ConfirmBookingWithOutbox(ctx, bookingID, OutboxEvent{
		ID: eventID, EventType: topics.TypeBookingConfirmed, EventKey: bookingID.String(), Payload: []byte(`{"booking_id":"` + bookingID.String() + `"}`), CreatedAt: time.Now(),
	})
	if err == nil || changed {
		t.Fatalf("duplicate outbox insert changed=%v err=%v, want rollback", changed, err)
	}

	var status string
	if err := p.QueryRow(ctx, `SELECT status FROM bookings WHERE id = $1`, bookingID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.StatusPending) {
		t.Fatalf("booking status=%s, want PENDING after outbox failure", status)
	}
}

func TestOutboxRelayFailureIsRetryableAndWorkersDoNotDoubleClaimIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	eventID := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO outbox (id, event_type, event_key, payload) VALUES ($1, $2, $3, '{}'::jsonb)`, eventID, topics.TypeBookingConfirmed, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	defer p.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, eventID)

	if count, err := repo.ProcessPendingOutbox(ctx, 1, func(context.Context, OutboxEvent) error {
		return errors.New("broker unavailable")
	}); err == nil || count != 0 {
		t.Fatalf("failed relay count=%d err=%v, want retryable failure", count, err)
	}
	var published bool
	if err := p.QueryRow(ctx, `SELECT published FROM outbox WHERE id = $1`, eventID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("failed publish marked outbox event as published")
	}

	var calls atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	counts := make(chan int, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			count, err := repo.ProcessPendingOutbox(ctx, 1, func(context.Context, OutboxEvent) error {
				calls.Add(1)
				time.Sleep(100 * time.Millisecond)
				return nil
			})
			counts <- count
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(counts)
	close(errs)
	if calls.Load() != 1 {
		t.Fatalf("publisher calls=%d, want one claimed row", calls.Load())
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var finalPublished bool
	if err := p.QueryRow(ctx, `SELECT published FROM outbox WHERE id = $1`, eventID).Scan(&finalPublished); err != nil {
		t.Fatal(err)
	}
	if !finalPublished {
		t.Fatal("successful relay did not mark outbox event published")
	}
}

func TestOutboxPublishSuccessBeforeMarkFailureRemainsPendingIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	eventID := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO outbox (id, event_type, event_key, payload) VALUES ($1, $2, $3, '{}'::jsonb)`, eventID, topics.TypeBookingConfirmed, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	defer p.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, eventID)

	publishCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if count, err := repo.ProcessPendingOutbox(publishCtx, 1, func(context.Context, OutboxEvent) error {
		cancel()
		return nil
	}); err == nil || count != 0 {
		t.Fatalf("publish-before-mark failure count=%d err=%v, want retryable error", count, err)
	}

	var published bool
	if err := p.QueryRow(ctx, `SELECT published FROM outbox WHERE id = $1`, eventID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("outbox event marked published after mark-delivered failure")
	}
}

func newBookingOutboxIntegrationFixture(t *testing.T) (*pgxpool.Pool, *PostgresBookingRepository) {
	t.Helper()
	p := bookingLifecycleFixture(t)
	return p, NewPostgresBookingRepository(p)
}
