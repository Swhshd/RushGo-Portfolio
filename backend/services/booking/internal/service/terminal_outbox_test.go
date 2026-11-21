package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

func TestTerminalStatusAndBatchRetainOutboxIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	s := NewBookingService(r, nil, nil, zap.NewNop())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, status := range []domain.BookingStatus{domain.StatusFailed, domain.StatusCancelled, domain.StatusExpired} {
		t.Run(string(status), func(t *testing.T) {
			var ids []uuid.UUID
			for range 2 {
				id := uuid.New()
				ids = append(ids, id)
				if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline) VALUES($1,$2,$3,'PENDING',725,1,true,1,'OPEN','usd','pessimistic',$4)`, id, uuid.New(), uuid.New(), time.Now().Add(-time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			for range 5 {
				if err := s.UpdateBookingStatusByIds(ctx, ids, status); err != nil {
					t.Fatal("terminal batch", err)
				}
			}
			for _, id := range ids {
				var count int
				var payload []byte
				var typ string
				if err := p.QueryRow(ctx, `SELECT count(*),min(event_type),min(payload::text)::jsonb FROM outbox WHERE event_key=$1`, id.String()).Scan(&count, &typ, &payload); err != nil {
					t.Fatal(err)
				}
				want := topics.TypeBookingFailed
				if status == domain.StatusCancelled {
					want = topics.TypeBookingCancelled
				}
				if count != 1 || typ != want {
					t.Fatal("terminal status omitted durable intent", count, typ)
				}
			}
		})
	}
}
