package service

import (
	"context"
	"time"

	"github.com/ticketbox/booking/internal/domain"
	"go.opentelemetry.io/otel"
)

// Reuses the read-only authority checks already used by the operations page.
// This is explicitly a bounded observation of the newest 100 orders in 31 days,
// not a second financial ledger or a global capacity assertion.
func (s *BookingService) RunOperationsMetrics(ctx context.Context) {
	meter := otel.Meter("github.com/ticketbox/booking/operations")
	paid, e1 := meter.Int64Gauge("rushgo.booking.observed_paid_unissued")
	complete, e2 := meter.Int64Gauge("rushgo.booking.payment_observation_available")
	capped, e3 := meter.Int64Gauge("rushgo.booking.payment_observation_capped")
	observed, e4 := meter.Int64Gauge("rushgo.booking.payment_observed_at_seconds")
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		now := time.Now().UTC()
		probe, cancel := context.WithTimeout(ctx, 10*time.Second)
		items, more, err := s.ListOperationsBookings(probe, domain.OperationsQuery{From: now.Add(-31 * 24 * time.Hour), To: now, Limit: 100})
		cancel()
		available, count, limit := int64(1), int64(0), int64(0)
		if err != nil {
			available = 0
		} else {
			for _, item := range items {
				if item.PaymentSourceState == "unavailable" {
					available = 0
				}
				for _, issue := range item.Issues {
					if issue.Kind == "paid_unissued" {
						count++
					}
				}
			}
			if more {
				limit = 1
			}
			paid.Record(ctx, count)
			capped.Record(ctx, limit)
			observed.Record(ctx, time.Now().Unix())
		}
		complete.Record(ctx, available)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
