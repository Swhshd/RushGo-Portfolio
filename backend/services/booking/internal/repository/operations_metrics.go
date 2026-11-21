package repository

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
)

// Observes bounded durable work, never mutates it. Source availability is a
// separate gauge so a failed query cannot turn an unknown backlog into zero.
func (r *PostgresBookingRepository) RunOperationsMetrics(ctx context.Context) {
	meter := otel.Meter("github.com/ticketbox/booking/operations")
	source, e1 := meter.Int64Gauge("rushgo.booking.operations.source_available")
	admission, e2 := meter.Int64Gauge("rushgo.booking.admission_pending_stale")
	outbox, e3 := meter.Int64Gauge("rushgo.booking.outbox_stale")
	observed, e4 := meter.Int64Gauge("rushgo.booking.operations.observed_at_seconds")
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		var a, o int64
		err := r.pool.QueryRow(probe, `SELECT
		 (SELECT count(*) FROM (SELECT 1 FROM ticket_admission_intents WHERE state='PENDING' AND created_at<clock_timestamp()-interval '5 minutes' LIMIT 10001) a),
		 (SELECT count(*) FROM (SELECT 1 FROM outbox WHERE NOT published AND created_at<clock_timestamp()-interval '5 minutes' LIMIT 10001) o)`).Scan(&a, &o)
		cancel()
		if err != nil {
			source.Record(ctx, 0)
		} else {
			source.Record(ctx, 1)
			admission.Record(ctx, a)
			outbox.Record(ctx, o)
			observed.Record(ctx, time.Now().Unix())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
