package database

import (
	"context"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Enabled only with the existing telemetry pipeline. No DB name, DSN or user
// identity is exported. Each observation makes a bounded real readiness probe.
func registerHealthMetric(ctx context.Context, pool *pgxpool.Pool) {
	if os.Getenv("OTEL_ENABLED") != "true" {
		return
	}
	service := os.Getenv("SERVICE_NAME")
	switch service {
	case "user-service", "event-service", "booking-service", "payment-service", "notification-service", "saga-service", "scheduler-service":
	default:
		return
	}
	meter := otel.Meter("github.com/ticketbox/pkg/database")
	gauge, err := meter.Int64ObservableGauge("rushgo.database.available")
	if err != nil {
		return
	}
	registration, err := meter.RegisterCallback(func(observation context.Context, observer metric.Observer) error {
		probe, cancel := context.WithTimeout(observation, time.Second)
		defer cancel()
		available := int64(1)
		if pool.Ping(probe) != nil {
			available = 0
		}
		observer.ObserveInt64(gauge, available, metric.WithAttributes(attribute.String("db_service", service)))
		return nil
	}, gauge)
	if err == nil && ctx.Done() != nil {
		go func() { <-ctx.Done(); _ = registration.Unregister() }()
	}
}
