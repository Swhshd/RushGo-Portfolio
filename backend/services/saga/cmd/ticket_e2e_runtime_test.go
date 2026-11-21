package main

import (
	"testing"

	"github.com/ticketbox/pkg/config"
)

func TestTicketE2ERuntimePinsSagaToIsolatedResources(t *testing.T) {
	valid := &config.Config{
		DatabaseURL:        "postgres://ticket_e2e:test-only@ticket-e2e-postgres:5432/rushgo_ticket_e2e_saga?sslmode=disable",
		KafkaBrokers:       []string{"ticket-e2e-kafka:9092"},
		GRPCPort:           "50055",
		EventServiceAddr:   "event-service:50052",
		BookingServiceAddr: "booking-e2e:50053",
		PaymentServiceAddr: "booking-e2e:50053",
	}
	project := "rushgo-ticket-e2e-20261024-0123456789ab"

	if err := validateTicketE2ERuntimeEndpoints(valid, project); err != nil {
		t.Fatalf("valid isolated runtime was rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{name: "shared Kafka broker", mutate: func(cfg *config.Config) { cfg.KafkaBrokers = []string{"shared-kafka:9092"} }},
		{name: "additional Kafka broker", mutate: func(cfg *config.Config) { cfg.KafkaBrokers = append(cfg.KafkaBrokers, "shared-kafka:9092") }},
		{name: "shared database", mutate: func(cfg *config.Config) {
			cfg.DatabaseURL = "postgres://ticket_e2e:test-only@shared-postgres:5432/rushgo_ticket_e2e_saga?sslmode=disable"
		}},
		{name: "database fallback host", mutate: func(cfg *config.Config) {
			cfg.DatabaseURL = "postgres://ticket_e2e:test-only@ticket-e2e-postgres:5432,shared-postgres:5432/rushgo_ticket_e2e_saga?sslmode=disable"
		}},
		{name: "external booking service", mutate: func(cfg *config.Config) { cfg.BookingServiceAddr = "booking.prod.internal:50053" }},
		{name: "external event service", mutate: func(cfg *config.Config) { cfg.EventServiceAddr = "event.prod.internal:50052" }},
		{name: "external payment service", mutate: func(cfg *config.Config) { cfg.PaymentServiceAddr = "payment.prod.internal:50053" }},
		{name: "wrong run identity", mutate: func(cfg *config.Config) { cfg.GRPCPort = "50054" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *valid
			cfg.KafkaBrokers = append([]string(nil), valid.KafkaBrokers...)
			tt.mutate(&cfg)
			if err := validateTicketE2ERuntimeEndpoints(&cfg, project); err == nil {
				t.Fatal("misdirected TEST_ONLY Saga runtime was accepted")
			}
		})
	}

	if err := validateTicketE2ERuntimeEndpoints(valid, ""); err != nil {
		t.Fatalf("ordinary non-C-SIM configuration should be unaffected: %v", err)
	}
}
