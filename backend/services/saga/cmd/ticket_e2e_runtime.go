package main

import (
	"errors"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/config"
)

var ticketE2EProjectPattern = regexp.MustCompile(`^rushgo-ticket-e2e-[0-9]{8}-[0-9a-f]{12}$`)

func validateTicketE2ERuntimeEndpoints(cfg *config.Config, project string) error {
	if strings.TrimSpace(project) == "" {
		return nil
	}
	if cfg == nil || !ticketE2EProjectPattern.MatchString(project) {
		return errors.New("Ticket E2E Saga runtime identity is invalid")
	}

	dbURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil || dbURL.Scheme != "postgres" {
		return errors.New("Ticket E2E Saga requires its isolated PostgreSQL database")
	}
	dbQuery, err := url.ParseQuery(dbURL.RawQuery)
	if err != nil || len(dbQuery) != 1 || len(dbQuery["sslmode"]) != 1 || dbQuery.Get("sslmode") != "disable" {
		return errors.New("Ticket E2E Saga requires its isolated PostgreSQL database")
	}
	dbConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil || dbConfig.ConnConfig.Host != "ticket-e2e-postgres" || dbConfig.ConnConfig.Port != 5432 ||
		dbConfig.ConnConfig.Database != "rushgo_ticket_e2e_saga" || dbConfig.ConnConfig.User != "ticket_e2e" || len(dbConfig.ConnConfig.Fallbacks) != 0 {
		return errors.New("Ticket E2E Saga requires its isolated PostgreSQL database")
	}

	if cfg.GRPCPort != "50055" || cfg.EventServiceAddr != "event-service:50052" ||
		cfg.BookingServiceAddr != "booking-e2e:50053" || cfg.PaymentServiceAddr != "booking-e2e:50053" ||
		len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "ticket-e2e-kafka:9092" {
		return errors.New("Ticket E2E Saga requires its isolated service and Kafka endpoints")
	}
	return nil
}
