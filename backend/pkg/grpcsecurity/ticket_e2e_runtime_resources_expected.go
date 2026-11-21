//go:build ticket_e2e

package grpcsecurity

import "errors"

var errTicketE2ERuntimeResourcesMismatch = errors.New("TEST_ONLY service resources do not match the isolated Ticket E2E endpoints")

var ticketE2EExpectedRuntimeResources = map[string]TicketE2ERuntimeResources{
	"ticket-e2e-user": {
		DatabaseURL:  "postgres://ticket_e2e:ticket_e2e_local@ticket-e2e-postgres:5432/rushgo_ticket_e2e_user?sslmode=disable",
		RedisURL:     "redis://ticket-e2e-redis:6379",
		KafkaBrokers: []string{"ticket-e2e-kafka:9092"},
	},
	"ticket-e2e-event": {
		DatabaseURL:  "postgres://ticket_e2e:ticket_e2e_local@ticket-e2e-postgres:5432/rushgo_ticket_e2e_event?sslmode=disable",
		RedisURL:     "redis://ticket-e2e-redis:6379",
		KafkaBrokers: []string{"ticket-e2e-kafka:9092"},
	},
}
