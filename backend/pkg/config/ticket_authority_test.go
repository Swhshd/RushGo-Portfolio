package config

import "testing"

func TestTicketAuthorityTargetsLoadFromEnvironment(t *testing.T) {
	t.Setenv("EVENT_TICKET_AUTHORITY_ADDR", "event-service:51052")
	t.Setenv("SAGA_TICKET_AUTHORITY_ADDR", "saga-service:51055")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventTicketAuthorityAddr != "event-service:51052" || cfg.SagaTicketAuthorityAddr != "saga-service:51055" {
		t.Fatalf("Ticket authority addresses = (%q, %q)", cfg.EventTicketAuthorityAddr, cfg.SagaTicketAuthorityAddr)
	}
}
