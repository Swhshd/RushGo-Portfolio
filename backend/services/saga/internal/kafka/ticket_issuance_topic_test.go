package kafka

import "testing"

func TestTicketIssuanceTopicUsesRunSpecificNamespaceOnlyWhenVerified(t *testing.T) {
	const topic = "ticket.issuance.events"
	const project = "rushgo-ticket-e2e-20261024-0123456789ab"
	if got := ticketIssuanceTopic(topic, project, true); got != topic+"."+project {
		t.Fatalf("run-specific Ticket topic = %q", got)
	}
	if got := ticketIssuanceTopic(topic, project, false); got != topic {
		t.Fatalf("unverified project identity changed Ticket topic to %q", got)
	}
	if got := ticketIssuanceTopic(topic, "", true); got != topic {
		t.Fatalf("empty project identity changed Ticket topic to %q", got)
	}
}
