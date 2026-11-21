package kafka

import "testing"

func TestTicketIssuanceKafkaIdentityUsesRunSpecificTopicAndGroup(t *testing.T) {
	const topic = "ticket.issuance.events"
	const group = "booking-ticket-issuance-v1"
	const project = "rushgo-ticket-e2e-20261024-0123456789ab"

	gotTopic, gotGroup := ticketIssuanceKafkaIdentity(topic, group, project, true)
	if gotTopic != topic+"."+project || gotGroup != group+"-"+project {
		t.Fatalf("run-specific Ticket Kafka identity = (%q, %q)", gotTopic, gotGroup)
	}
	gotTopic, gotGroup = ticketIssuanceKafkaIdentity(topic, group, project, false)
	if gotTopic != topic || gotGroup != group {
		t.Fatalf("invalid identity must not apply an unverified namespace: (%q, %q)", gotTopic, gotGroup)
	}
	gotTopic, gotGroup = ticketIssuanceKafkaIdentity(topic, group, "", true)
	if gotTopic != topic || gotGroup != group {
		t.Fatalf("production identity must retain configured topic/group: (%q, %q)", gotTopic, gotGroup)
	}
}
