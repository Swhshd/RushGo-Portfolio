package topics

import "testing"

func TestTicketIssuanceKafkaIdentityKeepsProductionNamesAndNamespacesVerifiedRuns(t *testing.T) {
	topic, group := TicketIssuanceKafkaIdentity("", true)
	if topic != TopicTicketIssuanceEvents || group != TicketIssuanceGroup {
		t.Fatalf("empty namespace identity = (%q, %q)", topic, group)
	}
	const namespace = "rushgo-ticket-e2e-20261024-0123456789ab"
	topic, group = TicketIssuanceKafkaIdentity(namespace, true)
	if topic != TopicTicketIssuanceEvents+"."+namespace || group != TicketIssuanceGroup+"-"+namespace {
		t.Fatalf("namespaced identity = (%q, %q)", topic, group)
	}
	topic, group = TicketIssuanceKafkaIdentity(namespace, false)
	if topic != TopicTicketIssuanceEvents || group != TicketIssuanceGroup {
		t.Fatalf("unverified namespace changed identity = (%q, %q)", topic, group)
	}
}
