package topics

// Kafka topics used across services. Producers and consumers must
// reference these constants instead of inlining topic strings so the
// publisher/subscriber contract lives in one place.
const (
	TopicBookingEvents        = "booking.events"
	TopicEventEvents          = "event.events"
	TopicUserEvents           = "user.events"
	TopicPaymentEvents        = "payment.events"
	TopicOrderEvents          = "order.events"
	TopicNotificationEvents   = "notification.events"
	TopicTicketIssuanceEvents = "ticket.issuance.events"
	TopicSchedulerConfigEvent = "scheduler.config.events"

	// TicketIssuanceGroup is the Booking consumer group that processes the
	// Ticket Issuance outbox. Run-scoped ticket-e2e builds append -<project>.
	TicketIssuanceGroup = "booking-ticket-issuance-v1"
)

// TicketIssuanceKafkaIdentity returns the effective topic and consumer group
// for a verified ticket-e2e namespace. An empty or unverified namespace keeps
// the production identity unchanged.
func TicketIssuanceKafkaIdentity(namespace string, namespaced bool) (topic, group string) {
	if !namespaced || namespace == "" {
		return TopicTicketIssuanceEvents, TicketIssuanceGroup
	}
	return TopicTicketIssuanceEvents + "." + namespace, TicketIssuanceGroup + "-" + namespace
}
