//go:build ticket_e2e

package grpcsecurity

import "slices"

func ValidateTicketE2ERuntimeResources(serviceName string, actual TicketE2ERuntimeResources) error {
	expected, ok := ticketE2EExpectedRuntimeResources[serviceName]
	if !ok || actual.DatabaseURL != expected.DatabaseURL || actual.RedisURL != expected.RedisURL ||
		!slices.Equal(actual.KafkaBrokers, expected.KafkaBrokers) {
		return errTicketE2ERuntimeResourcesMismatch
	}
	return nil
}
