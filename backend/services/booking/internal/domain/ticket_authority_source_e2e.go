//go:build ticket_e2e

package domain

// TicketAuthoritySourceAllowed is widened only in the dedicated ticket_e2e
// build. The production build does not compile this implementation.
func TicketAuthoritySourceAllowed(source string) bool {
	return source == "stripe_test_api" || source == "SIMULATED_PROVIDER_AUTHORITY"
}
