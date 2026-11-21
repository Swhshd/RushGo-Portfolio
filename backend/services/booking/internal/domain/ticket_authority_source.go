//go:build !ticket_e2e

package domain

import "github.com/ticketbox/pkg/providerevidence"

// TicketAuthoritySourceAllowed keeps the deployable Booking build bound to
// provider evidence produced by the Stripe TEST API verifier.
func TicketAuthoritySourceAllowed(source string) bool {
	return providerevidence.AllowedSource(source)
}
