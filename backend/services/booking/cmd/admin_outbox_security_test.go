package main

import "testing"

func TestAdminOutboxOwnerWiringRequiresSecureInboundAndUserTransports(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		bookingTestOnlyPlaintext bool
		userTestOnly             bool
		wantSecure               bool
	}{
		{name: "both transports use mTLS", wantSecure: true},
		{name: "Booking inbound plaintext", bookingTestOnlyPlaintext: true},
		{name: "User outbound plaintext", userTestOnly: true},
		{name: "both plaintext", bookingTestOnlyPlaintext: true, userTestOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := adminOutboxSecureWiring(tc.bookingTestOnlyPlaintext, tc.userTestOnly); got != tc.wantSecure {
				t.Fatalf("admin outbox owner secure wiring = %v, want %v", got, tc.wantSecure)
			}
		})
	}
}
