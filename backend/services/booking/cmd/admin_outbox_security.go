package main

// adminOutboxSecureWiring enables the new RPC only when inbound Booking and
// outbound User authentication transports both use their existing mTLS setup.
func adminOutboxSecureWiring(bookingTestOnlyPlaintext, userTestOnly bool) bool {
	return !bookingTestOnlyPlaintext && !userTestOnly
}
