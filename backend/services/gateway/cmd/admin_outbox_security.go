package main

// adminOutboxSecureWiring enables the new path only when both protected
// outbound transports use their existing mTLS configuration.
func adminOutboxSecureWiring(bookingTestOnly, userTestOnly bool) bool {
	return !bookingTestOnly && !userTestOnly
}

func notificationAdminOutboxSecureWiring(configured, notificationTestOnly, userTestOnly bool) bool {
	return configured && !notificationTestOnly && !userTestOnly
}
