package main

import "testing"

func TestAdminOutboxGatewayWiringRequiresBothProtectedTransports(t *testing.T) {
	for _, tc := range []struct {
		name            string
		bookingTestOnly bool
		userTestOnly    bool
		wantSecure      bool
	}{
		{name: "both transports use mTLS", wantSecure: true},
		{name: "Booking plaintext", bookingTestOnly: true},
		{name: "User plaintext", userTestOnly: true},
		{name: "both plaintext", bookingTestOnly: true, userTestOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := adminOutboxSecureWiring(tc.bookingTestOnly, tc.userTestOnly); got != tc.wantSecure {
				t.Fatalf("admin outbox secure wiring = %v, want %v", got, tc.wantSecure)
			}
		})
	}
}

func TestNotificationAdminOutboxGatewayWiringIsOptionalAndRequiresBothProtectedTransports(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		configured           bool
		notificationTestOnly bool
		userTestOnly         bool
		wantSecure           bool
	}{
		{name: "configured with mTLS", configured: true, wantSecure: true},
		{name: "not configured", configured: false},
		{name: "Notification plaintext", configured: true, notificationTestOnly: true},
		{name: "User plaintext", configured: true, userTestOnly: true},
		{name: "both protected transports plaintext", configured: true, notificationTestOnly: true, userTestOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := notificationAdminOutboxSecureWiring(tc.configured, tc.notificationTestOnly, tc.userTestOnly); got != tc.wantSecure {
				t.Fatalf("Notification outbox secure wiring = %v, want %v", got, tc.wantSecure)
			}
		})
	}
}
