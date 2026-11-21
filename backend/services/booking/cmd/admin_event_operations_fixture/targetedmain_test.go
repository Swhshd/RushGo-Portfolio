//go:build admin_event_operations_e2e && ticket_e2e

package main

import (
	"net/url"
	"strings"
	"testing"
)

func TestValidateFixtureDatabaseScope(t *testing.T) {
	const runID = "0123456789abcdef0123456789abcdef"
	const anotherRunID = "fedcba9876543210fedcba9876543210"
	legacyName := "/admin_event_operations_" + runID[:12] + "_booking"
	unifiedName := "/admin_unified_preview_" + runID[:12] + "_booking"
	otherLegacyName := "/admin_event_operations_" + anotherRunID[:12] + "_booking"
	otherUnifiedName := "/admin_unified_preview_" + anotherRunID[:12] + "_booking"
	legacyShortPrefix := "/admin_event_operations_" + runID[:11] + "_booking"
	unifiedLongPrefix := "/admin_unified_preview_" + runID[:13] + "_booking"

	tests := []struct {
		name       string
		runID      string
		database   string
		host       string
		wantReject bool
	}{
		{name: "exact legacy fixture database", runID: runID, database: legacyName, host: "127.0.0.1:5432"},
		{name: "exact unified preview database", runID: runID, database: unifiedName, host: "[::1]:5432"},
		{name: "legacy database from another run", runID: runID, database: otherLegacyName, host: "127.0.0.1:5432", wantReject: true},
		{name: "unified database from another run", runID: runID, database: otherUnifiedName, host: "127.0.0.1:5432", wantReject: true},
		{name: "legacy database with shortened run prefix", runID: runID, database: legacyShortPrefix, host: "127.0.0.1:5432", wantReject: true},
		{name: "unified database with extended run prefix", runID: runID, database: unifiedLongPrefix, host: "127.0.0.1:5432", wantReject: true},
		{name: "production database name", runID: runID, database: "/booking", host: "127.0.0.1:5432", wantReject: true},
		{name: "non-loopback host", runID: runID, database: unifiedName, host: "203.0.113.8:5432", wantReject: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			databaseURL := &url.URL{
				Scheme: "postgres",
				User:   url.UserPassword("evopsbook_fixture", "test-password"),
				Host:   test.host,
				Path:   test.database,
			}
			err := validateFixtureDatabaseScope(databaseURL, test.runID)
			if test.wantReject && err == nil {
				t.Fatal("expected database scope to be rejected")
			}
			if !test.wantReject && err != nil {
				t.Fatalf("valid isolated database rejected: %v", err)
			}
		})
	}
}

func TestMaskURLpasswordHidesDatabaseCredentials(t *testing.T) {
	const password = "known-secret-password"
	input := "connect failed: postgres://evopsbook_fixture:" + password + "@127.0.0.1:5432/admin_unified_preview_0123456789ab_booking"
	got := maskURLpassword(input)
	if strings.Contains(got, password) {
		t.Fatalf("masked diagnostic contains database password: %q", got)
	}
	if !strings.Contains(got, "postgres://evopsbook_fixture:[REDACTED]@127.0.0.1:5432/") {
		t.Fatalf("masked diagnostic lost useful connection context: %q", got)
	}
}
