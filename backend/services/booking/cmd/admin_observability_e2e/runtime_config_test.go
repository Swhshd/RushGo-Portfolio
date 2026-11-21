//go:build admin_observability_e2e

package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestUnifiedBookingFixtureIdentityRequiresExactOwnerRunAndService(t *testing.T) {
	runID := strings.Repeat("a", 32)
	base := fixtureRuntime{
		Acknowledgement: fixtureAck,
		RunID:           runID,
		TestIdentity:    "rushgo-admin-unified-preview/" + runID + "/booking",
		Service:         "booking",
		FixtureOwner:    "admin-unified-preview",
	}
	if _, err := validateFixtureIdentity(base, "booking", "admin-unified-preview-e2e-booking"); err != nil {
		t.Fatalf("valid unified Booking identity rejected: %v", err)
	}
	cases := []struct {
		name        string
		mutate      func(*fixtureRuntime)
		serviceName string
	}{
		{name: "unknown owner tag", mutate: func(cfg *fixtureRuntime) { cfg.FixtureOwner = "admin-unified-preview-extra" }, serviceName: "admin-unified-preview-e2e-booking"},
		{name: "wrong run identity", mutate: func(cfg *fixtureRuntime) { cfg.RunID = strings.Repeat("b", 32) }, serviceName: "admin-unified-preview-e2e-booking"},
		{name: "wrong service name", mutate: func(*fixtureRuntime) {}, serviceName: "admin-observability-v2-e2e-booking"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if _, err := validateFixtureIdentity(cfg, "booking", tc.serviceName); err == nil {
				t.Fatal("invalid unified Booking identity was accepted")
			}
		})
	}
}

func TestExistingBookingFixtureOwnersRemainSupported(t *testing.T) {
	for _, owner := range []string{"admin-observability-v2", "admin-event-operations"} {
		t.Run(owner, func(t *testing.T) {
			runID := strings.Repeat("a", 32)
			cfg := fixtureRuntime{
				Acknowledgement: fixtureAck,
				RunID:           runID,
				TestIdentity:    "rushgo-" + owner + "/" + runID + "/booking",
				Service:         "booking",
				FixtureOwner:    owner,
			}
			if _, err := validateFixtureIdentity(cfg, "booking", owner+"-e2e-booking"); err != nil {
				t.Fatalf("existing Booking fixture owner rejected: %v", err)
			}
		})
	}
}

func TestUnifiedBookingFixtureEndpointsRequireExactRunDatabaseAndBothOwnerTargets(t *testing.T) {
	base := validUnifiedBookingFixtureRuntime()
	if err := validateFixtureEndpoints(base); err != nil {
		t.Fatalf("valid unified Booking endpoints rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*fixtureRuntime)
	}{
		{name: "wrong database owner prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = fixtureDatabaseURL("admin_event_operations", "aaaaaaaaaaaa", "booking")
		}},
		{name: "wrong database run prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = fixtureDatabaseURL("admin_unified_preview", "bbbbbbbbbbbb", "booking")
		}},
		{name: "malformed run ID", mutate: func(cfg *fixtureRuntime) { cfg.RunID = "bad" }},
		{name: "unknown owner", mutate: func(cfg *fixtureRuntime) { cfg.FixtureOwner = "admin-unified-preview-extra" }},
		{name: "missing Event target", mutate: func(cfg *fixtureRuntime) { cfg.EventServiceAddr = "" }},
		{name: "missing Notification target", mutate: func(cfg *fixtureRuntime) { cfg.NotificationServiceAddr = "" }},
		{name: "missing shared Notification Redis", mutate: func(cfg *fixtureRuntime) { cfg.NotificationRedisURL = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if err := validateFixtureEndpoints(cfg); err == nil {
				t.Fatal("invalid unified Booking endpoints were accepted")
			}
		})
	}
}

func validUnifiedBookingFixtureRuntime() fixtureRuntime {
	runID := strings.Repeat("a", 32)
	redisURL := "redis://127.0.0.1:16379/1"
	return fixtureRuntime{
		RunID:                   runID,
		Service:                 "booking",
		FixtureOwner:            "admin-unified-preview",
		DatabaseURL:             fixtureDatabaseURL("admin_unified_preview", runID[:12], "booking"),
		BookingRedisURL:         redisURL,
		GatewayRedisURL:         redisURL,
		NotificationRedisURL:    redisURL,
		UserGRPCListen:          "127.0.0.1:15001",
		EventGRPCListen:         "127.0.0.1:15002",
		BookingGRPCListen:       "127.0.0.1:15003",
		NotificationGRPCListen:  "127.0.0.1:15004",
		GatewayHTTPListen:       "127.0.0.1:18000",
		UserServiceAddr:         "127.0.0.1:15001",
		EventServiceAddr:        "127.0.0.1:15002",
		BookingServiceAddr:      "127.0.0.1:15003",
		NotificationServiceAddr: "127.0.0.1:15004",
	}
}

func fixtureDatabaseURL(prefix, runPrefix, service string) string {
	return fmt.Sprintf("postgres://fixture@127.0.0.1:15432/%s_%s_%s", prefix, runPrefix, service)
}
