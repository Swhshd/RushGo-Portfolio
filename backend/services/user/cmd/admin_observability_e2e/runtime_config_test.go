//go:build admin_observability_e2e

package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestUnifiedUserFixtureIdentityRequiresExactOwnerRunAndService(t *testing.T) {
	runID := strings.Repeat("a", 32)
	base := fixtureRuntime{
		Acknowledgement: fixtureAck,
		RunID:           runID,
		TestIdentity:    "rushgo-admin-unified-preview/" + runID + "/user",
		Service:         "user",
		FixtureOwner:    "admin-unified-preview",
	}
	if _, err := validateFixtureIdentity(base, "user", "admin-unified-preview-e2e-user"); err != nil {
		t.Fatalf("valid unified User identity rejected: %v", err)
	}
	cases := []struct {
		name        string
		mutate      func(*fixtureRuntime)
		serviceName string
	}{
		{name: "unknown owner tag", mutate: func(cfg *fixtureRuntime) { cfg.FixtureOwner = "admin-unified-preview-extra" }, serviceName: "admin-unified-preview-e2e-user"},
		{name: "wrong run identity", mutate: func(cfg *fixtureRuntime) { cfg.RunID = strings.Repeat("b", 32) }, serviceName: "admin-unified-preview-e2e-user"},
		{name: "wrong service name", mutate: func(*fixtureRuntime) {}, serviceName: "admin-observability-v2-e2e-user"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if _, err := validateFixtureIdentity(cfg, "user", tc.serviceName); err == nil {
				t.Fatal("invalid unified User identity was accepted")
			}
		})
	}
}

func TestExistingUserFixtureOwnersRemainSupported(t *testing.T) {
	for _, owner := range []string{"admin-observability-v2", "admin-event-operations"} {
		t.Run(owner, func(t *testing.T) {
			runID := strings.Repeat("a", 32)
			cfg := fixtureRuntime{
				Acknowledgement: fixtureAck,
				RunID:           runID,
				TestIdentity:    "rushgo-" + owner + "/" + runID + "/user",
				Service:         "user",
				FixtureOwner:    owner,
			}
			if _, err := validateFixtureIdentity(cfg, "user", owner+"-e2e-user"); err != nil {
				t.Fatalf("existing User fixture owner rejected: %v", err)
			}
		})
	}
}

func TestUnifiedUserFixtureEndpointsRequireExactRunDatabaseAndBothOwnerTargets(t *testing.T) {
	base := validUnifiedUserFixtureRuntime()
	if err := validateFixtureEndpoints(base); err != nil {
		t.Fatalf("valid unified User endpoints rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*fixtureRuntime)
	}{
		{name: "wrong database owner prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = fixtureDatabaseURL("admin_event_operations", "aaaaaaaaaaaa", "user")
		}},
		{name: "wrong database run prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = fixtureDatabaseURL("admin_unified_preview", "bbbbbbbbbbbb", "user")
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
				t.Fatal("invalid unified User endpoints were accepted")
			}
		})
	}
}

func validUnifiedUserFixtureRuntime() fixtureRuntime {
	runID := strings.Repeat("a", 32)
	redisURL := "redis://127.0.0.1:16379/1"
	return fixtureRuntime{
		RunID:                   runID,
		Service:                 "user",
		FixtureOwner:            "admin-unified-preview",
		DatabaseURL:             fixtureDatabaseURL("admin_unified_preview", runID[:12], "user"),
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
