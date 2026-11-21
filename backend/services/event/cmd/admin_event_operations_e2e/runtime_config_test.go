//go:build admin_event_operations_e2e

package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestEventFixtureIdentityAcceptsExistingAndUnifiedOwnerWithExactRun(t *testing.T) {
	for _, owner := range []string{"admin-event-operations", "admin-unified-preview"} {
		t.Run(owner, func(t *testing.T) {
			cfg := validEventFixtureRuntime(owner)
			if err := validateEventFixtureIdentity(cfg, owner+"-e2e-event"); err != nil {
				t.Fatalf("valid Event fixture identity rejected: %v", err)
			}
		})
	}
}

func TestEventFixtureIdentityRejectsWrongOwnerRunAndService(t *testing.T) {
	base := validEventFixtureRuntime("admin-unified-preview")
	cases := []struct {
		name        string
		mutate      func(*fixtureRuntime)
		serviceName string
	}{
		{name: "unknown owner", mutate: func(cfg *fixtureRuntime) { cfg.FixtureOwner = "admin-unified-preview-extra" }, serviceName: "admin-unified-preview-e2e-event"},
		{name: "identity owner mismatch", mutate: func(cfg *fixtureRuntime) { cfg.FixtureOwner = "admin-event-operations" }, serviceName: "admin-unified-preview-e2e-event"},
		{name: "run mismatch", mutate: func(cfg *fixtureRuntime) { cfg.RunID = strings.Repeat("b", 32) }, serviceName: "admin-unified-preview-e2e-event"},
		{name: "service name mismatch", mutate: func(*fixtureRuntime) {}, serviceName: "admin-event-operations-e2e-event"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if err := validateEventFixtureIdentity(cfg, tc.serviceName); err == nil {
				t.Fatal("invalid Event fixture identity was accepted")
			}
		})
	}
}

func TestUnifiedEventFixtureEndpointsRequireOwnerAndRunSpecificDatabase(t *testing.T) {
	base := validEventFixtureRuntime("admin-unified-preview")
	if err := validateEventFixtureEndpoints(base); err != nil {
		t.Fatalf("valid unified Event endpoints rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*fixtureRuntime)
	}{
		{name: "wrong database owner prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = eventFixtureDatabaseURL("admin_event_operations", "aaaaaaaaaaaa")
		}},
		{name: "wrong database run prefix", mutate: func(cfg *fixtureRuntime) {
			cfg.DatabaseURL = eventFixtureDatabaseURL("admin_unified_preview", "bbbbbbbbbbbb")
		}},
		{name: "malformed run ID", mutate: func(cfg *fixtureRuntime) { cfg.RunID = "bad" }},
		{name: "non-loopback event listener", mutate: func(cfg *fixtureRuntime) { cfg.EventGRPCListen = "0.0.0.0:15002" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if err := validateEventFixtureEndpoints(cfg); err == nil {
				t.Fatal("invalid unified Event endpoints were accepted")
			}
		})
	}
}

func validEventFixtureRuntime(owner string) fixtureRuntime {
	runID := strings.Repeat("a", 32)
	prefix := "admin_unified_preview"
	if owner == "admin-event-operations" {
		prefix = "admin_event_operations"
	}
	return fixtureRuntime{
		Acknowledgement: fixtureAck,
		RunID:           runID,
		FixtureOwner:    owner,
		TestIdentity:    "rushgo-" + owner + "/" + runID + "/event",
		Service:         "event",
		DatabaseURL:     eventFixtureDatabaseURL(prefix, runID[:12]),
		EventGRPCListen: "127.0.0.1:15002",
	}
}

func eventFixtureDatabaseURL(prefix, runPrefix string) string {
	return fmt.Sprintf("postgres://fixture@127.0.0.1:15432/%s_%s_event", prefix, runPrefix)
}
