//go:build admin_observability_e2e

package main

import (
	"strings"
	"testing"
)

func TestValidateFixtureIdentityAcceptsOnlyExplicitUnifiedOwnerTag(t *testing.T) {
	for _, tc := range []struct {
		owner string
		want  bool
	}{
		{owner: "admin-observability-v2", want: true},
		{owner: "admin-event-operations", want: true},
		{owner: "admin-unified-preview", want: true},
		{owner: "admin-unified-preview-extra", want: false},
		{owner: "", want: true}, // Empty remains the legacy observability default.
	} {
		t.Run(tc.owner, func(t *testing.T) {
			cfg := fixtureRuntime{
				Acknowledgement: fixtureAck,
				RunID:           strings.Repeat("a", 32),
				Service:         "gateway",
				FixtureOwner:    tc.owner,
			}
			owner := tc.owner
			if owner == "" {
				owner = "admin-observability-v2"
			}
			cfg.TestIdentity = "rushgo-" + owner + "/" + cfg.RunID + "/gateway"
			_, err := validateFixtureIdentity(cfg, "gateway", owner+"-e2e-gateway")
			if got := err == nil; got != tc.want {
				t.Fatalf("identity accepted=%t, want %t; err=%v", got, tc.want, err)
			}
		})
	}
}

func TestUnifiedFixtureEndpointsRequireBothEventAndNotificationTargets(t *testing.T) {
	base := validUnifiedGatewayFixtureRuntime()
	if err := validateFixtureEndpoints(base); err != nil {
		t.Fatalf("valid unified Gateway endpoints rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*fixtureRuntime)
	}{
		{name: "missing Event bind", mutate: func(cfg *fixtureRuntime) { cfg.EventGRPCListen = "" }},
		{name: "missing Event target", mutate: func(cfg *fixtureRuntime) { cfg.EventServiceAddr = "" }},
		{name: "missing Notification bind", mutate: func(cfg *fixtureRuntime) { cfg.NotificationGRPCListen = "" }},
		{name: "missing Notification target", mutate: func(cfg *fixtureRuntime) { cfg.NotificationServiceAddr = "" }},
		{name: "missing Notification Redis", mutate: func(cfg *fixtureRuntime) { cfg.NotificationRedisURL = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if err := validateFixtureEndpoints(cfg); err == nil {
				t.Fatal("incomplete unified dependency targets were accepted")
			}
		})
	}
}

func validUnifiedGatewayFixtureRuntime() fixtureRuntime {
	runID := strings.Repeat("a", 32)
	return fixtureRuntime{
		RunID:                   runID,
		Service:                 "gateway",
		FixtureOwner:            "admin-unified-preview",
		BookingRedisURL:         "redis://127.0.0.1:16379/1",
		GatewayRedisURL:         "redis://127.0.0.1:16379/1",
		NotificationRedisURL:    "redis://127.0.0.1:16379/1",
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
