//go:build recoverycomponents

package main

import (
	"strings"
	"testing"
)

func TestRefundExplainBuildEnvironmentIncludesOnlyApprovedGoOverrides(t *testing.T) {
	buildOnly := map[string]string{
		"GOMODCACHE": "test-module-cache",
		"GOCACHE":    "test-build-cache",
		"GOMAXPROCS": "2",
		"GOMEMLIMIT": "384MiB",
		"GOGC":       "25",
		"GOFLAGS":    "-mod=readonly -trimpath",
	}
	for name, value := range buildOnly {
		t.Setenv(name, value)
	}
	t.Setenv("PGHOST", "127.0.0.1")
	t.Setenv("STRIPE_SECRET_KEY", "must-not-be-forwarded")
	t.Setenv("DATABASE_URL", "must-not-be-forwarded")
	t.Setenv(refundExplainAdminDSNEnv, "postgres://synthetic@127.0.0.1/refund?sslmode=disable")
	t.Setenv(refundExplainOwnerEnv, "private-owner-manifest.json")
	t.Setenv("GOPROXY", "https://example.invalid")
	t.Setenv("GOSUMDB", "example.invalid")
	t.Setenv("GOWORK", "must-not-be-forwarded")
	t.Setenv("GOTOOLCHAIN", "must-not-be-forwarded")

	buildEnvironment := refundExplainEnvironmentMap(t, refundExplainBuildEnvironment())
	for name, want := range buildOnly {
		if got := buildEnvironment[name]; got != want {
			t.Errorf("build environment %s = %q, want %q", name, got, want)
		}
	}
	for name, want := range map[string]string{
		"GOPROXY":     "off",
		"GOSUMDB":     "off",
		"GOWORK":      "off",
		"GOTOOLCHAIN": "local",
	} {
		if got := buildEnvironment[name]; got != want {
			t.Errorf("build environment %s = %q, want fixed value %q", name, got, want)
		}
	}
	for _, forbidden := range []string{
		"PGHOST", "STRIPE_SECRET_KEY", "DATABASE_URL",
		refundExplainAdminDSNEnv, refundExplainOwnerEnv,
	} {
		if _, ok := buildEnvironment[forbidden]; ok {
			t.Errorf("build environment unexpectedly contains %s", forbidden)
		}
	}

	cliEnvironment := refundExplainEnvironmentMap(t, refundExplainCLIEnvironment("synthetic-dsn", "synthetic-manifest"))
	for name := range buildOnly {
		if _, ok := cliEnvironment[name]; ok {
			t.Errorf("CLI environment unexpectedly contains build-only variable %s", name)
		}
	}
	for _, forbidden := range []string{"PGHOST", "STRIPE_SECRET_KEY", "DATABASE_URL", "GOPROXY", "GOSUMDB", "GOWORK", "GOTOOLCHAIN"} {
		if _, ok := cliEnvironment[forbidden]; ok {
			t.Errorf("CLI environment unexpectedly contains %s", forbidden)
		}
	}
	if cliEnvironment[refundExplainAdminDSNEnv] != "synthetic-dsn" ||
		cliEnvironment[refundExplainOwnerEnv] != "synthetic-manifest" {
		t.Error("CLI environment did not contain only its explicit component DSN and owner manifest")
	}
}

func refundExplainEnvironmentMap(t *testing.T, environment []string) map[string]string {
	t.Helper()
	result := make(map[string]string, len(environment))
	for _, entry := range environment {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("environment entry has no separator: %q", entry)
		}
		name = strings.ToUpper(name)
		if _, duplicate := result[name]; duplicate {
			t.Fatalf("environment entry %s is duplicated", name)
		}
		result[name] = value
	}
	return result
}
