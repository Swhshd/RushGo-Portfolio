package repository

import (
	"testing"
)

func TestRefundComponentPGXConfigAllowsOnlyValidatedLoopbackEndpointWithoutConnecting(t *testing.T) {
	manifest := refundExplainOwnerManifest{Host: "127.0.0.1", Port: 25439, Database: "component_refund_fixture"}
	valid := "postgres://component@127.0.0.1:25439/component_refund_fixture?sslmode=disable"
	config, err := refundComponentPGXConfig(valid, manifest)
	if err != nil {
		t.Fatalf("validated loopback config rejected: %v", err)
	}
	if config.ConnConfig.Host != manifest.Host || int(config.ConnConfig.Port) != manifest.Port ||
		config.ConnConfig.Database != manifest.Database || len(config.ConnConfig.Fallbacks) != 0 {
		t.Fatalf("effective config escaped owner manifest: host=%q port=%d database=%q fallbacks=%d",
			config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database, len(config.ConnConfig.Fallbacks))
	}
}

func TestRefundComponentPGXConfigRejectsQueryHostPortDatabaseAndServiceOverridesOffline(t *testing.T) {
	manifest := refundExplainOwnerManifest{Host: "127.0.0.1", Port: 25439, Database: "component_refund_fixture"}
	base := "postgres://component@127.0.0.1:25439/component_refund_fixture?sslmode=disable"
	for _, override := range []string{
		"host=example.com", "port=5433", "dbname=other", "database=other", "service=external", "servicefile=/tmp/pg_service.conf", "hostaddr=192.0.2.1",
	} {
		t.Run(override, func(t *testing.T) {
			if _, err := refundComponentPGXConfig(base+"&"+override, manifest); err == nil {
				t.Fatalf("DSN endpoint override %q was accepted", override)
			}
		})
	}
}

func TestRefundComponentPGXConfigRejectsPostgreSQLEnvironmentOverridesOffline(t *testing.T) {
	manifest := refundExplainOwnerManifest{Host: "127.0.0.1", Port: 25439, Database: "component_refund_fixture"}
	dsn := "postgres://component@127.0.0.1:25439/component_refund_fixture?sslmode=disable"
	for _, override := range []struct{ name, value string }{
		{name: "PGSERVICE", value: "untrusted-service"},
		{name: "PGHOST", value: "192.0.2.1"},
	} {
		t.Run(override.name, func(t *testing.T) {
			t.Setenv(override.name, override.value)
			if _, err := refundComponentPGXConfig(dsn, manifest); err == nil {
				t.Fatalf("%s environment override was accepted", override.name)
			}
		})
	}
}
