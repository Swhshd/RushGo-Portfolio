//go:build !aftersales_acceptance

package providerevidence

import "testing"

func TestProductionRejectsDouble(t *testing.T) {
	if AllowedSource("SIMULATED_PROVIDER_AUTHORITY") {
		t.Fatal("double allowed in production")
	}
}
