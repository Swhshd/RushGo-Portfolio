//go:build aftersales_acceptance

package providerevidence

import "testing"

func TestAcceptanceLabelsDouble(t *testing.T) {
	if !AllowedSource("SIMULATED_PROVIDER_AUTHORITY") {
		t.Fatal("double missing")
	}
}
