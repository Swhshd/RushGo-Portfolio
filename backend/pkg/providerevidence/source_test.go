package providerevidence

import "testing"

func TestSourceAlwaysRejectsUnknown(t *testing.T) {
	for _, s := range []string{"", "unknown", "stripe_live_api"} {
		if AllowedSource(s) {
			t.Fatal(s)
		}
	}
}
