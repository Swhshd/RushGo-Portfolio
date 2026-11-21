//go:build aftersales_acceptance

package providerevidence

// The isolated acceptance build labels provider doubles truthfully. It must
// never be deployed or supplied credentials for a real provider.
func AllowedSource(source string) bool {
	return source == "stripe_test_api" || source == "SIMULATED_PROVIDER_AUTHORITY"
}
