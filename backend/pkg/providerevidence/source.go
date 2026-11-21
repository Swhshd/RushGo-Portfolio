//go:build !aftersales_acceptance

package providerevidence

// AllowedSource is a compile-time boundary, never an environment bypass.
func AllowedSource(source string) bool { return source == "stripe_test_api" }
