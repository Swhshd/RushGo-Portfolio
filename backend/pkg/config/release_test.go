package config

import (
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func candidateConfig(t *testing.T) *viper.Viper {
	t.Helper()
	v := viper.New()
	values := map[string]string{"RUSHGO_ENV": "preproduction", "RUSHGO_PROVIDER_MODE": "STRIPE_SANDBOX_OR_TEST", "RUSHGO_APPLICATION_VERSION": "mvp-test", "RUSHGO_SOURCE_COMMIT": strings.Repeat("a", 40), "RUSHGO_BUILD_ID": "build-test", "RUSHGO_CONFIG_SCHEMA_VERSION": "1", "RUSHGO_MIGRATION_VERSION": "migration-digest", "RUSHGO_DEPLOYMENT_ID": "isolated-test"}
	for k, value := range values {
		v.Set(k, value)
	}
	return v
}

func TestReleaseRejectsAmbiguousModesAndImplicitSecrets(t *testing.T) {
	for _, tc := range []struct{ key, value string }{{"RUSHGO_ENV", "typo"}, {"RUSHGO_ENV", "production"}, {"RUSHGO_PROVIDER_MODE", "STRIPE_LIVE"}, {"RUSHGO_PROVIDER_MODE", "SIMULATED_PROVIDER"}, {"RUSHGO_BUILD_ID", ""}, {"RUSHGO_GRPC_TEST_ONLY_INSECURE", "I_UNDERSTAND_TEST_ONLY"}, {"STRIPE_SECRET_KEY", "sk_test_fixture"}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			v := candidateConfig(t)
			v.Set(tc.key, tc.value)
			if _, err := loadRelease(v); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}
func TestReleaseReadsOnlyExplicitPrivateSecretFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte("sk_test_synthetic\n"), 0600); err != nil {
		t.Fatal(err)
	}
	v := candidateConfig(t)
	v.Set("STRIPE_SECRET_KEY_FILE", p)
	if _, err := loadRelease(v); err != nil || v.GetString("STRIPE_SECRET_KEY") != "sk_test_synthetic" {
		t.Fatal("private secret failed")
	}
	v = candidateConfig(t)
	v.Set("STRIPE_SECRET_KEY_FILE", p)
	v.Set("STRIPE_SECRET_KEY", "sk_test_ambient")
	if _, err := loadRelease(v); err == nil {
		t.Fatal("ambiguous source accepted")
	}
	v = candidateConfig(t)
	v.Set("STRIPE_SECRET_KEY_FILE", p+"-missing")
	if _, err := loadRelease(v); err == nil || strings.Contains(err.Error(), p) {
		t.Fatal("missing source accepted or path leaked")
	}
}
func TestProviderModeAndLiveGuard(t *testing.T) {
	for _, cfg := range []Config{{StripeSecretKey: "sk_live_synthetic"}, {StripePublishableKey: "pk_live_synthetic"}, {Release: ReleaseIdentity{ProviderMode: "SIMULATED_PROVIDER"}, StripeSecretKey: "sk_test_synthetic"}, {ServiceName: "payment-service", Release: ReleaseIdentity{Environment: "preproduction"}}} {
		if validateExternal(&cfg) == nil {
			t.Fatal("unsafe provider configuration accepted")
		}
	}
}

func TestExternalUserMissingAndShortJWTRejectedBySharedConfig(t *testing.T) {
	for _, secret := range []string{"", "short", strings.Repeat("x", 31)} {
		c := &Config{ServiceName: "user-service", Release: ReleaseIdentity{Environment: "preproduction"}, JWTSecret: secret}
		err := validateExternal(c)
		if err == nil || !strings.Contains(err.Error(), "JWT") {
			t.Fatal("User secret was not checked before downstream setup")
		}
	}
}
func TestLoadRejectsProductionAndLiveConfig(t *testing.T) {
	t.Setenv("RUSHGO_ENV", "production")
	t.Setenv("RUSHGO_PROVIDER_MODE", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LIVE") {
		t.Fatalf("production Load = %v; want fail-closed LIVE rejection", err)
	}
	t.Setenv("RUSHGO_ENV", "stripe_test")
	t.Setenv("RUSHGO_PROVIDER_MODE", "STRIPE_LIVE")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LIVE") {
		t.Fatalf("STRIPE_LIVE Load = %v; want fail-closed rejection", err)
	}
	t.Setenv("RUSHGO_PROVIDER_MODE", "STRIPE_SANDBOX_OR_TEST")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "required configuration") {
		t.Fatalf("external missing identity Load = %v; want ErrNotConfigured", err)
	}
}

func TestLocalCompatibilityAndExplicitDemo(t *testing.T) {
	v := viper.New()
	r, err := loadRelease(v)
	if err != nil || r.Environment != "local" {
		t.Fatal("local compatibility")
	}
	v.Set("RUSHGO_ENV", "isolated_demo")
	if _, err = loadRelease(v); err == nil {
		t.Fatal("ambiguous demo accepted")
	}
	v.Set("RUSHGO_PROVIDER_MODE", "SIMULATED_PROVIDER")
	if _, err = loadRelease(v); err != nil {
		t.Fatal(err)
	}
}
