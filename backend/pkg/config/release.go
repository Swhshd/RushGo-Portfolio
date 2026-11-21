package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

// ReleaseIdentity contains only public diagnostic metadata, never credentials.
type ReleaseIdentity struct {
	ApplicationVersion  string `json:"applicationVersion"`
	SourceCommit        string `json:"sourceCommit"`
	BuildIdentity       string `json:"buildIdentity"`
	ConfigSchemaVersion string `json:"configSchemaVersion"`
	MigrationVersion    string `json:"migrationVersion"`
	Environment         string `json:"environment"`
	DeploymentIdentity  string `json:"deploymentIdentity"`
	ProviderMode        string `json:"providerMode"`
}

func (r ReleaseIdentity) External() bool {
	return r.Environment == "stripe_test" || r.Environment == "preproduction" || r.Environment == "production"
}

var publicID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var sourceID = regexp.MustCompile(`^[0-9a-f]{40}$`)
var secretNames = []string{"DATABASE_URL", "REDIS_URL", "JWT_SECRET", "SMTP_PASSWORD", "STRIPE_SECRET_KEY", "STRIPE_SECRET_WEBHOOK"}
var ErrNotConfigured = errors.New("required configuration missing")

func loadRelease(v *viper.Viper) (ReleaseIdentity, error) {
	r := ReleaseIdentity{ApplicationVersion: v.GetString("RUSHGO_APPLICATION_VERSION"), SourceCommit: v.GetString("RUSHGO_SOURCE_COMMIT"), BuildIdentity: v.GetString("RUSHGO_BUILD_ID"), ConfigSchemaVersion: v.GetString("RUSHGO_CONFIG_SCHEMA_VERSION"), MigrationVersion: v.GetString("RUSHGO_MIGRATION_VERSION"), Environment: v.GetString("RUSHGO_ENV"), DeploymentIdentity: v.GetString("RUSHGO_DEPLOYMENT_ID"), ProviderMode: v.GetString("RUSHGO_PROVIDER_MODE")}
	if r.Environment == "" {
		r.Environment = "local"
	}
	switch r.Environment {
	case "local", "isolated_demo", "stripe_test", "preproduction", "production":
	default:
		return r, errors.New("invalid RUSHGO_ENV")
	}
	if r.ProviderMode == "STRIPE_LIVE" || r.Environment == "production" {
		return r, errors.New("production financial activation is not authorized; LIVE remains disabled")
	}
	if r.External() {
		for _, name := range []string{"PGPASSWORD", "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE", "PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT"} {
			if os.Getenv(name) != "" {
				return r, errors.New("external configuration forbids ambient PostgreSQL credentials; configure the private DSN explicitly")
			}
		}
		if r.ProviderMode == "" || r.SourceCommit == "" || r.ConfigSchemaVersion == "" || r.ApplicationVersion == "" || r.BuildIdentity == "" || r.MigrationVersion == "" || r.DeploymentIdentity == "" {
			return r, ErrNotConfigured
		}
		if r.ProviderMode != "STRIPE_SANDBOX_OR_TEST" {
			return r, errors.New("external candidate requires explicit STRIPE_SANDBOX_OR_TEST")
		}
		if !sourceID.MatchString(r.SourceCommit) || r.ConfigSchemaVersion != "1" {
			return r, errors.New("release source/config schema identity missing or invalid")
		}
		for _, value := range []string{r.ApplicationVersion, r.BuildIdentity, r.MigrationVersion, r.DeploymentIdentity} {
			if !publicID.MatchString(value) {
				return r, errors.New("release identity missing or invalid")
			}
		}
		if v.GetString("RUSHGO_GRPC_TEST_ONLY_INSECURE") != "" {
			return r, errors.New("external environment forbids TEST_ONLY plaintext RPC")
		}
	}
	if r.Environment == "isolated_demo" && r.ProviderMode != "SIMULATED_PROVIDER" {
		return r, errors.New("isolated demo requires explicit SIMULATED_PROVIDER")
	}
	if r.ProviderMode != "" && r.ProviderMode != "SIMULATED_PROVIDER" && r.ProviderMode != "STRIPE_SANDBOX_OR_TEST" {
		return r, errors.New("unknown provider mode")
	}
	for _, name := range secretNames {
		path, inline := v.GetString(name+"_FILE"), v.GetString(name)
		if path == "" {
			if r.External() && inline != "" {
				return r, fmt.Errorf("%s requires an explicit private _FILE source", name)
			}
			continue
		}
		if inline != "" {
			return r, fmt.Errorf("%s and _FILE cannot both be configured", name)
		}
		info, err := os.Lstat(path)
		if err != nil || !filepath.IsAbs(path) || !info.Mode().IsRegular() || info.Size() > 65536 || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
			return r, fmt.Errorf("%s_FILE must be a private regular absolute file", name)
		}
		contents, err := os.ReadFile(path)
		value := strings.TrimSuffix(strings.TrimSuffix(string(contents), "\n"), "\r")
		if err != nil || value == "" || strings.ContainsAny(value, "\r\n\x00") {
			return r, fmt.Errorf("%s_FILE is unreadable or invalid", name)
		}
		v.Set(name, value)
	}
	return r, nil
}

func validateExternal(c *Config) error {
	// Reject LIVE even in local mode; this is additional to the provider's account gate.
	if strings.HasPrefix(c.StripeSecretKey, "sk_live_") || strings.HasPrefix(c.StripeSecretKey, "rk_live_") || strings.HasPrefix(c.StripePublishableKey, "pk_live_") {
		return errors.New("LIVE Stripe credentials are disabled")
	}
	if c.Release.ProviderMode == "SIMULATED_PROVIDER" && (c.StripeSecretKey != "" || c.StripeSecretWebhook != "" || c.StripePublishableKey != "") {
		return errors.New("simulated provider forbids Stripe credentials")
	}
	if !c.Release.External() {
		return nil
	}
	if c.ServiceName == "" {
		return ErrNotConfigured
	}
	if c.ServiceName == "user-service" && len(c.JWTSecret) < 32 {
		return errors.New("external User requires a JWT secret of at least 32 bytes")
	}
	if c.DatabaseURL != "" {
		u, err := url.Parse(c.DatabaseURL)
		if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Query().Get("sslmode") != "verify-full" {
			return errors.New("external database requires PostgreSQL verify-full TLS")
		}
		if u.User == nil || u.User.Username() == "" || u.Query().Get("sslrootcert") == "" {
			return errors.New("database identity and root CA must be explicit in the private DSN")
		}
		password, ok := u.User.Password()
		if !ok || password == "" {
			return errors.New("database credential must be explicit in the private DSN")
		}
	}
	if c.RedisURL != "" {
		u, err := url.Parse(c.RedisURL)
		if err != nil || u.Scheme != "rediss" || u.User == nil {
			return errors.New("external Redis requires TLS and ACL credentials")
		}
		password, ok := u.User.Password()
		if !ok || password == "" {
			return errors.New("external Redis requires ACL credentials")
		}
	}
	if c.ServiceName != "gateway-service" {
		if _, err := KafkaTLSFromEnv(); err != nil {
			return err
		}
	}
	if c.StripeSecretKey != "" && !strings.HasPrefix(c.StripeSecretKey, "sk_test_") && !strings.HasPrefix(c.StripeSecretKey, "rk_test_") {
		return errors.New("Stripe TEST key required")
	}
	if c.StripePublishableKey != "" && !strings.HasPrefix(c.StripePublishableKey, "pk_test_") {
		return errors.New("Stripe TEST publishable key required")
	}
	if c.ServiceName == "payment-service" && (c.StripeSecretKey == "" || !strings.HasPrefix(c.StripeSecretWebhook, "whsec_") || !strings.HasPrefix(os.Getenv("RUSHGO_STRIPE_EXPECTED_ACCOUNT"), "acct_")) {
		return errors.New("Payment TEST credentials, endpoint secret and expected account are required")
	}
	if c.AuthFrontendOrigin != "" {
		u, err := url.Parse(c.AuthFrontendOrigin)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return errors.New("external frontend origin requires HTTPS")
		}
	}
	return nil
}
