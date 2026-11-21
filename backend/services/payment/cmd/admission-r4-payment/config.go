//go:build admission_r4

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	processOptInEnv       = "RUSHGO_ADMISSION_R4_PAYMENT_PROCESS"
	dualDBOptInEnv        = "RUSHGO_ADMISSION_DUALDB_TEST_ONLY"
	dualDBOptInValue      = "I_UNDERSTAND_ISOLATED_DATABASES_AND_LOCAL_PROVIDER_ONLY"
	configFileEnv         = "RUSHGO_ADMISSION_R4_PAYMENT_CONFIG_FILE"
	runIDEnv              = "RUSHGO_ADMISSION_R4_RUN_ID"
	controlTokenEnv       = "RUSHGO_ADMISSION_R4_CONTROL_TOKEN"
	sourceCommitEnv       = "R4_SOURCE_COMMIT"
	fullServiceOptInEnv   = "RUSHGO_UNUSED_REFUND_FULLSERVICE_TEST_ONLY"
	fullServiceOptInValue = "I_UNDERSTAND_ISOLATED_SYNTHETIC_DEFAULT_WORKER"
)

var (
	commitPattern = regexp.MustCompile(`^[0-9a-f]+$`)
	namePattern   = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`)
	intentPattern = regexp.MustCompile(`^pi_r4_[0-9a-f]{32}$`)
)

// sourceCommit is set by the isolated R4 build with -ldflags.
var sourceCommit = "UNSET"

type bootstrapConfig struct {
	Version              int                    `json:"version"`
	RunID                string                 `json:"run_id"`
	SourceCommit         string                 `json:"source_commit"`
	DatabaseURL          string                 `json:"database_url"`
	ExpectedDatabase     string                 `json:"expected_database"`
	ExpectedUser         string                 `json:"expected_user"`
	GRPCListenAddress    string                 `json:"grpc_listen_address"`
	GRPCAdvertiseAddress string                 `json:"grpc_advertise_address"`
	ControlListenAddress string                 `json:"control_listen_address"`
	BookingAddress       string                 `json:"booking_address"`
	BookingServerName    string                 `json:"booking_server_name"`
	RuntimeMode          string                 `json:"runtime_mode,omitempty"`
	EventAddress         string                 `json:"event_address,omitempty"`
	KafkaBrokers         []string               `json:"kafka_brokers,omitempty"`
	DynamicPurchase      *dynamicPurchaseConfig `json:"dynamic_purchase,omitempty"`
	MaxBarrierWaitMS     int                    `json:"max_barrier_wait_ms"`
	Fixtures             []fixtureConfig        `json:"fixtures"`
}

type dynamicPurchaseConfig struct {
	Enabled bool   `json:"enabled"`
	UserID  string `json:"user_id"`
}

type fixtureConfig struct {
	PaymentID       string `json:"payment_id"`
	BookingID       string `json:"booking_id"`
	UserID          string `json:"user_id"`
	Generation      int64  `json:"generation"`
	PaymentIntentID string `json:"payment_intent_id"`
	ProviderAccount string `json:"provider_account"`
	AmountCents     int64  `json:"amount_cents"`
	Currency        string `json:"currency"`
	SeedFinancial   bool   `json:"seed_financial"`
}

func loadBootstrapConfig() (bootstrapConfig, string, error) {
	var cfg bootstrapConfig
	if os.Getenv(processOptInEnv) != "1" || os.Getenv(dualDBOptInEnv) != dualDBOptInValue {
		return cfg, "", errors.New("test-only process opt-in is missing")
	}
	if hasForbiddenEnvironment() {
		return cfg, "", errors.New("incompatible database or Stripe secret environment is present")
	}
	path := strings.TrimSpace(os.Getenv(configFileEnv))
	if path == "" {
		return cfg, "", errors.New("test-only config file is required")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return cfg, "", errors.New("test-only config file must be a private regular file")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return cfg, "", errors.New("test-only config file cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil || decoder.Decode(new(any)) != io.EOF {
		return cfg, "", errors.New("test-only config JSON is invalid")
	}
	if cfg.Version != 1 {
		return cfg, "", errors.New("unsupported test-only config version")
	}
	if err := validateRuntimeMode(&cfg, os.Getenv(fullServiceOptInEnv)); err != nil {
		return cfg, "", err
	}
	runID, err := uuid.Parse(cfg.RunID)
	if err != nil || runID == uuid.Nil || cfg.RunID != runID.String() || os.Getenv(runIDEnv) != cfg.RunID {
		return cfg, "", errors.New("run identity does not match the fresh run gate")
	}
	compiledCommit := strings.TrimSpace(sourceCommit)
	if !commitPattern.MatchString(compiledCommit) || (len(compiledCommit) != 40 && len(compiledCommit) != 64) ||
		os.Getenv(sourceCommitEnv) != compiledCommit || cfg.SourceCommit != compiledCommit {
		return cfg, "", errors.New("source commit does not match the compiled source gate")
	}
	token := os.Getenv(controlTokenEnv)
	if len(token) < 32 || strings.TrimSpace(token) != token {
		return cfg, "", errors.New("test control token must contain at least 32 non-space bytes")
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" || !namePattern.MatchString(cfg.ExpectedDatabase) || !namePattern.MatchString(cfg.ExpectedUser) ||
		cfg.BookingAddress == "" || strings.TrimSpace(cfg.BookingServerName) == "" {
		return cfg, "", errors.New("test-only database, authority, or provider identity is incomplete")
	}
	suffix := strings.ReplaceAll(cfg.RunID, "-", "")[:12]
	if !strings.Contains(cfg.ExpectedDatabase, suffix) || !strings.Contains(cfg.ExpectedUser, suffix) {
		return cfg, "", errors.New("expected database and user must be bound to this run id")
	}
	if err := validateInternalServiceAddress(cfg.BookingAddress); err != nil {
		return cfg, "", errors.New("Booking authority must be addressed on the isolated service network")
	}
	if err := validateInternalServiceAddress(cfg.GRPCAdvertiseAddress); err != nil {
		return cfg, "", errors.New("Payment advertise address must be on the isolated service network")
	}
	if err := validateDatabaseURL(cfg.DatabaseURL, cfg.ExpectedDatabase, cfg.ExpectedUser); err != nil {
		return cfg, "", err
	}
	if err := validateGRPCListenAddress(cfg.GRPCListenAddress); err != nil {
		return cfg, "", err
	}
	if strings.TrimSpace(cfg.GRPCAdvertiseAddress) == "" {
		return cfg, "", errors.New("gRPC advertise address is required")
	}
	if err := validateControlListenAddress(cfg.ControlListenAddress); err != nil {
		return cfg, "", err
	}
	if cfg.MaxBarrierWaitMS < 1 || cfg.MaxBarrierWaitMS > 30000 {
		return cfg, "", errors.New("test barrier deadline must be between 1 and 30000 milliseconds")
	}
	if len(cfg.Fixtures) == 0 || len(cfg.Fixtures) > 32 {
		return cfg, "", errors.New("one to thirty-two synthetic Payment fixtures are required")
	}
	seenPayments := make(map[string]struct{}, len(cfg.Fixtures))
	seenBookings := make(map[string]struct{}, len(cfg.Fixtures))
	seenIntents := make(map[string]struct{}, len(cfg.Fixtures))
	for _, fixture := range cfg.Fixtures {
		paymentID, paymentErr := uuid.Parse(fixture.PaymentID)
		bookingID, bookingErr := uuid.Parse(fixture.BookingID)
		userID, userErr := uuid.Parse(fixture.UserID)
		if paymentErr != nil || bookingErr != nil || userErr != nil || paymentID == uuid.Nil || bookingID == uuid.Nil || userID == uuid.Nil ||
			fixture.Generation != 1 || strings.TrimSpace(fixture.PaymentIntentID) == "" || fixture.AmountCents < 1 || fixture.AmountCents > 2147483647 ||
			fixture.ProviderAccount != "acct_r4_local" || !intentPattern.MatchString(fixture.PaymentIntentID) ||
			fixture.Currency != "usd" || !fixture.SeedFinancial ||
			fixture.PaymentID != paymentID.String() || fixture.BookingID != bookingID.String() || fixture.UserID != userID.String() {
			return cfg, "", errors.New("synthetic Payment fixture identity is invalid")
		}
		if _, ok := seenPayments[fixture.PaymentID]; ok {
			return cfg, "", errors.New("duplicate Payment fixture identity")
		}
		if _, ok := seenBookings[fixture.BookingID]; ok {
			return cfg, "", errors.New("duplicate Booking fixture identity")
		}
		if _, ok := seenIntents[fixture.PaymentIntentID]; ok {
			return cfg, "", errors.New("duplicate provider intent fixture identity")
		}
		seenPayments[fixture.PaymentID] = struct{}{}
		seenBookings[fixture.BookingID] = struct{}{}
		seenIntents[fixture.PaymentIntentID] = struct{}{}
	}
	return cfg, token, nil
}

func validateRuntimeMode(cfg *bootstrapConfig, fullServiceOptIn string) error {
	if cfg == nil {
		return errors.New("Payment test runtime mode is missing")
	}
	if cfg.RuntimeMode == "" {
		cfg.RuntimeMode = "controlled_r4"
	}
	if cfg.RuntimeMode != "controlled_r4" && cfg.RuntimeMode != "default_worker" {
		return errors.New("unsupported Payment test runtime mode")
	}
	if cfg.RuntimeMode != "default_worker" {
		if cfg.DynamicPurchase != nil {
			return errors.New("dynamic purchase is available only in default-worker mode")
		}
		return nil
	}
	if fullServiceOptIn != fullServiceOptInValue {
		return errors.New("full-service default-worker test opt-in is missing")
	}
	if err := validateInternalServiceAddress(cfg.EventAddress); err != nil {
		return errors.New("Event accounting authority must be on the isolated service network")
	}
	if len(cfg.Fixtures) < 2 || len(cfg.Fixtures) > 4 {
		return errors.New("default-worker mode requires two to four synthetic Payment fixtures")
	}
	for _, fixture := range cfg.Fixtures {
		if fixture.AmountCents != 100 || fixture.ProviderAccount != "acct_r4_local" {
			return errors.New("default-worker fixtures must be synthetic USD 1.00 Payments")
		}
	}
	if cfg.DynamicPurchase != nil {
		if err := validateDynamicPurchaseConfig(*cfg.DynamicPurchase, cfg.Fixtures, cfg.KafkaBrokers); err != nil {
			return err
		}
	} else if len(cfg.KafkaBrokers) != 0 {
		if err := validateKafkaBrokers(cfg.KafkaBrokers); err != nil {
			return err
		}
	}
	return nil
}

func validateDynamicPurchaseConfig(dynamic dynamicPurchaseConfig, fixtures []fixtureConfig, brokers []string) error {
	if !dynamic.Enabled {
		return errors.New("dynamic purchase block must explicitly enable the single purchase slot")
	}
	userID, err := uuid.Parse(dynamic.UserID)
	if err != nil || userID == uuid.Nil || dynamic.UserID != userID.String() {
		return errors.New("dynamic purchase buyer must be a canonical non-nil run-owned UUID")
	}
	for _, fixture := range fixtures {
		if fixture.UserID == dynamic.UserID {
			return errors.New("dynamic purchase buyer must be distinct from every static fixture buyer")
		}
	}
	if len(brokers) == 0 {
		return errors.New("dynamic purchase requires an explicit isolated Kafka broker list")
	}
	return validateKafkaBrokers(brokers)
}

func validateKafkaBrokers(brokers []string) error {
	if len(brokers) == 0 || len(brokers) > 4 {
		return errors.New("Kafka broker list must contain one to four isolated addresses")
	}
	seen := make(map[string]struct{}, len(brokers))
	for _, broker := range brokers {
		if strings.TrimSpace(broker) != broker || broker == "" || strings.Contains(broker, ",") || validateInternalServiceAddress(broker) != nil {
			return errors.New("Kafka brokers must be explicitly addressed on the isolated service network")
		}
		if _, ok := seen[broker]; ok {
			return errors.New("Kafka broker list contains a duplicate address")
		}
		seen[broker] = struct{}{}
	}
	return nil
}

func hasForbiddenEnvironment() bool {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		switch upper {
		case "PGHOST", "PGHOSTADDR", "PGPORT", "PGUSER", "PGPASSWORD", "PGPASSFILE", "PGDATABASE",
			"PGSERVICE", "PGSERVICEFILE", "PGOPTIONS", "PGSSLMODE", "PGREQUIRESSL", "PGSSLCOMPRESSION",
			"PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT", "PGSSLCRL", "PGSSLCRLDIR", "PGCONNECT_TIMEOUT",
			"PGCHANNELBINDING", "PGSSLSNI", "PGGSSENCMODE", "PGKRBSRVNAME", "PGGSSLIB", "PGAPPNAME",
			"PGTARGETSESSIONATTRS", "PGLOADBALANCEHOSTS", "PGCLIENTENCODING", "PGDATESTYLE", "PGTZ",
			"DATABASE_URL", "PAYMENT_DATABASE_URL":
			return true
		}
		if strings.HasPrefix(upper, "STRIPE_") && (strings.Contains(upper, "SECRET") || upper == "STRIPE_API_KEY") {
			return true
		}
	}
	return false
}

func validateDatabaseURL(dsn, expectedDatabase, expectedUser string) error {
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.User == nil || parsed.Hostname() == "" || parsed.Port() == "" ||
		parsed.User.Username() != expectedUser || strings.TrimPrefix(parsed.Path, "/") != expectedDatabase || parsed.User.String() == "" {
		return errors.New("Payment database URL does not match the run-specific owner identity")
	}
	password, hasPassword := parsed.User.Password()
	if !hasPassword || password == "" {
		return errors.New("Payment database URL must use the run-specific database user credential")
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(query) > 1 || (len(query) == 1 && (len(query["sslmode"]) != 1 || !validSSLMode(query.Get("sslmode")))) {
		return errors.New("Payment database URL has unsupported connection overrides")
	}
	host := parsed.Hostname()
	if !isInternalDatabaseHost(host) {
		return errors.New("Payment database host must be loopback, private, or a single-label test-network name")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil || poolConfig == nil || poolConfig.ConnConfig == nil {
		return errors.New("Payment database connection settings are invalid")
	}
	conn := poolConfig.ConnConfig
	if conn.Host != host || conn.Port == 0 || conn.Database != expectedDatabase || conn.User != expectedUser || len(conn.Fallbacks) != 0 {
		return errors.New("effective Payment database connection differs from the run-specific owner identity")
	}
	return nil
}

func validSSLMode(mode string) bool {
	switch mode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func isInternalDatabaseHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
	}
	if host == "localhost" {
		return true
	}
	return !strings.Contains(host, ".") && namePattern.MatchString(host)
}

func validateInternalServiceAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" || host == "" || !isInternalDatabaseHost(host) {
		return errors.New("service address is not internal")
	}
	return nil
}

func validateGRPCListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return errors.New("Payment gRPC listen address is invalid")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("Payment gRPC listener must be loopback or the internal-network wildcard")
	}
	return nil
}

func validateControlListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return errors.New("Payment control listen address is invalid")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	if address == "0.0.0.0:18081" {
		return nil
	}
	return errors.New("Payment control must use loopback or the isolated-network 0.0.0.0:18081 binding")
}
