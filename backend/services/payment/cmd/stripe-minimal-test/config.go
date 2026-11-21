//go:build stripe_minimal_test

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

const (
	optInEnv                   = "RUSHGO_STRIPE_MINIMAL_TEST"
	configEnv                  = "RUSHGO_STRIPE_MINIMAL_CONFIG_FILE"
	runIDEnv                   = "RUSHGO_STRIPE_MINIMAL_RUN_ID"
	sourceEnv                  = "RUSHGO_STRIPE_MINIMAL_SOURCE_COMMIT"
	tokenEnv                   = "RUSHGO_STRIPE_MINIMAL_CONTROL_TOKEN"
	stateEnv                   = "RUSHGO_STRIPE_MINIMAL_STATE_FILE"
	receiptRecoveryEnv         = "RUSHGO_STRIPE_MINIMAL_RECEIPT_RECOVERY"
	defaultWorkerOptInEnv      = "RUSHGO_STRIPE_UNUSED_REFUND_TEST_ONLY"
	defaultWorkerOptInValue    = "I_UNDERSTAND_ONE_TEST_PAYMENT_AND_REFUND"
	runtimeModeManual          = "manual"
	runtimeModeDefaultWorker   = "default_worker"
	financialWorkerModeDefault = "background_default_worker"
	approvedAccount            = "acct_1UN55BD3fWINmaYl"
	approvedRecoveryRunID      = "a0d8abe1-35d8-427e-9749-700719cdbb56"
	approvedRecoverySource     = "7b3484b2a2cc37686b0ff77c88037b24aff30f21"
	approvedRecoveryPaymentID  = "328b1960-d019-4303-92d9-d25b0d26804e"
	approvedRecoveryBookingID  = "1cdc1df1-aa45-433e-a629-6e7dd8d1b146"
	approvedRecoveryIntentID   = "pi_3UXYJ7D3fWINmaYl0Cr5Gxmm"
)

var fullCommit = regexp.MustCompile(`^[0-9a-f]{40}$`)

type testConfig struct {
	Version               int    `json:"version"`
	RuntimeMode           string `json:"runtime_mode,omitempty"`
	RunID                 string `json:"run_id"`
	SourceCommit          string `json:"source_commit"`
	DatabaseURL           string `json:"database_url"`
	ExpectedDatabase      string `json:"expected_database"`
	ExpectedUser          string `json:"expected_user"`
	ExpectedStripeAccount string `json:"expected_stripe_account"`
	ExpectedUserID        string `json:"expected_user_id"`
	BookingAddress        string `json:"booking_address"`
	EventAddress          string `json:"event_address"`
	KafkaBrokers          string `json:"kafka_brokers"`
	ListenAddress         string `json:"listen_address"`
	WebhookAddress        string `json:"webhook_address"`
	ControlAddress        string `json:"control_address"`
}

func loadTestConfig(sourceCommit string) (testConfig, string, string, error) {
	var c testConfig
	if os.Getenv(optInEnv) != "1" {
		return c, "", "", errors.New("explicit TEST-only process opt-in required")
	}
	if err := rejectConnectionOverrides(); err != nil {
		return c, "", "", err
	}
	path := strings.TrimSpace(os.Getenv(configEnv))
	if path == "" {
		return c, "", "", errors.New("private TEST config path required")
	}
	info, err := os.Stat(path)
	if err != nil || !privateRegularFile(info) {
		return c, "", "", errors.New("TEST config must be a private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, "", "", errors.New("TEST config cannot be read")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF {
		return c, "", "", errors.New("TEST config JSON invalid")
	}
	mode, err := validateRuntimeMode(c.RuntimeMode, os.Getenv(defaultWorkerOptInEnv))
	if err != nil {
		return c, "", "", err
	}
	c.RuntimeMode = mode
	run, err := uuid.Parse(c.RunID)
	if err != nil || run == uuid.Nil || c.RunID != run.String() || os.Getenv(runIDEnv) != c.RunID {
		return c, "", "", errors.New("run identity mismatch")
	}
	if err := validateSourceIdentity(c, sourceCommit, os.Getenv(sourceEnv), os.Getenv(receiptRecoveryEnv)); err != nil {
		return c, "", "", errors.New("source identity mismatch")
	}
	if c.Version != 1 || c.ExpectedStripeAccount != approvedAccount || !uuidPattern(c.ExpectedUserID) {
		return c, "", "", errors.New("TEST scope configuration mismatch")
	}
	if c.ExpectedDatabase == "" || c.ExpectedUser == "" || !strings.Contains(c.ExpectedDatabase, strings.ReplaceAll(c.RunID, "-", "")[:12]) || !strings.Contains(c.ExpectedUser, strings.ReplaceAll(c.RunID, "-", "")[:12]) {
		return c, "", "", errors.New("run-specific database identity required")
	}
	dsn, err := url.Parse(c.DatabaseURL)
	if err != nil || (dsn.Scheme != "postgres" && dsn.Scheme != "postgresql") || dsn.User == nil || dsn.User.Username() != c.ExpectedUser || strings.TrimPrefix(dsn.Path, "/") != c.ExpectedDatabase {
		return c, "", "", errors.New("database URL identity mismatch")
	}
	if strings.TrimSpace(c.BookingAddress) == "" || strings.TrimSpace(c.EventAddress) == "" || strings.TrimSpace(c.KafkaBrokers) == "" {
		return c, "", "", errors.New("Booking, Event, and Kafka addresses are required")
	}
	for _, address := range []string{c.ListenAddress, c.WebhookAddress, c.ControlAddress} {
		if !internalListen(address) {
			return c, "", "", errors.New("listeners must bind only to isolated-network wildcard addresses")
		}
	}
	key, webhook := os.Getenv("STRIPE_SECRET_KEY"), os.Getenv("STRIPE_SECRET_WEBHOOK")
	if !strings.HasPrefix(key, "sk_test_") && !strings.HasPrefix(key, "rk_test_") {
		return c, "", "", errors.New("TEST Stripe key required")
	}
	if !strings.HasPrefix(webhook, "whsec_") {
		return c, "", "", errors.New("signed Stripe listener secret required")
	}
	token := os.Getenv(tokenEnv)
	if len(token) < 32 || strings.TrimSpace(token) != token {
		return c, "", "", errors.New("private control token required")
	}
	if strings.TrimSpace(os.Getenv(stateEnv)) == "" {
		return c, "", "", errors.New("persistent TEST state path required")
	}
	return c, key, webhook, nil
}

func validateRuntimeMode(configured, optIn string) (string, error) {
	switch configured {
	case "", runtimeModeManual:
		if optIn != "" {
			return "", errors.New("default-worker opt-in requires runtime_mode")
		}
		return runtimeModeManual, nil
	case runtimeModeDefaultWorker:
		if optIn != defaultWorkerOptInValue {
			return "", errors.New("default-worker explicit opt-in required")
		}
		return runtimeModeDefaultWorker, nil
	default:
		return "", errors.New("TEST runtime mode invalid")
	}
}

func validateSourceIdentity(c testConfig, binarySource, configuredSource, recoveryRun string) error {
	if !fullCommit.MatchString(binarySource) {
		return errors.New("binary source identity invalid")
	}
	if recoveryRun == "" {
		if c.SourceCommit != binarySource || configuredSource != binarySource {
			return errors.New("source identity mismatch")
		}
		return nil
	}
	recoveryID, err := uuid.Parse(recoveryRun)
	if err != nil || recoveryID == uuid.Nil || recoveryID.String() != recoveryRun || recoveryRun != approvedRecoveryRunID || c.RunID != approvedRecoveryRunID ||
		c.SourceCommit != approvedRecoverySource || configuredSource != approvedRecoverySource || binarySource == approvedRecoverySource {
		return errors.New("receipt recovery source or run identity mismatch")
	}
	return nil
}

func rejectConnectionOverrides() error {
	blocked := map[string]bool{}
	for _, name := range []string{"PGHOST", "PGHOSTADDR", "PGPORT", "PGUSER", "PGPASSWORD", "PGPASSFILE", "PGDATABASE", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS", "PGSSLMODE", "PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT", "PGSSLCRL", "PGCONNECT_TIMEOUT", "PGAPPNAME", "PGTARGETSESSIONATTRS", "DATABASE_URL", "PAYMENT_DATABASE_URL", "STRIPE_API_KEY", "STRIPE_LIVE_SECRET_KEY"} {
		blocked[name] = true
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		if blocked[upper] || (strings.HasPrefix(upper, "STRIPE_") && strings.Contains(upper, "LIVE")) {
			return errors.New("conflicting database or Stripe live credential environment is present")
		}
	}
	return nil
}

func uuidPattern(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && s == id.String()
}

func internalListen(s string) bool {
	host, port, ok := strings.Cut(s, ":")
	return ok && host == "0.0.0.0" && port != ""
}
