//go:build ticket_e2e

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/config"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type capturedSimulatedEvents struct {
	events []pkgkafka.Event
}

func (p *capturedSimulatedEvents) Publish(_ context.Context, topic, key string, event pkgkafka.Event) error {
	if topic != topics.TopicPaymentEvents || key == "" {
		return context.Canceled
	}
	p.events = append(p.events, event)
	return nil
}

func (*capturedSimulatedEvents) Close() error { return nil }

func validTicketE2EConfig() *config.Config {
	return &config.Config{
		DatabaseURL:        "postgres://ticket_e2e:test-only@ticket-e2e-postgres:5432/rushgo_ticket_e2e_booking?sslmode=disable",
		RedisURL:           "redis://ticket-e2e-redis:6379",
		GRPCPort:           "50053",
		UserServiceAddr:    "user-service:50051",
		EventServiceAddr:   "event-service:50052",
		PaymentServiceAddr: "booking-e2e:50053",
		SagaServiceAddr:    "saga-service:50055",
		KafkaBrokers:       []string{"ticket-e2e-kafka:9092"},
	}
}

func validTicketE2EEnvironment() map[string]string {
	return map[string]string{
		"TICKET_E2E_MODE":       simulatedProviderAuthoritySource,
		"TICKET_E2E_FIXTURE_ID": uuid.NewString(),
		"TICKET_E2E_PROJECT":    "rushgo-ticket-e2e-20261024-0123456789ab",
		"COMPOSE_PROJECT_NAME":  "rushgo-ticket-e2e-20261024-0123456789ab",
	}
}

func ticketE2EEnvironmentEntries(environment map[string]string) []string {
	entries := make([]string, 0, len(environment))
	for key, value := range environment {
		entries = append(entries, key+"="+value)
	}
	return entries
}

func TestTicketE2ERuntimeRequiresExactIsolatedIdentityAndNoStripeCredentials(t *testing.T) {
	cfg := validTicketE2EConfig()
	env := validTicketE2EEnvironment()
	getenv := func(name string) string { return env[name] }
	if err := validateTicketE2EStartupWithEnvironment(cfg, getenv, ticketE2EEnvironmentEntries(env)); err != nil {
		t.Fatalf("valid isolated ticket E2E runtime rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*config.Config, map[string]string)
	}{
		{name: "wrong mode", mutate: func(_ *config.Config, values map[string]string) { values["TICKET_E2E_MODE"] = "true" }},
		{name: "wrong database", mutate: func(cfg *config.Config, _ map[string]string) {
			cfg.DatabaseURL = "postgres://app:test@main-postgres:5432/ticketbox_booking?sslmode=disable"
		}},
		{name: "wrong payment service", mutate: func(cfg *config.Config, _ map[string]string) { cfg.PaymentServiceAddr = "payment:50054" }},
		{name: "wrong Redis endpoint", mutate: func(cfg *config.Config, _ map[string]string) { cfg.RedisURL = "redis://main-redis:6379" }},
		{name: "wrong user service", mutate: func(cfg *config.Config, _ map[string]string) { cfg.UserServiceAddr = "main-user:50051" }},
		{name: "wrong Event service", mutate: func(cfg *config.Config, _ map[string]string) { cfg.EventServiceAddr = "main-event:50052" }},
		{name: "wrong Saga service", mutate: func(cfg *config.Config, _ map[string]string) { cfg.SagaServiceAddr = "main-saga:50055" }},
		{name: "wrong Kafka", mutate: func(cfg *config.Config, _ map[string]string) { cfg.KafkaBrokers = []string{"main-kafka:9092"} }},
		{name: "external telemetry enabled", mutate: func(cfg *config.Config, _ map[string]string) {
			cfg.OTEL.Enabled = true
			cfg.OTEL.ExporterEndpoint = "https://telemetry.example.invalid"
		}},
		{name: "Stripe credential present", mutate: func(_ *config.Config, values map[string]string) { values["STRIPE_SECRET_KEY"] = "present" }},
		{name: "unrecognized Stripe credential present", mutate: func(_ *config.Config, values map[string]string) { values["LOCAL_STRIPE_WEBHOOK_SECRET"] = "present" }},
		{name: "Stripe credential in loaded config", mutate: func(cfg *config.Config, _ map[string]string) { cfg.StripeSecretKey = "present" }},
		{name: "invalid fixture identity", mutate: func(_ *config.Config, values map[string]string) { values["TICKET_E2E_FIXTURE_ID"] = "not-a-uuid" }},
		{name: "missing project identity", mutate: func(_ *config.Config, values map[string]string) { values["TICKET_E2E_PROJECT"] = "" }},
		{name: "wrong project identity", mutate: func(_ *config.Config, values map[string]string) {
			values["TICKET_E2E_PROJECT"] = "rushgo-ticket-e2e-20261023"
		}},
		{name: "compose project mismatch", mutate: func(_ *config.Config, values map[string]string) { values["COMPOSE_PROJECT_NAME"] = "another-project" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changedConfig := *cfg
			changedEnv := map[string]string{}
			for key, value := range env {
				changedEnv[key] = value
			}
			tc.mutate(&changedConfig, changedEnv)
			if err := validateTicketE2EStartupWithEnvironment(&changedConfig, func(name string) string { return changedEnv[name] }, ticketE2EEnvironmentEntries(changedEnv)); err == nil {
				t.Fatal("unsafe ticket E2E runtime identity accepted")
			}
		})
	}
}

func TestTicketE2ERuntimeRejectsEffectivePostgresURLOverrides(t *testing.T) {
	cases := []struct {
		name         string
		query        string
		wantHost     string
		wantPort     uint16
		wantDatabase string
		wantUser     string
	}{
		{name: "host", query: "host=main-postgres", wantHost: "main-postgres", wantPort: 5432, wantDatabase: "rushgo_ticket_e2e_booking", wantUser: "ticket_e2e"},
		{name: "port", query: "port=5433", wantHost: "ticket-e2e-postgres", wantPort: 5433, wantDatabase: "rushgo_ticket_e2e_booking", wantUser: "ticket_e2e"},
		{name: "database", query: "dbname=ticketbox_booking", wantHost: "ticket-e2e-postgres", wantPort: 5432, wantDatabase: "ticketbox_booking", wantUser: "ticket_e2e"},
		{name: "user", query: "user=main_user", wantHost: "ticket-e2e-postgres", wantPort: 5432, wantDatabase: "rushgo_ticket_e2e_booking", wantUser: "main_user"},
		{name: "host address runtime parameter", query: "hostaddr=127.0.0.1", wantHost: "ticket-e2e-postgres", wantPort: 5432, wantDatabase: "rushgo_ticket_e2e_booking", wantUser: "ticket_e2e"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTicketE2EConfig()
			cfg.DatabaseURL += "&" + tc.query
			parsed, err := pgxpool.ParseConfig(cfg.DatabaseURL)
			if err != nil {
				t.Fatalf("pgx did not accept the override probe: %v", err)
			}
			conn := parsed.ConnConfig
			if conn.Host != tc.wantHost || conn.Port != tc.wantPort || conn.Database != tc.wantDatabase || conn.User != tc.wantUser {
				t.Fatalf("probe did not override the effective pgx target: host=%q port=%d database=%q user=%q", conn.Host, conn.Port, conn.Database, conn.User)
			}
			env := validTicketE2EEnvironment()
			if err := validateTicketE2EStartupWithEnvironment(cfg, func(name string) string { return env[name] }, ticketE2EEnvironmentEntries(env)); err == nil {
				t.Fatal("unsafe effective PostgreSQL target accepted")
			}
		})
	}
}

func TestTicketE2ERuntimeRejectsPostgresAuthorityFallbackHosts(t *testing.T) {
	cfg := validTicketE2EConfig()
	cfg.DatabaseURL = "postgres://ticket_e2e:test-only@ticket-e2e-postgres:5432,main-postgres:5432/rushgo_ticket_e2e_booking?sslmode=disable"
	parsed, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("pgx did not accept the fallback-host probe: %v", err)
	}
	if len(parsed.ConnConfig.Fallbacks) == 0 {
		t.Fatal("probe did not create an effective PostgreSQL fallback target")
	}
	env := validTicketE2EEnvironment()
	if err := validateTicketE2EStartupWithEnvironment(cfg, func(name string) string { return env[name] }, ticketE2EEnvironmentEntries(env)); err == nil {
		t.Fatal("unsafe PostgreSQL fallback target accepted")
	}
}

func TestSimulatedPaymentAuthorityBindsIdentityAndFailsClosedUntilCollected(t *testing.T) {
	fixtureID := uuid.NewString()
	publisher := &capturedSimulatedEvents{}
	provider, err := newSimulatedPaymentAuthority(fixtureID, publisher, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	bookingID, userID := uuid.NewString(), uuid.NewString()
	created, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{
		BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2000, Currency: "usd",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.CreateState != "create_ready" || !strings.HasPrefix(created.PaymentIntentId, "pi_simulated_") {
		t.Fatalf("synthetic Payment response mislabeled or incomplete: %+v", created)
	}
	pending, err := provider.GetFinancialPayment(context.Background(), &paymentv1.FinancialPaymentRequest{BookingId: bookingID, Generation: 1})
	if err != nil || pending.ProviderOutcome != "unknown" || pending.ProviderStatus != "requires_payment_method" || pending.ReconciliationState != "pending" {
		t.Fatalf("unconfirmed synthetic Provider state must remain unknown/pending: entry=%+v error=%v", pending, err)
	}
	before, err := provider.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: created.Id, BookingId: bookingID, Generation: 1,
	})
	if err != nil || before.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN {
		t.Fatalf("uncollected simulated payment was not fail-closed: response=%+v error=%v", before, err)
	}
	wrongIdentity, err := provider.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: created.Id, BookingId: uuid.NewString(), Generation: 1,
	})
	if err != nil || wrongIdentity.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN {
		t.Fatalf("mismatched Booking identity was not fail-closed: response=%+v error=%v", wrongIdentity, err)
	}
	if err := provider.confirmAndPublish(context.Background(), bookingID, created.Id); err != nil {
		t.Fatal(err)
	}
	after, err := provider.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: created.Id, BookingId: bookingID, Generation: 1,
	})
	if err != nil || after.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE ||
		after.Source != simulatedProviderAuthoritySource || after.ObservedAt == nil || !after.ObservedAt.AsTime().Before(time.Now().Add(time.Second)) {
		t.Fatalf("valid simulated evidence missing provenance: response=%+v error=%v", after, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].Type != topics.TypePaymentSucceed {
		t.Fatalf("synthetic settlement wake was not published: %+v", publisher.events)
	}
	intent, err := simulatedPaymentIntentWakeID(publisher.events[0].Data)
	if err != nil || intent != created.PaymentIntentId {
		t.Fatalf("synthetic wake was not bound to the fixture intent: intent=%q error=%v", intent, err)
	}
}

func TestSimulatedPaymentAuthorityRejectsIdentityConflictsAndUnknownRiskState(t *testing.T) {
	provider, err := newSimulatedPaymentAuthority(uuid.NewString(), &capturedSimulatedEvents{}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	bookingID, userID := uuid.NewString(), uuid.NewString()
	first, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2000, Currency: "usd"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2000, Currency: "usd"})
	if err != nil || retry.Id != first.Id {
		t.Fatalf("same identity retry did not reuse the synthetic payment: first=%+v retry=%+v error=%v", first, retry, err)
	}
	if _, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2100, Currency: "usd"}); err == nil {
		t.Fatal("amount mismatch reused a synthetic payment")
	}
	if err := provider.confirmAndPublish(context.Background(), bookingID, first.Id); err != nil {
		t.Fatal(err)
	}
	provider.refundState = paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_UNKNOWN
	response, err := provider.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{PaymentId: first.Id, BookingId: bookingID, Generation: 1})
	if err != nil || response.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN {
		t.Fatalf("unknown refund state was not fail-closed: response=%+v error=%v", response, err)
	}
}

func TestSimulatedPaymentAuthorityReturnsStableExplicitSyntheticClientSecret(t *testing.T) {
	provider, err := newSimulatedPaymentAuthority(uuid.NewString(), &capturedSimulatedEvents{}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	bookingID, userID := uuid.NewString(), uuid.NewString()
	request := &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2500, Currency: "usd"}
	first, err := provider.CreatePayment(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := provider.CreatePayment(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.PaymentIntentClientSecret == "" || !strings.HasPrefix(first.PaymentIntentClientSecret, "simulated://SIMULATED_PROVIDER_AUTHORITY/") {
		t.Fatalf("synthetic Payment secret lacks explicit test provenance: %q", first.PaymentIntentClientSecret)
	}
	if retry.PaymentIntentClientSecret != first.PaymentIntentClientSecret {
		t.Fatal("synthetic Payment retry changed its client secret")
	}
}

func TestSimulatedPaymentAuthorityResolvesOnlyExactUncollectedTerminalPayment(t *testing.T) {
	publisher := &capturedSimulatedEvents{}
	provider, err := newSimulatedPaymentAuthority(uuid.NewString(), publisher, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	bookingID, userID, decisionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	created, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2500, Currency: "usd"})
	if err != nil {
		t.Fatal(err)
	}
	request := &paymentv1.PaymentResolutionRequest{PaymentId: created.Id, BookingId: bookingID, Generation: 1, DecisionId: decisionID, Reason: "FAILED"}
	resolved, err := provider.RequestPaymentResolution(context.Background(), request)
	if err != nil {
		t.Fatalf("uncollected synthetic Payment was not safely resolved: %v", err)
	}
	if resolved.Status != "canceled" || resolved.ProviderOutcome != "not_collected" || resolved.ProviderStatus != "canceled" || resolved.ReconciliationState != "resolved" || resolved.RefundState != "not_required" {
		t.Fatalf("synthetic non-collection resolution has unexpected financial state: %+v", resolved)
	}
	retry, err := provider.RequestPaymentResolution(context.Background(), request)
	if err != nil || retry.Id != created.Id || retry.ReconciliationState != "resolved" {
		t.Fatalf("same terminal decision was not idempotent: response=%+v error=%v", retry, err)
	}
	if err := provider.confirmAndPublish(context.Background(), bookingID, created.Id); err == nil {
		t.Fatal("resolved non-collection accepted a late synthetic collection")
	}
	if len(publisher.events) != 0 {
		t.Fatalf("resolved non-collection emitted a collection wake: %+v", publisher.events)
	}
}

func TestSimulatedPaymentAuthorityNeverResolvesCollectedPaymentAsRefunded(t *testing.T) {
	provider, err := newSimulatedPaymentAuthority(uuid.NewString(), &capturedSimulatedEvents{}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	bookingID, userID := uuid.NewString(), uuid.NewString()
	created, err := provider.CreatePayment(context.Background(), &paymentv1.CreatePaymentRequest{BookingId: bookingID, UserId: userID, PaymentGeneration: 1, Price: 2500, Currency: "usd"})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.confirmAndPublish(context.Background(), bookingID, created.Id); err != nil {
		t.Fatal(err)
	}
	_, err = provider.RequestPaymentResolution(context.Background(), &paymentv1.PaymentResolutionRequest{
		PaymentId: created.Id, BookingId: bookingID, Generation: 1, DecisionId: uuid.NewString(), Reason: "FAILED",
	})
	if err == nil {
		t.Fatal("simulated adapter claimed it could compensate a collected payment")
	}
	entry, err := provider.GetFinancialPayment(context.Background(), &paymentv1.FinancialPaymentRequest{BookingId: bookingID, Generation: 1})
	if err != nil || entry.ProviderOutcome != "collected" || entry.Status != "success" {
		t.Fatalf("refused simulated compensation changed collected state: entry=%+v error=%v", entry, err)
	}
}
