//go:build ticket_e2e

package grpcsecurity

import (
	"os"
	"strings"
	"testing"
)

func TestTestOnlyPlaintextRequiresPinnedSyntheticRuntime(t *testing.T) {
	setTicketE2ERuntime(t, "gateway")
	if ticketE2EBuildSourceHead == "" || ticketE2EBuildSourceFingerprint == "" || ticketE2EBuildProject == "" {
		if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
			t.Fatal("ticket_e2e binary without embedded candidate identity enabled plaintext")
		}
		return
	}
	creds, enabled, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway)
	if err != nil || !enabled || creds == nil {
		t.Fatalf("isolated TEST_ONLY transport = (%v, %t, %v), want enabled credentials", creds, enabled, err)
	}
	t.Setenv("TICKET_E2E_PROJECT", "another-project")
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("TEST_ONLY plaintext accepted a different Compose project")
	}
}

func TestTicketE2ERuntimeIdentityRequiresExactEventBuildAndProject(t *testing.T) {
	previousHead, previousFingerprint, previousProject := ticketE2EBuildSourceHead, ticketE2EBuildSourceFingerprint, ticketE2EBuildProject
	ticketE2EBuildSourceHead = strings.Repeat("a", 40)
	ticketE2EBuildSourceFingerprint = strings.Repeat("b", 64)
	ticketE2EBuildProject = "rushgo-ticket-e2e-20261024-0123456789ab"
	t.Cleanup(func() {
		ticketE2EBuildSourceHead, ticketE2EBuildSourceFingerprint, ticketE2EBuildProject = previousHead, previousFingerprint, previousProject
	})
	t.Setenv("SERVICE_NAME", "ticket-e2e-event")
	t.Setenv("TICKET_E2E_MODE", "SIMULATED_PROVIDER_AUTHORITY")
	t.Setenv("TICKET_E2E_PROJECT", ticketE2EBuildProject)
	t.Setenv("COMPOSE_PROJECT_NAME", ticketE2EBuildProject)
	t.Setenv("TICKET_E2E_SOURCE_HEAD", ticketE2EBuildSourceHead)
	t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", ticketE2EBuildSourceFingerprint)
	if err := ValidateTicketE2ERuntimeIdentity("ticket-e2e-event"); err != nil {
		t.Fatalf("valid pinned Event TEST_ONLY runtime rejected: %v", err)
	}

	t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", strings.Repeat("c", 64))
	if err := ValidateTicketE2ERuntimeIdentity("ticket-e2e-event"); err == nil {
		t.Fatal("Event TEST_ONLY runtime accepted a source fingerprint different from its binary")
	}
}

func TestTicketE2ERuntimeResourcesRejectUnpinnedEndpoints(t *testing.T) {
	cases := []struct {
		name      string
		service   string
		resources TicketE2ERuntimeResources
	}{
		{
			name:    "User",
			service: "ticket-e2e-user",
			resources: TicketE2ERuntimeResources{
				DatabaseURL:  "postgres://ticket_e2e:ticket_e2e_local@ticket-e2e-postgres:5432/rushgo_ticket_e2e_user?sslmode=disable",
				RedisURL:     "redis://ticket-e2e-redis:6379",
				KafkaBrokers: []string{"ticket-e2e-kafka:9092"},
			},
		},
		{
			name:    "Event",
			service: "ticket-e2e-event",
			resources: TicketE2ERuntimeResources{
				DatabaseURL:  "postgres://ticket_e2e:ticket_e2e_local@ticket-e2e-postgres:5432/rushgo_ticket_e2e_event?sslmode=disable",
				RedisURL:     "redis://ticket-e2e-redis:6379",
				KafkaBrokers: []string{"ticket-e2e-kafka:9092"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+" accepts pinned endpoints", func(t *testing.T) {
			if err := ValidateTicketE2ERuntimeResources(tc.service, tc.resources); err != nil {
				t.Fatalf("pinned %s TEST_ONLY endpoints rejected: %v", tc.name, err)
			}
		})
		for _, mutate := range []struct {
			name string
			edit func(*TicketE2ERuntimeResources)
		}{
			{name: "external database", edit: func(r *TicketE2ERuntimeResources) {
				r.DatabaseURL = strings.Replace(r.DatabaseURL, "ticket-e2e-postgres", "main-postgres", 1)
			}},
			{name: "multi-host database", edit: func(r *TicketE2ERuntimeResources) {
				r.DatabaseURL = strings.Replace(r.DatabaseURL, "ticket-e2e-postgres:5432", "ticket-e2e-postgres:5432,main-postgres:5432", 1)
			}},
			{name: "external Redis", edit: func(r *TicketE2ERuntimeResources) { r.RedisURL = "redis://shared-redis:6379" }},
			{name: "external Kafka", edit: func(r *TicketE2ERuntimeResources) { r.KafkaBrokers = []string{"shared-kafka:9092"} }},
		} {
			t.Run(tc.name+" rejects "+mutate.name, func(t *testing.T) {
				changed := tc.resources
				changed.KafkaBrokers = append([]string(nil), tc.resources.KafkaBrokers...)
				mutate.edit(&changed)
				if err := ValidateTicketE2ERuntimeResources(tc.service, changed); err == nil {
					t.Fatalf("%s TEST_ONLY endpoints accepted %s", tc.name, mutate.name)
				}
			})
		}
	}
}

func TestTestOnlyPlaintextUserTransportIsPinned(t *testing.T) {
	previousHead, previousFingerprint, previousProject := ticketE2EBuildSourceHead, ticketE2EBuildSourceFingerprint, ticketE2EBuildProject
	ticketE2EBuildSourceHead = strings.Repeat("a", 40)
	ticketE2EBuildSourceFingerprint = strings.Repeat("b", 64)
	ticketE2EBuildProject = "rushgo-ticket-e2e-20261024-0123456789ab"
	t.Cleanup(func() {
		ticketE2EBuildSourceHead = previousHead
		ticketE2EBuildSourceFingerprint = previousFingerprint
		ticketE2EBuildProject = previousProject
	})

	for _, caller := range []string{"gateway", "booking"} {
		t.Run(caller, func(t *testing.T) {
			setTicketE2ERuntime(t, caller)
			credentials, enabled, err := ClientCredentialsFromEnv(ServiceUser, serviceIdentityFor(caller))
			if err != nil || !enabled || credentials == nil {
				t.Fatalf("isolated %s→User credentials = (%v, %t, %v), want pinned plaintext exception", caller, credentials, enabled, err)
			}
		})
	}

	t.Run("User server", func(t *testing.T) {
		setTicketE2ERuntime(t, "user")
		credentials, enabled, err := ServerCredentialsFromEnv(ServiceUser)
		if err != nil || !enabled || credentials == nil {
			t.Fatalf("isolated User server credentials = (%v, %t, %v), want pinned plaintext exception", credentials, enabled, err)
		}
		t.Setenv("USER_SERVICE_ADDR", "other-service:50051")
		if _, _, err := ServerCredentialsFromEnv(ServiceUser); err == nil {
			t.Fatal("TEST_ONLY User server accepted a runtime marker outside the pinned synthetic User endpoint")
		}
	})

	t.Run("unapproved caller", func(t *testing.T) {
		setTicketE2ERuntime(t, "saga")
		if _, _, err := ClientCredentialsFromEnv(ServiceUser, ServiceIdentitySaga); err == nil {
			t.Fatal("TEST_ONLY User client credentials were issued to a caller outside Gateway/Booking")
		}
	})
}

func TestTestOnlyPlaintextRejectsRuntimeIdentityThatDiffersFromBuild(t *testing.T) {
	if ticketE2EBuildSourceHead == "" || ticketE2EBuildSourceFingerprint == "" || ticketE2EBuildProject == "" {
		t.Skip("requires candidate identity linked into the test binary")
	}
	setTicketE2ERuntime(t, "gateway")
	wrongHead := strings.Repeat("a", 40)
	if wrongHead == ticketE2EBuildSourceHead {
		wrongHead = strings.Repeat("b", 40)
	}
	t.Setenv("TICKET_E2E_SOURCE_HEAD", wrongHead)
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("TEST_ONLY plaintext accepted a runtime source HEAD that differs from its build")
	}
	t.Setenv("TICKET_E2E_SOURCE_HEAD", ticketE2EBuildSourceHead)
	wrongFingerprint := strings.Repeat("c", 64)
	if wrongFingerprint == ticketE2EBuildSourceFingerprint {
		wrongFingerprint = strings.Repeat("d", 64)
	}
	t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", wrongFingerprint)
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("TEST_ONLY plaintext accepted a runtime fingerprint that differs from its build")
	}
}

func TestTestOnlyPlaintextRejectsNonFixtureBookingEndpoint(t *testing.T) {
	if ticketE2EBuildSourceHead == "" || ticketE2EBuildSourceFingerprint == "" || ticketE2EBuildProject == "" {
		t.Skip("requires candidate identity linked into the test binary")
	}
	setTicketE2ERuntime(t, "gateway")
	t.Setenv("BOOKING_SERVICE_ADDR", "attacker-controlled:50053")
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("TEST_ONLY plaintext accepted a non-fixture Booking endpoint")
	}
}

func TestTestOnlyPlaintextRejectsStripeEnvironment(t *testing.T) {
	if ticketE2EBuildSourceHead == "" || ticketE2EBuildSourceFingerprint == "" || ticketE2EBuildProject == "" {
		t.Skip("requires candidate identity linked into the test binary")
	}
	setTicketE2ERuntime(t, "gateway")
	t.Setenv("STRIPE_SYNTHETIC_TEST_MARKER", "synthetic-only")
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("TEST_ONLY plaintext accepted a non-empty Stripe environment variable")
	}
}

func TestTestOnlyPlaintextProjectIdentityIsBoundToBuild(t *testing.T) {
	previousHead, previousFingerprint, previousProject := ticketE2EBuildSourceHead, ticketE2EBuildSourceFingerprint, ticketE2EBuildProject
	ticketE2EBuildSourceHead = strings.Repeat("a", 40)
	ticketE2EBuildSourceFingerprint = strings.Repeat("b", 64)
	ticketE2EBuildProject = "rushgo-ticket-e2e-20261024-0123456789ab"
	t.Cleanup(func() {
		ticketE2EBuildSourceHead = previousHead
		ticketE2EBuildSourceFingerprint = previousFingerprint
		ticketE2EBuildProject = previousProject
	})
	setTicketE2ERuntime(t, "gateway")
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err != nil {
		t.Fatalf("build-bound project identity rejected: %v", err)
	}
	for _, project := range []string{"rushgo-ticket-e2e-20261023", "rushgo-ticket-e2e-20261024-ffffffffffff", ""} {
		t.Run(project, func(t *testing.T) {
			t.Setenv("TICKET_E2E_PROJECT", project)
			if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
				t.Fatalf("TEST_ONLY plaintext accepted project %q outside the build-bound run identity", project)
			}
		})
	}
}

func TestTicketE2EKafkaIdentityIsBoundToEmbeddedRun(t *testing.T) {
	previousHead, previousFingerprint, previousProject := ticketE2EBuildSourceHead, ticketE2EBuildSourceFingerprint, ticketE2EBuildProject
	ticketE2EBuildSourceHead = strings.Repeat("a", 40)
	ticketE2EBuildSourceFingerprint = strings.Repeat("b", 64)
	ticketE2EBuildProject = "rushgo-ticket-e2e-20261024-0123456789ab"
	t.Cleanup(func() {
		ticketE2EBuildSourceHead = previousHead
		ticketE2EBuildSourceFingerprint = previousFingerprint
		ticketE2EBuildProject = previousProject
	})
	setTicketE2ERuntime(t, "booking")
	if got, ok := TicketE2EKafkaIdentity(); !ok || got != ticketE2EBuildProject {
		t.Fatalf("valid run-specific Kafka identity = (%q, %t)", got, ok)
	}

	for _, name := range []string{"project", "compose", "source head", "source fingerprint", "mode", "service"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "project":
				t.Setenv("TICKET_E2E_PROJECT", "rushgo-ticket-e2e-20261024-ffffffffffff")
			case "compose":
				t.Setenv("COMPOSE_PROJECT_NAME", "another-project")
			case "source head":
				t.Setenv("TICKET_E2E_SOURCE_HEAD", strings.Repeat("c", 40))
			case "source fingerprint":
				t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", strings.Repeat("d", 64))
			case "mode":
				t.Setenv("TICKET_E2E_MODE", "")
			case "service":
				t.Setenv("SERVICE_NAME", "ticket-e2e-gateway")
			}
			if got, ok := TicketE2EKafkaIdentity(); ok || got != "" {
				t.Fatalf("mismatched %s identity accepted: (%q, %t)", name, got, ok)
			}
		})
	}
}

func setTicketE2ERuntime(t *testing.T, service string) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.Contains(strings.ToUpper(name), "STRIPE") {
			t.Setenv(name, "")
		}
	}
	t.Setenv(TestOnlyInsecureEnv, testOnlyInsecureAck)
	t.Setenv("TICKET_E2E_MODE", "SIMULATED_PROVIDER_AUTHORITY")
	t.Setenv("TICKET_E2E_PROJECT", ticketE2EBuildProject)
	t.Setenv("COMPOSE_PROJECT_NAME", ticketE2EBuildProject)
	t.Setenv("TICKET_E2E_SOURCE_HEAD", ticketE2EBuildSourceHead)
	t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", ticketE2EBuildSourceFingerprint)
	t.Setenv("SERVICE_NAME", "ticket-e2e-"+service)
	t.Setenv("BOOKING_SERVICE_ADDR", "booking-e2e:50053")
	t.Setenv("PAYMENT_SERVICE_ADDR", "booking-e2e:50053")
	t.Setenv("USER_SERVICE_ADDR", "user-service:50051")
}
