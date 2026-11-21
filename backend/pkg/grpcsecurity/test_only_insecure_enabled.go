//go:build ticket_e2e

package grpcsecurity

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// These values are injected only into the isolated ticket_e2e image. Empty
// values mean this tagged binary has no pinned candidate identity and must not
// enable plaintext transport.
var (
	ticketE2EBuildSourceHead        string
	ticketE2EBuildSourceFingerprint string
	ticketE2EBuildProject           string
)

var (
	testOnlySourceHeadPattern  = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	testOnlyFingerprintPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	testOnlyProjectPattern     = regexp.MustCompile(`^rushgo-ticket-e2e-[0-9]{8}-[0-9a-f]{12}$`)
)

func testOnlyCredentials(target, localIdentity string) (credentials.TransportCredentials, error) {
	localServiceName := strings.TrimPrefix(localIdentity, "spiffe://rushgo.internal/")
	expectedServiceName := map[string]string{
		"gateway": "ticket-e2e-gateway",
		"booking": "ticket-e2e-booking",
		"saga":    "ticket-e2e-saga",
		"user":    "ticket-e2e-user",
	}[localServiceName]
	if expectedServiceName == "" || os.Getenv("TICKET_E2E_MODE") != "SIMULATED_PROVIDER_AUTHORITY" ||
		!testOnlyProjectPattern.MatchString(ticketE2EBuildProject) ||
		os.Getenv("TICKET_E2E_PROJECT") != ticketE2EBuildProject ||
		os.Getenv("COMPOSE_PROJECT_NAME") != ticketE2EBuildProject ||
		!testOnlySourceHeadPattern.MatchString(ticketE2EBuildSourceHead) ||
		!testOnlyFingerprintPattern.MatchString(ticketE2EBuildSourceFingerprint) ||
		os.Getenv("TICKET_E2E_SOURCE_HEAD") != ticketE2EBuildSourceHead ||
		os.Getenv("TICKET_E2E_SOURCE_FINGERPRINT") != ticketE2EBuildSourceFingerprint ||
		os.Getenv("SERVICE_NAME") != expectedServiceName {
		return nil, errors.New("insecure transport requires the pinned isolated Ticket E2E build and runtime identity")
	}
	endpointVariable := map[string]string{
		ServiceBooking: "BOOKING_SERVICE_ADDR",
		ServicePayment: "PAYMENT_SERVICE_ADDR",
		ServiceUser:    "USER_SERVICE_ADDR",
	}[target]
	expectedEndpoint := "booking-e2e:50053"
	if target == ServiceUser {
		expectedEndpoint = "user-service:50051"
	}
	if endpointVariable == "" {
		return nil, fmt.Errorf("unsupported TEST_ONLY insecure gRPC target %q", target)
	}
	if strings.TrimSpace(os.Getenv(endpointVariable)) != expectedEndpoint {
		return nil, fmt.Errorf("TEST_ONLY insecure transport target %s is outside the pinned synthetic service", endpointVariable)
	}
	for _, item := range os.Environ() {
		name, value, found := strings.Cut(item, "=")
		if found && strings.Contains(strings.ToUpper(name), "STRIPE") && strings.TrimSpace(value) != "" {
			return nil, errors.New("TEST_ONLY insecure transport refuses Stripe credentials")
		}
	}
	return insecure.NewCredentials(), nil
}

// ValidateTicketE2ERuntimeIdentity fails closed unless this TEST_ONLY binary's
// embedded Candidate identity matches the dedicated C-SIM runtime identity.
func ValidateTicketE2ERuntimeIdentity(expectedServiceName string) error {
	if expectedServiceName != "ticket-e2e-user" && expectedServiceName != "ticket-e2e-event" &&
		expectedServiceName != "ticket-e2e-booking" && expectedServiceName != "ticket-e2e-saga" && expectedServiceName != "ticket-e2e-gateway" {
		return errors.New("unsupported Ticket E2E service identity")
	}
	if os.Getenv("TICKET_E2E_MODE") != "SIMULATED_PROVIDER_AUTHORITY" ||
		!testOnlyProjectPattern.MatchString(ticketE2EBuildProject) ||
		os.Getenv("TICKET_E2E_PROJECT") != ticketE2EBuildProject ||
		os.Getenv("COMPOSE_PROJECT_NAME") != ticketE2EBuildProject ||
		!testOnlySourceHeadPattern.MatchString(ticketE2EBuildSourceHead) ||
		!testOnlyFingerprintPattern.MatchString(ticketE2EBuildSourceFingerprint) ||
		os.Getenv("TICKET_E2E_SOURCE_HEAD") != ticketE2EBuildSourceHead ||
		os.Getenv("TICKET_E2E_SOURCE_FINGERPRINT") != ticketE2EBuildSourceFingerprint ||
		os.Getenv("SERVICE_NAME") != expectedServiceName {
		return errors.New("Ticket E2E runtime identity does not match the pinned build")
	}
	return nil
}

// TicketE2EKafkaIdentity returns a namespace only when a tagged binary's
// embedded source and run identity match the explicit TEST_ONLY environment.
func TicketE2EKafkaIdentity() (string, bool) {
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName != "ticket-e2e-booking" && serviceName != "ticket-e2e-saga" {
		return "", false
	}
	if ValidateTicketE2ERuntimeIdentity(serviceName) != nil {
		return "", false
	}
	return ticketE2EBuildProject, true
}
