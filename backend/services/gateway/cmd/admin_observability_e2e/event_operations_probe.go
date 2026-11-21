//go:build admin_observability_e2e

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func runEventOperationsProbe() string {
	cfg, err := loadFixtureRuntime("gateway")
	if err != nil || (cfg.FixtureOwner != "admin-event-operations" && cfg.FixtureOwner != fixtureUnifiedOwner) {
		return "runtime_guard"
	}
	if err := requireFixtureTLSFiles(
		"RUSHGO_GRPC_CLIENT_CERT_FILE", "RUSHGO_GRPC_CLIENT_KEY_FILE", "RUSHGO_GRPC_SERVER_CA_FILE",
		"ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_CERT_FILE", "ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_KEY_FILE",
	); err != nil {
		return "mtls_files"
	}
	eventIDText := strings.TrimSpace(os.Getenv("ADMIN_EVENT_OPERATIONS_EVENT_ID"))
	eventID, err := uuid.Parse(eventIDText)
	if err != nil || eventID == uuid.Nil {
		return "event_id"
	}
	missingID := strings.TrimSpace(os.Getenv("ADMIN_EVENT_OPERATIONS_MISSING_EVENT_ID"))
	if _, err := uuid.Parse(missingID); err != nil || missingID == eventIDText {
		return "missing_event_id"
	}
	tokenPath := strings.TrimSpace(os.Getenv("ADMIN_OBSERVABILITY_E2E_OLD_ROLE_TOKEN_FILE"))
	if tokenPath == "" || !filepath.IsAbs(tokenPath) || !pathWithin(cfg.PrivateRuntimeDir, tokenPath) {
		return "token_file"
	}
	token, err := readPrivateToken(tokenPath)
	if err != nil {
		return "token_file"
	}
	scenario := strings.TrimSpace(os.Getenv("ADMIN_EVENT_OPERATIONS_PROBE_SCENARIO"))
	if scenario != "admin" && scenario != "demoted_admin" {
		return "scenario"
	}

	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "event_connection"
	}
	defer eventConn.Close()
	eventClient := eventv1.NewEventServiceClient(eventConn)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	eventDetail, err := eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: eventID.String()})
	if err != nil || eventDetail == nil || eventDetail.GetId() != eventID.String() || eventDetail.GetTitle() == "" {
		return "event_get_event"
	}
	eventList, err := eventClient.ListEvents(ctx, &eventv1.ListEventsRequest{Page: 1, PageSize: 20})
	if err != nil || eventList == nil || eventList.GetTotal() < 1 {
		return "event_list_events"
	}
	found := false
	for _, item := range eventList.GetEvents() {
		if item != nil && item.GetId() == eventID.String() {
			found = true
			break
		}
	}
	if !found {
		return "event_list_events_identity"
	}
	if _, err := eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: missingID}); status.Code(err) != codes.NotFound {
		return "event_missing_not_found"
	}

	bookingCredentials, testOnlyPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityGateway)
	if err != nil || testOnlyPlaintext {
		return "booking_gateway_mtls"
	}
	bookingServerName := strings.TrimSpace(os.Getenv("RUSHGO_BOOKING_GRPC_SERVER_NAME"))
	if bookingServerName == "" {
		return "booking_server_name"
	}
	if err := callAdminEventOperations(cfg.BookingServiceAddr, bookingCredentials, token, eventID.String(), scenario); err != nil {
		return "booking_direct_authorization"
	}
	if scenario == "admin" {
		ownerCert, certErr := readRequiredFixtureTLSFile("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_CERT_FILE", cfg.PrivateRuntimeDir)
		ownerKey, keyErr := readRequiredFixtureTLSFile("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_KEY_FILE", cfg.PrivateRuntimeDir)
		ca, caErr := readRequiredFixtureTLSFile("RUSHGO_GRPC_SERVER_CA_FILE", cfg.PrivateRuntimeDir)
		if certErr != nil || keyErr != nil || caErr != nil {
			return "booking_wrong_identity_files"
		}
		wrongIdentity, credentialErr := grpcsecurity.NewClientCredentials(ownerCert, ownerKey, ca, bookingServerName, grpcsecurity.ServiceIdentityBooking)
		if credentialErr != nil {
			return "booking_wrong_identity_credentials"
		}
		wrongIdentityErr := probeWrongIdentityTLS("booking_operations", func() error {
			return callAdminEventOperations(cfg.BookingServiceAddr, wrongIdentity, token, eventID.String(), "wrong_identity")
		})
		if wrongIdentityErr == nil {
			return "booking_wrong_identity_accepted"
		}
		if !isExpectedWrongIdentityTLSRejection(wrongIdentityErr) {
			return "booking_wrong_identity_inconclusive"
		}
		wrongListErr := probeWrongIdentityTLS("booking_ticket_list", func() error {
			return callTicketListOnly(cfg.BookingServiceAddr, wrongIdentity, token, eventID.String())
		})
		if wrongListErr == nil {
			return "booking_ticket_list_wrong_identity_accepted"
		}
		if !isExpectedWrongIdentityTLSRejection(wrongListErr) {
			return "booking_ticket_list_wrong_identity_inconclusive"
		}
		if err := callAdminEventOperations(cfg.BookingServiceAddr, bookingCredentials, token, eventID.String(), scenario); err != nil {
			return "booking_admin_direct_after_wrong_identity"
		}
	}

	fmt.Println("event_get_event=OK")
	fmt.Println("event_list_events=OK")
	fmt.Println("event_missing_event=NOT_FOUND")
	if scenario == "admin" {
		fmt.Println("booking_admin_direct_before=OK")
		fmt.Println("booking_wrong_mtls_identity=REJECTED")
		fmt.Println("booking_admin_direct_after=OK")
	} else {
		fmt.Println("booking_admin_role_demoted=PERMISSION_DENIED")
	}
	return ""
}

func callTicketListOnly(target string, transport credentials.TransportCredentials, token, eventID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err = bookingv1.NewBookingServiceClient(conn).ListAdminEventTickets(ctx, &bookingv1.ListAdminEventTicketsRequest{EventId: eventID, StatusFilter: "ALL", PageSize: 20})
	return err
}

func callAdminEventOperations(target string, transport credentials.TransportCredentials, token, eventID, scenario string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := bookingv1.NewBookingServiceClient(conn).GetAdminEventTicketOperations(ctx, &bookingv1.GetAdminEventTicketOperationsRequest{EventId: eventID})
	if scenario == "demoted_admin" {
		if status.Code(err) != codes.PermissionDenied {
			return errors.New("current role was not denied")
		}
		_, listErr := bookingv1.NewBookingServiceClient(conn).ListAdminEventTickets(ctx, &bookingv1.ListAdminEventTicketsRequest{EventId: eventID, PageSize: 20, StatusFilter: "ALL"})
		if status.Code(listErr) != codes.PermissionDenied {
			return errors.New("ticket list current role was not denied")
		}
		return nil
	}
	if scenario == "wrong_identity" {
		return err
	}
	if err != nil || response == nil || response.GetEventId() != eventID || response.GetSourceRecordLimit() != 5000 ||
		response.GetObservedAt() == nil || response.GetObservedAt().CheckValid() != nil {
		return errors.New("Booking projection unavailable")
	}
	recent := response.GetRecentCheckins()
	latest := response.GetLatestCheckinAt()
	if len(recent) == 0 && latest != nil || len(recent) > 0 && (latest == nil || !recent[0].AsTime().Equal(latest.AsTime())) {
		return errors.New("latest check-in timestamp mismatch")
	}
	list, listErr := bookingv1.NewBookingServiceClient(conn).ListAdminEventTickets(ctx, &bookingv1.ListAdminEventTicketsRequest{EventId: eventID, PageSize: 20, StatusFilter: "ALL"})
	if listErr != nil || list == nil || list.GetEventId() != eventID || list.GetSourceRecordLimit() != 5000 || list.GetObservedAt() == nil || list.GetObservedAt().CheckValid() != nil {
		return errors.New("Booking ticket list direct projection unavailable")
	}
	return nil
}

func isExpectedWrongIdentityTLSRejection(err error) bool {
	if err == nil || status.Code(err) != codes.Unavailable {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "tls:") && strings.Contains(message, "bad certificate")
}

// TLS rejection can race the HTTP/2 preface write, exposing only a connection
// close to grpc-go. Retry a fresh connection to obtain explicit TLS evidence;
// a generic transport failure alone must never satisfy this security probe.
func probeWrongIdentityTLS(stage string, call func() error) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		err = call()
		explicitRejection := isExpectedWrongIdentityTLSRejection(err)
		// Do not print raw errors: they may contain addresses or credentials.
		fmt.Fprintf(os.Stderr, "WRONG_IDENTITY_PROBE stage=%s attempt=%d grpc_code=%s tls_certificate_rejected=%t\n",
			stage, attempt, status.Code(err), explicitRejection)
		if explicitRejection || status.Code(err) != codes.Unavailable {
			return err
		}
	}
	return err
}
