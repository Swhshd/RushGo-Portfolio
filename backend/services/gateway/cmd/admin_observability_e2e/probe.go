//go:build admin_observability_e2e

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func runGatewayProbe() string {
	cfg, err := loadFixtureRuntime("gateway")
	if err != nil {
		return "runtime_guard"
	}
	if err := requireFixtureTLSFiles(
		"RUSHGO_GRPC_CLIENT_CERT_FILE", "RUSHGO_GRPC_CLIENT_KEY_FILE", "RUSHGO_GRPC_SERVER_CA_FILE",
		"RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_SERVER_CA_FILE",
		"RUSHGO_ADMIN_OBSERVABILITY_E2E_UNTRUSTED_CLIENT_CERT_FILE",
		"RUSHGO_ADMIN_OBSERVABILITY_E2E_UNTRUSTED_CLIENT_KEY_FILE",
		"RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_URI_CLIENT_CERT_FILE",
		"RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_URI_CLIENT_KEY_FILE",
	); err != nil {
		return "probe_mtls_files"
	}
	tokenPath := strings.TrimSpace(os.Getenv("ADMIN_OBSERVABILITY_E2E_OLD_ROLE_TOKEN_FILE"))
	if tokenPath == "" || !filepath.IsAbs(tokenPath) || !pathWithin(cfg.PrivateRuntimeDir, tokenPath) {
		return "probe_token_file"
	}
	token, err := readPrivateToken(tokenPath)
	if err != nil {
		return "probe_token_file"
	}
	scenario := os.Getenv("ADMIN_OBSERVABILITY_E2E_PROBE_SCENARIO")
	if scenario != "admin" && scenario != "non_admin" && scenario != "demoted_admin" {
		return "probe_scenario"
	}
	bookingServerName := strings.TrimSpace(os.Getenv("RUSHGO_BOOKING_GRPC_SERVER_NAME"))
	notificationServerName := strings.TrimSpace(os.Getenv("RUSHGO_NOTIFICATION_GRPC_SERVER_NAME"))
	wrongIdentity := strings.TrimSpace(os.Getenv("RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_URI_IDENTITY"))
	wrongIdentityURL, identityErr := url.Parse(wrongIdentity)
	if bookingServerName == "" || notificationServerName == "" || identityErr != nil ||
		wrongIdentityURL.Scheme != "spiffe" || wrongIdentityURL.Host != "rushgo.internal" ||
		wrongIdentityURL.Path == "" || strings.Contains(strings.TrimPrefix(wrongIdentityURL.Path, "/"), "/") ||
		wrongIdentity != grpcsecurity.ServiceIdentitySaga {
		return "probe_identity_config"
	}

	bookingCorrect, bookingPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityGateway)
	if err != nil || bookingPlaintext {
		return "booking_mtls_transport"
	}
	notificationCorrect, notificationPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceNotification, grpcsecurity.ServiceIdentityGateway)
	if err != nil || notificationPlaintext {
		return "notification_mtls_transport"
	}
	serverCAPEM, err := readRequiredFixtureTLSFile("RUSHGO_GRPC_SERVER_CA_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "server_ca"
	}
	clientCertPEM, err := readRequiredFixtureTLSFile("RUSHGO_GRPC_CLIENT_CERT_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "client_certificate"
	}
	clientKeyPEM, err := readRequiredFixtureTLSFile("RUSHGO_GRPC_CLIENT_KEY_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "client_key"
	}
	wrongCAPEM, err := readRequiredFixtureTLSFile("RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_SERVER_CA_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "wrong_server_ca"
	}
	untrustedCertPEM, err := readRequiredFixtureTLSFile("RUSHGO_ADMIN_OBSERVABILITY_E2E_UNTRUSTED_CLIENT_CERT_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "untrusted_client_certificate"
	}
	untrustedKeyPEM, err := readRequiredFixtureTLSFile("RUSHGO_ADMIN_OBSERVABILITY_E2E_UNTRUSTED_CLIENT_KEY_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "untrusted_client_key"
	}
	wrongURICertPEM, err := readRequiredFixtureTLSFile("RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_URI_CLIENT_CERT_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "wrong_uri_certificate"
	}
	wrongURIKeyPEM, err := readRequiredFixtureTLSFile("RUSHGO_ADMIN_OBSERVABILITY_E2E_WRONG_URI_CLIENT_KEY_FILE", cfg.PrivateRuntimeDir)
	if err != nil {
		return "wrong_uri_key"
	}

	bookingWrongCA, err := grpcsecurity.NewClientCredentials(clientCertPEM, clientKeyPEM, wrongCAPEM, bookingServerName, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		return "booking_wrong_ca_credentials"
	}
	notificationWrongCA, err := grpcsecurity.NewClientCredentials(clientCertPEM, clientKeyPEM, wrongCAPEM, notificationServerName, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		return "notification_wrong_ca_credentials"
	}
	bookingUntrustedClient, err := grpcsecurity.NewClientCredentials(untrustedCertPEM, untrustedKeyPEM, serverCAPEM, bookingServerName, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		return "booking_untrusted_client_credentials"
	}
	notificationUntrustedClient, err := grpcsecurity.NewClientCredentials(untrustedCertPEM, untrustedKeyPEM, serverCAPEM, notificationServerName, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		return "notification_untrusted_client_credentials"
	}
	bookingWrongURI, err := grpcsecurity.NewClientCredentials(wrongURICertPEM, wrongURIKeyPEM, serverCAPEM, bookingServerName, wrongIdentity)
	if err != nil {
		return "booking_wrong_uri_credentials"
	}
	notificationWrongURI, err := grpcsecurity.NewClientCredentials(wrongURICertPEM, wrongURIKeyPEM, serverCAPEM, notificationServerName, wrongIdentity)
	if err != nil {
		return "notification_wrong_uri_credentials"
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCAPEM) {
		return "server_ca"
	}
	bookingMissingClient := credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: bookingServerName, MinVersion: tls.VersionTLS12})
	notificationMissingClient := credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: notificationServerName, MinVersion: tls.VersionTLS12})

	bookingRequest := fixtureOutboxRequest("booking")
	notificationRequest := fixtureOutboxRequest("notification")
	if scenario == "non_admin" {
		if !expectBookingStatus(cfg.BookingServiceAddr, bookingWrongCA, token, bookingRequest, codes.Unavailable) ||
			!expectNotificationStatus(cfg.NotificationServiceAddr, notificationWrongCA, token, notificationRequest, codes.Unavailable) {
			return "wrong_ca_not_rejected"
		}
		if !expectBookingStatus(cfg.BookingServiceAddr, bookingUntrustedClient, token, bookingRequest, codes.Unavailable) ||
			!expectNotificationStatus(cfg.NotificationServiceAddr, notificationUntrustedClient, token, notificationRequest, codes.Unavailable) {
			return "untrusted_client_not_rejected"
		}
		if !expectBookingStatus(cfg.BookingServiceAddr, bookingMissingClient, token, bookingRequest, codes.Unavailable) ||
			!expectNotificationStatus(cfg.NotificationServiceAddr, notificationMissingClient, token, notificationRequest, codes.Unavailable) {
			return "missing_client_certificate_not_rejected"
		}
		bookingWrongURIStatus, bookingWrongURIRejected := rejectedBookingStatus(cfg.BookingServiceAddr, bookingWrongURI, token, bookingRequest)
		notificationWrongURIStatus, notificationWrongURIRejected := rejectedNotificationStatus(cfg.NotificationServiceAddr, notificationWrongURI, token, notificationRequest)
		if !bookingWrongURIRejected || !notificationWrongURIRejected {
			return "wrong_uri_not_rejected"
		}
		if !expectBookingStatus(cfg.BookingServiceAddr, bookingCorrect, token, bookingRequest, codes.PermissionDenied) ||
			!expectNotificationStatus(cfg.NotificationServiceAddr, notificationCorrect, token, notificationRequest, codes.PermissionDenied) {
			return "non_admin_not_rejected"
		}
		fmt.Println("booking_tls_wrong_ca=UNAVAILABLE")
		fmt.Println("notification_tls_wrong_ca=UNAVAILABLE")
		fmt.Println("booking_tls_untrusted_client_ca=UNAVAILABLE")
		fmt.Println("notification_tls_untrusted_client_ca=UNAVAILABLE")
		fmt.Println("booking_tls_missing_client_certificate=UNAVAILABLE")
		fmt.Println("notification_tls_missing_client_certificate=UNAVAILABLE")
		fmt.Printf("booking_tls_wrong_uri_identity=%s\n", safeProbeCode(bookingWrongURIStatus))
		fmt.Printf("notification_tls_wrong_uri_identity=%s\n", safeProbeCode(notificationWrongURIStatus))
		fmt.Println("booking_direct_non_admin=PERMISSION_DENIED")
		fmt.Println("notification_direct_non_admin=PERMISSION_DENIED")
		return ""
	}

	want := codes.OK
	label := "admin"
	if scenario == "demoted_admin" {
		want = codes.PermissionDenied
		label = "admin_role_demoted"
	}
	if !expectBookingStatus(cfg.BookingServiceAddr, bookingCorrect, token, bookingRequest, want) {
		return "booking_direct_authorization"
	}
	if !expectNotificationStatus(cfg.NotificationServiceAddr, notificationCorrect, token, notificationRequest, want) {
		return "notification_direct_authorization"
	}
	statusLabel := "OK"
	if want == codes.PermissionDenied {
		statusLabel = "PERMISSION_DENIED"
	}
	fmt.Printf("booking_direct_%s=%s\n", label, statusLabel)
	fmt.Printf("notification_direct_%s=%s\n", label, statusLabel)
	return ""
}

func readPrivateToken(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("token file unavailable")
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(contents) == 0 || len(contents) > 8192 {
		return "", errors.New("token file invalid")
	}
	token := strings.TrimSpace(string(contents))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("token file invalid")
	}
	return token, nil
}

func readRequiredFixtureTLSFile(name, runtimeDir string) ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(name))
	if path == "" || !filepath.IsAbs(path) || !pathWithin(runtimeDir, path) {
		return nil, errors.New("fixture TLS file unavailable")
	}
	return os.ReadFile(path)
}

func fixtureOutboxRequest(source string) *bookingv1.ListAdminOutboxEventsRequest {
	now := time.Now().UTC()
	return &bookingv1.ListAdminOutboxEventsRequest{
		Source: source, Status: "all", From: now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		To: now.Format(time.RFC3339Nano), Limit: 20,
	}
}

func expectBookingStatus(target string, transport credentials.TransportCredentials, token string, request *bookingv1.ListAdminOutboxEventsRequest, want codes.Code) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return false
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err = bookingv1.NewBookingServiceClient(conn).ListAdminOutboxEvents(ctx, request)
	if err == nil {
		return want == codes.OK
	}
	return status.Code(err) == want
}

func expectNotificationStatus(target string, transport credentials.TransportCredentials, token string, request *bookingv1.ListAdminOutboxEventsRequest, want codes.Code) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return false
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err = notificationv1.NewNotificationAdminServiceClient(conn).ListAdminOutboxEvents(ctx, request)
	if err == nil {
		return want == codes.OK
	}
	return status.Code(err) == want
}

func rejectedBookingStatus(target string, transport credentials.TransportCredentials, token string, request *bookingv1.ListAdminOutboxEventsRequest) (codes.Code, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return status.Code(err), false
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err = bookingv1.NewBookingServiceClient(conn).ListAdminOutboxEvents(ctx, request)
	if err == nil {
		return codes.OK, false
	}
	code := status.Code(err)
	return code, code == codes.PermissionDenied
}

func rejectedNotificationStatus(target string, transport credentials.TransportCredentials, token string, request *bookingv1.ListAdminOutboxEventsRequest) (codes.Code, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return status.Code(err), false
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err = notificationv1.NewNotificationAdminServiceClient(conn).ListAdminOutboxEvents(ctx, request)
	if err == nil {
		return codes.OK, false
	}
	code := status.Code(err)
	return code, code == codes.PermissionDenied || code == codes.Unavailable
}

func safeProbeCode(code codes.Code) string {
	switch code {
	case codes.OK:
		return "OK"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.Unavailable:
		return "UNAVAILABLE"
	default:
		return "OTHER"
	}
}
