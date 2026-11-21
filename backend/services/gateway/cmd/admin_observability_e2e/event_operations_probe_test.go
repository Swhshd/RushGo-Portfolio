//go:build admin_observability_e2e

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWrongIdentityProbeRetainsTLSRejection(t *testing.T) {
	target, wrongIdentity := startWrongIdentityProbeServer(t)
	err := probeWrongIdentityTLS("operations", func() error {
		return callAdminEventOperations(target, wrongIdentity, "probe-token", "00000000-0000-4000-8000-000000000001", "wrong_identity")
	})
	if err == nil {
		t.Fatal("wrong-identity probe must retain the TLS rejection for explicit classification")
	}
	if !isExpectedWrongIdentityTLSRejection(err) {
		message := strings.ToLower(err.Error())
		t.Fatalf("actual wrong-identity TLS rejection was not recognized (code=%s handshake=%t tls=%t bad_certificate=%t identity_not_authorized=%t connection=%t refused=%t certificate=%t remote=%t)",
			status.Code(err), strings.Contains(message, "authentication handshake failed"), strings.Contains(message, "tls:"),
			strings.Contains(message, "bad certificate"), strings.Contains(message, "identity is not authorized"),
			strings.Contains(message, "connection"), strings.Contains(message, "refused"), strings.Contains(message, "certificate"), strings.Contains(message, "remote"))
	}
}

func TestTicketListWrongIdentityProbeRetainsTLSRejection(t *testing.T) {
	target, wrongIdentity := startWrongIdentityProbeServer(t)
	err := probeWrongIdentityTLS("ticket_list", func() error {
		return callTicketListOnly(target, wrongIdentity, "probe-token", "00000000-0000-4000-8000-000000000001")
	})
	if !isExpectedWrongIdentityTLSRejection(err) {
		t.Fatalf("local synthetic ticket-list rejection: %v", err)
	}
}

func TestWrongIdentityTLSProbeRequiresExplicitRejectionWithinBound(t *testing.T) {
	reset := status.Error(codes.Unavailable, "connection reset by peer")
	denied := status.Error(codes.Unavailable, "remote error: tls: bad certificate")
	for _, tc := range []struct {
		name         string
		errors       []error
		wantCode     codes.Code
		wantCalls    int
		wantRejected bool
	}{
		{"explicit rejection", []error{denied}, codes.Unavailable, 1, true},
		{"reset then explicit rejection", []error{reset, denied}, codes.Unavailable, 2, true},
		{"persistent network failure", []error{reset, reset, reset, denied}, codes.Unavailable, 3, false},
		{"accepted immediately", []error{nil, denied}, codes.OK, 1, false},
		{"accepted after reset", []error{reset, nil, denied}, codes.OK, 2, false},
		{"internal failure", []error{status.Error(codes.Internal, "failure"), denied}, codes.Internal, 1, false},
		{"deadline", []error{status.Error(codes.DeadlineExceeded, "timeout"), denied}, codes.DeadlineExceeded, 1, false},
		{"RPC rejection is not TLS rejection", []error{status.Error(codes.PermissionDenied, "denied"), denied}, codes.PermissionDenied, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := probeWrongIdentityTLS("ticket_list", func() error {
				if calls >= len(tc.errors) {
					t.Fatal("unexpected additional RPC attempt")
				}
				err := tc.errors[calls]
				calls++
				return err
			})
			if calls != tc.wantCalls || status.Code(err) != tc.wantCode || isExpectedWrongIdentityTLSRejection(err) != tc.wantRejected {
				t.Fatalf("calls=%d code=%s rejected=%t; want %d %s %t", calls, status.Code(err), isExpectedWrongIdentityTLSRejection(err), tc.wantCalls, tc.wantCode, tc.wantRejected)
			}
		})
	}
}

func TestWrongIdentityProbeRejectsGenericTransportFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil},
		{name: "connection unavailable", err: status.Error(codes.Unavailable, "connection refused")},
		{name: "timeout", err: status.Error(codes.DeadlineExceeded, "context deadline exceeded")},
		{name: "permission denied", err: status.Error(codes.PermissionDenied, "direct caller denied")},
		{name: "explicit TLS certificate rejection", err: status.Error(codes.Unavailable, "remote error: tls: bad certificate"), want: true},
		{name: "handshake without certificate rejection", err: status.Error(codes.Unavailable, "authentication handshake failed: connection reset")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isExpectedWrongIdentityTLSRejection(tc.err); got != tc.want {
				t.Fatalf("TLS identity rejection classification = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestEventOperationsProbeRuntimeGuardAllowsOnlyLegacyAndUnifiedOwners(t *testing.T) {
	for _, tc := range []struct {
		owner string
		want  string
	}{
		{owner: "admin-event-operations", want: "mtls_files"},
		{owner: fixtureUnifiedOwner, want: "mtls_files"},
		{owner: "admin-observability-v2", want: "runtime_guard"},
		{owner: fixtureUnifiedOwner + "-extra", want: "runtime_guard"},
	} {
		t.Run(tc.owner, func(t *testing.T) {
			configureEventProbeRuntime(t, tc.owner)
			if got := runEventOperationsProbe(); got != tc.want {
				t.Fatalf("probe stage = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEventOperationsProbeAdminFlowAgainstLocalGRPCStubs(t *testing.T) {
	eventID := "00000000-0000-4000-8000-000000000001"
	missingEventID := "00000000-0000-4000-8000-000000000002"
	caCert, caKey, caPEM := newProbeTestCA(t)
	bookingServerCert, bookingServerKey := newProbeTestLeaf(t, caCert, caKey, "booking.grpc.test.rushgo.internal", "", x509.ExtKeyUsageServerAuth)
	gatewayClientCert, gatewayClientKey := newProbeTestLeaf(t, caCert, caKey, "gateway-client", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth)
	ownerClientCert, ownerClientKey := newProbeTestLeaf(t, caCert, caKey, "booking-owner", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)

	bookingServerCredentials, err := grpcsecurity.NewServerCredentials(bookingServerCert, bookingServerKey, caPEM, []string{grpcsecurity.ServiceIdentityGateway})
	if err != nil {
		t.Fatalf("create local Booking mTLS credentials: %v", err)
	}
	bookingStub := &eventOperationsBookingProbeStub{eventID: eventID, expectedToken: "probe-token"}
	bookingServer := grpc.NewServer(grpc.Creds(bookingServerCredentials))
	bookingv1.RegisterBookingServiceServer(bookingServer, bookingStub)
	bookingAddress := serveProbeGRPCServer(t, bookingServer)

	eventStub := &eventOperationsEventProbeStub{eventID: eventID}
	eventServer := grpc.NewServer()
	eventv1.RegisterEventServiceServer(eventServer, eventStub)
	eventAddress := serveProbeGRPCServer(t, eventServer)

	runtimeDir := configureEventProbeRuntimeAt(t, fixtureUnifiedOwner, eventAddress, bookingAddress)
	clientCertPath := writeProbeRuntimeFile(t, runtimeDir, "gateway-client.crt", gatewayClientCert)
	clientKeyPath := writeProbeRuntimeFile(t, runtimeDir, "gateway-client.key", gatewayClientKey)
	ownerCertPath := writeProbeRuntimeFile(t, runtimeDir, "booking-owner.crt", ownerClientCert)
	ownerKeyPath := writeProbeRuntimeFile(t, runtimeDir, "booking-owner.key", ownerClientKey)
	caPath := writeProbeRuntimeFile(t, runtimeDir, "ca.crt", caPEM)
	tokenPath := writeProbeRuntimeFile(t, runtimeDir, "admin.access", []byte("probe-token\n"))
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", clientCertPath)
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", clientKeyPath)
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", caPath)
	t.Setenv("RUSHGO_BOOKING_GRPC_SERVER_NAME", "booking.grpc.test.rushgo.internal")
	t.Setenv("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_CERT_FILE", ownerCertPath)
	t.Setenv("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_KEY_FILE", ownerKeyPath)
	t.Setenv("ADMIN_OBSERVABILITY_E2E_OLD_ROLE_TOKEN_FILE", tokenPath)
	t.Setenv("ADMIN_EVENT_OPERATIONS_PROBE_SCENARIO", "admin")
	t.Setenv("ADMIN_EVENT_OPERATIONS_EVENT_ID", eventID)
	t.Setenv("ADMIN_EVENT_OPERATIONS_MISSING_EVENT_ID", missingEventID)

	if stage := runEventOperationsProbe(); stage != "" {
		t.Fatalf("admin event operations probe failed at %q", stage)
	}
	if got := bookingStub.getCalls.Load(); got != 2 {
		t.Fatalf("authorized GetAdminEventTicketOperations calls = %d, want 2", got)
	}
	if got := bookingStub.listCalls.Load(); got != 2 {
		t.Fatalf("authorized ListAdminEventTickets calls = %d, want 2", got)
	}
}

func configureEventProbeRuntime(t *testing.T, owner string) string {
	return configureEventProbeRuntimeAt(t, owner, "127.0.0.1:15002", "127.0.0.1:15003")
}

func configureEventProbeRuntimeAt(t *testing.T, owner, eventAddress, bookingAddress string) string {
	t.Helper()
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		upper := strings.ToUpper(name)
		for _, forbidden := range []string{"PROXY", "SMTP", "KAFKA", "STRIPE", "OTEL", "TOKEN", "PASSWORD", "JWT_SECRET"} {
			if strings.Contains(upper, forbidden) {
				t.Setenv(name, "")
				break
			}
		}
	}
	runID := strings.Repeat("a", 32)
	runtimeDir := filepath.Join(t.TempDir(), runID)
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatalf("create private runtime fixture: %v", err)
	}
	redisURL := "redis://127.0.0.1:16379/1"
	cfg := fixtureRuntime{
		Acknowledgement:         fixtureAck,
		RunID:                   runID,
		TestIdentity:            "rushgo-" + owner + "/" + runID + "/gateway",
		Service:                 "gateway",
		FixtureOwner:            owner,
		PrivateRuntimeDir:       runtimeDir,
		BookingRedisURL:         redisURL,
		GatewayRedisURL:         redisURL,
		NotificationRedisURL:    redisURL,
		JWTSecret:               strings.Repeat("x", 32),
		JWTAccessTokenTTL:       900,
		JWTRefreshTokenTTL:      3600,
		UserGRPCListen:          "127.0.0.1:15001",
		EventGRPCListen:         eventAddress,
		BookingGRPCListen:       bookingAddress,
		NotificationGRPCListen:  "127.0.0.1:15004",
		GatewayHTTPListen:       "127.0.0.1:18000",
		UserServiceAddr:         "127.0.0.1:15001",
		EventServiceAddr:        eventAddress,
		BookingServiceAddr:      bookingAddress,
		NotificationServiceAddr: "127.0.0.1:15004",
	}
	if owner == "admin-event-operations" {
		cfg.NotificationRedisURL = ""
		cfg.NotificationGRPCListen = ""
		cfg.NotificationServiceAddr = ""
	}
	config, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal fixture runtime: %v", err)
	}
	configPath := filepath.Join(runtimeDir, "gateway.json")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatalf("write fixture runtime: %v", err)
	}
	t.Setenv("ADMIN_OBSERVABILITY_E2E_ACK", fixtureAck)
	t.Setenv("ADMIN_OBSERVABILITY_E2E_CONFIG", configPath)
	t.Setenv("SERVICE_NAME", owner+"-e2e-gateway")
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", "")
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", "")
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", "")
	t.Setenv("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_CERT_FILE", "")
	t.Setenv("ADMIN_EVENT_OPERATIONS_BOOKING_OWNER_KEY_FILE", "")
	t.Setenv("RUSHGO_GRPC_TEST_ONLY_INSECURE", "")
	return runtimeDir
}

type eventOperationsEventProbeStub struct {
	eventv1.UnimplementedEventServiceServer
	eventID string
}

func (s *eventOperationsEventProbeStub) GetEvent(_ context.Context, request *eventv1.GetEventRequest) (*eventv1.EventDetail, error) {
	if request.GetEventId() != s.eventID {
		return nil, status.Error(codes.NotFound, "event not found")
	}
	return &eventv1.EventDetail{Id: s.eventID, Title: "Synthetic event"}, nil
}

func (s *eventOperationsEventProbeStub) ListEvents(context.Context, *eventv1.ListEventsRequest) (*eventv1.ListEventsResponse, error) {
	return &eventv1.ListEventsResponse{Events: []*eventv1.EventDetail{{Id: s.eventID, Title: "Synthetic event"}}, Total: 1}, nil
}

type eventOperationsBookingProbeStub struct {
	bookingv1.UnimplementedBookingServiceServer
	eventID       string
	expectedToken string
	getCalls      atomic.Int32
	listCalls     atomic.Int32
}

func (s *eventOperationsBookingProbeStub) GetAdminEventTicketOperations(ctx context.Context, request *bookingv1.GetAdminEventTicketOperationsRequest) (*bookingv1.GetAdminEventTicketOperationsResponse, error) {
	if !probeAuthorizationMatches(ctx, s.expectedToken) {
		return nil, status.Error(codes.Unauthenticated, "admin probe token required")
	}
	if request.GetEventId() != s.eventID {
		return nil, status.Error(codes.NotFound, "event not found")
	}
	s.getCalls.Add(1)
	return &bookingv1.GetAdminEventTicketOperationsResponse{
		EventId: s.eventID, ObservedAt: timestamppb.Now(), SourceRecordLimit: 5000,
	}, nil
}

func (s *eventOperationsBookingProbeStub) ListAdminEventTickets(ctx context.Context, request *bookingv1.ListAdminEventTicketsRequest) (*bookingv1.ListAdminEventTicketsResponse, error) {
	if !probeAuthorizationMatches(ctx, s.expectedToken) {
		return nil, status.Error(codes.Unauthenticated, "admin probe token required")
	}
	if request.GetEventId() != s.eventID {
		return nil, status.Error(codes.NotFound, "event not found")
	}
	s.listCalls.Add(1)
	return &bookingv1.ListAdminEventTicketsResponse{
		EventId: s.eventID, ObservedAt: timestamppb.Now(), StatusFilter: "ALL", PageSize: 20, SourceRecordLimit: 5000,
	}, nil
}

func probeAuthorizationMatches(ctx context.Context, token string) bool {
	values, ok := metadata.FromIncomingContext(ctx)
	return ok && len(values.Get("authorization")) == 1 && values.Get("authorization")[0] == "Bearer "+token
}

func serveProbeGRPCServer(t *testing.T, server *grpc.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for local gRPC probe server: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func writeProbeRuntimeFile(t *testing.T, runtimeDir, name string, contents []byte) string {
	t.Helper()
	path := filepath.Join(runtimeDir, name)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write private local gRPC fixture file: %v", err)
	}
	return path
}

func startWrongIdentityProbeServer(t *testing.T) (string, credentials.TransportCredentials) {
	t.Helper()

	caCert, caKey, caPEM := newProbeTestCA(t)
	serverCert, serverKey := newProbeTestLeaf(t, caCert, caKey, "booking.grpc.test.rushgo.internal", "", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := newProbeTestLeaf(t, caCert, caKey, "booking-client", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)

	serverCredentials, err := grpcsecurity.NewServerCredentials(serverCert, serverKey, caPEM, []string{grpcsecurity.ServiceIdentityGateway})
	if err != nil {
		t.Fatalf("create local TLS server credentials: %v", err)
	}
	clientCredentials, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, caPEM, "booking.grpc.test.rushgo.internal", grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		t.Fatalf("create local wrong-identity client credentials: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(serverCredentials))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String(), clientCredentials
}

func newProbeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate local CA key: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "admin-event-operations probe test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create local CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse local CA certificate: %v", err)
	}
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func newProbeTestLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, commonName, identity string, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate local leaf key: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if identity != "" {
		parsed, err := url.Parse(identity)
		if err != nil {
			t.Fatalf("parse local SPIFFE identity: %v", err)
		}
		template.URIs = []*url.URL{parsed}
	} else {
		template.DNSNames = []string{commonName}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create local leaf certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal local leaf key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}
