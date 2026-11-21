package grpcsecurity

import (
	"context"
	"crypto/x509"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEventRecoverySagaMTLSReadOnly(t *testing.T) {
	ca := newTestPKI(t, "recovery")
	before, after := validWindow()
	cert, key := ca.leaf(t, "event", "event-service", ServiceIdentityEvent, before, after, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := ca.leaf(t, "saga", "", ServiceIdentitySaga, before, after, x509.ExtKeyUsageClientAuth)
	dir := t.TempDir()
	set := func(env, name string, value []byte) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, value, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(env, p)
	}
	set("RUSHGO_GRPC_SERVER_CERT_FILE", "server.pem", cert)
	set("RUSHGO_GRPC_SERVER_KEY_FILE", "server.key", key)
	set("RUSHGO_GRPC_CLIENT_CA_FILE", "ca.pem", ca.caPEM)
	set("RUSHGO_GRPC_SERVER_CA_FILE", "server-ca.pem", ca.caPEM)
	set("RUSHGO_GRPC_CLIENT_CERT_FILE", "client.pem", clientCert)
	set("RUSHGO_GRPC_CLIENT_KEY_FILE", "client.key", clientKey)
	t.Setenv("RUSHGO_EVENT_GRPC_SERVER_NAME", "event-service")
	serverCreds, err := TicketAuthorityServerCredentialsFromEnv(ServiceEvent)
	if err != nil {
		t.Fatal(err)
	}
	clientCreds, err := EventRecoveryClientCredentialsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	methods := []string{eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName, eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName, eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName}
	server := grpc.NewServer(grpc.Creds(serverCreds), grpc.UnaryInterceptor(TicketAuthorityMethodsAuthorizationInterceptor(methods...)))
	if err := RegisterServiceWithOnlyMethods(server, eventv1.EventService_ServiceDesc, eventTicketAuthorityStub{}, methods...); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	conn, err := grpc.NewClient("passthrough:///event-service", grpc.WithTransportCredentials(clientCreds), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := eventv1.NewEventServiceClient(conn)
	if _, err := client.VerifyBookingTicketEligibility(ctx, &eventv1.VerifyBookingTicketEligibilityRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireUnusedRefundTimePermit(ctx, &eventv1.RefundTimeBinding{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Saga acquire: %v", err)
	}
	if _, err := client.ReleaseUnusedRefundTimePermit(ctx, &eventv1.ReleaseUnusedRefundTimePermitRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Saga release: %v", err)
	}
}
