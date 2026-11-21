package grpcsecurity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"testing"
	"time"

	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestAdmissionRequiresVerifiedOwnerIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, caller, want string
		verified           bool
		code               codes.Code
	}{
		{"booking", ServiceIdentityBooking, ServiceIdentityBooking, true, codes.OK},
		{"payment", ServiceIdentityPayment, ServiceIdentityPayment, true, codes.OK},
		{"saga recovery", ServiceIdentitySaga, ServiceIdentitySaga, true, codes.OK},
		{"saga cannot use booking authority", ServiceIdentitySaga, ServiceIdentityBooking, true, codes.PermissionDenied},
		{"booking cannot use recovery authority", ServiceIdentityBooking, ServiceIdentitySaga, true, codes.PermissionDenied},
		{"unverified saga", ServiceIdentitySaga, ServiceIdentitySaga, false, codes.Unauthenticated},
		{"gateway denied", ServiceIdentityGateway, ServiceIdentityBooking, true, codes.PermissionDenied},
		{"wrong owner", ServiceIdentityPayment, ServiceIdentityBooking, true, codes.PermissionDenied},
		{"unverified certificate", ServiceIdentityBooking, ServiceIdentityBooking, false, codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uri, _ := url.Parse(tc.caller)
			cert := &x509.Certificate{URIs: []*url.URL{uri}}
			state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
			if tc.verified {
				state.VerifiedChains = [][]*x509.Certificate{{cert}}
			}
			ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
			if got := status.Code(RequireAdmissionOwner(ctx, tc.want)); got != tc.code {
				t.Fatalf("code %v want %v", got, tc.code)
			}
		})
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-service-identity", ServiceIdentityBooking))
	if status.Code(RequireAdmissionOwner(ctx, ServiceIdentityBooking)) != codes.Unauthenticated {
		t.Fatal("metadata impersonation accepted")
	}
}

type admissionWireGuard struct {
	admissionv1.UnimplementedPaymentAdmissionCoordinatorServer
}

func (admissionWireGuard) GetAdmission(ctx context.Context, _ *admissionv1.Identity) (*admissionv1.Permit, error) {
	if err := RequireAdmissionOwner(ctx, ServiceIdentityBooking); err != nil {
		return nil, err
	}
	return &admissionv1.Permit{State: "PENDING"}, nil
}

// A real TLS handshake and generated gRPC call establish that a trusted Gateway
// certificate may connect but cannot invoke Booking-owned admission methods.
func TestAdmissionWireMTLSRejectsGatewayAndUntrustedCA(t *testing.T) {
	ca := newTestPKI(t, "admission test CA")
	wrongCA := newTestPKI(t, "untrusted admission CA")
	before, after := validWindow()
	cert, key := ca.leaf(t, "payment", testServerName, ServiceIdentityPayment, before, after, x509.ExtKeyUsageServerAuth)
	serverCred, err := NewServerCredentials(cert, key, ca.caPEM, []string{ServiceIdentityBooking, ServiceIdentityGateway})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.Creds(serverCred))
	admissionv1.RegisterPaymentAdmissionCoordinatorServer(server, admissionWireGuard{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	for _, tc := range []struct {
		name, identity string
		signer         testPKI
		code           codes.Code
	}{
		{"booking", ServiceIdentityBooking, ca, codes.OK}, {"gateway", ServiceIdentityGateway, ca, codes.PermissionDenied}, {"untrusted CA", ServiceIdentityBooking, wrongCA, codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert, key := tc.signer.leaf(t, tc.identity, "", tc.identity, before, after, x509.ExtKeyUsageClientAuth)
			clientCred, err := NewClientCredentials(cert, key, ca.caPEM, testServerName, tc.identity)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient("passthrough:///admission-test", grpc.WithTransportCredentials(clientCred), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = admissionv1.NewPaymentAdmissionCoordinatorClient(conn).GetAdmission(ctx, &admissionv1.Identity{}, grpc.WaitForReady(true))
			if status.Code(err) != tc.code {
				t.Fatalf("status %v want %v", err, tc.code)
			}
		})
	}
}
