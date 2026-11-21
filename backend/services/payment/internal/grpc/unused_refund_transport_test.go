package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"
)

// Actual TLS handshake and generated client/server serialization. This checks
// transport authorization, not PostgreSQL or provider execution.
func TestUnusedRefundMTLSTransport(t *testing.T) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	serial := int64(2)
	cert := func(identity string, server bool) tls.Certificate {
		t.Helper()
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		uri, e := url.Parse(identity)
		if e != nil {
			t.Fatal(e)
		}
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
		serial++
		if server {
			tmpl.DNSNames = []string{"payment-service"}
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		der, e := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	listener := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer(ggrpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert(grpcsecurity.ServiceIdentityPayment, true)}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12})))
	paymentv1.RegisterPaymentServiceServer(server, NewPaymentServer(nil, zap.NewNop()))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	for _, tc := range []struct {
		name, identity string
		want           codes.Code
	}{{"booking", grpcsecurity.ServiceIdentityBooking, codes.InvalidArgument}, {"gateway", grpcsecurity.ServiceIdentityGateway, codes.PermissionDenied}, {"payment", grpcsecurity.ServiceIdentityPayment, codes.PermissionDenied}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := ggrpc.DialContext(ctx, "passthrough:///refund-test", ggrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), ggrpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert(tc.identity, false)}, RootCAs: roots, ServerName: "payment-service", MinVersion: tls.VersionTLS12})), ggrpc.WithBlock())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			// Forged metadata cannot turn a verified Gateway peer into Booking.
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-service-identity", grpcsecurity.ServiceIdentityBooking))
			rpc := paymentv1.NewPaymentServiceClient(client)
			_, e1 := rpc.AcceptApprovedUnusedRefundDecision(ctx, &paymentv1.AcceptApprovedUnusedRefundDecisionRequest{})
			_, e2 := rpc.GetUnusedRefundPaymentSnapshot(ctx, &paymentv1.UnusedRefundPaymentSnapshotRequest{})
			if status.Code(e1) != tc.want || status.Code(e2) != tc.want {
				t.Fatalf("real transport authority mismatch: %v / %v", e1, e2)
			}
		})
	}
}
