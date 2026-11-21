package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestUnusedRefundInternalRPCsRequireBookingMTLSBeforeParsingRequests(t *testing.T) {
	server := NewPaymentServer(nil, zap.NewNop())
	for _, tc := range []struct {
		name   string
		caller string
		want   codes.Code
	}{
		{name: "plaintext", want: codes.Unauthenticated},
		{name: "gateway", caller: grpcsecurity.ServiceIdentityGateway, want: codes.PermissionDenied},
		{name: "payment", caller: grpcsecurity.ServiceIdentityPayment, want: codes.PermissionDenied},
		{name: "booking malformed request", caller: grpcsecurity.ServiceIdentityBooking, want: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := unusedRefundServiceIdentityContext(tc.caller)
			_, acceptErr := server.AcceptApprovedUnusedRefundDecision(ctx, &paymentv1.AcceptApprovedUnusedRefundDecisionRequest{})
			_, snapshotErr := server.GetUnusedRefundPaymentSnapshot(ctx, &paymentv1.UnusedRefundPaymentSnapshotRequest{})
			for _, err := range []error{acceptErr, snapshotErr} {
				if status.Code(err) != tc.want {
					t.Fatalf("internal refund RPC error=%v code=%s, want %s", err, status.Code(err), tc.want)
				}
			}
		})
	}
}

func TestPaidRecoverySnapshotRequiresSagaMTLSBeforeParsingRequest(t *testing.T) {
	server := NewPaymentServer(nil, zap.NewNop())
	for _, tc := range []struct {
		name   string
		caller string
		want   codes.Code
	}{
		{name: "plaintext", want: codes.Unauthenticated},
		{name: "gateway", caller: grpcsecurity.ServiceIdentityGateway, want: codes.PermissionDenied},
		{name: "booking", caller: grpcsecurity.ServiceIdentityBooking, want: codes.PermissionDenied},
		{name: "payment", caller: grpcsecurity.ServiceIdentityPayment, want: codes.PermissionDenied},
		{name: "saga malformed request", caller: grpcsecurity.ServiceIdentitySaga, want: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.GetPaidRecoveryPaymentSnapshot(unusedRefundServiceIdentityContext(tc.caller), &paymentv1.FinancialPaymentRequest{})
			if status.Code(err) != tc.want {
				t.Fatalf("paid recovery RPC error = %v, want %s", err, tc.want)
			}
		})
	}
}

func unusedRefundServiceIdentityContext(identity string) context.Context {
	ctx := context.Background()
	if identity == "" {
		return ctx
	}
	uri, _ := url.Parse(identity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}},
	}}})
}
