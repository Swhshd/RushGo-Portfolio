package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestAdmissionHandlersRejectCallerBeforeCoordinator(t *testing.T) {
	server := NewAdmissionServer(nil)
	for _, tc := range []struct {
		name, caller string
		code         codes.Code
	}{
		{"plaintext", "", codes.Unauthenticated}, {"gateway", grpcsecurity.ServiceIdentityGateway, codes.PermissionDenied},
		{"payment", grpcsecurity.ServiceIdentityPayment, codes.PermissionDenied}, {"booking bad input", grpcsecurity.ServiceIdentityBooking, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.caller != "" {
				uri, _ := url.Parse(tc.caller)
				cert := &x509.Certificate{URIs: []*url.URL{uri}}
				ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}}})
			}
			_, prepare := server.PrepareAdmission(ctx, &admissionv1.Identity{})
			_, get := server.GetAdmission(ctx, &admissionv1.Identity{})
			_, reconcile := server.ReconcileAdmission(ctx, &admissionv1.ResolutionRequest{})
			for _, err := range []error{prepare, get, reconcile} {
				if status.Code(err) != tc.code {
					t.Fatalf("status %v want %v", err, tc.code)
				}
			}
		})
	}
}
