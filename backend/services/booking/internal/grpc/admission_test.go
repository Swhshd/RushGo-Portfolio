package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"testing"

	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestAdmissionCheckInErrorsUseExistingStatusClasses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{service.ErrTicketAdmissionUnknown, codes.Unavailable}, {admission.ErrUnavailable, codes.Unavailable},
		{admission.ErrConflict, codes.AlreadyExists}, {admission.ErrStale, codes.FailedPrecondition}, {admission.ErrBlocked, codes.FailedPrecondition},
	} {
		if got := status.Code(ticketCheckInError(fmt.Errorf("context: %w", tc.err))); got != tc.code {
			t.Fatalf("%v: got %v want %v", tc.err, got, tc.code)
		}
	}
}

func TestAdmissionHandlerRejectsCallerBeforeRepository(t *testing.T) {
	server := NewAdmissionServer(nil)
	for _, tc := range []struct {
		name, caller string
		code         codes.Code
	}{
		{"plaintext", "", codes.Unauthenticated}, {"gateway", grpcsecurity.ServiceIdentityGateway, codes.PermissionDenied},
		{"booking", grpcsecurity.ServiceIdentityBooking, codes.PermissionDenied}, {"payment bad input", grpcsecurity.ServiceIdentityPayment, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.caller != "" {
				uri, _ := url.Parse(tc.caller)
				cert := &x509.Certificate{URIs: []*url.URL{uri}}
				ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}}})
			}
			_, err := server.ResolveAdmission(ctx, &admissionv1.ResolutionRequest{})
			if status.Code(err) != tc.code {
				t.Fatalf("status %v want %v", err, tc.code)
			}
		})
	}
}
