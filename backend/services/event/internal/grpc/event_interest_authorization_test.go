package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestExternalEventInterestsRequireVerifiedGatewayCertificate(t *testing.T) {
	gateway, _ := url.Parse("spiffe://rushgo.internal/gateway")
	scheduler, _ := url.Parse("spiffe://rushgo.internal/scheduler")
	certificate := func(identity *url.URL, verified bool) context.Context {
		leaf := &x509.Certificate{URIs: []*url.URL{identity}}
		state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
		if verified {
			state.VerifiedChains = [][]*x509.Certificate{{leaf}}
		}
		return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	}
	for _, environment := range []string{"stripe_test", "preproduction", "production"} {
		t.Run(environment, func(t *testing.T) {
			t.Setenv("RUSHGO_ENV", environment)
			for _, denied := range []struct {
				name string
				ctx  context.Context
			}{
				{"missing transport", context.Background()},
				{"forged metadata", metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-service-identity", "gateway"))},
				{"unverified certificate", certificate(gateway, false)},
				{"other verified service", certificate(scheduler, true)},
			} {
				t.Run(denied.name, func(t *testing.T) {
					if err := requireGatewayInterestCaller(denied.ctx); err == nil {
						t.Fatal("untrusted caller reached owner-scoped event interests")
					}
				})
			}
			if err := requireGatewayInterestCaller(certificate(gateway, true)); err != nil {
				t.Fatalf("verified Gateway rejected: %v", err)
			}
		})
	}
}
