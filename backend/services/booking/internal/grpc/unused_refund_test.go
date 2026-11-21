package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
	"time"
)

func TestUnusedRefundRPCFailClosedAndCurrentAdmin(t *testing.T) {
	s := NewBookingServer(nil, zap.NewNop())
	s.SetUnusedTicketRefundService(service.NewUnusedTicketRefundService(nil, nil, nil), true)
	s.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "user"}})
	s.SetAdminOutboxPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "user"}})
	_, err := s.RequestBookingRefund(context.Background(), &bookingv1.RequestBookingRefundRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("plaintext accepted: %v", err)
	}
	gateway := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer fixture")
	_, err = s.SetEventRefundApprover(gateway, &bookingv1.SetEventRefundApproverRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ordinary principal granted authority: %v", err)
	}
	if s.refunds.ApprovalEnabled() {
		t.Fatal("missing Event authority enabled approval")
	}
	_, err = s.GetApprovedUnusedRefundDecision(gateway, &bookingv1.GetApprovedUnusedRefundDecisionRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gateway read internal proof: %v", err)
	}
}

func TestUnusedRefundBookingRealMTLSTransport(t *testing.T) {
	ca := newAdminOutboxTLSCA(t, "unused-refund-transport")
	leaf := ca.leaf(t, "booking-service", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageServerAuth)
	serverCred, err := grpcsecurity.NewServerCredentials(leaf.certificate, leaf.privateKey, ca.pem, []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentityPayment})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer(ggrpc.Creds(serverCred))
	s := NewBookingServer(nil, zap.NewNop())
	s.SetUnusedTicketRefundService(service.NewUnusedTicketRefundService(nil, nil, nil), true)
	s.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.New(), Role: "user"}})
	bookingv1.RegisterBookingServiceServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	for _, identity := range []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentityPayment} {
		t.Run(identity, func(t *testing.T) {
			clientLeaf := ca.leaf(t, "", identity, x509.ExtKeyUsageClientAuth)
			pair, err := tls.X509KeyPair(clientLeaf.certificate, clientLeaf.privateKey)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(ca.pem) {
				t.Fatal("fixture CA")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := ggrpc.DialContext(ctx, "passthrough:///booking-refund", ggrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), ggrpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: roots, ServerName: "booking-service", MinVersion: tls.VersionTLS12})), ggrpc.WithBlock())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			rpc := bookingv1.NewBookingServiceClient(conn)
			_, buyerErr := rpc.GetBookingRefund(ctx, &bookingv1.GetBookingRefundRequest{})
			_, proofErr := rpc.GetApprovedUnusedRefundDecision(ctx, &bookingv1.GetApprovedUnusedRefundDecisionRequest{})
			buyerWant, proofWant := codes.InvalidArgument, codes.PermissionDenied
			if identity == grpcsecurity.ServiceIdentityPayment {
				buyerWant, proofWant = codes.PermissionDenied, codes.InvalidArgument
			}
			if status.Code(buyerErr) != buyerWant || status.Code(proofErr) != proofWant {
				t.Fatalf("real RPC identity boundary %v / %v", buyerErr, proofErr)
			}
		})
	}
}
