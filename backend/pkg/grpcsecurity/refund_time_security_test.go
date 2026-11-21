package grpcsecurity

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestRefundTimeAuthorityMethodIsolation(t *testing.T) {
	ca := newTestPKI(t, "refund-time")
	before, after := validWindow()
	methods := []string{eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName, eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName, eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName}
	filter := TicketAuthorityMethodsAuthorizationInterceptor(methods...)
	for _, identity := range []string{ServiceIdentityBooking, ServiceIdentityGateway, ServiceIdentitySaga} {
		certPEM, _ := ca.leaf(t, "client", "", identity, before, after, x509.ExtKeyUsageClientAuth)
		block, _ := pem.Decode(certPEM)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range append(methods, "/event.v1.EventService/UpdateEvent") {
			called := false
			_, err = filter(verifiedPeerContext(cert), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { called = true; return nil, nil })
			allowed := (identity == ServiceIdentityBooking && method != "/event.v1.EventService/UpdateEvent") ||
				(identity == ServiceIdentitySaga && method == eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName)
			if allowed != (err == nil && called) {
				t.Fatalf("identity/method leak %s %s %v", identity, method, err)
			}
		}
	}
	for _, method := range methods[1:] {
		_, err := RejectTicketAuthorityMethodsOnLegacyListener()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { t.Fatal("legacy reached handler"); return nil, nil })
		if status.Code(err) != codes.Unauthenticated {
			t.Fatal(err)
		}
	}
	server := grpc.NewServer()
	defer server.Stop()
	if err := RegisterServiceWithOnlyMethods(server, eventv1.EventService_ServiceDesc, &eventv1.UnimplementedEventServiceServer{}, methods...); err != nil {
		t.Fatal(err)
	}
	if len(server.GetServiceInfo()["event.v1.EventService"].Methods) != 3 {
		t.Fatal("expanded descriptor")
	}
}
