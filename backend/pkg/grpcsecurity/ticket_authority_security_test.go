package grpcsecurity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
)

func TestTicketAuthorityMethodAuthorizationUsesVerifiedBookingIdentity(t *testing.T) {
	ca := newTestPKI(t, "Ticket authority test CA")
	notBefore, notAfter := validWindow()
	bookingPEM, _ := ca.leaf(t, "booking", "", ServiceIdentityBooking, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	bookingBlock, _ := pem.Decode(bookingPEM)
	bookingCert, err := x509.ParseCertificate(bookingBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	otherPEM, _ := ca.leaf(t, "saga", "", ServiceIdentitySaga, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	otherBlock, _ := pem.Decode(otherPEM)
	otherCert, err := x509.ParseCertificate(otherBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	method := eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName
	interceptor := TicketAuthorityMethodAuthorizationInterceptor(method)
	cases := []struct {
		name       string
		ctx        context.Context
		method     string
		wantCode   codes.Code
		wantCalled bool
	}{
		{name: "verified booking identity", ctx: verifiedPeerContext(bookingCert), method: method, wantCalled: true},
		{name: "missing peer", ctx: context.Background(), method: method, wantCode: codes.Unauthenticated},
		{name: "self reported metadata is not identity", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-service-name", "booking")), method: method, wantCode: codes.Unauthenticated},
		{name: "unverified tls peer", ctx: peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{bookingCert}}}}), method: method, wantCode: codes.Unauthenticated},
		{name: "wrong verified service", ctx: verifiedPeerContext(otherCert), method: method, wantCode: codes.PermissionDenied},
		{name: "wrong method", ctx: verifiedPeerContext(bookingCert), method: sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName, wantCode: codes.PermissionDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			_, err := interceptor(tc.ctx, nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				called = true
				return "authorized", nil
			})
			if tc.wantCalled {
				if err != nil || !called {
					t.Fatalf("verified Booking call = called:%t err:%v, want handler", called, err)
				}
				return
			}
			if called || status.Code(err) != tc.wantCode {
				t.Fatalf("call = called:%t err:%v, want code %s", called, err, tc.wantCode)
			}
		})
	}
}

func TestTicketAuthorityLegacyListenerBlocksBothSensitiveMethods(t *testing.T) {
	interceptor := RejectTicketAuthorityMethodsOnLegacyListener()
	methods := []string{
		eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName,
		sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName,
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			called := false
			_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
				called = true
				return "legacy", nil
			})
			if called || status.Code(err) != codes.Unauthenticated {
				t.Fatalf("legacy listener call = called:%t err:%v, want Unauthenticated", called, err)
			}
		})
	}
	t.Run("ordinary methods remain available", func(t *testing.T) {
		called := false
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/event.v1.EventService/GetEvent"}, func(context.Context, any) (any, error) {
			called = true
			return "ok", nil
		})
		if err != nil || !called {
			t.Fatalf("unrelated legacy RPC was rejected: called:%t err:%v", called, err)
		}
	})
}

func TestServiceDescWithOnlyMethodExposesExactlyOneUnaryMethod(t *testing.T) {
	for _, tc := range []struct {
		name   string
		desc   grpc.ServiceDesc
		method string
	}{
		{name: "Event", desc: eventv1.EventService_ServiceDesc, method: eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName},
		{name: "Saga", desc: sagav1.SagaOrchestratorService_ServiceDesc, method: sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filtered, err := ServiceDescWithOnlyMethod(tc.desc, tc.method)
			if err != nil {
				t.Fatal(err)
			}
			wantName := tc.method[strings.LastIndex(tc.method, "/")+1:]
			if len(filtered.Methods) != 1 || filtered.Methods[0].MethodName == "" || len(filtered.Streams) != 0 || filtered.ServiceName != tc.desc.ServiceName {
				t.Fatalf("filtered descriptor = service:%s methods:%d streams:%d", filtered.ServiceName, len(filtered.Methods), len(filtered.Streams))
			}
			if filtered.Methods[0].MethodName != wantName {
				t.Fatalf("filtered method = %q, want %q", filtered.Methods[0].MethodName, wantName)
			}
		})
	}
	if _, err := ServiceDescWithOnlyMethod(eventv1.EventService_ServiceDesc, sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName); err == nil {
		t.Fatal("service descriptor accepted a method from another gRPC service")
	}
}

func TestTicketAuthorityMTLSCallsReturnAuthoritativeIneligibleState(t *testing.T) {
	ca := newTestPKI(t, "Ticket authority integration CA")
	notBefore, notAfter := validWindow()
	bookingCert, bookingKey := ca.leaf(t, "booking", "", ServiceIdentityBooking, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	eventCert, eventKey := ca.leaf(t, "event", "event-service", ServiceIdentityEvent, notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	sagaCert, sagaKey := ca.leaf(t, "saga", "saga-service", ServiceIdentitySaga, notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	bookingCredentials, err := NewClientCredentials(bookingCert, bookingKey, ca.caPEM, "event-service", ServiceIdentityBooking)
	if err != nil {
		t.Fatal(err)
	}
	eventCredentials, err := NewServerCredentials(eventCert, eventKey, ca.caPEM, []string{ServiceIdentityBooking})
	if err != nil {
		t.Fatal(err)
	}
	sagaCredentials, err := NewServerCredentials(sagaCert, sagaKey, ca.caPEM, []string{ServiceIdentityBooking})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Event eligibility over mutual TLS", func(t *testing.T) {
		response, err := invokeEventTicketAuthority(t, eventCredentials, bookingCredentials, false)
		if err != nil {
			t.Fatal(err)
		}
		if response.GetState() != eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_DENIED {
			t.Fatalf("TLS identity changed Event business eligibility: %v", response.GetState())
		}
	})
	t.Run("Saga eligibility over mutual TLS", func(t *testing.T) {
		sagaClientCredentials, err := NewClientCredentials(bookingCert, bookingKey, ca.caPEM, "saga-service", ServiceIdentityBooking)
		if err != nil {
			t.Fatal(err)
		}
		response, err := invokeSagaTicketAuthority(t, sagaCredentials, sagaClientCredentials)
		if err != nil {
			t.Fatal(err)
		}
		if response.GetState() != sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_DENIED {
			t.Fatalf("TLS identity changed Saga business eligibility: %v", response.GetState())
		}
	})
	t.Run("Event rejects plaintext", func(t *testing.T) {
		_, err := invokeEventTicketAuthority(t, eventCredentials, insecure.NewCredentials(), true)
		if err == nil {
			t.Fatal("Event ticket authority accepted a plaintext RPC")
		}
	})
	t.Run("Saga rejects plaintext", func(t *testing.T) {
		_, err := invokeSagaTicketAuthority(t, sagaCredentials, insecure.NewCredentials())
		if err == nil {
			t.Fatal("Saga ticket authority accepted a plaintext RPC")
		}
	})
	t.Run("legacy Event listener rejects plaintext authority RPC", func(t *testing.T) {
		_, err := invokeEventTicketAuthority(t, nil, insecure.NewCredentials(), true)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("legacy Event listener returned %v, want Unauthenticated", err)
		}
	})
	t.Run("legacy Saga listener rejects plaintext authority RPC", func(t *testing.T) {
		_, err := invokeSagaLegacyTicketAuthority(t)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("legacy Saga listener returned %v, want Unauthenticated", err)
		}
	})
}

func TestTicketAuthorityMTLSRejectsWrongCallerCANameAndExpiry(t *testing.T) {
	ca := newTestPKI(t, "Ticket authority accepted CA")
	wrongCA := newTestPKI(t, "Ticket authority wrong CA")
	notBefore, notAfter := validWindow()
	eventCert, eventKey := ca.leaf(t, "event", "event-service", ServiceIdentityEvent, notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	serverCredentials, err := NewServerCredentials(eventCert, eventKey, ca.caPEM, []string{ServiceIdentityBooking})
	if err != nil {
		t.Fatal(err)
	}
	newClient := func(pki testPKI, identity string, start, end time.Time) credentials.TransportCredentials {
		t.Helper()
		cert, key := pki.leaf(t, identity, "", identity, start, end, x509.ExtKeyUsageClientAuth)
		creds, err := NewClientCredentials(cert, key, ca.caPEM, "event-service", identity)
		if err != nil {
			t.Fatal(err)
		}
		return creds
	}
	tests := []struct {
		name  string
		creds credentials.TransportCredentials
	}{
		{name: "wrong client CA", creds: newClient(wrongCA, ServiceIdentityBooking, notBefore, notAfter)},
		{name: "unapproved peer identity", creds: newClient(ca, ServiceIdentitySaga, notBefore, notAfter)},
		{name: "expired client certificate", creds: newClient(ca, ServiceIdentityBooking, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))},
		{name: "wrong server SAN", creds: mustClientCredentials(t, ca, ServiceIdentityBooking, "not-event-service", notBefore, notAfter)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := testRPC(t, serverCredentials, tc.creds); err == nil {
				t.Fatal("invalid Ticket authority TLS identity unexpectedly succeeded")
			}
		})
	}
	t.Run("missing client certificate", func(t *testing.T) {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca.caPEM) {
			t.Fatal("could not load test server root")
		}
		creds := credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "event-service", MinVersion: tls.VersionTLS12})
		if err := testRPC(t, serverCredentials, creds); err == nil {
			t.Fatal("server accepted a missing Booking client certificate")
		}
	})
}

func TestTicketAuthorityCredentialLoadersIgnoreTestOnlyPlaintextSwitch(t *testing.T) {
	ca := newTestPKI(t, "Ticket authority environment CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "event-service", "event-service", ServiceIdentityEvent, notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := ca.leaf(t, "booking", "", ServiceIdentityBooking, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	write := func(name string, value []byte) string {
		t.Helper()
		path := t.TempDir() + "/" + name
		if err := os.WriteFile(path, value, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Setenv(TestOnlyInsecureEnv, testOnlyInsecureAck)
	t.Setenv("RUSHGO_GRPC_SERVER_CERT_FILE", write("server-cert.pem", serverCert))
	t.Setenv("RUSHGO_GRPC_SERVER_KEY_FILE", write("server-key.pem", serverKey))
	t.Setenv("RUSHGO_GRPC_CLIENT_CA_FILE", write("client-ca.pem", ca.caPEM))
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", write("booking-cert.pem", clientCert))
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", write("booking-key.pem", clientKey))
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", write("server-ca.pem", ca.caPEM))
	t.Setenv("RUSHGO_EVENT_GRPC_SERVER_NAME", "event-service")
	serverCredentials, err := TicketAuthorityServerCredentialsFromEnv(ServiceEvent)
	if err != nil {
		t.Fatal(err)
	}
	clientCredentials, err := TicketAuthorityClientCredentialsFromEnv(ServiceEvent)
	if err != nil {
		t.Fatal(err)
	}
	if err := testRPC(t, serverCredentials, clientCredentials); err != nil {
		t.Fatalf("Ticket authority credential loaders did not preserve mTLS under TEST_ONLY env: %v", err)
	}
	if _, err := TicketAuthorityClientCredentialsFromEnv(ServicePayment); err == nil {
		t.Fatal("Ticket authority credentials were issued for a non-Event/Saga target")
	}
}

func verifiedPeerContext(cert *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}})
}

type eventTicketAuthorityStub struct {
	eventv1.UnimplementedEventServiceServer
}

func (eventTicketAuthorityStub) VerifyBookingTicketEligibility(context.Context, *eventv1.VerifyBookingTicketEligibilityRequest) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	return &eventv1.VerifyBookingTicketEligibilityResponse{State: eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_DENIED, ReasonCode: "ACCOUNTING_NOT_READY"}, nil
}

type sagaTicketAuthorityStub struct {
	sagav1.UnimplementedSagaOrchestratorServiceServer
}

func (sagaTicketAuthorityStub) VerifyTicketIssuanceSaga(context.Context, *sagav1.VerifyTicketIssuanceSagaRequest) (*sagav1.VerifyTicketIssuanceSagaResponse, error) {
	return &sagav1.VerifyTicketIssuanceSagaResponse{State: sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_DENIED, ReasonCode: "SAGA_INCOMPLETE"}, nil
}

func invokeEventTicketAuthority(t *testing.T, serverCredentials, clientCredentials credentials.TransportCredentials, legacy bool) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	serverOptions := []grpc.ServerOption{}
	if serverCredentials != nil {
		serverOptions = append(serverOptions, grpc.Creds(serverCredentials), grpc.UnaryInterceptor(TicketAuthorityMethodAuthorizationInterceptor(eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName)))
	} else if legacy {
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(RejectTicketAuthorityMethodsOnLegacyListener()))
	}
	server := grpc.NewServer(serverOptions...)
	if serverCredentials != nil {
		if err := RegisterServiceWithOnlyMethod(server, eventv1.EventService_ServiceDesc, eventTicketAuthorityStub{}, eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName); err != nil {
			t.Fatal(err)
		}
	} else {
		eventv1.RegisterEventServiceServer(server, eventTicketAuthorityStub{})
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient("passthrough:///event-service", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(clientCredentials))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return eventv1.NewEventServiceClient(conn).VerifyBookingTicketEligibility(ctx, &eventv1.VerifyBookingTicketEligibilityRequest{})
}

func invokeSagaTicketAuthority(t *testing.T, serverCredentials, clientCredentials credentials.TransportCredentials) (*sagav1.VerifyTicketIssuanceSagaResponse, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.Creds(serverCredentials), grpc.UnaryInterceptor(TicketAuthorityMethodAuthorizationInterceptor(sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName)))
	if err := RegisterServiceWithOnlyMethod(server, sagav1.SagaOrchestratorService_ServiceDesc, sagaTicketAuthorityStub{}, sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient("passthrough:///saga-service", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(clientCredentials))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return sagav1.NewSagaOrchestratorServiceClient(conn).VerifyTicketIssuanceSaga(ctx, &sagav1.VerifyTicketIssuanceSagaRequest{})
}

func invokeSagaLegacyTicketAuthority(t *testing.T) (*sagav1.VerifyTicketIssuanceSagaResponse, error) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(RejectTicketAuthorityMethodsOnLegacyListener()))
	sagav1.RegisterSagaOrchestratorServiceServer(server, sagaTicketAuthorityStub{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient("passthrough:///saga-service", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return sagav1.NewSagaOrchestratorServiceClient(conn).VerifyTicketIssuanceSaga(ctx, &sagav1.VerifyTicketIssuanceSagaRequest{})
}

func mustClientCredentials(t *testing.T, ca testPKI, identity, serverName string, notBefore, notAfter time.Time) credentials.TransportCredentials {
	t.Helper()
	cert, key := ca.leaf(t, identity, "", identity, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	creds, err := NewClientCredentials(cert, key, ca.caPEM, serverName, identity)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}
