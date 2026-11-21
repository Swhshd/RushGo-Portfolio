package grpcsecurity

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func clientTLS(t *testing.T, certPEM, keyPEM, caPEM []byte, serverName string) *tls.Config {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid test CA")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
}

func observedEvents(observer *handshakeRecorder, remote string, stage string) []HandshakeEvent {
	var matched []HandshakeEvent
	for _, event := range observer.snapshot() {
		if event.RemoteAddr == remote && event.Stage == stage {
			matched = append(matched, event)
		}
	}
	return matched
}

type handshakeRecorder struct {
	mu     sync.Mutex
	events []HandshakeEvent
}

func (r *handshakeRecorder) ObserveServerHandshake(event HandshakeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *handshakeRecorder) snapshot() []HandshakeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]HandshakeEvent(nil), r.events...)
}

func waitForEvents(t *testing.T, recorder *handshakeRecorder, remote, stage string) []HandshakeEvent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if events := observedEvents(recorder, remote, stage); len(events) > 0 {
			return events
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %s event for remote %s; events=%+v", stage, remote, recorder.snapshot())
	return nil
}

func TestServerDiagnosticsCorrelateGoodBadGood(t *testing.T) {
	ca := newTestPKI(t, "RushGo diagnostics test CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "user", testServerName, ServiceIdentityUser,
		notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	gatewayCert, gatewayKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway,
		notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	paymentCert, paymentKey := ca.leaf(t, "payment", "", ServiceIdentityPayment,
		notBefore, notAfter, x509.ExtKeyUsageClientAuth)

	recorder := &handshakeRecorder{}
	serverCred, err := NewServerCredentialsWithDiagnostics(ServiceUser, serverCert, serverKey,
		ca.caPEM, []string{ServiceIdentityGateway}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(serverCred))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	type probe struct {
		name    string
		cert    []byte
		key     []byte
		wantErr bool
		wantID  string
	}
	probes := []probe{
		{name: "gateway", cert: gatewayCert, key: gatewayKey, wantID: ServiceIdentityGateway},
		{name: "payment", cert: paymentCert, key: paymentKey, wantErr: true, wantID: ServiceIdentityPayment},
		{name: "gateway-again", cert: gatewayCert, key: gatewayKey, wantID: ServiceIdentityGateway},
	}
	var observed []string
	for _, item := range probes {
		raw, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		remote := raw.LocalAddr().String()
		conn := tls.Client(raw, clientTLS(t, item.cert, item.key, ca.caPEM, testServerName))
		handshakeErr := conn.Handshake()
		_ = conn.Close()
		if item.wantErr != (handshakeErr != nil) {
			t.Fatalf("%s: handshake error=%v, wantErr=%v", item.name, handshakeErr, item.wantErr)
		}
		result := waitForEvents(t, recorder, remote, HandshakeStageResult)
		if len(result) != 1 {
			t.Fatalf("%s: expected one result event for %s, got %+v", item.name, remote, result)
		}
		event := result[0]
		if event.ClientIdentity != item.wantID || !event.IdentityVerified {
			t.Fatalf("%s: identity=%q verified=%v, want %q", item.name,
				event.ClientIdentity, event.IdentityVerified, item.wantID)
		}
		if event.TLSVersion != "1.2" {
			t.Fatalf("%s: tls=%q, want negotiated 1.2", item.name, event.TLSVersion)
		}
		if item.wantErr {
			if event.ReasonCode != HandshakeReasonIdentityNotAllowed {
				t.Fatalf("%s: reason=%q, want %q", item.name, event.ReasonCode,
					HandshakeReasonIdentityNotAllowed)
			}
		} else {
			if event.ReasonCode != HandshakeReasonCompleted {
				t.Fatalf("%s: reason=%q, want completed", item.name, event.ReasonCode)
			}
			if event.ALPN != "h2" {
				t.Fatalf("%s: alpn=%q, want h2", item.name, event.ALPN)
			}
		}
		if event.ClientCertSHA256 == "" || event.ServerCertSHA256 == "" {
			t.Fatalf("%s: missing certificate fingerprints", item.name)
		}
		observed = append(observed, event.ClientCertSHA256)
	}
	if observed[0] == observed[1] || observed[1] == observed[2] || observed[0] != observed[2] {
		t.Fatalf("client leaf fingerprints do not distinguish the probes: %v", observed)
	}
	gatewaySum := sha256.Sum256(gatewayCertDER(t, gatewayCert))
	if observed[0] != hex.EncodeToString(gatewaySum[:]) {
		t.Fatalf("gateway fingerprint mismatch")
	}
}

func gatewayCertDER(t *testing.T, certPEM []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("invalid certificate PEM")
	}
	return block.Bytes
}

func TestServerDiagnosticsKeepIdentityAuthorizationUnchanged(t *testing.T) {
	ca := newTestPKI(t, "RushGo diagnostics authorization CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "user", testServerName, ServiceIdentityUser,
		notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	paymentCert, paymentKey := ca.leaf(t, "payment", "", ServiceIdentityPayment,
		notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	recorder := &handshakeRecorder{}
	serverCred, err := NewServerCredentialsWithDiagnostics(ServiceUser, serverCert, serverKey,
		ca.caPEM, []string{ServiceIdentityGateway}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(serverCred))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(raw, clientTLS(t, paymentCert, paymentKey, ca.caPEM, testServerName))
	if err := conn.Handshake(); err == nil {
		t.Fatal("payment identity unexpectedly completed the handshake")
	}
	_ = conn.Close()
	events := waitForEvents(t, recorder, raw.LocalAddr().String(), HandshakeStageResult)
	if events[0].ReasonCode != HandshakeReasonIdentityNotAllowed {
		t.Fatalf("reason=%q, want identity not allowed", events[0].ReasonCode)
	}
}

func TestClassifyServerHandshakeError(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{name: "unknown authority", err: x509.UnknownAuthorityError{},
			reason: HandshakeReasonChainUnknownAuthority},
		{name: "expired", err: x509.CertificateInvalidError{Reason: x509.Expired},
			reason: HandshakeReasonCertificateInvalid},
		{name: "incompatible usage", err: x509.CertificateInvalidError{Reason: x509.IncompatibleUsage},
			reason: HandshakeReasonCertificateInvalid},
		{name: "missing certificate", err: errors.New("tls: client didn't provide a certificate"),
			reason: HandshakeReasonClientCertMissing},
		{name: "deadline", err: context.DeadlineExceeded,
			reason: HandshakeReasonTransportFailure},
		{name: "unclassified", err: errors.New("tls: unexpected message"),
			reason: HandshakeReasonFailedUnclassified},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			reason, detail := classifyServerHandshakeError(item.err)
			if reason != item.reason {
				t.Fatalf("reason=%q, want %q", reason, item.reason)
			}
			if detail == "" {
				t.Fatal("expected a fixed detail")
			}
		})
	}
}
