package grpcsecurity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testServerName = "booking.internal"

type testPKI struct {
	caPEM []byte
	ca    *x509.Certificate
	caKey *ecdsa.PrivateKey
}

func newTestPKI(t *testing.T, commonName string) testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testPKI{caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca: cert, caKey: key}
}

func (p testPKI) leaf(t *testing.T, commonName, dnsName, uriIdentity string, notBefore, notAfter time.Time, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		DNSNames:              []string{},
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
	}
	if dnsName != "" {
		tmpl.DNSNames = []string{dnsName}
	}
	if uriIdentity != "" {
		parsed, err := url.Parse(uriIdentity)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{parsed}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func validWindow() (time.Time, time.Time) {
	now := time.Now()
	return now.Add(-time.Minute), now.Add(time.Hour)
}

func testRPC(t *testing.T, serverCred credentials.TransportCredentials, clientCred credentials.TransportCredentials) error {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.Creds(serverCred))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.NewClient("passthrough:///secure-test", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(clientCred))
	if err != nil {
		return err
	}
	defer conn.Close()
	var response grpc_health_v1.HealthCheckResponse
	return conn.Invoke(ctx, "/grpc.health.v1.Health/Check", &emptypb.Empty{}, &response)
}

func TestMutualTLSRequiresTrustedClientAndServerIdentity(t *testing.T) {
	ca := newTestPKI(t, "RushGo test CA")
	wrongCA := newTestPKI(t, "Untrusted test CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "booking", testServerName, "", notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	serverCred, err := NewServerCredentials(serverCert, serverKey, ca.caPEM, []string{ServiceIdentityGateway})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("valid mutual TLS", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, testServerName, ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err != nil {
			t.Fatalf("authorized mTLS call failed: %v", err)
		}
	})
	t.Run("plaintext rejected", func(t *testing.T) {
		if err := testRPC(t, serverCred, insecure.NewCredentials()); err == nil {
			t.Fatal("plaintext RPC unexpectedly succeeded")
		}
	})
	t.Run("client cert signed by wrong CA rejected", func(t *testing.T) {
		clientCert, clientKey := wrongCA.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, testServerName, ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("client cert signed by an untrusted CA unexpectedly succeeded")
		}
	})
	t.Run("expired client cert rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, testServerName, ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("expired client cert unexpectedly succeeded")
		}
	})
	t.Run("missing client cert rejected", func(t *testing.T) {
		rootCAs := x509.NewCertPool()
		rootCAs.AppendCertsFromPEM(ca.caPEM)
		clientCred := credentials.NewTLS(&tls.Config{RootCAs: rootCAs, ServerName: testServerName, MinVersion: tls.VersionTLS12})
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("mTLS server accepted a missing client cert")
		}
	})
	t.Run("unapproved service identity rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "unknown", "", "spiffe://rushgo.internal/unknown", notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, testServerName, "spiffe://rushgo.internal/unknown")
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("trusted but unauthorized service identity unexpectedly succeeded")
		}
	})
	t.Run("wrong server name rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, "wrong.internal", ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("wrong server name unexpectedly succeeded")
		}
	})
}

func TestUserServiceMutualTLSAuthorizesOnlyGatewayAndBooking(t *testing.T) {
	ca := newTestPKI(t, "RushGo User service test CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "user-service", "user-service", "spiffe://rushgo.internal/user", notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	serverCred, err := NewServerCredentials(serverCert, serverKey, ca.caPEM, serverCallers["user"])
	if err != nil {
		t.Fatal(err)
	}

	for _, identity := range []string{ServiceIdentityGateway, ServiceIdentityBooking} {
		t.Run(identity, func(t *testing.T) {
			clientCert, clientKey := ca.leaf(t, "authorized caller", "", identity, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
			clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, "user-service", identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := testRPC(t, serverCred, clientCred); err != nil {
				t.Fatalf("authorized User service mTLS call failed: %v", err)
			}
		})
	}

	t.Run("plaintext rejected", func(t *testing.T) {
		if err := testRPC(t, serverCred, insecure.NewCredentials()); err == nil {
			t.Fatal("User service accepted a plaintext RPC")
		}
	})
	t.Run("other trusted service identity rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "saga", "", ServiceIdentitySaga, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, "user-service", ServiceIdentitySaga)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("User service accepted a caller outside its Gateway/Booking allowlist")
		}
	})
	t.Run("wrong user server SAN rejected", func(t *testing.T) {
		clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
		clientCred, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, "booking-service", ServiceIdentityGateway)
		if err != nil {
			t.Fatal(err)
		}
		if err := testRPC(t, serverCred, clientCred); err == nil {
			t.Fatal("client accepted the User service certificate under the wrong server name")
		}
	})
}

func TestUserServiceCredentialsLoadFromEnvironment(t *testing.T) {
	ca := newTestPKI(t, "RushGo User config test CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "user-service", "user-service", "spiffe://rushgo.internal/user", notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	notificationCert, notificationKey := ca.leaf(t, "notification", "", ServiceIdentityNotification, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	write := func(name string, value []byte) string {
		t.Helper()
		path := t.TempDir() + "/" + name
		if err := os.WriteFile(path, value, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Setenv(TestOnlyInsecureEnv, "")
	t.Setenv("RUSHGO_GRPC_SERVER_CERT_FILE", write("user-server-cert.pem", serverCert))
	t.Setenv("RUSHGO_GRPC_SERVER_KEY_FILE", write("user-server-key.pem", serverKey))
	t.Setenv("RUSHGO_GRPC_CLIENT_CA_FILE", write("client-ca.pem", ca.caPEM))
	serverCred, testOnly, err := ServerCredentialsFromEnv("user")
	if err != nil || testOnly || serverCred == nil {
		t.Fatalf("User server credentials = (%v, %t, %v), want configured TLS credentials", serverCred, testOnly, err)
	}
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", write("gateway-client-cert.pem", clientCert))
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", write("gateway-client-key.pem", clientKey))
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", write("server-ca.pem", ca.caPEM))
	t.Setenv("RUSHGO_USER_GRPC_SERVER_NAME", "user-service")
	clientCred, testOnly, err := ClientCredentialsFromEnv("user", ServiceIdentityGateway)
	if err != nil || testOnly || clientCred == nil {
		t.Fatalf("Gateway→User credentials = (%v, %t, %v), want configured TLS credentials", clientCred, testOnly, err)
	}
	if err := testRPC(t, serverCred, clientCred); err != nil {
		t.Fatalf("configured Gateway→User mTLS handshake failed: %v", err)
	}
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", write("notification-client-cert.pem", notificationCert))
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", write("notification-client-key.pem", notificationKey))
	notificationClientCred, testOnly, err := ClientCredentialsFromEnv(ServiceUser, ServiceIdentityNotification)
	if err != nil || testOnly || notificationClientCred == nil {
		t.Fatalf("Notification→User credentials = (%v, %t, %v), want configured TLS credentials", notificationClientCred, testOnly, err)
	}
	if err := testRPC(t, serverCred, notificationClientCred); err != nil {
		t.Fatalf("configured Notification→User mTLS handshake failed: %v", err)
	}
	if _, _, err := ClientCredentialsFromEnv("user", ServiceIdentitySaga); err == nil {
		t.Fatal("User client credentials were issued to a caller outside the Gateway/Booking/Notification scope")
	}
}

func TestNotificationServiceCredentialsLoadFromEnvironment(t *testing.T) {
	ca := newTestPKI(t, "RushGo Notification config test CA")
	notBefore, notAfter := validWindow()
	serverCert, serverKey := ca.leaf(t, "notification-service", "notification-service", ServiceIdentityNotification, notBefore, notAfter, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := ca.leaf(t, "gateway", "", ServiceIdentityGateway, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	write := func(name string, value []byte) string {
		t.Helper()
		path := t.TempDir() + "/" + name
		if err := os.WriteFile(path, value, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Setenv(TestOnlyInsecureEnv, "")
	t.Setenv("RUSHGO_GRPC_SERVER_CERT_FILE", write("notification-server-cert.pem", serverCert))
	t.Setenv("RUSHGO_GRPC_SERVER_KEY_FILE", write("notification-server-key.pem", serverKey))
	t.Setenv("RUSHGO_GRPC_CLIENT_CA_FILE", write("client-ca.pem", ca.caPEM))
	serverCred, testOnly, err := ServerCredentialsFromEnv(ServiceNotification)
	if err != nil || testOnly || serverCred == nil {
		t.Fatalf("Notification server credentials = (%v, %t, %v), want configured TLS credentials", serverCred, testOnly, err)
	}
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", write("gateway-client-cert.pem", clientCert))
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", write("gateway-client-key.pem", clientKey))
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", write("server-ca.pem", ca.caPEM))
	t.Setenv("RUSHGO_NOTIFICATION_GRPC_SERVER_NAME", "notification-service")
	clientCred, testOnly, err := ClientCredentialsFromEnv(ServiceNotification, ServiceIdentityGateway)
	if err != nil || testOnly || clientCred == nil {
		t.Fatalf("Gateway→Notification credentials = (%v, %t, %v), want configured TLS credentials", clientCred, testOnly, err)
	}
	if err := testRPC(t, serverCred, clientCred); err != nil {
		t.Fatalf("configured Gateway→Notification mTLS handshake failed: %v", err)
	}
	if _, _, err := ClientCredentialsFromEnv(ServiceNotification, ServiceIdentityBooking); err == nil {
		t.Fatal("Notification client credentials were issued to a non-Gateway identity")
	}
}

func TestClientCredentialsRejectsIdentityMismatch(t *testing.T) {
	ca := newTestPKI(t, "RushGo test CA")
	notBefore, notAfter := validWindow()
	clientCert, clientKey := ca.leaf(t, "saga", "", ServiceIdentitySaga, notBefore, notAfter, x509.ExtKeyUsageClientAuth)
	if _, err := NewClientCredentials(clientCert, clientKey, ca.caPEM, testServerName, ServiceIdentityGateway); err == nil {
		t.Fatal("client certificate with a different service URI unexpectedly loaded")
	}
}

func TestSecureCredentialsFailClosedOnMissingConfiguration(t *testing.T) {
	t.Setenv("RUSHGO_GRPC_TEST_ONLY_INSECURE", "I_UNDERSTAND_TEST_ONLY")
	t.Setenv("RUSHGO_GRPC_CLIENT_CERT_FILE", "")
	t.Setenv("RUSHGO_GRPC_CLIENT_KEY_FILE", "")
	t.Setenv("RUSHGO_GRPC_SERVER_CA_FILE", "")
	t.Setenv("TICKET_E2E_MODE", "")
	t.Setenv("TICKET_E2E_PROJECT", "")
	t.Setenv("TICKET_E2E_SOURCE_HEAD", "")
	t.Setenv("TICKET_E2E_SOURCE_FINGERPRINT", "")
	t.Setenv("SERVICE_NAME", "")
	t.Setenv("RUSHGO_BOOKING_GRPC_SERVER_NAME", "")
	t.Setenv("RUSHGO_USER_GRPC_SERVER_NAME", "")
	if _, _, err := ClientCredentialsFromEnv(ServiceBooking, ServiceIdentityGateway); err == nil {
		t.Fatal("production build unexpectedly accepted missing secure transport configuration")
	}
	if _, _, err := ClientCredentialsFromEnv(ServiceUser, ServiceIdentityGateway); err == nil {
		t.Fatal("production build unexpectedly accepted a User client without TLS material")
	}
	if _, _, err := ServerCredentialsFromEnv(ServiceBooking); err == nil {
		t.Fatal("production build unexpectedly accepted a server without TLS material")
	}
	if _, _, err := ServerCredentialsFromEnv(ServiceUser); err == nil {
		t.Fatal("production build unexpectedly accepted a User server without TLS material")
	}
	if _, _, err := ClientCredentialsFromEnv(ServiceNotification, ServiceIdentityGateway); err == nil {
		t.Fatal("Notification client unexpectedly accepted the TEST_ONLY plaintext exception")
	}
	if _, _, err := ServerCredentialsFromEnv(ServiceNotification); err == nil {
		t.Fatal("Notification server unexpectedly accepted the TEST_ONLY plaintext exception")
	}
}
