package grpcsecurity

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"google.golang.org/grpc/credentials"
)

const (
	ServiceBooking      = "booking"
	ServiceEvent        = "event"
	ServicePayment      = "payment"
	ServiceSaga         = "saga"
	ServiceUser         = "user"
	ServiceNotification = "notification"

	ServiceIdentityGateway      = "spiffe://rushgo.internal/gateway"
	ServiceIdentityBooking      = "spiffe://rushgo.internal/booking"
	ServiceIdentityEvent        = "spiffe://rushgo.internal/event"
	ServiceIdentitySaga         = "spiffe://rushgo.internal/saga"
	ServiceIdentityScheduler    = "spiffe://rushgo.internal/scheduler"
	ServiceIdentityPayment      = "spiffe://rushgo.internal/payment"
	ServiceIdentityNotification = "spiffe://rushgo.internal/notification"
	ServiceIdentityUser         = "spiffe://rushgo.internal/user"

	TestOnlyInsecureEnv = "RUSHGO_GRPC_TEST_ONLY_INSECURE"
	testOnlyInsecureAck = "I_UNDERSTAND_TEST_ONLY"
)

var ErrTestOnlyInsecureUnavailable = errors.New("TEST_ONLY plaintext transport is unavailable in this build")

var serverCallers = map[string][]string{
	ServiceBooking:      {ServiceIdentityGateway, ServiceIdentitySaga, ServiceIdentityScheduler, ServiceIdentityPayment},
	ServiceEvent:        {ServiceIdentityBooking},
	ServiceNotification: {ServiceIdentityGateway},
	ServicePayment:      {ServiceIdentityGateway, ServiceIdentitySaga, ServiceIdentityScheduler, ServiceIdentityBooking},
	ServiceSaga:         {ServiceIdentityBooking},
	ServiceUser:         {ServiceIdentityGateway, ServiceIdentityBooking, ServiceIdentityNotification},
}

func NewServerCredentials(certPEM, keyPEM, clientCAPEM []byte, allowedClientIdentities []string) (credentials.TransportCredentials, error) {
	return newServerCredentials("", certPEM, keyPEM, clientCAPEM, allowedClientIdentities, nil)
}

// NewServerCredentialsWithDiagnostics builds the same server credentials as
// NewServerCredentials and attaches a structured handshake observer. The
// observer only records the existing authorization decision; it cannot change
// the allow-list or the returned TLS error.
func NewServerCredentialsWithDiagnostics(service string, certPEM, keyPEM, clientCAPEM []byte,
	allowedClientIdentities []string, observer HandshakeObserver) (credentials.TransportCredentials, error) {
	return newServerCredentials(service, certPEM, keyPEM, clientCAPEM, allowedClientIdentities, observer)
}

func newServerCredentials(service string, certPEM, keyPEM, clientCAPEM []byte,
	allowedClientIdentities []string, observer HandshakeObserver) (credentials.TransportCredentials, error) {
	if len(allowedClientIdentities) == 0 {
		return nil, errors.New("at least one authorized gRPC client identity is required")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load gRPC server key pair: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("gRPC client CA bundle contains no certificates")
	}
	allowed := make(map[string]struct{}, len(allowedClientIdentities))
	for _, identity := range allowedClientIdentities {
		if !validServiceIdentity(identity) {
			return nil, fmt.Errorf("invalid authorized gRPC client identity %q", identity)
		}
		allowed[identity] = struct{}{}
	}
	config := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		ClientCAs:        clientCAs,
		ClientAuth:       tls.RequireAndVerifyClientCert,
		MinVersion:       tls.VersionTLS12,
		VerifyConnection: func(state tls.ConnectionState) error { return verifyClientIdentity(state, allowed) },
	}
	var diagnostics *serverHandshakeDiagnostics
	if observer != nil {
		diagnostics = newServerHandshakeDiagnostics(service, cert, observer)
		base := config
		config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			diagnostics.begin(hello)
			clone := base.Clone()
			clone.GetConfigForClient = nil
			clone.VerifyConnection = func(state tls.ConnectionState) error {
				err := verifyClientIdentity(state, allowed)
				remote := ""
				if hello != nil && hello.Conn != nil && hello.Conn.RemoteAddr() != nil {
					remote = hello.Conn.RemoteAddr().String()
				}
				diagnostics.recordIdentity(remote, state, err)
				return err
			}
			return clone, nil
		}
	}
	transport := credentials.NewTLS(config)
	if diagnostics != nil {
		return &diagnosticTransportCredentials{inner: transport, diagnostics: diagnostics}, nil
	}
	return transport, nil
}

func verifyClientIdentity(state tls.ConnectionState, allowed map[string]struct{}) error {
	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return errors.New("verified gRPC client certificate required")
	}
	for _, uri := range state.PeerCertificates[0].URIs {
		if _, ok := allowed[uri.String()]; ok {
			return nil
		}
	}
	return errors.New("gRPC client certificate identity is not authorized")
}

func NewClientCredentials(certPEM, keyPEM, rootCAPEM []byte, serverName, clientIdentity string) (credentials.TransportCredentials, error) {
	if strings.TrimSpace(serverName) == "" {
		return nil, errors.New("gRPC target server name is required")
	}
	if !validServiceIdentity(clientIdentity) {
		return nil, errors.New("valid local gRPC service identity is required")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load gRPC client key pair: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return nil, errors.New("gRPC client certificate is missing")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse gRPC client certificate: %w", err)
	}
	cert.Leaf = leaf
	if !hasServiceIdentity(leaf, clientIdentity) {
		return nil, errors.New("gRPC client certificate does not match this service identity")
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(rootCAPEM) {
		return nil, errors.New("gRPC root CA bundle contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

func ServerCredentialsFromEnv(service string) (credentials.TransportCredentials, bool, error) {
	return ServerCredentialsFromEnvWithDiagnostics(service, nil)
}

// ServerCredentialsFromEnvWithDiagnostics is ServerCredentialsFromEnv plus an
// optional handshake observer. A nil observer preserves the exact previous
// behavior; a plaintext test-only environment does not produce TLS events.
func ServerCredentialsFromEnvWithDiagnostics(service string, observer HandshakeObserver) (credentials.TransportCredentials, bool, error) {
	allowed, ok := serverCallers[service]
	if !ok {
		return nil, false, fmt.Errorf("unsupported gRPC mTLS server %q", service)
	}
	localIdentity := serviceIdentityFor(service)
	if testOnly, requested, err := testOnlyCredentialsFromEnv(service, localIdentity); requested || err != nil {
		return testOnly, requested, err
	}
	certPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CERT_FILE")
	if err != nil {
		return nil, false, err
	}
	keyPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_KEY_FILE")
	if err != nil {
		return nil, false, err
	}
	caPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CA_FILE")
	if err != nil {
		return nil, false, err
	}
	creds, err := NewServerCredentialsWithDiagnostics(service, certPEM, keyPEM, caPEM, allowed, observer)
	return creds, false, err
}

func ClientCredentialsFromEnv(target, clientIdentity string) (credentials.TransportCredentials, bool, error) {
	if !validServiceIdentity(clientIdentity) {
		return nil, false, errors.New("valid local gRPC service identity is required")
	}
	if target == ServiceUser && clientIdentity != ServiceIdentityGateway && clientIdentity != ServiceIdentityBooking && clientIdentity != ServiceIdentityNotification {
		return nil, false, errors.New("User gRPC target is restricted to Gateway, Booking, and Notification client identities")
	}
	if target == ServiceNotification && clientIdentity != ServiceIdentityGateway {
		return nil, false, errors.New("Notification gRPC target is restricted to the Gateway client identity")
	}
	if testOnly, requested, err := testOnlyCredentialsFromEnv(target, clientIdentity); requested || err != nil {
		return testOnly, requested, err
	}
	if target != ServiceBooking && target != ServicePayment && target != ServiceUser && target != ServiceNotification {
		return nil, false, fmt.Errorf("unsupported protected gRPC target %q", target)
	}
	certPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_CERT_FILE")
	if err != nil {
		return nil, false, err
	}
	keyPEM, err := readRequiredEnvFile("RUSHGO_GRPC_CLIENT_KEY_FILE")
	if err != nil {
		return nil, false, err
	}
	caPEM, err := readRequiredEnvFile("RUSHGO_GRPC_SERVER_CA_FILE")
	if err != nil {
		return nil, false, err
	}
	serverNameEnv := "RUSHGO_" + strings.ToUpper(target) + "_GRPC_SERVER_NAME"
	serverName := strings.TrimSpace(os.Getenv(serverNameEnv))
	if serverName == "" {
		return nil, false, fmt.Errorf("required gRPC TLS setting %s is empty", serverNameEnv)
	}
	creds, err := NewClientCredentials(certPEM, keyPEM, caPEM, serverName, clientIdentity)
	return creds, false, err
}

func readRequiredEnvFile(name string) ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(name))
	if path == "" {
		return nil, fmt.Errorf("required gRPC TLS setting %s is empty", name)
	}
	if strings.HasSuffix(name, "KEY_FILE") && runtime.GOOS != "windows" {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("%s must be a private regular key file", name)
		}
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read required gRPC TLS file from %s: %w", name, err)
	}
	if len(contents) == 0 {
		return nil, fmt.Errorf("required gRPC TLS file from %s is empty", name)
	}
	return contents, nil
}

func validServiceIdentity(identity string) bool {
	return strings.HasPrefix(identity, "spiffe://rushgo.internal/") && len(strings.TrimPrefix(identity, "spiffe://rushgo.internal/")) > 0 && !strings.Contains(strings.TrimPrefix(identity, "spiffe://rushgo.internal/"), "/")
}

func hasServiceIdentity(cert *x509.Certificate, identity string) bool {
	if cert == nil {
		return false
	}
	for _, uri := range cert.URIs {
		if uri.String() == identity {
			return true
		}
	}
	return false
}

func serviceIdentityFor(service string) string {
	return "spiffe://rushgo.internal/" + service
}

func testOnlyCredentialsFromEnv(target, localIdentity string) (credentials.TransportCredentials, bool, error) {
	setting := strings.TrimSpace(os.Getenv(TestOnlyInsecureEnv))
	if setting == "" {
		return nil, false, nil
	}
	if setting != testOnlyInsecureAck {
		return nil, true, fmt.Errorf("%s must equal %q for the isolated TEST_ONLY exception", TestOnlyInsecureEnv, testOnlyInsecureAck)
	}
	if target != ServiceBooking && target != ServicePayment && target != ServiceUser {
		return nil, true, fmt.Errorf("TEST_ONLY plaintext exception does not permit target %q", target)
	}
	creds, err := testOnlyCredentials(target, localIdentity)
	return creds, true, err
}
