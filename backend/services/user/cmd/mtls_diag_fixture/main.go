// Command mtls_diag_fixture is a loopback-only local diagnostic fixture.
//
// It reuses the production User gRPC TLS constructor, method authorization
// interceptor and service registration, but deliberately does not start the
// User database, Kafka or recovery queue. It is not the production User
// service binary and must never be deployed. It exists so a local client can
// correlate a rejected Payment leaf with the server's own TLS identity-check
// event, without weakening certificate or role policy.
package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/ticketbox/pkg/grpcsecurity"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
)

const fixtureAck = "I_UNDERSTAND_LOCAL_ONLY"

var outputMu sync.Mutex

type startupRecord struct {
	Event            string `json:"event"`
	Port             int    `json:"port"`
	PID              int    `json:"pid"`
	ProcessStartUTC  string `json:"processStartUtc"`
	RunID            string `json:"runId"`
	ServerCertSHA256 string `json:"serverCertSha256"`
	BuildVersion     string `json:"buildVersion,omitempty"`
	VCSRevision      string `json:"vcsRevision,omitempty"`
	VCSModified      bool   `json:"vcsModified"`
}

type handshakeRecord struct {
	Event string `json:"event"`
	grpcsecurity.HandshakeEvent
}

func writeJSON(value any) {
	outputMu.Lock()
	defer outputMu.Unlock()
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode diagnostic record:", err)
		return
	}
	fmt.Println(string(encoded))
}

func certificateFingerprint(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(contents)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("server certificate file has no certificate")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return "", err
	}
	sum := sha256.Sum256(block.Bytes)
	return hex.EncodeToString(sum[:]), nil
}

func main() {
	if os.Getenv("RUSHGO_MTLS_DIAG_FIXTURE") != fixtureAck {
		fmt.Fprintln(os.Stderr, "refusing to run: local diagnostic fixture acknowledgement missing")
		os.Exit(2)
	}
	runID := os.Getenv("RUSHGO_MTLS_DIAG_RUN_ID")
	if runID == "" {
		fmt.Fprintln(os.Stderr, "refusing to run: RUSHGO_MTLS_DIAG_RUN_ID is required")
		os.Exit(2)
	}
	observer := grpcsecurity.HandshakeObserverFunc(func(event grpcsecurity.HandshakeEvent) {
		writeJSON(handshakeRecord{Event: "grpc_handshake", HandshakeEvent: event})
	})
	transport, testOnly, err := grpcsecurity.ServerCredentialsFromEnvWithDiagnostics(
		grpcsecurity.ServiceUser, observer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "transport initialization refused:", err)
		os.Exit(2)
	}
	if testOnly {
		fmt.Fprintln(os.Stderr, "refusing to run: plaintext transport is not accepted")
		os.Exit(2)
	}
	certPath := os.Getenv("RUSHGO_GRPC_SERVER_CERT_FILE")
	certFingerprint, err := certificateFingerprint(certPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server certificate inspection failed:", err)
		os.Exit(2)
	}
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "0"
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loopback listen failed:", err)
		os.Exit(2)
	}
	server := grpc.NewServer(
		grpc.Creds(transport),
		grpc.ChainUnaryInterceptor(grpcsecurity.UserServiceMethodAuthorizationInterceptor()),
		grpc.ChainStreamInterceptor(grpcsecurity.UserServiceStreamAuthorizationInterceptor()),
	)
	userv1.RegisterUserServiceServer(server, userv1.UnimplementedUserServiceServer{})
	started := time.Now().UTC()
	writeJSON(startupRecord{
		Event:            "fixture_started",
		Port:             listener.Addr().(*net.TCPAddr).Port,
		PID:              os.Getpid(),
		ProcessStartUTC:  started.Format(time.RFC3339Nano),
		RunID:            runID,
		ServerCertSHA256: certFingerprint,
	})
	go func() {
		if err := server.Serve(listener); err != nil {
			fmt.Fprintln(os.Stderr, "grpc serve stopped:", err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	server.GracefulStop()
	_ = listener.Close()
}
