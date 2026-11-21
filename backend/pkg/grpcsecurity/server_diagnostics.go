package grpcsecurity

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"
)

// Handshake diagnostic stages and fixed reason codes. These values are
// observations only: the TLS authorization result is never changed by them.
const (
	HandshakeStageClientHello   = "CLIENT_HELLO"
	HandshakeStageIdentityCheck = "IDENTITY_CHECK"
	HandshakeStageResult        = "HANDSHAKE_RESULT"

	HandshakeReasonCompleted             = "HANDSHAKE_COMPLETED"
	HandshakeReasonIdentityVerified      = "IDENTITY_VERIFIED"
	HandshakeReasonIdentityNotAllowed    = "CLIENT_IDENTITY_NOT_ALLOWED"
	HandshakeReasonClientCertMissing     = "CLIENT_CERTIFICATE_MISSING"
	HandshakeReasonClientCertUnverified  = "CLIENT_CERTIFICATE_NOT_VERIFIED"
	HandshakeReasonChainUnknownAuthority = "CERTIFICATE_CHAIN_UNKNOWN_AUTHORITY"
	HandshakeReasonCertificateInvalid    = "CERTIFICATE_INVALID"
	HandshakeReasonTransportFailure      = "TRANSPORT_FAILURE"
	HandshakeReasonFailedUnclassified    = "HANDSHAKE_FAILED_UNCLASSIFIED"
)

// HandshakeEvent is one structured, secret-free observation from the server
// side of a TLS handshake. ClientIdentity is populated only when the chain was
// actually verified; unverified names are never reported as authenticated.
type HandshakeEvent struct {
	Stage              string   `json:"stage"`
	ReasonCode         string   `json:"reasonCode"`
	Service            string   `json:"service"`
	RunID              string   `json:"runId,omitempty"`
	AttemptID          string   `json:"attemptId"`
	TimeUTC            string   `json:"timeUtc"`
	RemoteAddr         string   `json:"remoteAddr"`
	LocalAddr          string   `json:"localAddr"`
	OfferedTLSVersions []string `json:"offeredTlsVersions,omitempty"`
	TLSVersion         string   `json:"tlsVersion,omitempty"`
	ALPN               string   `json:"alpn,omitempty"`
	ClientIdentity     string   `json:"clientIdentity,omitempty"`
	IdentityVerified   bool     `json:"identityVerified"`
	ClientCertSHA256   string   `json:"clientCertSha256,omitempty"`
	ServerCertSHA256   string   `json:"serverCertSha256"`
	ErrorDetail        string   `json:"errorDetail,omitempty"`
	ServerBuildVersion string   `json:"serverBuildVersion,omitempty"`
	ServerVCSRevision  string   `json:"serverVcsRevision,omitempty"`
	ServerVCSModified  bool     `json:"serverVcsModified"`
	ProcessStartUTC    string   `json:"processStartUtc"`
	ProcessID          int      `json:"processId"`
	Hostname           string   `json:"hostname,omitempty"`
}

// HandshakeObserver receives handshake observations synchronously. It must not
// retain or block on the connection; production adapters should write one
// bounded structured log record and return.
type HandshakeObserver interface {
	ObserveServerHandshake(HandshakeEvent)
}

// HandshakeObserverFunc adapts a function to HandshakeObserver.
type HandshakeObserverFunc func(HandshakeEvent)

func (f HandshakeObserverFunc) ObserveServerHandshake(event HandshakeEvent) {
	if f != nil {
		f(event)
	}
}

type handshakeAttempt struct {
	id               string
	remote           string
	local            string
	offered          []string
	identity         string
	identityVerified bool
	identityErr      error
	clientCertSHA256 string
	tlsVersion       string
	alpn             string
}

type serverHandshakeDiagnostics struct {
	service          string
	observer         HandshakeObserver
	runID            string
	serverCertSHA256 string
	processStartUTC  string
	processID        int
	hostname         string
	buildVersion     string
	vcsRevision      string
	vcsModified      bool
	sequence         atomic.Uint64
	mu               sync.Mutex
	attempts         map[string]*handshakeAttempt
}

var serverProcessStartedUTC = time.Now().UTC()

func serverBuildIdentity() (version, revision string, modified bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", "", false
	}
	version = info.Main.Version
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return version, revision, modified
}

func newServerHandshakeDiagnostics(service string, serverCert tls.Certificate,
	observer HandshakeObserver) *serverHandshakeDiagnostics {
	hostname, _ := os.Hostname()
	version, revision, modified := serverBuildIdentity()
	fingerprint := ""
	if len(serverCert.Certificate) > 0 {
		sum := sha256.Sum256(serverCert.Certificate[0])
		fingerprint = hex.EncodeToString(sum[:])
	}
	return &serverHandshakeDiagnostics{
		service:          service,
		observer:         observer,
		runID:            strings.TrimSpace(os.Getenv("RUSHGO_MTLS_DIAG_RUN_ID")),
		serverCertSHA256: fingerprint,
		processStartUTC:  serverProcessStartedUTC.Format(time.RFC3339Nano),
		processID:        os.Getpid(),
		hostname:         hostname,
		buildVersion:     version,
		vcsRevision:      revision,
		vcsModified:      modified,
		attempts:         make(map[string]*handshakeAttempt),
	}
}

func (d *serverHandshakeDiagnostics) begin(hello *tls.ClientHelloInfo) *handshakeAttempt {
	remote, local := "", ""
	if hello != nil && hello.Conn != nil {
		if hello.Conn.RemoteAddr() != nil {
			remote = hello.Conn.RemoteAddr().String()
		}
		if hello.Conn.LocalAddr() != nil {
			local = hello.Conn.LocalAddr().String()
		}
	}
	attempt := &handshakeAttempt{
		id:      fmt.Sprintf("%s-%d", d.processStartUTC, d.sequence.Add(1)),
		remote:  remote,
		local:   local,
		offered: tlsVersionNames(helloSupportedVersions(hello)),
	}
	d.mu.Lock()
	d.attempts[remote] = attempt
	d.mu.Unlock()
	return attempt
}

func (d *serverHandshakeDiagnostics) ensureAttempt(remote, local string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.attempts[remote]; !ok {
		d.attempts[remote] = &handshakeAttempt{
			id:     fmt.Sprintf("%s-%d", d.processStartUTC, d.sequence.Add(1)),
			remote: remote, local: local,
		}
	}
}

func (d *serverHandshakeDiagnostics) recordIdentity(remote string, state tls.ConnectionState, err error) {
	var event HandshakeEvent
	d.mu.Lock()
	attempt := d.attempts[remote]
	if attempt == nil {
		attempt = &handshakeAttempt{
			id:     fmt.Sprintf("%s-%d", d.processStartUTC, d.sequence.Add(1)),
			remote: remote,
		}
		d.attempts[remote] = attempt
	}
	if len(state.PeerCertificates) > 0 {
		sum := sha256.Sum256(state.PeerCertificates[0].Raw)
		attempt.clientCertSHA256 = hex.EncodeToString(sum[:])
	}
	if len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 {
		attempt.identityVerified = true
		attempt.identity = firstServiceIdentity(state.VerifiedChains[0][0])
	}
	if state.Version != 0 {
		attempt.tlsVersion = tlsVersionName(state.Version)
	}
	if state.NegotiatedProtocol != "" {
		attempt.alpn = state.NegotiatedProtocol
	}
	attempt.identityErr = err
	reason := HandshakeReasonIdentityVerified
	if err != nil {
		reason = HandshakeReasonIdentityNotAllowed
	}
	event = d.eventFor(attempt, HandshakeStageIdentityCheck, reason, err)
	d.mu.Unlock()
	d.observe(event)
}

func (d *serverHandshakeDiagnostics) finish(remote string, info credentials.AuthInfo, err error) {
	var event HandshakeEvent
	d.mu.Lock()
	attempt := d.attempts[remote]
	if attempt == nil {
		attempt = &handshakeAttempt{
			id:     fmt.Sprintf("%s-%d", d.processStartUTC, d.sequence.Add(1)),
			remote: remote,
		}
	}
	delete(d.attempts, remote)
	if err == nil {
		if tlsInfo, ok := info.(credentials.TLSInfo); ok {
			state := tlsInfo.State
			if len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 {
				attempt.identityVerified = true
				attempt.identity = firstServiceIdentity(state.VerifiedChains[0][0])
			}
			if len(state.PeerCertificates) > 0 {
				sum := sha256.Sum256(state.PeerCertificates[0].Raw)
				attempt.clientCertSHA256 = hex.EncodeToString(sum[:])
			}
			attempt.tlsVersion = tlsVersionName(state.Version)
			attempt.alpn = state.NegotiatedProtocol
		}
		event = d.eventFor(attempt, HandshakeStageResult, HandshakeReasonCompleted, nil)
	} else {
		reason, detail := classifyServerHandshakeError(err)
		if attempt.identityErr != nil {
			reason = HandshakeReasonIdentityNotAllowed
			detail = "identity_not_in_allowed_list"
		}
		event = d.eventFor(attempt, HandshakeStageResult, reason, err)
		if detail != "" && event.ErrorDetail == "" {
			event.ErrorDetail = detail
		}
	}
	d.mu.Unlock()
	d.observe(event)
}

func (d *serverHandshakeDiagnostics) eventFor(attempt *handshakeAttempt, stage, reason string,
	err error) HandshakeEvent {
	event := HandshakeEvent{
		Stage:              stage,
		ReasonCode:         reason,
		Service:            d.service,
		RunID:              d.runID,
		AttemptID:          attempt.id,
		TimeUTC:            time.Now().UTC().Format(time.RFC3339Nano),
		RemoteAddr:         attempt.remote,
		LocalAddr:          attempt.local,
		OfferedTLSVersions: append([]string(nil), attempt.offered...),
		TLSVersion:         attempt.tlsVersion,
		ALPN:               attempt.alpn,
		ClientIdentity:     attempt.identity,
		IdentityVerified:   attempt.identityVerified,
		ClientCertSHA256:   attempt.clientCertSHA256,
		ServerCertSHA256:   d.serverCertSHA256,
		ServerBuildVersion: d.buildVersion,
		ServerVCSRevision:  d.vcsRevision,
		ServerVCSModified:  d.vcsModified,
		ProcessStartUTC:    d.processStartUTC,
		ProcessID:          d.processID,
		Hostname:           d.hostname,
	}
	if err != nil {
		event.ErrorDetail = sanitizeHandshakeDetail(err.Error())
	}
	return event
}

func (d *serverHandshakeDiagnostics) observe(event HandshakeEvent) {
	if d.observer != nil {
		d.observer.ObserveServerHandshake(event)
	}
}

func firstServiceIdentity(certificate *x509.Certificate) string {
	if certificate == nil {
		return ""
	}
	for _, uri := range certificate.URIs {
		if strings.HasPrefix(uri.String(), "spiffe://rushgo.internal/") {
			return uri.String()
		}
	}
	if len(certificate.URIs) > 0 {
		return certificate.URIs[0].String()
	}
	return ""
}

func helloSupportedVersions(hello *tls.ClientHelloInfo) []uint16 {
	if hello == nil {
		return nil
	}
	return hello.SupportedVersions
}

func tlsVersionNames(versions []uint16) []string {
	names := make([]string, 0, len(versions))
	for _, version := range versions {
		if name := tlsVersionName(version); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	default:
		return ""
	}
}

func sanitizeHandshakeDetail(detail string) string {
	detail = strings.ReplaceAll(detail, "\n", " ")
	detail = strings.ReplaceAll(detail, "\r", " ")
	if len(detail) > 200 {
		detail = detail[:200]
	}
	return detail
}

// classifyServerHandshakeError maps a TLS server error to fixed reason codes.
// It deliberately does not guess an identity that was never verified.
func classifyServerHandshakeError(err error) (string, string) {
	if err == nil {
		return HandshakeReasonCompleted, ""
	}
	var verification *tls.CertificateVerificationError
	if errors.As(err, &verification) {
		err = verification.Err
	}
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return HandshakeReasonChainUnknownAuthority, "unknown_authority"
	}
	var invalid x509.CertificateInvalidError
	if errors.As(err, &invalid) {
		detail := "certificate_invalid"
		switch invalid.Reason {
		case x509.Expired:
			detail = "expired_or_not_yet_valid"
		case x509.IncompatibleUsage:
			detail = "incompatible_extended_key_usage"
		case x509.NameConstraintsWithoutSANs:
			detail = "name_constraints_without_sans"
		case x509.UnconstrainedName:
			detail = "unconstrained_name"
		case x509.TooManyConstraints:
			detail = "too_many_constraints"
		case x509.CANotAuthorizedForThisName:
			detail = "ca_not_authorized_for_name"
		}
		return HandshakeReasonCertificateInvalid, detail
	}
	var hostname x509.HostnameError
	if errors.As(err, &hostname) {
		return HandshakeReasonCertificateInvalid, "hostname_mismatch"
	}
	if strings.Contains(err.Error(), "client didn't provide a certificate") {
		return HandshakeReasonClientCertMissing, "client_certificate_missing"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return HandshakeReasonTransportFailure, "connection_closed"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return HandshakeReasonTransportFailure, "deadline"
	}
	return HandshakeReasonFailedUnclassified, sanitizeHandshakeDetail(err.Error())
}

type diagnosticTransportCredentials struct {
	inner       credentials.TransportCredentials
	diagnostics *serverHandshakeDiagnostics
}

func (c *diagnosticTransportCredentials) ClientHandshake(ctx context.Context, authority string,
	rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.inner.ClientHandshake(ctx, authority, rawConn)
}

func (c *diagnosticTransportCredentials) ServerHandshake(rawConn net.Conn) (net.Conn,
	credentials.AuthInfo, error) {
	remote, local := "", ""
	if rawConn.RemoteAddr() != nil {
		remote = rawConn.RemoteAddr().String()
	}
	if rawConn.LocalAddr() != nil {
		local = rawConn.LocalAddr().String()
	}
	c.diagnostics.ensureAttempt(remote, local)
	conn, info, err := c.inner.ServerHandshake(rawConn)
	c.diagnostics.finish(remote, info, err)
	return conn, info, err
}

func (c *diagnosticTransportCredentials) Info() credentials.ProtocolInfo {
	return c.inner.Info()
}

func (c *diagnosticTransportCredentials) Clone() credentials.TransportCredentials {
	return &diagnosticTransportCredentials{inner: c.inner.Clone(), diagnostics: c.diagnostics}
}

func (c *diagnosticTransportCredentials) OverrideServerName(serverNameOverride string) error {
	return c.inner.OverrideServerName(serverNameOverride)
}
