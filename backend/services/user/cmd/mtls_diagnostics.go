package main

import (
	"go.uber.org/zap"

	"github.com/ticketbox/pkg/grpcsecurity"
)

// userHandshakeObserver emits one structured, secret-free record per TLS
// handshake phase. It never changes the authorization result: the observer is
// called after the existing decision has already been made.
func userHandshakeObserver(logger *zap.Logger) grpcsecurity.HandshakeObserver {
	return grpcsecurity.HandshakeObserverFunc(func(event grpcsecurity.HandshakeEvent) {
		fields := []zap.Field{
			zap.String("stage", event.Stage),
			zap.String("reason_code", event.ReasonCode),
			zap.String("service", event.Service),
			zap.String("run_id", event.RunID),
			zap.String("attempt_id", event.AttemptID),
			zap.String("remote_addr", event.RemoteAddr),
			zap.String("local_addr", event.LocalAddr),
			zap.Strings("offered_tls_versions", event.OfferedTLSVersions),
			zap.String("tls_version", event.TLSVersion),
			zap.String("alpn", event.ALPN),
			zap.String("client_identity", event.ClientIdentity),
			zap.Bool("identity_verified", event.IdentityVerified),
			zap.String("client_cert_sha256", event.ClientCertSHA256),
			zap.String("server_cert_sha256", event.ServerCertSHA256),
			zap.String("error_detail", event.ErrorDetail),
			zap.String("server_build_version", event.ServerBuildVersion),
			zap.String("server_vcs_revision", event.ServerVCSRevision),
			zap.Bool("server_vcs_modified", event.ServerVCSModified),
			zap.String("process_start_utc", event.ProcessStartUTC),
			zap.Int("process_id", event.ProcessID),
			zap.String("hostname", event.Hostname),
		}
		if event.Stage == grpcsecurity.HandshakeStageResult &&
			event.ReasonCode != grpcsecurity.HandshakeReasonCompleted {
			logger.Warn("gRPC mTLS handshake rejected", fields...)
			return
		}
		logger.Info("gRPC mTLS handshake observed", fields...)
	})
}
