// Package originalissuance contains guards for the single approved TEST order.
// It is used only by explicit recovery commands, never ordinary service startup.
package originalissuance

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/grpcsecurity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const Run = "4b7a98d6-35d5-4fa3-ad46-8ed131621a47"
const Booking = "003217d4-d350-46b4-81df-7988b42a04ad"
const Payment = "5431a3c2-d2d0-4710-97e7-a00a9f4a182e"
const Event = "a1777e72-d86b-4e60-b1ac-ea6163a1bb2c"
const Intent = "pi_3UXwxSD3fWINmaYl2s0bueSl"
const Account = "acct_1UN55BD3fWINmaYl"

// BuildSource is pinned by package.py even on toolchains without VCS build info.
var BuildSource = "unbound"

// Open permits only the fixed original-run database identity over a Unix socket.
// The runner separately proves whether storage is a snapshot copy or original.
func Open(ctx context.Context, service string, readonly bool) (*pgxpool.Pool, error) {
	if os.Getenv("RUSHGO_ORIGINAL_ISSUANCE_RUN") != Run {
		return nil, errors.New("original_run_required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, errors.New("database_config_invalid")
	}
	want := "r4_" + service + "_4b7a98d635d5"
	if !filepath.IsAbs(cfg.ConnConfig.Host) || cfg.ConnConfig.User != want || cfg.ConnConfig.Database != want || cfg.ConnConfig.Port != 5432 {
		return nil, errors.New("database_scope_mismatch")
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	if readonly {
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database_open_failed")
	}
	var name, user, ro string
	var super bool
	err = pool.QueryRow(ctx, `SELECT current_database(),current_user,current_setting('transaction_read_only'),rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&name, &user, &ro, &super)
	if err != nil || name != want || user != want || super || (readonly && ro != "on") {
		pool.Close()
		return nil, errors.New("database_runtime_identity_failed")
	}
	return pool, nil
}

// Serve exposes exactly one existing authority method, over the original mTLS
// policy, on loopback only. No worker, broker, webhook, or mutation RPC starts.
func Serve(ctx context.Context, service string, desc grpc.ServiceDesc, impl any, method string) error {
	var creds credentials.TransportCredentials
	var err error
	if service == grpcsecurity.ServicePayment {
		var test bool
		creds, test, err = grpcsecurity.ServerCredentialsFromEnv(service)
		if test {
			return errors.New("plaintext_forbidden")
		}
	} else {
		creds, err = grpcsecurity.TicketAuthorityServerCredentialsFromEnv(service)
	}
	if err != nil {
		return errors.New("authority_tls_config_failed")
	}
	server := grpc.NewServer(grpc.Creds(creds), grpc.ChainUnaryInterceptor(grpcsecurity.TicketAuthorityMethodAuthorizationInterceptor(method), TargetOnly))
	if err = grpcsecurity.RegisterServiceWithOnlyMethod(server, desc, impl, method); err != nil {
		return errors.New("authority_registration_failed")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return errors.New("authority_listener_failed")
	}
	defer listener.Close()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "READY", "address": listener.Addr().String(), "service": service, "sourceCommit": BuildSource})
	go func() { <-ctx.Done(); server.Stop() }()
	err = server.Serve(listener)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func TargetOnly(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	b, ok := req.(interface{ GetBookingId() string })
	if !ok || b.GetBookingId() != Booking {
		return nil, status.Error(codes.PermissionDenied, "original booking required")
	}
	g, ok := req.(interface{ GetGeneration() int64 })
	if !ok || g.GetGeneration() != 1 {
		return nil, status.Error(codes.PermissionDenied, "original generation required")
	}
	if p, ok := req.(interface{ GetPaymentId() string }); ok && p.GetPaymentId() != Payment {
		return nil, status.Error(codes.PermissionDenied, "original payment required")
	}
	if e, ok := req.(interface{ GetEventId() string }); ok && e.GetEventId() != Event {
		return nil, status.Error(codes.PermissionDenied, "original event required")
	}
	return next(ctx, req)
}

func Context() (context.Context, context.CancelFunc) {
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(parent, 120*time.Second)
	return ctx, func() { cancel(); stop() }
}

func Main(run func(context.Context) error) {
	ctx, cancel := Context()
	defer cancel()
	if err := run(ctx); err != nil {
		// Commands must supply fixed categories; never expose a raw pgx/gRPC error.
		reason := "authority_start_or_run_failed"
		if regexp.MustCompile(`^[a-z_]{1,80}$`).MatchString(err.Error()) {
			reason = err.Error()
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "STOP", "reason": reason})
		os.Exit(1)
	}
}
