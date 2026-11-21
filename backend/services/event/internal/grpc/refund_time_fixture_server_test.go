package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/pkg/grpcsecurity"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type refundTimeFixtureConfig struct {
	ServerCert string `json:"server_cert"`
	ServerKey  string `json:"server_key"`
	ClientCA   string `json:"client_ca"`
	ReadyFile  string `json:"ready_file"`
	StopFile   string `json:"stop_file"`
}

type refundTimeFixtureReady struct {
	Address string `json:"address"`
	Schema  string `json:"schema"`
}

func TestRefundTimeFixtureServer(t *testing.T) {
	configPath := strings.TrimSpace(os.Getenv("REFUND_TIME_FIXTURE_CONFIG"))
	if configPath == "" {
		t.Skip("REFUND_TIME_FIXTURE_CONFIG is not set; coordination fixture server is opt-in")
	}
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("read Event refund fixture configuration")
	}
	var config refundTimeFixtureConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		t.Fatal("parse Event refund fixture configuration")
	}
	if config.ServerCert == "" || config.ServerKey == "" || config.ClientCA == "" || config.ReadyFile == "" || config.StopFile == "" {
		t.Fatal("Event refund fixture configuration is incomplete")
	}
	if _, err := os.Lstat(config.ReadyFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Event refund fixture ready file already exists")
	}
	if _, err := os.Lstat(config.StopFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Event refund fixture stop file already exists")
	}
	certPEM, err := os.ReadFile(config.ServerCert)
	if err != nil {
		t.Fatal("read Event refund fixture server certificate")
	}
	keyPEM, err := os.ReadFile(config.ServerKey)
	if err != nil {
		t.Fatal("read Event refund fixture server key")
	}
	caPEM, err := os.ReadFile(config.ClientCA)
	if err != nil {
		t.Fatal("read Event refund fixture client CA")
	}
	credentials, err := grpcsecurity.NewServerCredentials(certPEM, keyPEM, caPEM, []string{grpcsecurity.ServiceIdentityBooking})
	if err != nil {
		t.Fatal("initialize Booking-only Event refund fixture mTLS")
	}

	pool, schema := newRefundTimeFixtureDatabase(t)
	defer pool.Close()
	authority := repository.NewPostgresRefundTimeRepository(pool)
	eventServer := NewEventServer(nil, zap.NewNop())
	eventServer.SetRefundTimeAuthority(authority)
	methods := []string{
		eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName,
		eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName,
	}
	server := grpc.NewServer(
		grpc.Creds(credentials),
		grpc.UnaryInterceptor(grpcsecurity.TicketAuthorityMethodsAuthorizationInterceptor(methods...)),
	)
	if err := grpcsecurity.RegisterServiceWithOnlyMethods(server, eventv1.EventService_ServiceDesc, eventServer, methods...); err != nil {
		t.Fatal("register Event refund fixture methods")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen for Event refund fixture")
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(server.Stop)

	address := listener.Addr().String()
	ready, err := json.Marshal(refundTimeFixtureReady{Address: address, Schema: schema})
	if err != nil {
		t.Fatal("encode Event refund fixture readiness")
	}
	if err := writeRefundTimeReadyFile(config.ReadyFile, ready); err != nil {
		t.Fatal("publish Event refund fixture readiness")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(120 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Fatalf("Event refund fixture server stopped unexpectedly: %v", err)
			}
			t.Fatal("Event refund fixture server stopped before the stop signal")
		case <-ticker.C:
			if _, err := os.Stat(config.StopFile); err == nil {
				stopRefundTimeFixtureServer(t, server, serveDone)
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("read Event refund fixture stop signal: %v", err)
			}
		case <-deadline.C:
			server.Stop()
			t.Fatal("Event refund fixture timed out after 120 seconds")
		}
	}
}

func newRefundTimeFixtureDatabase(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("EVENT_REFUND_TEST_DATABASE_URL"))
	if rawURL == "" {
		t.Fatal("EVENT_REFUND_TEST_DATABASE_URL is required when the coordination fixture is enabled")
	}
	databaseURL, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal("invalid isolated Event refund fixture database URL")
	}
	address := net.ParseIP(databaseURL.Hostname())
	if address == nil || !address.IsPrivate() || databaseURL.Port() != "25443" || databaseURL.Path != "/ticketbox_event_refund_test" {
		t.Fatal("BLOCKED: Event refund fixture requires its private-IP :25443 /ticketbox_event_refund_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, rawURL)
	if err != nil {
		t.Fatal("connect to isolated Event refund fixture database")
	}
	var databaseName string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil || databaseName != "ticketbox_event_refund_test" {
		admin.Close()
		t.Fatal("connected database identity does not match Event refund fixture database")
	}
	var uuidOSSPresent bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='uuid-ossp')`).Scan(&uuidOSSPresent); err != nil || !uuidOSSPresent {
		admin.Close()
		t.Fatal("Event refund fixture database must already have uuid-ossp installed")
	}

	schema := "event_refund_time_fixture_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal("create disposable Event refund fixture schema")
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Errorf("drop disposable Event refund fixture schema: %v", err)
		}
	})
	query := databaseURL.Query()
	query.Set("search_path", schema+",public")
	databaseURL.RawQuery = query.Encode()
	migrationCtx, migrationCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer migrationCancel()
	command := exec.CommandContext(migrationCtx, "migrate", "-path", "../../migrations", "-database", databaseURL.String(), "up")
	output, err := command.CombinedOutput()
	if err != nil {
		safeOutput := strings.ReplaceAll(string(output), rawURL, "[isolated DSN]")
		safeOutput = strings.ReplaceAll(safeOutput, databaseURL.String(), "[isolated DSN]")
		t.Fatalf("apply actual Event refund fixture migrations: %s", safeOutput)
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL.String())
	if err != nil {
		t.Fatal("parse Event refund fixture database URL")
	}
	poolConfig.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("connect to disposable Event refund fixture schema")
	}
	t.Cleanup(pool.Close)
	var currentSchema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil || currentSchema != schema {
		t.Fatal("Event refund fixture connection escaped its disposable schema")
	}
	return pool, schema
}

func writeRefundTimeReadyFile(path string, ready []byte) error {
	tmp := path + ".tmp-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := os.WriteFile(tmp, ready, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func stopRefundTimeFixtureServer(t *testing.T, server *grpc.Server, serveDone <-chan error) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		server.Stop()
		<-stopped
	}
	select {
	case err := <-serveDone:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Fatalf("stop Event refund fixture server: %v", err)
		}
	case <-time.After(5 * time.Second):
		server.Stop()
		t.Fatal("Event refund fixture gRPC Serve did not stop")
	}
}
