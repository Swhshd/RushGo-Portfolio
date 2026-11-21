//go:build admin_event_operations_e2e

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	eventgrpc "github.com/ticketbox/event/internal/grpc"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	"github.com/ticketbox/pkg/database"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	fixtureAck                  = "I_ACKNOWLEDGE_ISOLATED_SYNTHETIC_ADMIN_OBSERVABILITY"
	fixtureEventOperationsOwner = "admin-event-operations"
	fixtureUnifiedOwner         = "admin-unified-preview"
)

var runIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

type fixtureRuntime struct {
	Acknowledgement         string `json:"acknowledgement"`
	RunID                   string `json:"run_id"`
	FixtureOwner            string `json:"fixture_owner"`
	TestIdentity            string `json:"test_identity"`
	Service                 string `json:"service"`
	PrivateRuntimeDir       string `json:"private_runtime_dir"`
	DatabaseURL             string `json:"database_url"`
	BookingRedisURL         string `json:"booking_redis_url"`
	GatewayRedisURL         string `json:"gateway_redis_url"`
	NotificationRedisURL    string `json:"notification_redis_url"`
	JWTSecret               string `json:"jwt_secret"`
	JWTAccessTokenTTL       int    `json:"jwt_access_token_ttl"`
	JWTRefreshTokenTTL      int    `json:"jwt_refresh_token_ttl"`
	UserGRPCListen          string `json:"user_grpc_listen"`
	EventGRPCListen         string `json:"event_grpc_listen"`
	BookingGRPCListen       string `json:"booking_grpc_listen"`
	NotificationGRPCListen  string `json:"notification_grpc_listen"`
	GatewayHTTPListen       string `json:"gateway_http_listen"`
	UserServiceAddr         string `json:"user_service_addr"`
	EventServiceAddr        string `json:"event_service_addr"`
	EventGRPCServerName     string `json:"event_grpc_server_name"`
	BookingServiceAddr      string `json:"booking_service_addr"`
	NotificationServiceAddr string `json:"notification_service_addr"`
	TrustedProxies          string `json:"trusted_proxies"`
}

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("ADMIN_EVENT_OPERATIONS_EVENT=REFUSED\n")
		os.Exit(2)
	}
}

func run() error {
	cfg, err := loadFixtureRuntime()
	if err != nil {
		return errors.New("runtime guard")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, zap.NewNop())
	if err != nil {
		return errors.New("postgres unavailable")
	}
	defer pool.Close()

	eventRepo := repository.NewPostgresEventRepository(pool)
	tierRepo := repository.NewPostgresTicketTierRepository(pool)
	eventService := service.NewEventService(eventRepo, tierRepo, nil, zap.NewNop())
	server := eventgrpc.NewEventServer(eventService, zap.NewNop())
	desc, err := filteredServiceDesc(&eventv1.EventService_ServiceDesc, []string{
		eventv1.EventService_GetEvent_FullMethodName,
		eventv1.EventService_ListEvents_FullMethodName,
	})
	if err != nil {
		return errors.New("event rpc allowlist")
	}
	listener, err := net.Listen("tcp", cfg.EventGRPCListen)
	if err != nil {
		return errors.New("loopback bind failed")
	}
	grpcServer := grpc.NewServer()
	grpcServer.RegisterService(&desc, server)
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	if err := grpcServer.Serve(listener); err != nil && ctx.Err() == nil {
		return errors.New("event serve failed")
	}
	return nil
}

func loadFixtureRuntime() (fixtureRuntime, error) {
	var cfg fixtureRuntime
	if os.Getenv("ADMIN_OBSERVABILITY_E2E_ACK") != fixtureAck {
		return cfg, errors.New("fixture acknowledgement missing")
	}
	configPath := strings.TrimSpace(os.Getenv("ADMIN_OBSERVABILITY_E2E_CONFIG"))
	if !filepath.IsAbs(configPath) {
		return cfg, errors.New("fixture config path required")
	}
	file, err := os.Open(configPath)
	if err != nil {
		return cfg, errors.New("fixture config unavailable")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, errors.New("fixture config invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return cfg, errors.New("fixture config trailing data")
	}
	if err := validateEventFixtureIdentity(cfg, os.Getenv("SERVICE_NAME")); err != nil {
		return cfg, err
	}
	runtimeDir, err := filepath.Abs(cfg.PrivateRuntimeDir)
	if err != nil || filepath.Base(filepath.Clean(runtimeDir)) != cfg.RunID {
		return cfg, errors.New("fixture runtime identity invalid")
	}
	info, err := os.Stat(runtimeDir)
	if err != nil || !info.IsDir() || !pathWithin(runtimeDir, configPath) {
		return cfg, errors.New("fixture runtime unavailable")
	}
	cfg.PrivateRuntimeDir = runtimeDir
	if err := validateEventFixtureEndpoints(cfg); err != nil {
		return cfg, err
	}
	for _, item := range os.Environ() {
		name, value, found := strings.Cut(item, "=")
		if !found || strings.TrimSpace(value) == "" {
			continue
		}
		upper := strings.ToUpper(name)
		if strings.Contains(upper, "PROXY") || strings.Contains(upper, "KAFKA") || strings.Contains(upper, "STRIPE") ||
			strings.Contains(upper, "TOKEN") || strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "JWT_SECRET") {
			return cfg, errors.New("external or credential environment is forbidden")
		}
	}
	return cfg, nil
}

func validateEventFixtureIdentity(cfg fixtureRuntime, serviceName string) error {
	if cfg.FixtureOwner != fixtureEventOperationsOwner && cfg.FixtureOwner != fixtureUnifiedOwner {
		return errors.New("unsupported fixture owner")
	}
	if cfg.Acknowledgement != fixtureAck || cfg.Service != "event" || !runIDPattern.MatchString(cfg.RunID) ||
		cfg.TestIdentity != "rushgo-"+cfg.FixtureOwner+"/"+cfg.RunID+"/event" ||
		serviceName != cfg.FixtureOwner+"-e2e-event" || cfg.TrustedProxies != "" {
		return errors.New("fixture scope mismatch")
	}
	return nil
}

func validateEventFixtureEndpoints(cfg fixtureRuntime) error {
	if !runIDPattern.MatchString(cfg.RunID) {
		return errors.New("fixture run identity invalid")
	}
	databasePrefix := ""
	switch cfg.FixtureOwner {
	case fixtureEventOperationsOwner:
		databasePrefix = "admin_event_operations_"
	case fixtureUnifiedOwner:
		databasePrefix = "admin_unified_preview_"
	default:
		return errors.New("unsupported fixture owner")
	}
	databaseURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil || (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") || databaseURL.User == nil ||
		!loopbackHost(databaseURL.Hostname()) || databaseURL.Port() == "" || databaseURL.Path != "/"+databasePrefix+cfg.RunID[:12]+"_event" {
		return errors.New("owner-specific loopback database required")
	}
	if !loopbackAddress(cfg.EventGRPCListen) {
		return errors.New("loopback-only event listener required")
	}
	return nil
}

func loopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || !validPort(port) {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validPort(raw string) bool {
	port, err := strconv.Atoi(raw)
	return err == nil && port > 0 && port <= 65535
}

func pathWithin(root, candidate string) bool {
	rootPath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	candidatePath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootPath, candidatePath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func filteredServiceDesc(source *grpc.ServiceDesc, names []string) (grpc.ServiceDesc, error) {
	if source == nil || len(names) == 0 {
		return grpc.ServiceDesc{}, errors.New("service methods required")
	}
	allowed := make(map[string]struct{}, len(names))
	prefix := "/" + source.ServiceName + "/"
	for _, fullMethod := range names {
		if !strings.HasPrefix(fullMethod, prefix) {
			return grpc.ServiceDesc{}, errors.New("invalid method allowlist")
		}
		allowed[strings.TrimPrefix(fullMethod, prefix)] = struct{}{}
	}
	filtered := *source
	filtered.Methods = nil
	filtered.Streams = nil
	for _, method := range source.Methods {
		if _, ok := allowed[method.MethodName]; ok {
			filtered.Methods = append(filtered.Methods, method)
			delete(allowed, method.MethodName)
		}
	}
	if len(allowed) != 0 || len(filtered.Methods) != len(names) {
		return grpc.ServiceDesc{}, errors.New("generated event descriptor mismatch")
	}
	return filtered, nil
}
