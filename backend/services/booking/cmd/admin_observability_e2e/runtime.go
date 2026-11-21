//go:build admin_observability_e2e

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const fixtureAck = "I_ACKNOWLEDGE_ISOLATED_SYNTHETIC_ADMIN_OBSERVABILITY"
const fixtureUnifiedOwner = "admin-unified-preview"

type fixtureRuntime struct {
	Acknowledgement         string `json:"acknowledgement"`
	RunID                   string `json:"run_id"`
	TestIdentity            string `json:"test_identity"`
	Service                 string `json:"service"`
	FixtureOwner            string `json:"fixture_owner"`
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

var fixtureRunIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func loadFixtureRuntime(expectedService string) (fixtureRuntime, error) {
	var cfg fixtureRuntime
	if os.Getenv("ADMIN_OBSERVABILITY_E2E_ACK") != fixtureAck {
		return cfg, errors.New("fixture acknowledgement missing")
	}
	configPath := strings.TrimSpace(os.Getenv("ADMIN_OBSERVABILITY_E2E_CONFIG"))
	if configPath == "" || !filepath.IsAbs(configPath) {
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
		return fixtureRuntime{}, errors.New("fixture config invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fixtureRuntime{}, errors.New("fixture config has trailing data")
	}
	cfg, err = validateFixtureIdentity(cfg, expectedService, os.Getenv("SERVICE_NAME"))
	if err != nil {
		return fixtureRuntime{}, err
	}
	runtimeDir, err := filepath.Abs(cfg.PrivateRuntimeDir)
	if err != nil || filepath.Base(filepath.Clean(runtimeDir)) != cfg.RunID {
		return fixtureRuntime{}, errors.New("private runtime directory mismatch")
	}
	info, err := os.Stat(runtimeDir)
	if err != nil || !info.IsDir() {
		return fixtureRuntime{}, errors.New("private runtime directory unavailable")
	}
	if !pathWithin(runtimeDir, configPath) {
		return fixtureRuntime{}, errors.New("fixture config is outside its private runtime directory")
	}
	cfg.PrivateRuntimeDir = runtimeDir
	if err := validateFixtureEndpoints(cfg); err != nil {
		return fixtureRuntime{}, err
	}
	if len(cfg.JWTSecret) < 32 || cfg.JWTAccessTokenTTL < 1 || cfg.JWTRefreshTokenTTL < 1 {
		return fixtureRuntime{}, errors.New("synthetic auth configuration required")
	}
	if err := validateFixtureProcessEnvironment(); err != nil {
		return fixtureRuntime{}, err
	}
	return cfg, nil
}

func validateFixtureEndpoints(cfg fixtureRuntime) error {
	if !isSupportedFixtureOwner(cfg.FixtureOwner) || !fixtureRunIDPattern.MatchString(cfg.RunID) {
		return errors.New("exact fixture owner and run identity required")
	}
	if cfg.Service == "gateway" {
		if cfg.DatabaseURL != "" {
			return errors.New("Gateway fixture must not receive a database target")
		}
	} else {
		databaseURL, err := url.Parse(cfg.DatabaseURL)
		databasePrefix := fixtureDatabasePrefix(cfg.FixtureOwner)
		expectedDatabase := "/" + databasePrefix + cfg.RunID[:12] + "_" + cfg.Service
		if err != nil || (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") || databaseURL.User == nil ||
			!loopbackHost(databaseURL.Hostname()) || databaseURL.Port() == "" || databaseURL.Path != expectedDatabase {
			return errors.New("isolated owner-specific loopback PostgreSQL target required")
		}
	}
	redisURLs := []string{cfg.BookingRedisURL, cfg.GatewayRedisURL, cfg.NotificationRedisURL}
	if cfg.FixtureOwner == "admin-event-operations" {
		if cfg.BookingRedisURL == "" || cfg.BookingRedisURL != cfg.GatewayRedisURL || cfg.NotificationRedisURL != "" {
			return errors.New("Booking and Gateway must share the isolated revocation store")
		}
		redisURLs = redisURLs[:2]
	} else if cfg.BookingRedisURL == "" || cfg.BookingRedisURL != cfg.GatewayRedisURL || cfg.BookingRedisURL != cfg.NotificationRedisURL {
		return errors.New("all isolated services must share the private revocation store")
	}
	for _, raw := range redisURLs {
		redisURL, parseErr := url.Parse(raw)
		if parseErr != nil || redisURL.Scheme != "redis" || !loopbackHost(redisURL.Hostname()) || redisURL.Port() == "" {
			return errors.New("isolated loopback Redis target required")
		}
		databaseNumber, parseErr := strconv.Atoi(strings.TrimPrefix(redisURL.Path, "/"))
		if parseErr != nil || databaseNumber < 1 || databaseNumber > 15 {
			return errors.New("non-default isolated Redis database required")
		}
	}
	addresses := []string{cfg.UserGRPCListen, cfg.BookingGRPCListen, cfg.GatewayHTTPListen, cfg.UserServiceAddr, cfg.BookingServiceAddr}
	switch cfg.FixtureOwner {
	case "admin-event-operations":
		addresses = append(addresses, cfg.EventGRPCListen, cfg.EventServiceAddr)
	case fixtureUnifiedOwner:
		addresses = append(addresses, cfg.EventGRPCListen, cfg.EventServiceAddr, cfg.NotificationGRPCListen, cfg.NotificationServiceAddr)
	default:
		addresses = append(addresses, cfg.NotificationGRPCListen, cfg.NotificationServiceAddr)
	}
	for _, address := range addresses {
		if !loopbackAddress(address) {
			return errors.New("loopback-only fixture bind and service targets required")
		}
	}
	if cfg.TrustedProxies != "" {
		return errors.New("fixture trusted proxies must remain disabled")
	}
	return nil
}

func validateFixtureIdentity(cfg fixtureRuntime, expectedService, serviceName string) (fixtureRuntime, error) {
	fixtureOwner := cfg.FixtureOwner
	if fixtureOwner == "" {
		fixtureOwner = "admin-observability-v2"
	}
	if !isSupportedFixtureOwner(fixtureOwner) || cfg.Acknowledgement != fixtureAck || cfg.Service != expectedService ||
		!fixtureRunIDPattern.MatchString(cfg.RunID) ||
		cfg.TestIdentity != "rushgo-"+fixtureOwner+"/"+cfg.RunID+"/"+expectedService ||
		serviceName != fixtureOwner+"-e2e-"+expectedService {
		return fixtureRuntime{}, errors.New("fixture identity mismatch")
	}
	cfg.FixtureOwner = fixtureOwner
	return cfg, nil
}

func isSupportedFixtureOwner(fixtureOwner string) bool {
	switch fixtureOwner {
	case "admin-observability-v2", "admin-event-operations", fixtureUnifiedOwner:
		return true
	default:
		return false
	}
}

func fixtureDatabasePrefix(fixtureOwner string) string {
	switch fixtureOwner {
	case "admin-event-operations":
		return "admin_event_operations_"
	case fixtureUnifiedOwner:
		return "admin_unified_preview_"
	default:
		return "admin_observability_v2_"
	}
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

func validPort(port string) bool {
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535
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

func requireFixtureTLSFiles(names ...string) error {
	root := strings.TrimSpace(os.Getenv("ADMIN_OBSERVABILITY_E2E_CONFIG"))
	if root == "" {
		return errors.New("fixture runtime is unavailable")
	}
	var cfg fixtureRuntime
	data, err := os.ReadFile(root)
	if err != nil || json.Unmarshal(bytes.TrimSpace(data), &cfg) != nil {
		return errors.New("fixture runtime is unavailable")
	}
	for _, name := range names {
		path := strings.TrimSpace(os.Getenv(name))
		if path == "" || !filepath.IsAbs(path) || !pathWithin(cfg.PrivateRuntimeDir, path) {
			return errors.New("synthetic mTLS file is outside the private runtime directory")
		}
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() {
			return errors.New("synthetic mTLS file unavailable")
		}
	}
	if os.Getenv("RUSHGO_GRPC_TEST_ONLY_INSECURE") != "" {
		return errors.New("plaintext gRPC transport is forbidden")
	}
	return nil
}

func validateFixtureProcessEnvironment() error {
	for _, item := range os.Environ() {
		name, value, found := strings.Cut(item, "=")
		if !found || strings.TrimSpace(value) == "" {
			continue
		}
		upper := strings.ToUpper(name)
		if upper == "ADMIN_OBSERVABILITY_E2E_OLD_ROLE_TOKEN_FILE" {
			continue
		}
		for _, forbidden := range []string{"PROXY", "SMTP", "KAFKA", "STRIPE", "OTEL", "TOKEN", "PASSWORD", "JWT_SECRET"} {
			if strings.Contains(upper, forbidden) {
				return errors.New("fixture process has an external or credential environment setting")
			}
		}
	}
	return nil
}

func filteredServiceDesc(source *grpc.ServiceDesc, allowedFullMethods []string) (grpc.ServiceDesc, error) {
	if source == nil || len(allowedFullMethods) == 0 {
		return grpc.ServiceDesc{}, errors.New("service method allowlist required")
	}
	allowed := make(map[string]struct{}, len(allowedFullMethods))
	prefix := "/" + source.ServiceName + "/"
	for _, method := range allowedFullMethods {
		if !strings.HasPrefix(method, prefix) || len(method) == len(prefix) {
			return grpc.ServiceDesc{}, errors.New("invalid service method allowlist")
		}
		allowed[strings.TrimPrefix(method, prefix)] = struct{}{}
	}
	desc := *source
	desc.Methods = nil
	desc.Streams = nil
	for _, method := range source.Methods {
		if _, ok := allowed[method.MethodName]; ok {
			desc.Methods = append(desc.Methods, method)
			delete(allowed, method.MethodName)
		}
	}
	if len(allowed) != 0 || len(desc.Methods) != len(allowedFullMethods) {
		return grpc.ServiceDesc{}, errors.New("generated service descriptor does not match allowlist")
	}
	return desc, nil
}
