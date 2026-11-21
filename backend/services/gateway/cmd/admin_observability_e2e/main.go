//go:build admin_observability_e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/gateway/internal/router"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "probe-event-operations" {
		if stage := runEventOperationsProbe(); stage != "" {
			fmt.Fprintf(os.Stderr, "ADMIN_EVENT_OPERATIONS_E2E_PROBE=FAIL stage=%s\n", stage)
			os.Exit(2)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "probe" {
		if stage := runGatewayProbe(); stage != "" {
			fmt.Fprintf(os.Stderr, "ADMIN_OBSERVABILITY_E2E_PROBE=FAIL stage=%s\n", stage)
			os.Exit(2)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "ADMIN_OBSERVABILITY_E2E_GATEWAY=REFUSED stage=usage")
		os.Exit(2)
	}
	if stage := runGatewayFixture(); stage != "" {
		fmt.Fprintf(os.Stderr, "ADMIN_OBSERVABILITY_E2E_GATEWAY_STARTUP=REFUSED stage=%s\n", stage)
		os.Exit(2)
	}
}

func runGatewayFixture() string {
	cfg, err := loadFixtureRuntime("gateway")
	if err != nil {
		return "runtime_guard"
	}
	if err := requireFixtureTLSFiles("RUSHGO_GRPC_CLIENT_CERT_FILE", "RUSHGO_GRPC_CLIENT_KEY_FILE", "RUSHGO_GRPC_SERVER_CA_FILE"); err != nil {
		return "client_mtls_files"
	}
	userCredentials, userTestOnlyPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityGateway)
	if err != nil || userTestOnlyPlaintext {
		return "user_mtls_transport"
	}
	bookingCredentials, bookingTestOnlyPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityGateway)
	if err != nil || bookingTestOnlyPlaintext {
		return "booking_mtls_transport"
	}
	userConn, err := grpc.NewClient(cfg.UserServiceAddr, grpc.WithTransportCredentials(userCredentials))
	if err != nil {
		return "user_connection"
	}
	defer userConn.Close()
	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, grpc.WithTransportCredentials(bookingCredentials))
	if err != nil {
		return "booking_connection"
	}
	defer bookingConn.Close()
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "event_connection"
	}
	defer eventConn.Close()

	var notificationClient notificationv1.NotificationAdminServiceClient
	secureNotification := false
	if cfg.FixtureOwner != "admin-event-operations" {
		notificationCredentials, notificationTestOnlyPlaintext, credentialErr := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceNotification, grpcsecurity.ServiceIdentityGateway)
		if credentialErr != nil || notificationTestOnlyPlaintext {
			return "notification_mtls_transport"
		}
		notificationConn, connErr := grpc.NewClient(cfg.NotificationServiceAddr, grpc.WithTransportCredentials(notificationCredentials))
		if connErr != nil {
			return "notification_connection"
		}
		defer notificationConn.Close()
		notificationClient = notificationv1.NewNotificationAdminServiceClient(notificationConn)
		secureNotification = true
	}

	redisOptions, err := redis.ParseURL(cfg.GatewayRedisURL)
	if err != nil {
		return "redis_config"
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return "redis"
	}

	gin.SetMode(gin.ReleaseMode)
	engine, err := router.SetupRouterWithNotificationOutboxSecurity(
		userv1.NewUserServiceClient(userConn), eventv1.NewEventServiceClient(eventConn), bookingv1.NewBookingServiceClient(bookingConn), nil, nil,
		redisClient, nil, "", true, true, notificationClient, secureNotification,
	)
	if err != nil {
		return "router"
	}
	listener, err := net.Listen("tcp", cfg.GatewayHTTPListen)
	if err != nil {
		return "loopback_bind"
	}
	server := &http.Server{
		Handler:           allowFixtureHTTP(engine, cfg.FixtureOwner),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		return "serve"
	}
	return ""
}

func allowFixtureHTTP(engine http.Handler, fixtureOwner string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !isSupportedFixtureOwner(fixtureOwner) {
			writeFixtureJSONError(w, http.StatusNotFound, "not_found", "route not found")
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/__e2e/health" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /api/auth/login", "POST /api/auth/refresh", "POST /api/auth/logout",
			"GET /api/users/me":
			engine.ServeHTTP(w, r)
			return
		}

		if r.Method == http.MethodGet && r.URL.Path == "/api/scheduler/jobs" && fixtureOwner == fixtureUnifiedOwner {
			writeFixtureJSONError(w, http.StatusServiceUnavailable, "not_running", "scheduler is not running in this fixture")
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/admin/outbox-events" {
			if fixtureOwner == "admin-event-operations" {
				writeFixtureJSONError(w, http.StatusServiceUnavailable, "source_unavailable", "outbox source unavailable")
				return
			}
			if fixtureOwner == "admin-observability-v2" || fixtureOwner == fixtureUnifiedOwner {
				engine.ServeHTTP(w, r)
				return
			}
		}
		if r.Method == http.MethodGet && isEventCatalogFixtureOwner(fixtureOwner) && isPublicEventReadPath(r.URL.Path) {
			engine.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && isTicketOperationsPath(r.URL.Path) {
			engine.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && isEventCatalogFixtureOwner(fixtureOwner) && isTicketListPath(r.URL.Path) {
			engine.ServeHTTP(w, r)
			return
		}
		writeFixtureJSONError(w, http.StatusNotFound, "not_found", "route not found")
	})
}

func writeFixtureJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": message})
}

func isEventCatalogFixtureOwner(fixtureOwner string) bool {
	return fixtureOwner == "admin-event-operations" || fixtureOwner == fixtureUnifiedOwner
}

func isPublicEventReadPath(path string) bool {
	if path == "/api/events" {
		return true
	}
	parts := strings.Split(path, "/")
	return len(parts) == 4 && parts[0] == "" && parts[1] == "api" && parts[2] == "events" && parts[3] != "" && len(parts[3]) <= 64
}

func isTicketOperationsPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "api" || parts[2] != "admin" || parts[3] != "events" || parts[5] != "ticket-operations" {
		return false
	}
	return parts[4] != "" && len(parts[4]) <= 64
}

func isTicketListPath(path string) bool {
	parts := strings.Split(path, "/")
	return len(parts) == 6 && parts[0] == "" && parts[1] == "api" && parts[2] == "admin" && parts[3] == "events" && parts[5] == "tickets" && parts[4] != "" && len(parts[4]) <= 64
}
