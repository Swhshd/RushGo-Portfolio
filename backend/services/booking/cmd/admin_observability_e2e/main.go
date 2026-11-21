//go:build admin_observability_e2e

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
	bookinggrpc "github.com/ticketbox/booking/internal/grpc"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	if stage := runBookingFixture(); stage != "" {
		fmt.Fprintf(os.Stderr, "ADMIN_OBSERVABILITY_E2E_BOOKING_STARTUP=REFUSED stage=%s\n", stage)
		os.Exit(2)
	}
}

func runBookingFixture() string {
	cfg, err := loadFixtureRuntime("booking")
	if err != nil {
		return "runtime_guard"
	}
	if err := requireFixtureTLSFiles("RUSHGO_GRPC_SERVER_CERT_FILE", "RUSHGO_GRPC_SERVER_KEY_FILE", "RUSHGO_GRPC_CLIENT_CA_FILE"); err != nil {
		return "server_mtls_files"
	}
	bookingTransport, bookingTestOnlyPlaintext, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServiceBooking)
	if err != nil || bookingTestOnlyPlaintext {
		return "server_mtls_transport"
	}
	if err := requireFixtureTLSFiles("RUSHGO_GRPC_CLIENT_CERT_FILE", "RUSHGO_GRPC_CLIENT_KEY_FILE", "RUSHGO_GRPC_SERVER_CA_FILE"); err != nil {
		return "user_client_mtls_files"
	}
	userCredentials, userTestOnlyPlaintext, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityBooking)
	if err != nil || userTestOnlyPlaintext {
		return "user_client_mtls_transport"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, zap.NewNop())
	if err != nil {
		return "postgres"
	}
	defer pool.Close()

	redisOptions, err := redis.ParseURL(cfg.BookingRedisURL)
	if err != nil {
		return "redis_config"
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return "redis"
	}

	userConn, err := grpc.NewClient(cfg.UserServiceAddr, grpc.WithTransportCredentials(userCredentials))
	if err != nil {
		return "user_connection"
	}
	defer userConn.Close()
	userClient := userv1.NewUserServiceClient(userConn)
	revocations := fixtureTokenRevocations{client: redisClient}
	ticketAuthenticator := bookinggrpc.NewTicketPrincipalAuthenticator(userClient, revocations)

	bookingRepo := repository.NewPostgresBookingRepository(pool)
	bookingServer := bookinggrpc.NewBookingServer(nil, zap.NewNop())
	bookingServer.SetTicketPrincipalAuthenticator(ticketAuthenticator)
	bookingServer.SetAdminOutboxPrincipalAuthenticator(bookinggrpc.NewAdminOutboxPrincipalAuthenticator(ticketAuthenticator, userClient))
	bookingServer.SetAdminOutboxService(service.NewAdminOutboxService(bookingRepo))
	bookingServer.SetAdminEventTicketOperationsService(service.NewAdminEventTicketOperationsService(bookingRepo))
	bookingServer.SetAdminEventTicketListService(service.NewAdminEventTicketListService(bookingRepo))
	// This fixture rejects every TEST_ONLY plaintext transport before enabling
	// the protected method, so the secure flag reflects actual mTLS credentials.
	bookingServer.SetAdminOutboxSecure(true)
	bookingServer.SetAdminEventTicketOperationsSecure(true)
	bookingServer.SetAdminBookingDetailSecure(true)
	bookingServer.SetAdminEventTicketListSecure(true)

	desc, err := filteredServiceDesc(&bookingv1.BookingService_ServiceDesc, []string{
		bookingv1.BookingService_ListAdminOutboxEvents_FullMethodName,
		bookingv1.BookingService_GetAdminEventTicketOperations_FullMethodName,
		bookingv1.BookingService_ListAdminEventTickets_FullMethodName,
	})
	if err != nil {
		return "rpc_allowlist"
	}
	grpcServer := grpc.NewServer(grpc.Creds(bookingTransport))
	grpcServer.RegisterService(&desc, bookingServer)

	listener, err := net.Listen("tcp", cfg.BookingGRPCListen)
	if err != nil {
		return "loopback_bind"
	}
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	if err := grpcServer.Serve(listener); err != nil && ctx.Err() == nil {
		return "serve"
	}
	return ""
}

type fixtureTokenRevocations struct {
	client *redis.Client
}

func (r fixtureTokenRevocations) IsRevoked(ctx context.Context, token string) (bool, error) {
	if r.client == nil || strings.TrimSpace(token) == "" {
		return false, errors.New("revocation store unavailable")
	}
	count, err := r.client.Exists(ctx, "blacklist:"+token).Result()
	if err != nil {
		return false, errors.New("revocation check unavailable")
	}
	return count > 0, nil
}
