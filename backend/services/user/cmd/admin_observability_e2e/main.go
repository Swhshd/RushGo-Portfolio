//go:build admin_observability_e2e

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	usergrpc "github.com/ticketbox/user/internal/grpc"
	"github.com/ticketbox/user/internal/repository"
	"github.com/ticketbox/user/internal/service"
)

func main() {
	if stage := runUserFixture(); stage != "" {
		fmt.Fprintf(os.Stderr, "ADMIN_OBSERVABILITY_E2E_USER_STARTUP=REFUSED stage=%s\n", stage)
		os.Exit(2)
	}
}

func runUserFixture() string {
	cfg, err := loadFixtureRuntime("user")
	if err != nil {
		return "runtime_guard"
	}
	if err := requireFixtureTLSFiles("RUSHGO_GRPC_SERVER_CERT_FILE", "RUSHGO_GRPC_SERVER_KEY_FILE", "RUSHGO_GRPC_CLIENT_CA_FILE"); err != nil {
		return "mtls_files"
	}
	transport, testOnlyPlaintext, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServiceUser)
	if err != nil || testOnlyPlaintext {
		return "mtls_transport"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, zap.NewNop())
	if err != nil {
		return "postgres"
	}
	defer pool.Close()

	userRepo := repository.NewPostgresUserRepository(pool)
	authRepo := repository.NewPostgresAuthRepository(pool)
	jwtManager := service.NewJWTManager(cfg.JWTSecret, cfg.JWTAccessTokenTTL)
	authService := service.NewAuthService(userRepo, authRepo, jwtManager, cfg.JWTRefreshTokenTTL, zap.NewNop(), service.AuthDependencies{})
	userServer := usergrpc.NewUserServer(authService, nil, zap.NewNop())
	var validateTokenCount atomic.Uint64
	var currentRoleValidationCount atomic.Uint64
	var bookingCurrentRoleValidationCount atomic.Uint64
	var notificationCurrentRoleValidationCount atomic.Uint64

	allowedMethods := []string{
		userv1.UserService_Login_FullMethodName,
		userv1.UserService_RefreshToken_FullMethodName,
		userv1.UserService_Logout_FullMethodName,
		userv1.UserService_GetProfile_FullMethodName,
		userv1.UserService_ValidateToken_FullMethodName,
	}
	desc, err := filteredServiceDesc(&userv1.UserService_ServiceDesc, allowedMethods)
	if err != nil {
		return "rpc_allowlist"
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(transport),
		grpc.ChainUnaryInterceptor(
			grpcsecurity.UserServiceMethodAuthorizationInterceptor(),
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if info != nil && info.FullMethod == userv1.UserService_ValidateToken_FullMethodName {
					validateTokenCount.Add(1)
					if request, ok := req.(*userv1.ValidateTokenRequest); ok && request.GetRequireCurrentRole() {
						currentRoleValidationCount.Add(1)
						if remotePeer, ok := peer.FromContext(ctx); ok {
							var tlsInfo credentials.TLSInfo
							switch authInfo := remotePeer.AuthInfo.(type) {
							case credentials.TLSInfo:
								tlsInfo = authInfo
							case *credentials.TLSInfo:
								if authInfo != nil {
									tlsInfo = *authInfo
								}
							}
							if len(tlsInfo.State.PeerCertificates) > 0 {
								for _, identity := range tlsInfo.State.PeerCertificates[0].URIs {
									switch identity.String() {
									case grpcsecurity.ServiceIdentityBooking:
										bookingCurrentRoleValidationCount.Add(1)
									case grpcsecurity.ServiceIdentityNotification:
										notificationCurrentRoleValidationCount.Add(1)
									}
								}
							}
						}
					}
				}
				return handler(ctx, req)
			},
		),
	)
	grpcServer.RegisterService(&desc, userServer)

	listener, err := net.Listen("tcp", cfg.UserGRPCListen)
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
	fmt.Fprintf(os.Stdout, "ADMIN_OBSERVABILITY_E2E_USER_VALIDATE_TOKEN_CALLS=%d\n", validateTokenCount.Load())
	fmt.Fprintf(os.Stdout, "ADMIN_OBSERVABILITY_E2E_USER_CURRENT_ROLE_VALIDATION_CALLS=%d\n", currentRoleValidationCount.Load())
	fmt.Fprintf(os.Stdout, "ADMIN_OBSERVABILITY_E2E_USER_BOOKING_CURRENT_ROLE_VALIDATION_CALLS=%d\n", bookingCurrentRoleValidationCount.Load())
	fmt.Fprintf(os.Stdout, "ADMIN_OBSERVABILITY_E2E_USER_NOTIFICATION_CURRENT_ROLE_VALIDATION_CALLS=%d\n", notificationCurrentRoleValidationCount.Load())
	return ""
}
