package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/observability"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	usergrpc "github.com/ticketbox/user/internal/grpc"
	userkafka "github.com/ticketbox/user/internal/kafka"
	"github.com/ticketbox/user/internal/repository"
	"github.com/ticketbox/user/internal/service"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	if err := grpcsecurity.ValidateTicketE2ERuntimeIdentity("ticket-e2e-user"); err != nil {
		logger.Fatal("Ticket E2E User runtime identity refused", zap.Error(err))
	}
	if err := grpcsecurity.ValidateTicketE2ERuntimeResources("ticket-e2e-user", grpcsecurity.TicketE2ERuntimeResources{
		DatabaseURL:  cfg.DatabaseURL,
		RedisURL:     cfg.RedisURL,
		KafkaBrokers: cfg.KafkaBrokers,
	}); err != nil {
		logger.Fatal("Ticket E2E User resource identity refused", zap.Error(err))
	}
	if err := validateUserAuthConfig(cfg); err != nil {
		logger.Fatal("Invalid auth configuration")
	}
	userTransport, testOnlyPlaintext, err := grpcsecurity.ServerCredentialsFromEnvWithDiagnostics(
		grpcsecurity.ServiceUser, userHandshakeObserver(logger))
	if err != nil {
		logger.Fatal("User gRPC transport security initialization refused", zap.Error(err))
	}
	telemetry, err := observability.InitializeFromConfig(context.Background(), cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer telemetry.Shutdown(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	userRepo := repository.NewPostgresUserRepository(pool)
	authRepo := repository.NewPostgresAuthRepository(pool)
	jwtManager := service.NewJWTManager(cfg.JWTSecret, cfg.JWTAccessTokenTTL)
	sender, err := service.NewSMTPMailSender(service.SMTPConfig{Host: cfg.SMTPHost, Port: cfg.SMTPPort, TLSMode: cfg.SMTPTLSMode, Username: cfg.SMTPUsername, Password: cfg.SMTPPassword, From: cfg.SMTPFrom})
	if err != nil {
		logger.Fatal("Invalid auth mail configuration")
	}
	var origin *url.URL
	if cfg.AuthFrontendOrigin != "" {
		origin, err = service.ValidateFrontendOrigin(cfg.AuthFrontendOrigin)
		if err != nil {
			logger.Fatal("Invalid auth frontend origin")
		}
	}
	if cfg.SMTPHost != "" && origin == nil {
		logger.Fatal("Auth frontend origin required for mail")
	}
	authService := service.NewAuthService(userRepo, authRepo, jwtManager, cfg.JWTRefreshTokenTTL, logger, service.AuthDependencies{MailSender: sender, FrontendOrigin: origin, Metrics: telemetry.Metrics})
	recoveryQueue := service.NewRecoveryQueue(authService.ProcessRecoveryJob)
	recoveryQueue.Start(ctx)
	authService.SetRecoveryQueue(recoveryQueue)
	defer recoveryQueue.Shutdown(context.Background())

	producer := userkafka.NewUserEventProducer(cfg.KafkaBrokers, logger)
	defer producer.Close()

	unaryInterceptors := []grpc.UnaryServerInterceptor{middleware.UnaryLoggingInterceptor(logger)}
	// The isolated C-SIM plaintext mode has no authenticated caller identity;
	// production mTLS mode always installs the method-level authorization policy.
	if !testOnlyPlaintext {
		unaryInterceptors = append(unaryInterceptors, grpcsecurity.UserServiceMethodAuthorizationInterceptor())
	}
	serverOptions := []grpc.ServerOption{
		grpc.Creds(userTransport),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
	}
	if !testOnlyPlaintext {
		serverOptions = append(serverOptions, grpc.ChainStreamInterceptor(grpcsecurity.UserServiceStreamAuthorizationInterceptor()))
	}
	serverOptions = append(serverOptions, telemetry.UserAuthGRPCServerOptions()...)
	grpcServer := grpc.NewServer(serverOptions...)
	userServer := usergrpc.NewUserServer(authService, producer, logger)
	userv1.RegisterUserServiceServer(grpcServer, userServer)
	reflection.Register(grpcServer)
	if testOnlyPlaintext {
		logger.Warn("TEST_ONLY plaintext User gRPC server enabled for isolated Ticket E2E")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("Shutting down user service...")
		grpcServer.GracefulStop()
		cancel()
	}()

	logger.Info("User service started", zap.String("port", cfg.GRPCPort))
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("gRPC serve failed", zap.Error(err))
	}
}
