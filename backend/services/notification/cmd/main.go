package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	notifgrpc "github.com/ticketbox/notification/internal/grpc"
	notifkafka "github.com/ticketbox/notification/internal/kafka"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/observability"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	if err := run(logger); err != nil {
		logger.Fatal("Notification service failed closed", zap.Error(err))
	}
}

func notificationSender(cfg *config.Config, logger *zap.Logger,
	reliabilityHooks *reliabilityhook.Controller) (service.NotificationSender, string, error) {
	if reliabilityHooks.Enabled() {
		logSender := service.NewLogSender(logger)
		logSender.SetIncludeNotificationIdentity(true)
		return logSender, "log", nil
	}
	smtpSender, err := service.NewSMTPNotificationSender(service.SMTPConfig{
		Host: cfg.SMTPHost, Port: cfg.SMTPPort, TLSMode: cfg.SMTPTLSMode,
		Username: cfg.SMTPUsername, Password: cfg.SMTPPassword, From: cfg.SMTPFrom,
		FrontendOrigin: cfg.AuthFrontendOrigin,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		return nil, "", errors.New("Notification secure SMTP configuration is required")
	}
	return smtpSender, "smtp", nil
}

func run(logger *zap.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return errors.New("Notification configuration unavailable")
	}
	telemetry, err := observability.InitializeFromConfig(context.Background(), cfg, logger)
	if err != nil {
		return errors.New("Notification observability configuration unavailable")
	}
	defer telemetry.Shutdown(context.Background())

	serverCredentials, testOnly, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServiceNotification)
	if err != nil || testOnly || serverCredentials == nil {
		return errors.New("Notification gRPC mutual TLS configuration is required")
	}
	userCredentials, userTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityNotification)
	if err != nil || userTestOnly || userCredentials == nil || strings.TrimSpace(cfg.UserServiceAddr) == "" {
		return errors.New("Notification User mutual TLS configuration is required")
	}
	userConn, err := grpc.NewClient(cfg.UserServiceAddr, grpc.WithTransportCredentials(userCredentials))
	if err != nil {
		return errors.New("Notification User client configuration unavailable")
	}
	defer userConn.Close()
	userClient := userv1.NewUserServiceClient(userConn)

	reliabilityHooks, err := reliabilityhook.FromEnv()
	if err != nil {
		return errors.New("Notification reliability hooks unavailable")
	}

	sender, senderAdapter, err := notificationSender(cfg, logger, reliabilityHooks)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		return errors.New("Notification database unavailable")
	}
	defer pool.Close()

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return errors.New("Notification revocation store configuration unavailable")
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	pingCtx, cancelPing := context.WithTimeout(ctx, 3*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		return errors.New("Notification revocation store unavailable")
	}
	cancelPing()

	repo := repository.NewPostgresNotificationRepository(pool)
	svc := service.NewNotificationService(repo, sender, logger, telemetry.Metrics)
	svc.SetRecipientLookup(userNotificationRecipientLookup{client: userClient})
	worker := service.NewDeliveryWorker(repo, sender, logger, telemetry.Metrics)
	worker.SetReliabilityHooks(reliabilityHooks)
	workerDone := make(chan struct{})
	consumerDone := make(chan error, 1)
	consumer := notifkafka.NewNotificationConsumer(cfg.KafkaBrokers, svc, logger)
	go func() { consumerDone <- consumer.Start(ctx) }()
	go func() {
		defer close(workerDone)
		if err := worker.Start(ctx); err != nil {
			logger.Error("Notification delivery worker stopped", zap.Error(err))
		}
	}()

	listenAddress := strings.TrimSpace(os.Getenv("RUSHGO_NOTIFICATION_ADMIN_GRPC_ADDR"))
	if listenAddress == "" {
		listenAddress = ":" + cfg.GRPCPort
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return errors.New("Notification gRPC listener unavailable")
	}
	defer listener.Close()
	adminOutbox := service.NewAdminOutboxService(repo)
	inbox := service.NewNotificationInboxService(repo)
	adminServer := notifgrpc.NewAdminOutboxServer(
		adminOutbox, userClient, redisTokenRevocations{client: redisClient}, true, inbox,
	)
	grpcOptions := []grpc.ServerOption{
		grpc.Creds(serverCredentials), grpc.MaxRecvMsgSize(1 << 20), grpc.MaxSendMsgSize(1 << 20),
	}
	grpcOptions = append(grpcOptions, telemetry.GRPCServerOptions()...)
	grpcServer := grpc.NewServer(grpcOptions...)
	notificationv1.RegisterNotificationAdminServiceServer(grpcServer, adminServer)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()

	logger.Info("Notification service started with authenticated inbox RPCs",
		zap.String("adapter", senderAdapter))
	sigCtx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-sigCtx.Done():
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("Notification gRPC server stopped", zap.Error(err))
		}
	case err := <-consumerDone:
		if err != nil {
			logger.Error("Notification Kafka consumer stopped", zap.Error(err))
		}
	}

	logger.Info("Shutting down Notification service")
	cancel()
	grpcDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcDone) }()
	select {
	case <-grpcDone:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
	}
	select {
	case <-workerDone:
	case <-time.After(15 * time.Second):
		logger.Warn("Notification delivery worker shutdown timed out")
	}
	return nil
}

type userNotificationRecipientLookup struct {
	client userv1.UserServiceClient
}

func (r userNotificationRecipientLookup) LookupNotificationRecipient(ctx context.Context, id uuid.UUID) (service.NotificationRecipient, error) {
	if r.client == nil || id == uuid.Nil {
		return service.NotificationRecipient{}, service.ErrRecipientUnavailable
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := r.client.GetNotificationRecipient(callCtx, &userv1.GetProfileRequest{UserId: id.String()})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return service.NotificationRecipient{}, errors.Join(pkgkafka.ErrPermanent, service.ErrRecipientUnavailable)
		}
		return service.NotificationRecipient{}, service.ErrRecipientUnavailable
	}
	if resp == nil || resp.GetUserId() != id.String() {
		return service.NotificationRecipient{}, errors.Join(pkgkafka.ErrPermanent, service.ErrRecipientUnavailable)
	}
	return service.NotificationRecipient{
		UserID: id, Email: resp.GetEmail(), Locale: resp.GetLocale(), EmailVerified: resp.GetEmailVerified(),
	}, nil
}

type redisTokenRevocations struct {
	client *redis.Client
}

func (r redisTokenRevocations) IsRevoked(ctx context.Context, token string) (bool, error) {
	if r.client == nil || token == "" {
		return false, errors.New("Notification revocation store unavailable")
	}
	exists, err := r.client.Exists(ctx, "blacklist:"+token).Result()
	if err != nil {
		return false, errors.New("Notification revocation check failed")
	}
	return exists > 0, nil
}
