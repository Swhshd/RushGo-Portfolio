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

	"github.com/redis/go-redis/v9"
	notificationgrpc "github.com/ticketbox/notification/internal/grpc"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const adminObservabilityListenEnv = "RUSHGO_NOTIFICATION_ADMIN_GRPC_ADDR"

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	if err := runAdminObservability(logger); err != nil {
		logger.Fatal("Notification admin observability service failed", zap.Error(err))
	}
}

func runAdminObservability(logger *zap.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return errors.New("configuration unavailable")
	}
	listenAddress := strings.TrimSpace(os.Getenv(adminObservabilityListenEnv))
	if listenAddress == "" {
		return errors.New("Notification admin gRPC listen address is required")
	}

	serverCredentials, testOnly, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServiceNotification)
	if err != nil || testOnly || serverCredentials == nil {
		return errors.New("Notification admin gRPC mutual TLS configuration is required")
	}
	userCredentials, userTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityNotification)
	if err != nil || userTestOnly || userCredentials == nil {
		return errors.New("Notification admin User mutual TLS configuration is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		return errors.New("Notification admin database connection unavailable")
	}
	defer pool.Close()

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return errors.New("Notification admin revocation store configuration unavailable")
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	pingCtx, cancelPing := context.WithTimeout(ctx, 3*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		return errors.New("Notification admin revocation store unavailable")
	}
	cancelPing()

	userConn, err := grpc.NewClient(cfg.UserServiceAddr, grpc.WithTransportCredentials(userCredentials))
	if err != nil {
		return errors.New("Notification admin User client unavailable")
	}
	defer userConn.Close()
	userClient := userv1.NewUserServiceClient(userConn)

	reader := repository.NewPostgresNotificationRepository(pool)
	adminOutbox := service.NewAdminOutboxService(reader)
	inbox := service.NewNotificationInboxService(reader)
	server := grpc.NewServer(grpc.Creds(serverCredentials), grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	notificationv1.RegisterNotificationAdminServiceServer(server, notificationgrpc.NewAdminOutboxServer(
		adminOutbox, userClient, redisTokenRevocations{client: redisClient}, true, inbox,
	))

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return errors.New("Notification admin gRPC listen failed")
	}
	defer listener.Close()

	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	logger.Info("Notification read-only Admin outbox RPC started")
	select {
	case <-ctx.Done():
		server.GracefulStop()
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return errors.New("Notification admin gRPC serving failed")
		}
	}
	return nil
}

type redisTokenRevocations struct {
	client *redis.Client
}

func (r redisTokenRevocations) IsRevoked(ctx context.Context, token string) (bool, error) {
	if r.client == nil || token == "" {
		return false, errors.New("token revocation store unavailable")
	}
	exists, err := r.client.Exists(ctx, "blacklist:"+token).Result()
	if err != nil {
		return false, errors.New("token revocation check failed")
	}
	return exists > 0, nil
}
