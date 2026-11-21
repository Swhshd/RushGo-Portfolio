package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ticketbox/gateway/internal/realtime"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"

	"github.com/ticketbox/gateway/internal/router"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	eventInternalTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	schedulerInternalTLS, err := grpcsecurity.InternalClientCredentials("scheduler", grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	if err := validateGatewayProxyConfig(cfg); err != nil {
		logger.Fatal("Invalid trusted proxies configuration")
	}
	userCredentials, userTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		logger.Fatal("User gRPC transport security initialization refused", zap.Error(err))
	}
	bookingCredentials, bookingTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		logger.Fatal("Booking gRPC transport security initialization refused", zap.Error(err))
	}
	paymentCredentials, paymentTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServicePayment, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		logger.Fatal("Payment gRPC transport security initialization refused", zap.Error(err))
	}
	telemetry, err := observability.InitializeFromConfig(context.Background(), cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := telemetry.Shutdown(shutdownCtx); err != nil {
			logger.Warn("Failed to shut down observability", zap.Error(err))
		}
	}()

	dialOptions := func(target string) []grpc.DialOption {
		transport := eventInternalTLS
		if target != "event" {
			transport = schedulerInternalTLS
		}
		return append([]grpc.DialOption{
			grpc.WithTransportCredentials(transport),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 * 1024 * 1024)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16 * 1024 * 1024)),
		}, telemetry.GRPCDialOptions()...)
	}

	// Connect to gRPC services
	userDial := []grpc.DialOption{grpc.WithTransportCredentials(userCredentials), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 * 1024 * 1024)), grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16 * 1024 * 1024))}
	userConn, err := grpc.NewClient(cfg.UserServiceAddr, append(userDial, telemetry.UserAuthGRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to user service", zap.Error(err))
	}
	defer userConn.Close()
	if userTestOnly {
		logger.Warn("TEST_ONLY plaintext simulated User gRPC client enabled for isolated Ticket E2E")
	}

	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, dialOptions("event")...)
	if err != nil {
		logger.Fatal("Failed to connect to event service", zap.Error(err))
	}
	defer eventConn.Close()

	bookingDialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(bookingCredentials),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 * 1024 * 1024)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16 * 1024 * 1024)),
	}
	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, append(bookingDialOptions, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to booking service", zap.Error(err))
	}
	defer bookingConn.Close()
	if bookingTestOnly {
		logger.Warn("TEST_ONLY plaintext Booking gRPC client enabled for isolated Ticket E2E")
	}

	paymentDialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(paymentCredentials),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 * 1024 * 1024)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16 * 1024 * 1024)),
	}
	paymentConn, err := grpc.NewClient(cfg.PaymentServiceAddr, append(paymentDialOptions, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to payment service", zap.Error(err))
	}
	defer paymentConn.Close()
	if paymentTestOnly {
		logger.Warn("TEST_ONLY plaintext simulated Payment gRPC client enabled for isolated Ticket E2E")
	}

	schedulerConn, err := grpc.NewClient(cfg.SchedulerServiceAddr, dialOptions("scheduler")...)
	if err != nil {
		logger.Fatal("Failed to connect to scheduler service", zap.Error(err))
	}
	defer schedulerConn.Close()

	var notificationClient notificationv1.NotificationAdminServiceClient
	notificationTestOnly := false
	notificationAddr := strings.TrimSpace(os.Getenv("NOTIFICATION_SERVICE_ADDR"))
	if notificationAddr != "" {
		notificationCredentials, testOnly, credentialsErr := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceNotification, grpcsecurity.ServiceIdentityGateway)
		if credentialsErr != nil || testOnly {
			logger.Warn("Notification admin outbox mTLS unavailable; Notification source disabled", zap.Error(credentialsErr))
		} else {
			notificationConn, dialErr := grpc.NewClient(notificationAddr, append([]grpc.DialOption{
				grpc.WithTransportCredentials(notificationCredentials),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 * 1024 * 1024)),
				grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16 * 1024 * 1024)),
			}, telemetry.GRPCDialOptions()...)...)
			if dialErr != nil {
				logger.Warn("Notification admin outbox connection unavailable; Notification source disabled", zap.Error(dialErr))
			} else {
				defer notificationConn.Close()
				notificationClient = notificationv1.NewNotificationAdminServiceClient(notificationConn)
				notificationTestOnly = testOnly
			}
		}
	}

	userClient := userv1.NewUserServiceClient(userConn)
	eventClient := eventv1.NewEventServiceClient(eventConn)
	bookingClient := bookingv1.NewBookingServiceClient(bookingConn)
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)
	schedulerClient := schedulerv1.NewSchedulerServiceClient(schedulerConn)
	// Connect to Redis
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Fatal("Failed to parse Redis URL", zap.Error(err))
	}
	redisClient := redis.NewClient(opt)
	defer redisClient.Close()
	redisPubSubClient := redis.NewClient(opt)
	defer redisPubSubClient.Close()
	realtimeHub := realtime.NewHubWithTelemetry(redisPubSubClient, telemetry.Metrics, telemetry.Propagator)

	// Setup router
	requireRequestID := true
	if raw := os.Getenv("BOOKING_REQUIRE_IDEMPOTENCY_KEY"); raw != "" {
		required, err := strconv.ParseBool(raw)
		if err != nil {
			logger.Fatal("Invalid Booking key requirement")
		}
		requireRequestID = required
	}
	secureAdminOutbox := adminOutboxSecureWiring(bookingTestOnly, userTestOnly)
	secureNotificationOutbox := notificationAdminOutboxSecureWiring(notificationClient != nil, notificationTestOnly, userTestOnly)
	r, err := router.SetupRouterWithNotificationOutboxSecurity(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, cfg.TrustedProxies, requireRequestID, secureAdminOutbox, notificationClient, secureNotificationOutbox, telemetry)
	if err != nil {
		logger.Fatal("Invalid trusted proxies configuration")
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.HTTPPort),
		Handler: r,
	}

	go func() {
		logger.Info("Gateway started", zap.String("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server failed", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gateway...")
	realtimeHub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
