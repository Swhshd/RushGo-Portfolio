package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	redis_pkg "github.com/ticketbox/pkg/redis"
	"github.com/ticketbox/pkg/reliabilityhook"
	schedulergrpc "github.com/ticketbox/scheduler/internal/grpc"
	cronjob "github.com/ticketbox/scheduler/internal/jobs"
	"github.com/ticketbox/scheduler/internal/kafka"
	"github.com/ticketbox/scheduler/internal/repository"
	service_outbound "github.com/ticketbox/scheduler/internal/service/outbound"
	service_scheduler "github.com/ticketbox/scheduler/internal/service/scheduler"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	eventInternalTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentityScheduler)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	sagaInternalTLS, err := grpcsecurity.InternalClientCredentials("saga", grpcsecurity.ServiceIdentityScheduler)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	bookingCredentials, _, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityScheduler)
	if err != nil {
		logger.Fatal("Booking gRPC transport security initialization refused", zap.Error(err))
	}
	paymentCredentials, _, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServicePayment, grpcsecurity.ServiceIdentityScheduler)
	if err != nil {
		logger.Fatal("Payment gRPC transport security initialization refused", zap.Error(err))
	}
	telemetry, err := observability.InitializeFromConfig(context.Background(), cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize observability", zap.Error(err))
	}
	defer telemetry.Shutdown(context.Background())
	reliabilityHooks, err := reliabilityhook.FromEnv()
	if err != nil {
		logger.Fatal("Failed to initialize reliability hooks", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	redisCln, err := newSchedulerRedisClient(cfg.RedisURL)
	if err != nil {
		logger.Fatal("Invalid Redis configuration")
	}
	defer redisCln.Close()

	redisClient := redis_pkg.NewClient(redisCln)

	schedulerCfgRepo := repository.NewSchedulerConfigRepo(pool)
	outboundEventRepo := repository.NewOutboundEventRepository(pool)
	uow := repository.NewUnitOfWork(pool)

	cronManager := cronjob.NewCronjobManager(logger, schedulerCfgRepo, cronjob.WithSecond, cronjob.WithCronjobErrHandler)
	if err := cronManager.ConfigureReliabilityHook(reliabilityHooks); err != nil {
		logger.Fatal("Failed to configure Scheduler reload reliability hook", zap.Error(err))
	}

	schedulerSvc := service_scheduler.NewSchedulerService(logger, schedulerCfgRepo, outboundEventRepo, uow, cronManager)
	if err := schedulerSvc.ConfigureReliabilityHook(reliabilityHooks); err != nil {
		logger.Fatal("Failed to configure Scheduler reliability hooks", zap.Error(err))
	}
	outboundEventProducer := kafka.NewOutboundEventProducer(logger, cfg.KafkaBrokers)
	relay := service_outbound.NewRelay(outboundEventRepo, outboundEventProducer.Producer(), logger)
	if err := relay.ConfigureReliabilityHook(reliabilityHooks); err != nil {
		logger.Fatal("Failed to configure Scheduler relay reliability hook", zap.Error(err))
	}
	defer outboundEventProducer.Producer().Close()
	relayDone := make(chan struct{})
	schedulerServer := schedulergrpc.NewSchedulerServiceServer(schedulerSvc, logger)
	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(middleware.UnaryLoggingInterceptor(logger)),
	}
	serverOptions = append(serverOptions, telemetry.GRPCServerOptions()...)
	internalOptions, err := grpcsecurity.InternalServerOptions("scheduler")
	if err != nil {
		logger.Fatal("Internal RPC server transport refused", zap.Error(err))
	}
	serverOptions = append(serverOptions, internalOptions...)
	grpcServer := grpc.NewServer(serverOptions...)
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, schedulerServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Fail to listen", zap.Error(err))
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("Shutting down scheduler service...")
		grpcServer.GracefulStop()
		if err := cronManager.Stop(); err != nil {
			logger.Error("Fail to stop cron manager", zap.Error(err))
		}
		cancel()
	}()

	schedulerConfigConsumer := kafka.NewSchedulerConfigConsumer(cfg.KafkaBrokers, schedulerSvc, logger)

	go func() { defer close(relayDone); _ = relay.Start(ctx) }()

	// Cronjob init is synchronous on purpose: robfig cron Start() is already
	// non-blocking, so no extra goroutine is needed. Fail fast here so the
	// service never looks healthy while jobs are silently dead.
	if err := cronManager.LoadJobSchedulerConfigs(ctx); err != nil {
		logger.Fatal("Fail to load job scheduler configs", zap.Error(err))
	}

	// Connect to event service client
	clientOptions := func(target string) []grpc.DialOption {
		transport := eventInternalTLS
		if target != "event" {
			transport = sagaInternalTLS
		}
		return append([]grpc.DialOption{grpc.WithTransportCredentials(transport)}, telemetry.GRPCDialOptions()...)
	}
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, clientOptions("event")...)
	if err != nil {
		logger.Fatal("Failed to connect to event service", zap.Error(err))
	}
	defer eventConn.Close()

	eventClient := eventv1.NewEventServiceClient(eventConn)

	// Connect to booking service client
	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(bookingCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to booking service", zap.Error(err))
	}
	defer bookingConn.Close()

	bookingClient := bookingv1.NewBookingServiceClient(bookingConn)

	// Connect to payment service client
	paymentConn, err := grpc.NewClient(cfg.PaymentServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(paymentCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to payment service", zap.Error(err))
	}
	defer paymentConn.Close()

	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)

	sagaConn, err := grpc.NewClient(cfg.SagaServiceAddr, clientOptions("saga")...)
	if err != nil {
		logger.Fatal("Failed to connect to saga service", zap.Error(err))
	}
	defer sagaConn.Close()
	sagaClient := sagav1.NewSagaOrchestratorServiceClient(sagaConn)

	// Register job
	reservationCleanerJob := cronjob.NewReservationCleanerJob(eventClient, bookingClient, paymentClient, sagaClient, redisClient, logger, telemetry.Metrics)
	if err := cronManager.RegisterJob(ctx, reservationCleanerJob); err != nil {
		logger.Fatal("Fail to register reservation cleaner job", zap.Error(err))
	}
	// .... continue register here

	cronManager.Start()
	go func() {
		if err := schedulerConfigConsumer.Start(ctx); err != nil {
			logger.Error("scheduler config consumer stopped", zap.Error(err))
		}
	}()
	go cronManager.StartReload(ctx)
	logger.Info("Scheduler service started", zap.String("port", cfg.GRPCPort))
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("gRPC server failed", zap.Error(err))
	}
	cancel()
	select {
	case <-relayDone:
	case <-time.After(12 * time.Second):
		logger.Warn("scheduler relay shutdown timeout")
	}
}
