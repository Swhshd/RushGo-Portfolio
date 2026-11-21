package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ticketbox/event/internal/admission"
	eventkafka "github.com/ticketbox/event/internal/kafka"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/observability"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/pkg/reliabilityhook"

	eventgrpc "github.com/ticketbox/event/internal/grpc"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	if err := grpcsecurity.ValidateTicketE2ERuntimeIdentity("ticket-e2e-event"); err != nil {
		logger.Fatal("Ticket E2E Event runtime identity refused", zap.Error(err))
	}
	if err := grpcsecurity.ValidateTicketE2ERuntimeResources("ticket-e2e-event", grpcsecurity.TicketE2ERuntimeResources{
		DatabaseURL:  cfg.DatabaseURL,
		RedisURL:     cfg.RedisURL,
		KafkaBrokers: cfg.KafkaBrokers,
	}); err != nil {
		logger.Fatal("Ticket E2E Event resource identity refused", zap.Error(err))
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	eventRepo := repository.NewPostgresEventRepository(pool)
	interestRepo := repository.NewPostgresEventInterestRepository(pool)
	reminderProducer := eventkafka.NewEventReminderProducer(cfg.KafkaBrokers, logger)
	defer reminderProducer.Close()
	refundTimeRepo := repository.NewPostgresRefundTimeRepository(pool)
	tierRepo := repository.NewPostgresTicketTierRepository(pool)
	seatRepo := repository.NewPostgresSeatRepository(pool)
	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Fatal("Failed to parse Redis URL", zap.Error(err))
	}
	redisOptions.ContextTimeoutEnabled = true
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	repairHook, err := reliabilityhook.FromEnv()
	if err != nil {
		logger.Fatal("Failed to initialize reliability repair hook", zap.Error(err))
	}
	seatAdmission := admission.NewRedisAdmission(redisClient)
	reconciler := admission.NewReconcilerWithLogger(seatAdmission, seatRepo, logger, telemetry.Metrics)
	workerCount := 1
	repairDone := make(chan struct{}, 2)
	if repairHook.Enabled() {
		if err := reconciler.SetAdmissionRepairBarrier(repairHook, "sweeper-1"); err != nil {
			logger.Fatal("Failed to configure first admission repair worker", zap.Error(err))
		}
		secondOptions, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			logger.Fatal("Failed to parse Redis URL for second repair worker", zap.Error(err))
		}
		secondOptions.ContextTimeoutEnabled = true
		secondRedisClient := redis.NewClient(secondOptions)
		defer secondRedisClient.Close()
		second := admission.NewReconcilerWithLogger(
			admission.NewRedisAdmission(secondRedisClient), seatRepo, logger, telemetry.Metrics)
		if err := second.SetAdmissionRepairBarrier(repairHook, "sweeper-2"); err != nil {
			logger.Fatal("Failed to configure second admission repair worker", zap.Error(err))
		}
		workerCount++
		go func() { _ = second.Start(ctx); repairDone <- struct{}{} }()
	}
	go func() { _ = reconciler.Start(ctx); repairDone <- struct{}{} }()
	reminderDone := make(chan struct{}, 2)
	go func() {
		defer func() { reminderDone <- struct{}{} }()
		collectDueSaleTransitions(ctx, interestRepo, logger)
	}()
	go func() {
		defer func() { reminderDone <- struct{}{} }()
		relayEventReminderOutbox(ctx, interestRepo, reminderProducer, logger)
	}()
	mode := admission.Mode(cfg.SeatAdmissionMode)
	seatRealtimePublisher := service.NewRedisSeatRealtimePublisher(redisClient)
	eventService := service.NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(
		eventRepo, tierRepo, seatRepo, logger, seatAdmission, mode, seatRealtimePublisher, telemetry.Metrics,
	)
	interestService := service.NewEventInterestService(eventService, interestRepo)

	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(middleware.UnaryLoggingInterceptor(logger), grpcsecurity.RejectTicketAuthorityMethodsOnLegacyListener()),
		grpc.MaxRecvMsgSize(16 * 1024 * 1024),
		grpc.MaxSendMsgSize(16 * 1024 * 1024),
	}
	serverOptions = append(serverOptions, telemetry.GRPCServerOptions()...)
	internalOptions, err := grpcsecurity.InternalServerOptions("event")
	if err != nil {
		logger.Fatal("Internal RPC server transport refused", zap.Error(err))
	}
	serverOptions = append(serverOptions, internalOptions...)
	grpcServer := grpc.NewServer(serverOptions...)
	eventServer := eventgrpc.NewEventServer(eventService, logger)
	eventServer.SetEventInterestService(interestService)
	eventServer.SetRefundTimeAuthority(refundTimeRepo)
	eventv1.RegisterEventServiceServer(grpcServer, eventServer)
	reflection.Register(grpcServer)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}
	ticketAuthorityListenAddr := strings.TrimSpace(os.Getenv("RUSHGO_EVENT_TICKET_AUTHORITY_LISTEN_ADDR"))
	if ticketAuthorityListenAddr == "" {
		logger.Fatal("RUSHGO_EVENT_TICKET_AUTHORITY_LISTEN_ADDR is required; Ticket eligibility has no plaintext fallback")
	}
	ticketAuthorityCredentials, err := grpcsecurity.TicketAuthorityServerCredentialsFromEnv(grpcsecurity.ServiceEvent)
	if err != nil {
		logger.Fatal("Event Ticket authority mTLS initialization refused", zap.Error(err))
	}
	ticketAuthorityServer := grpc.NewServer(
		grpc.Creds(ticketAuthorityCredentials),
		grpc.ChainUnaryInterceptor(middleware.UnaryLoggingInterceptor(logger), grpcsecurity.TicketAuthorityMethodsAuthorizationInterceptor(
			eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName,
			eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName,
			eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName,
		)),
	)
	if err := grpcsecurity.RegisterServiceWithOnlyMethods(ticketAuthorityServer, eventv1.EventService_ServiceDesc, eventServer,
		eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName,
		eventv1.EventService_AcquireUnusedRefundTimePermit_FullMethodName,
		eventv1.EventService_ReleaseUnusedRefundTimePermit_FullMethodName,
	); err != nil {
		logger.Fatal("Failed to register Event authority methods", zap.Error(err))
	}
	ticketAuthorityListener, err := net.Listen("tcp", ticketAuthorityListenAddr)
	if err != nil {
		logger.Fatal("Failed to listen for Event Ticket authority", zap.Error(err))
	}
	ticketAuthorityServeDone := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		<-sigCh
		logger.Info("Shutting down event service...")
		cancel()
		stopGRPCServersAndWaitForServe(5*time.Second, ticketAuthorityServeDone, grpcServer, ticketAuthorityServer)
		close(shutdownDone)
	}()

	go func() {
		defer close(ticketAuthorityServeDone)
		if err := ticketAuthorityServer.Serve(ticketAuthorityListener); shouldLogGRPCServeFailure(err) {
			logger.Fatal("Event Ticket authority gRPC serve failed", zap.Error(err))
		}
	}()

	logger.Info("Event service started", zap.String("port", cfg.GRPCPort))
	logger.Info("Event authority mTLS listener started", zap.String("address", ticketAuthorityListenAddr))
	if err := grpcServer.Serve(lis); shouldLogGRPCServeFailure(err) {
		logger.Fatal("gRPC serve failed", zap.Error(err))
	}
	cancel()
	shutdownDeadline := time.NewTimer(4 * time.Second)
	defer shutdownDeadline.Stop()
	remainingRepair, remainingReminder := workerCount, 2
	for remainingRepair+remainingReminder > 0 {
		select {
		case <-repairDone:
			remainingRepair--
		case <-reminderDone:
			remainingReminder--
		case <-shutdownDeadline.C:
			logger.Warn("event background worker shutdown timed out")
			remainingRepair, remainingReminder = 0, 0
		}
	}
	<-shutdownDone
}

func collectDueSaleTransitions(ctx context.Context, repo *repository.PostgresEventInterestRepository, logger *zap.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if count, err := repo.CollectDueSalesTransitions(ctx, 100); err != nil && ctx.Err() == nil {
			logger.Warn("scheduled event sale transition collection failed", zap.Error(err))
		} else if count > 0 {
			logger.Info("scheduled event sale transitions collected", zap.Int("events", count))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func relayEventReminderOutbox(ctx context.Context, repo *repository.PostgresEventInterestRepository, producer *eventkafka.EventReminderProducer, logger *zap.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if count, err := repo.ProcessPendingReminderFanoutPage(ctx, 100); err != nil && ctx.Err() == nil {
			logger.Warn("event reminder fanout page failed", zap.Error(err))
		} else if count > 0 {
			logger.Info("event reminder fanout subscriptions expanded", zap.Int("subscriptions", count))
		}
		_, err := repo.ProcessPendingReminderOutbox(ctx, 100, func(ctx context.Context, item repository.ReminderOutboxEvent) error {
			publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return producer.Publish(publishCtx, item)
		})
		if err != nil && ctx.Err() == nil {
			logger.Warn("event reminder outbox relay failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
