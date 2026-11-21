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

	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	sagapkg "github.com/ticketbox/pkg/saga"
	saga_grpc "github.com/ticketbox/saga/internal/grpc"
	"github.com/ticketbox/saga/internal/kafka"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"github.com/ticketbox/saga/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("[SAGA Service]: cannot load config", zap.Error(err))
	}
	eventInternalTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentitySaga)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	reliabilityHooks, err := reliabilityhook.FromEnv()
	if err != nil {
		logger.Fatal("Saga reliability hook configuration invalid", zap.Error(err))
	}
	ticketE2EProject, valid := grpcsecurity.TicketE2EKafkaIdentity()
	if !valid {
		logger.Fatal("Saga TEST_ONLY Kafka topic identity does not match its embedded Ticket E2E run")
	}
	paymentCredentials, paymentTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServicePayment, grpcsecurity.ServiceIdentitySaga)
	if err != nil {
		logger.Fatal("Payment gRPC transport security initialization refused", zap.Error(err))
	}
	bookingCredentials, bookingTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentitySaga)
	if err != nil {
		logger.Fatal("Booking gRPC transport security initialization refused", zap.Error(err))
	}
	if paymentTestOnly != bookingTestOnly {
		logger.Fatal("Saga TEST_ONLY clients must both use the isolated Ticket E2E identity")
	}
	if err := validateTicketE2ERuntimeEndpoints(cfg, ticketE2EProject); err != nil {
		logger.Fatal("Saga Ticket E2E endpoint isolation refused", zap.Error(err))
	}
	telemetry, err := observability.InitializeFromConfig(context.Background(), cfg, logger)
	if err != nil {
		logger.Fatal("[SAGA Service]: cannot initialize observability", zap.Error(err))
	}
	defer telemetry.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("[SAGA Service]: Cannot create postgres poll", zap.Error(err))
	}
	// Connect to Event Service via gRPC
	clientOptions := func() []grpc.DialOption {
		return append([]grpc.DialOption{grpc.WithTransportCredentials(eventInternalTLS)}, telemetry.GRPCDialOptions()...)
	}
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, clientOptions()...)
	if err != nil {
		logger.Fatal("Failed to connect to event service", zap.Error(err))
	}
	defer eventConn.Close()
	eventClient := eventv1.NewEventServiceClient(eventConn)

	paymentConn, err := grpc.NewClient(cfg.PaymentServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(paymentCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to payment service", zap.Error(err))
	}
	defer paymentConn.Close()
	if paymentTestOnly {
		logger.Warn("TEST_ONLY plaintext simulated Payment gRPC client enabled for isolated Ticket E2E")
	}
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)

	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(bookingCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to booking service", zap.Error(err))
	}
	defer bookingConn.Close()
	if bookingTestOnly {
		logger.Warn("TEST_ONLY plaintext Booking gRPC client enabled for isolated Ticket E2E")
	}
	bookingClient := bookingv1.NewBookingServiceClient(bookingConn)

	sagaRepository := repository.NewSagaRepository(pool)
	sagaStepRegistry := registry.NewSagaStepRegistry()
	sagaService := service.NewSagaServiceWithMetrics(logger, sagaRepository, sagaStepRegistry, bookingClient, paymentClient, eventClient, telemetry.Metrics)
	if err := sagaService.ConfigureReliabilityHook(reliabilityHooks); err != nil {
		logger.Fatal("Saga reliability hook configuration invalid", zap.Error(err))
	}
	recoveryConfig, err := service.RecoveryConfigFromEnv()
	if err != nil {
		logger.Fatal("[SAGA Service]: invalid recovery configuration", zap.Error(err))
	}
	recoveryWorker, err := service.NewRecoveryWorker(sagaRepository, sagaService, logger, recoveryConfig)
	if err != nil {
		logger.Fatal("[SAGA Service]: cannot create recovery worker", zap.Error(err))
	}
	ticketIssuanceProducer := kafka.NewSagaOrchestratorProducer(cfg.KafkaBrokers, logger)
	defer ticketIssuanceProducer.Close()
	ticketIssuanceOutbox := service.NewTicketIssuanceOutboxRelay(sagaRepository, ticketIssuanceProducer, logger)
	ticketIssuanceOutboxDone := make(chan struct{})
	go func() {
		defer close(ticketIssuanceOutboxDone)
		ticketIssuanceOutbox.Run(ctx, time.Second, 25)
	}()
	sagaStepRegistry.Register(string(sagapkg.RESERVED_SEAT_STEP), registry.SagaStepProcessor{
		Execute:    sagaService.ReservedOrCompensateBatchSeats,
		Compensate: sagaService.ReservedOrCompensateBatchSeats,
	})
	sagaStepRegistry.Register(string(sagapkg.CREATE_PAYMENT_INTENT_STEP), registry.SagaStepProcessor{
		Execute:    sagaService.CreatePayment,
		Compensate: sagaService.RefundPayment,
	})

	sagaStepRegistry.Register(string(sagapkg.UPDATE_SEAT_BOOKED), registry.SagaStepProcessor{
		Execute:    sagaService.UpdateBatchSeatStatus,
		Compensate: sagaService.UpdateBatchSeatStatus,
	})

	sagaStepRegistry.Register(string(sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED), registry.SagaStepProcessor{
		Execute:    sagaService.UpdateBookingStatus,
		Compensate: sagaService.UpdateBookingStatus,
	})

	sagaStepRegistry.Register(string(sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER), registry.SagaStepProcessor{
		Execute:    sagaService.UpdateAvailableSeatNumber,
		Compensate: sagaService.UpdateAvailableSeatNumber,
	})

	serverOptions := []grpc.ServerOption{grpc.ChainUnaryInterceptor(sagaLegacyUnaryServerInterceptors(reliabilityHooks, logger)...)}
	serverOptions = append(serverOptions, telemetry.GRPCServerOptions()...)
	internalOptions, err := grpcsecurity.InternalServerOptions("saga")
	if err != nil {
		logger.Fatal("Internal RPC server transport refused", zap.Error(err))
	}
	serverOptions = append(serverOptions, internalOptions...)
	grpcServer := grpc.NewServer(serverOptions...)
	sagaGrpcServer := saga_grpc.NewSagaOrchestratorServer(sagaService, logger)
	sagav1.RegisterSagaOrchestratorServiceServer(grpcServer, sagaGrpcServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}
	ticketAuthorityListenAddr := strings.TrimSpace(os.Getenv("RUSHGO_SAGA_TICKET_AUTHORITY_LISTEN_ADDR"))
	if ticketAuthorityListenAddr == "" {
		logger.Fatal("RUSHGO_SAGA_TICKET_AUTHORITY_LISTEN_ADDR is required; Ticket eligibility has no plaintext fallback")
	}
	ticketAuthorityCredentials, err := grpcsecurity.TicketAuthorityServerCredentialsFromEnv(grpcsecurity.ServiceSaga)
	if err != nil {
		logger.Fatal("Saga Ticket authority mTLS initialization refused", zap.Error(err))
	}
	ticketAuthorityServer := grpc.NewServer(
		grpc.Creds(ticketAuthorityCredentials),
		grpc.ChainUnaryInterceptor(middleware.UnaryLoggingInterceptor(logger), grpcsecurity.TicketAuthorityMethodAuthorizationInterceptor(sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName)),
	)
	if err := grpcsecurity.RegisterServiceWithOnlyMethod(ticketAuthorityServer, sagav1.SagaOrchestratorService_ServiceDesc, sagaGrpcServer, sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName); err != nil {
		logger.Fatal("Failed to register Saga Ticket authority method", zap.Error(err))
	}
	ticketAuthorityListener, err := net.Listen("tcp", ticketAuthorityListenAddr)
	if err != nil {
		logger.Fatal("Failed to listen for Saga Ticket authority", zap.Error(err))
	}
	go func() {
		if err := ticketAuthorityServer.Serve(ticketAuthorityListener); shouldLogGRPCServeFailure(err) {
			logger.Fatal("Saga Ticket authority gRPC serve failed", zap.Error(err))
		}
	}()
	logger.Info("Saga service has started", zap.String("port", cfg.GRPCPort))
	logger.Info("Saga Ticket authority mTLS listener started", zap.String("address", ticketAuthorityListenAddr))

	go func() {
		if err := grpcServer.Serve(lis); shouldLogGRPCServeFailure(err) {
			logger.Fatal("gRPC serve failed", zap.Error(err))
		}
	}()

	sagaComsumer := kafka.NewSagaOrchestratorConsumer(cfg.KafkaBrokers, sagaService, logger)

	consumerDone := make(chan error, 1)
	go func() { consumerDone <- sagaComsumer.Start(ctx) }()
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- recoveryWorker.Start(ctx) }()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan
	logger.Info("Saga service is shutting down...")
	cancel()
	grpcDone := gracefulStopAsync(grpcServer)
	ticketAuthorityDone := gracefulStopAsync(ticketAuthorityServer)
	shutdownTimer := time.NewTimer(5 * time.Second)
	defer shutdownTimer.Stop()
	grpcStopped, ticketAuthorityStopped, consumerStopped, recoveryStopped, ticketOutboxStopped := false, false, false, false, false
	grpcWait, ticketAuthorityWait, consumerWait, recoveryWait, ticketOutboxWait := grpcDone, ticketAuthorityDone, consumerDone, recoveryDone, ticketIssuanceOutboxDone
	for !grpcStopped || !ticketAuthorityStopped || !consumerStopped || !recoveryStopped || !ticketOutboxStopped {
		select {
		case <-grpcWait:
			grpcStopped = true
			grpcWait = nil
		case <-ticketAuthorityWait:
			ticketAuthorityStopped = true
			ticketAuthorityWait = nil
		case err := <-consumerWait:
			consumerStopped = true
			consumerWait = nil
			if err != nil {
				logger.Warn("Saga consumer stopped with an error", zap.Error(err))
			}
		case err := <-recoveryWait:
			recoveryStopped = true
			recoveryWait = nil
			if err != nil {
				logger.Warn("Saga recovery worker stopped with an error", zap.Error(err))
			}
		case <-ticketOutboxWait:
			ticketOutboxStopped = true
			ticketOutboxWait = nil
		case <-shutdownTimer.C:
			logger.Warn("Saga service shutdown deadline elapsed")
			grpcServer.Stop()
			ticketAuthorityServer.Stop()
			return
		}
	}
}
