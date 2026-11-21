package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	bookinggrpc "github.com/ticketbox/booking/internal/grpc"
	bookingkafka "github.com/ticketbox/booking/internal/kafka"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/observability"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	eventInternalTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	sagaInternalTLS, err := grpcsecurity.InternalClientCredentials("saga", grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	reliabilityHook, err := reliabilityhook.FromEnv()
	if err != nil {
		logger.Fatal("Invalid isolated reliability-hook configuration", zap.Error(err))
	}
	if err := validateTicketE2EStartup(cfg); err != nil {
		logger.Fatal("Booking startup validation refused", zap.Error(err))
	}
	if _, valid := grpcsecurity.TicketE2EKafkaIdentity(); !valid {
		logger.Fatal("Booking TEST_ONLY Kafka topic/group identity does not match its embedded Ticket E2E run")
	}
	bookingTransport, testOnlyPlaintext, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServiceBooking)
	if err != nil {
		logger.Fatal("Booking gRPC transport security initialization refused", zap.Error(err))
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

	userCredentials, userTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceUser, grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		logger.Fatal("User gRPC transport security initialization refused", zap.Error(err))
	}
	userConn, err := grpc.NewClient(cfg.UserServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(userCredentials)}, telemetry.UserAuthGRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to user service", zap.Error(err))
	}
	defer userConn.Close()
	if userTestOnly {
		logger.Warn("TEST_ONLY plaintext simulated User gRPC client enabled for isolated Ticket E2E")
	}
	userClient := userv1.NewUserServiceClient(userConn)

	// Connect to Event Service via gRPC
	eventDialOptions := append([]grpc.DialOption{grpc.WithTransportCredentials(eventInternalTLS)}, telemetry.GRPCDialOptions()...)
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, eventDialOptions...)
	if err != nil {
		logger.Fatal("Failed to connect to event service", zap.Error(err))
	}
	defer eventConn.Close()
	eventClient := eventv1.NewEventServiceClient(eventConn)
	sagaConn, err := grpc.NewClient(cfg.SagaServiceAddr, bookingSagaClientDialOptions(reliabilityHook,
		append([]grpc.DialOption{grpc.WithTransportCredentials(sagaInternalTLS)}, telemetry.GRPCDialOptions()...)...)...)
	if err != nil {
		logger.Fatal("Failed to connect to saga service", zap.Error(err))
	}
	defer sagaConn.Close()
	sagaClient := sagav1.NewSagaOrchestratorServiceClient(sagaConn)
	if strings.TrimSpace(cfg.EventTicketAuthorityAddr) == "" || strings.TrimSpace(cfg.SagaTicketAuthorityAddr) == "" {
		logger.Fatal("Ticket authority gRPC targets must be configured; plaintext fallback is disabled")
	}
	eventTicketCredentials, err := grpcsecurity.TicketAuthorityClientCredentialsFromEnv(grpcsecurity.ServiceEvent)
	if err != nil {
		logger.Fatal("Event Ticket authority mTLS initialization refused", zap.Error(err))
	}
	eventTicketConn, err := grpc.NewClient(cfg.EventTicketAuthorityAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(eventTicketCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to initialize Event Ticket authority mTLS client", zap.Error(err))
	}
	defer eventTicketConn.Close()
	eventTicketClient := eventv1.NewEventServiceClient(eventTicketConn)
	sagaTicketCredentials, err := grpcsecurity.TicketAuthorityClientCredentialsFromEnv(grpcsecurity.ServiceSaga)
	if err != nil {
		logger.Fatal("Saga Ticket authority mTLS initialization refused", zap.Error(err))
	}
	sagaTicketConn, err := grpc.NewClient(cfg.SagaTicketAuthorityAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(sagaTicketCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to initialize Saga Ticket authority mTLS client", zap.Error(err))
	}
	defer sagaTicketConn.Close()
	sagaTicketClient := sagav1.NewSagaOrchestratorServiceClient(sagaTicketConn)
	paymentCredentials, paymentTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServicePayment, grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		logger.Fatal("Payment gRPC transport security initialization refused", zap.Error(err))
	}
	paymentConn, err := grpc.NewClient(cfg.PaymentServiceAddr, append([]grpc.DialOption{grpc.WithTransportCredentials(paymentCredentials)}, telemetry.GRPCDialOptions()...)...)
	if err != nil {
		logger.Fatal("Failed to connect to payment service", zap.Error(err))
	}
	defer paymentConn.Close()
	if paymentTestOnly {
		logger.Warn("TEST_ONLY plaintext simulated Payment gRPC client enabled for isolated Ticket E2E")
	}
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)
	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Fatal("Failed to parse Redis URL", zap.Error(err))
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()

	bookingRepo := repository.NewPostgresBookingRepository(pool)
	bookingEventProducer := bookingkafka.NewBookingEventProducer(cfg.KafkaBrokers, logger)
	defer bookingEventProducer.Close()
	bookingService := service.NewBookingService(bookingRepo, eventClient, sagaClient, logger)
	bookingService.SetCheckoutPaymentClient(paymentClient)
	if err := bookingService.ConfigureReliabilityHook(reliabilityHook); err != nil {
		logger.Fatal("Cannot configure isolated reliability hook", zap.Error(err))
	}
	ticketAuthority := service.NewGRPCTicketAuthority(eventTicketClient, sagaTicketClient, paymentClient)
	ticketIssuanceService := service.NewTicketIssuanceService(bookingRepo, ticketAuthority)
	ticketManager := service.NewTicketManager(bookingRepo, ticketAuthority)
	// Admission has no plaintext test exception. Active coordination fails closed
	// without this authenticated client, including legacy Ticket E2E processes.
	if !paymentTestOnly {
		ticketManager.SetAdmissionCoordinator(admission.NewCoordinatorClient(paymentConn))
	}
	ticketConsumer := bookingkafka.NewTicketIssuanceConsumer(cfg.KafkaBrokers, "booking-ticket-issuance-v1", ticketIssuanceService, logger)
	ticketConsumerDone := make(chan error, 1)
	go func() { ticketConsumerDone <- ticketConsumer.Start(ctx) }()
	bookingService.RequireRequestID = true
	if raw := os.Getenv("BOOKING_REQUIRE_IDEMPOTENCY_KEY"); raw != "" {
		required, err := strconv.ParseBool(raw)
		if err != nil {
			logger.Fatal("Invalid Booking key requirement")
		}
		bookingService.RequireRequestID = required
	}
	dispatchConfig, err := service.DispatchConfigFromEnv()
	if err != nil {
		logger.Fatal("Invalid Booking dispatch config", zap.Error(err))
	}
	if err := bookingService.ConfigureDispatch(dispatchConfig); err != nil {
		logger.Fatal("Invalid Booking dispatch config", zap.Error(err))
	}
	dispatchWorker, err := service.NewDispatchWorker(bookingRepo, bookingService, logger)
	if err != nil {
		logger.Fatal("Cannot initialize Booking dispatch", zap.Error(err))
	}
	dispatchDone := make(chan error, 1)
	go func() { dispatchDone <- dispatchWorker.Start(ctx) }()
	var refundPayment service.RefundPaymentAuthority
	var refundClient paymentv1.PaymentServiceClient
	if !paymentTestOnly {
		refundPayment = unusedRefundPaymentClient{client: paymentClient}
		refundClient = paymentClient
	}
	// Event grants durable pre-start qualification; Booking retains final decision ownership.
	refundService := service.NewUnusedTicketRefundService(bookingRepo, unusedRefundEventClient{client: eventTicketClient}, refundPayment)
	refundDone := make(chan struct{})
	go func() { defer close(refundDone); refundService.Run(ctx) }()
	outboxRelay := service.NewOutboxRelay(bookingRepo, unusedRefundPublisher{fallback: bookingEventProducer, payment: refundClient}, logger, telemetry.Metrics)
	go outboxRelay.Run(ctx, time.Second, 100)
	if cfg.OTEL.Enabled {
		go bookingRepo.RunOperationsMetrics(ctx)
		go bookingService.RunOperationsMetrics(ctx)
	}

	if testOnlyPlaintext {
		logger.Warn("TEST_ONLY plaintext Booking gRPC transport enabled for isolated Ticket E2E")
	}
	serverOptions := []grpc.ServerOption{
		grpc.Creds(bookingTransport),
		grpc.ChainUnaryInterceptor(bookingUnaryServerInterceptors(reliabilityHook, logger)...),
	}
	serverOptions = append(serverOptions, telemetry.GRPCServerOptions()...)
	grpcServer := grpc.NewServer(serverOptions...)
	closeTicketE2ESimulatedAuthority, err := registerTicketE2ESimulatedAuthority(ctx, grpcServer, cfg, logger)
	if err != nil {
		logger.Fatal("Ticket E2E authority initialization refused", zap.Error(err))
	}
	defer closeTicketE2ESimulatedAuthority()
	bookingServer := bookinggrpc.NewBookingServer(bookingService, logger, ticketManager)
	ticketAuthenticator := bookinggrpc.NewTicketPrincipalAuthenticator(userClient, redisTicketTokenRevocations{client: redisClient})
	bookingServer.SetTicketPrincipalAuthenticator(ticketAuthenticator)
	bookingServer.SetUnusedTicketRefundService(refundService, adminOutboxSecureWiring(testOnlyPlaintext, userTestOnly) && !paymentTestOnly)
	bookingServer.SetAdminOutboxPrincipalAuthenticator(bookinggrpc.NewAdminOutboxPrincipalAuthenticator(ticketAuthenticator, userClient))
	bookingServer.SetAdminOutboxService(service.NewAdminOutboxService(bookingRepo))
	bookingServer.SetAdminOutboxSecure(adminOutboxSecureWiring(testOnlyPlaintext, userTestOnly))
	bookingServer.SetAdminEventTicketOperationsService(service.NewAdminEventTicketOperationsService(bookingRepo))
	bookingServer.SetAdminEventTicketOperationsSecure(adminOutboxSecureWiring(testOnlyPlaintext, userTestOnly))
	bookingServer.SetAdminBookingDetailSecure(adminOutboxSecureWiring(testOnlyPlaintext, userTestOnly))
	bookingServer.SetAdminEventTicketListService(service.NewAdminEventTicketListService(bookingRepo))
	bookingServer.SetAdminEventTicketListSecure(adminOutboxSecureWiring(testOnlyPlaintext, userTestOnly))
	bookingv1.RegisterBookingServiceServer(grpcServer, bookingServer)
	admissionv1.RegisterBookingAdmissionAuthorityServer(grpcServer, bookinggrpc.NewAdmissionServer(bookingRepo))
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	logger.Info("Booking service started", zap.String("port", cfg.GRPCPort))
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(lis) }()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
	case err := <-serveDone:
		if err != nil {
			logger.Error("gRPC serve failed", zap.Error(err))
		}
	}
	logger.Info("Shutting down booking service...")
	cancel()
	grpcDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcDone) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for grpcDone != nil || dispatchDone != nil || ticketConsumerDone != nil || refundDone != nil {
		select {
		case <-grpcDone:
			grpcDone = nil
		case <-refundDone:
			refundDone = nil
		case err := <-dispatchDone:
			dispatchDone = nil
			if err != nil {
				logger.Warn("Booking dispatch stopped", zap.Error(err))
			}
		case err := <-ticketConsumerDone:
			ticketConsumerDone = nil
			if err != nil {
				logger.Warn("Ticket issuance consumer stopped", zap.Error(err))
			}
		case <-timer.C:
			grpcServer.Stop()
			return
		}
	}
}
