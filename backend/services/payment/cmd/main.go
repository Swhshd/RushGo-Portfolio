package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	stripe_go "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/gateway/stripe"
	payment_grpc "github.com/ticketbox/payment/internal/grpc"
	payment_http "github.com/ticketbox/payment/internal/http"
	"github.com/ticketbox/payment/internal/kafka"
	"github.com/ticketbox/payment/internal/paymentruntime"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/observability"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
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
	eventInternalTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentityPayment)
	if err != nil {
		logger.Fatal("Internal RPC transport configuration refused", zap.Error(err))
	}
	if cfg.Release.External() && cfg.ServiceName != "payment-service" {
		logger.Fatal("Payment deployment requires payment-service identity")
	}
	reliabilityHooks, err := reliabilityhook.FromEnv()
	if err != nil {
		logger.Fatal("Payment reliability hook configuration invalid", zap.Error(err))
	}
	bookingCredentials, bookingTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityPayment)
	if err != nil {
		logger.Fatal("Booking gRPC transport security initialization refused", zap.Error(err))
	}
	if bookingTestOnly {
		logger.Fatal("Payment admission requires authenticated Booking transport")
	}
	paymentTransport, _, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServicePayment)
	if err != nil {
		logger.Fatal("Payment gRPC transport security initialization refused", zap.Error(err))
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

	backend := stripe.NewRefundAuditAPIBackend()
	stripeClient := stripe_go.NewClient(cfg.StripeSecretKey, stripe_go.WithBackends(&stripe_go.Backends{API: backend}))

	paymentProducer := kafka.NewPaymentProducer(cfg.KafkaBrokers, logger)
	defer paymentProducer.Close()

	stripeGateway := stripe.NewStripePaymentGateway(logger, cfg.StripeSecretWebhook, paymentProducer, stripeClient, telemetry.Metrics)

	paymentRepo := repository.NewPostgresPaymentRepository(pool)
	paymentService := service.NewPaymentService(paymentRepo, logger, stripeGateway)
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Second)
	account, err := stripeGateway.VerifyTestAccount(verifyCtx, cfg.StripeSecretKey, os.Getenv("RUSHGO_STRIPE_EXPECTED_ACCOUNT"))
	verifyCancel()
	if err != nil {
		logger.Fatal("managed Stripe TEST writer activation failed", zap.Error(err))
	}
	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, grpc.WithTransportCredentials(bookingCredentials))
	if err != nil {
		logger.Fatal("Booking authority client unavailable")
	}
	defer bookingConn.Close()
	paymentService.SetFinancialAuthority(bookingv1.NewBookingServiceClient(bookingConn), account, false)
	admissionCoordinator := service.NewAdmissionCoordinator(paymentRepo, paymentService, admission.NewOutcomeClient(bookingConn))
	stripeGateway.SetAdmissionReadBackend(backend, cfg.StripeSecretKey)
	if cfg.EventServiceAddr == "" {
		logger.Fatal("Event accounting authority unavailable")
	}
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, grpc.WithTransportCredentials(eventInternalTLS))
	if err != nil {
		logger.Fatal("Event accounting authority unavailable")
	}
	defer eventConn.Close()
	paymentService.SetAccountingReadiness(service.NewEventAccountingReadiness(eventv1.NewEventServiceClient(eventConn)))
	stripeGateway.SetWebhookScope(account, false)
	financialWorker := service.NewFinancialWorker(paymentService)
	financialWorker.SetReceiptPublisher(paymentProducer)
	financialWorker.SetMetrics(telemetry.Metrics)
	// Only the explicitly named primary Payment process owns T23 receipt
	// boundaries. A second process remains able to claim work after recovery.
	if cfg.ServiceName == "payment-service" && reliabilityHooks.Enabled() {
		financialWorker.SetReliabilityHook(reliabilityHooks)
		paymentService.SetReceiptReliabilityHook(reliabilityHooks)
	}
	if reliabilityHooks.Enabled() &&
		(cfg.ServiceName == "payment-service" || cfg.ServiceName == "payment-service-2") {
		financialWorker.SetW28RefundHook(reliabilityHooks)
	}
	serverOptions := []grpc.ServerOption{grpc.UnaryInterceptor(middleware.UnaryLoggingInterceptor(logger))}
	serverOptions = append(serverOptions, telemetry.GRPCServerOptions()...)
	serverOptions = append(serverOptions, grpc.Creds(paymentTransport))
	grpcServer := grpc.NewServer(serverOptions...)
	paymentServer := payment_grpc.NewPaymentServer(paymentService, logger)
	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentServer)
	admissionv1.RegisterPaymentAdmissionCoordinatorServer(grpcServer, payment_grpc.NewAdmissionServer(admissionCoordinator))
	reflection.Register(grpcServer)

	// HTTP server for Webhook stripe/zalopay/momo payment gateway
	mux := http.NewServeMux()
	stripeWebhookGateway := stripe.NewStripePaymentGateway(logger, cfg.StripeSecretWebhook, paymentProducer, stripeClient, telemetry.Metrics)
	stripeWebhookGateway.SetWebhookScope(account, false)
	paymentHttpServerStripeHandler := payment_http.NewPaymentHttpHandler(paymentService, logger, stripeWebhookGateway)
	// Momo, zalopay,...

	mux.Handle("/webhooks/stripe", telemetry.HTTPHandler("POST /webhooks/stripe", http.HandlerFunc(paymentHttpServerStripeHandler.WebhookHandler)))
	httpServer := &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	httpListener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		logger.Fatal("Failed to listen for payment webhooks", zap.Error(err))
	}

	// GRPC Server for internal communication
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		_ = httpListener.Close()
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	workers := paymentruntime.Start(ctx, admissionCoordinator.Start, financialWorker.Start)
	httpServeResult := make(chan error, 1)
	go func() { httpServeResult <- httpServer.Serve(httpListener) }()
	grpcServeResult := make(chan error, 1)
	go func() { grpcServeResult <- grpcServer.Serve(lis) }()
	logger.Info("Payment service has started", zap.String("port", cfg.GRPCPort))
	logger.Info("Payment http server has started on 8081 port")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	var httpServeErr, grpcServeErr error
	var httpServeReturned, grpcServeReturned bool
	select {
	case sig := <-sigChan:
		logger.Info("Payment service is shutting down", zap.String("signal", sig.String()))
	case httpServeErr = <-httpServeResult:
		httpServeReturned = true
		if !errors.Is(httpServeErr, http.ErrServerClosed) {
			logger.Error("Payment webhook server stopped unexpectedly", zap.Error(httpServeErr))
		}
	case grpcServeErr = <-grpcServeResult:
		grpcServeReturned = true
		if !errors.Is(grpcServeErr, grpc.ErrServerStopped) {
			logger.Error("Payment gRPC server stopped unexpectedly", zap.Error(grpcServeErr))
		}
	case <-workers.Done():
		if err := workers.Err(); err != nil {
			logger.Error("Payment background task stopped unexpectedly", zap.Error(err))
		} else {
			logger.Error("Payment background tasks stopped unexpectedly")
		}
	}

	cancel()

	httpShutdownResult := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		err := httpServer.Shutdown(shutdownCtx)
		if err != nil {
			if closeErr := httpServer.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
				logger.Warn("Payment webhook server forced close failed", zap.Error(closeErr))
			}
		}
		httpShutdownResult <- err
	}()

	grpcStopDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopDone)
	}()
	grpcStopTimer := time.NewTimer(15 * time.Second)
	grpcGracefulTimedOut := false
	select {
	case <-grpcStopDone:
	case <-grpcStopTimer.C:
		select {
		case <-grpcStopDone:
		default:
			grpcGracefulTimedOut = true
			logger.Warn("Payment gRPC graceful shutdown exceeded its deadline")
			forcedStopDone := make(chan struct{})
			go func() {
				grpcServer.Stop()
				<-grpcStopDone
				close(forcedStopDone)
			}()
			forcedStopTimer := time.NewTimer(5 * time.Second)
			select {
			case <-forcedStopDone:
			case <-forcedStopTimer.C:
				logger.Error("Payment gRPC forced stop did not finish; exiting without closing dependencies")
				_ = logger.Sync()
				os.Exit(1)
			}
			if !forcedStopTimer.Stop() {
				select {
				case <-forcedStopTimer.C:
				default:
				}
			}
		}
	}
	if !grpcStopTimer.Stop() {
		select {
		case <-grpcStopTimer.C:
		default:
		}
	}

	httpShutdownErr := <-httpShutdownResult
	if httpShutdownErr != nil {
		logger.Warn("Payment webhook server graceful shutdown exceeded its deadline", zap.Error(httpShutdownErr))
	}
	if !httpServeReturned {
		httpServeErr = <-httpServeResult
		httpServeReturned = true
	}
	if !grpcServeReturned {
		grpcServeErr = <-grpcServeResult
		grpcServeReturned = true
	}
	if httpServeErr != nil && !errors.Is(httpServeErr, http.ErrServerClosed) {
		logger.Error("Payment webhook server exit failed", zap.Error(httpServeErr))
	}
	if grpcServeErr != nil && !errors.Is(grpcServeErr, grpc.ErrServerStopped) {
		logger.Error("Payment gRPC server exit failed", zap.Error(grpcServeErr))
	}

	workerShutdownCtx, workerShutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	workerErr := workers.StopAndWait(workerShutdownCtx)
	workerShutdownCancel()
	if errors.Is(workerErr, paymentruntime.ErrTasksStillRunning) {
		logger.Error("Payment background tasks exceeded the shutdown deadline; exiting without closing their dependencies", zap.Error(workerErr))
		_ = logger.Sync()
		os.Exit(1)
	}
	if workerErr != nil {
		logger.Error("Payment background task shutdown failed", zap.Error(workerErr))
	}
	if grpcGracefulTimedOut || httpShutdownErr != nil {
		logger.Error("Payment server shutdown required forced close; exiting without closing dependencies")
		_ = logger.Sync()
		os.Exit(1)
	}
}
