//go:build admission_r4

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	payment_grpc "github.com/ticketbox/payment/internal/grpc"
	paymentkafka "github.com/ticketbox/payment/internal/kafka"
	"github.com/ticketbox/payment/internal/paymentruntime"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type readyMessage struct {
	RunID          string `json:"run_id"`
	SourceCommit   string `json:"source_commit"`
	GRPCAddress    string `json:"grpc_address"`
	ControlAddress string `json:"control_address"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "admission R4 Payment startup refused:", err.Error())
		os.Exit(1)
	}
}

func run() error {
	verifyOnly, err := restartVerificationMode(os.Args[1:])
	if err != nil {
		return err
	}
	cfg, token, err := loadBootstrapConfig()
	if err != nil {
		return err
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer startupCancel()
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil || poolConfig == nil {
		return errors.New("Payment database settings are invalid")
	}
	poolConfig.MaxConns = 8
	poolConfig.MinConns = 0
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(startupCtx, poolConfig)
	if err != nil {
		return errors.New("run-specific Payment database is unavailable")
	}
	closePool := true
	defer func() {
		if closePool {
			pool.Close()
		}
	}()
	if err := pool.Ping(startupCtx); err != nil {
		return errors.New("run-specific Payment database is unavailable")
	}
	var database, databaseUser string
	if err := pool.QueryRow(startupCtx, "SELECT current_database(),current_user").Scan(&database, &databaseUser); err != nil ||
		database != cfg.ExpectedDatabase || databaseUser != cfg.ExpectedUser {
		return errors.New("connected Payment database identity differs from the configured run owner")
	}
	if cfg.DynamicPurchase != nil && cfg.RuntimeMode != "default_worker" {
		if err := requireUnusedDynamicBuyer(startupCtx, pool, cfg.DynamicPurchase.UserID); err != nil {
			return err
		}
	}

	repo := repository.NewPostgresPaymentRepository(pool)
	if verifyOnly {
		_, resumed, err := loadDurableProvider(startupCtx, pool, cfg)
		if err == nil && !resumed {
			resumed, err = verifyStaticRestart(startupCtx, pool, cfg)
		}
		if err != nil {
			return err
		}
		if !resumed {
			return errors.New("static restart verification requires existing fixtures")
		}
		result, _ := json.Marshal(map[string]string{"result": "PASS", "mode": "read-only", "run_id": cfg.RunID, "base_source_commit": cfg.SourceCommit, "build_source_commit": buildSourceCommit})
		fmt.Println(string(result))
		return nil
	}
	var provider *localProvider
	if cfg.RuntimeMode == "default_worker" {
		provider, err = initializeDurableProvider(startupCtx, pool, repo, cfg)
	} else {
		err = initializeFinancialFixtures(startupCtx, pool, repo, cfg)
		if err == nil {
			provider, err = newLocalProvider(cfg)
		}
	}
	if err != nil {
		return err
	}

	serverCredentials, testOnly, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServicePayment)
	if err != nil || testOnly {
		return errors.New("authenticated Payment gRPC server credentials are required")
	}
	clientServerName := strings.TrimSpace(os.Getenv("RUSHGO_BOOKING_GRPC_SERVER_NAME"))
	if clientServerName == "" || clientServerName != cfg.BookingServerName {
		return errors.New("Booking TLS server name does not match the run configuration")
	}
	bookingCredentials, testOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityPayment)
	if err != nil || testOnly {
		return errors.New("authenticated Payment-to-Booking mTLS credentials are required")
	}
	bookingConn, err := grpc.NewClient(cfg.BookingAddress, grpc.WithTransportCredentials(bookingCredentials))
	if err != nil {
		return errors.New("Booking authority client could not be created")
	}
	closeBooking := true
	defer func() {
		if closeBooking {
			_ = bookingConn.Close()
		}
	}()
	bookingConn.Connect()
	if err := waitBookingReady(startupCtx, bookingConn); err != nil {
		return errors.New("Booking authority mTLS connection did not become ready")
	}
	startupCancel()

	logger := zap.NewNop()
	paymentService := service.NewPaymentService(repo, logger, provider)
	paymentService.SetFinancialAuthority(bookingv1.NewBookingServiceClient(bookingConn), cfg.Fixtures[0].ProviderAccount, false)
	var eventConn *grpc.ClientConn
	closeEventConn := true
	if cfg.RuntimeMode == "default_worker" {
		eventConn, err = grpc.NewClient(cfg.EventAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return errors.New("Event accounting authority client could not be created")
		}
		defer func() {
			if closeEventConn {
				_ = eventConn.Close()
			}
		}()
		paymentService.SetAccountingReadiness(service.NewEventAccountingReadiness(eventv1.NewEventServiceClient(eventConn)))
	}
	coordinator := service.NewAdmissionCoordinator(repo, paymentService, admission.NewOutcomeClient(bookingConn))
	worker := service.NewFinancialWorker(paymentService)
	var receiptProducer *paymentkafka.PaymentProducer
	if cfg.DynamicPurchase != nil && cfg.DynamicPurchase.Enabled {
		receiptProducer = paymentkafka.NewPaymentProducer(cfg.KafkaBrokers, logger)
		worker.SetReceiptPublisher(receiptProducer)
	}
	closeReceiptProducer := receiptProducer != nil
	defer func() {
		if closeReceiptProducer {
			_ = receiptProducer.Close()
		}
	}()
	recovery := newAdmissionRecovery(coordinator, logger)
	fixtures := make(map[uuid.UUID]fixtureConfig, len(cfg.Fixtures))
	for _, fixture := range cfg.Fixtures {
		paymentID, _ := uuid.Parse(fixture.PaymentID)
		fixtures[paymentID] = fixture
	}
	runtime := &serviceRuntime{
		cfg: cfg, token: token, pool: pool, repo: repo, payment: paymentService,
		worker: worker, coordinator: coordinator, bookingConn: bookingConn,
		provider: provider, recovery: recovery, prepareGate: newPrepareReplyGate(time.Duration(cfg.MaxBarrierWaitMS) * time.Millisecond),
		fixtures: fixtures, logger: logger,
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCListenAddress)
	if err != nil {
		return errors.New("Payment gRPC listener could not be opened")
	}
	controlListener, err := net.Listen("tcp", cfg.ControlListenAddress)
	if err != nil {
		_ = grpcListener.Close()
		return errors.New("Payment internal control listener could not be opened")
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials), grpc.UnaryInterceptor(runtime.unaryInterceptor()))
	paymentv1.RegisterPaymentServiceServer(grpcServer, payment_grpc.NewPaymentServer(paymentService, logger))
	admissionv1.RegisterPaymentAdmissionCoordinatorServer(grpcServer, payment_grpc.NewAdmissionServer(coordinator))
	reflection.Register(grpcServer)
	controlServer := &http.Server{
		Handler: runtime.controlHandler(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 15 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var workers *paymentruntime.Group
	if cfg.RuntimeMode == "default_worker" {
		workers = paymentruntime.Start(ctx, coordinator.Start, worker.Start)
	} else {
		go recovery.start(ctx)
	}
	grpcErr := make(chan error, 1)
	httpErr := make(chan error, 1)
	go func() { grpcErr <- grpcServer.Serve(grpcListener) }()
	go func() { httpErr <- controlServer.Serve(controlListener) }()
	ready, _ := json.Marshal(readyMessage{
		RunID: cfg.RunID, SourceCommit: cfg.SourceCommit,
		GRPCAddress: cfg.GRPCAdvertiseAddress, ControlAddress: cfg.ControlListenAddress,
	})
	_, _ = fmt.Fprintf(os.Stdout, "ADMISSION_R4_PAYMENT_READY %s\n", ready)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-grpcErr:
		stop()
	case serveErr = <-httpErr:
		stop()
	case <-groupDone(workers):
		if workers != nil {
			serveErr = workers.Err()
		}
		stop()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	_ = controlServer.Shutdown(shutdownCtx)
	_ = controlListener.Close()
	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	_ = grpcListener.Close()
	if workers != nil {
		workerShutdownCtx, workerShutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		workerErr := workers.StopAndWait(workerShutdownCtx)
		workerShutdownCancel()
		if workerErr != nil {
			if errors.Is(workerErr, paymentruntime.ErrTasksStillRunning) {
				// The runtime group still owns these dependencies. Leave them open;
				// main exits immediately after this return.
				closePool = false
				closeBooking = false
				closeEventConn = false
				closeReceiptProducer = false
			}
			return errors.New("Payment background workers did not shut down cleanly")
		}
	}
	if receiptProducer != nil {
		if err := receiptProducer.Close(); err != nil {
			return errors.New("Payment receipt event producer did not shut down cleanly")
		}
		closeReceiptProducer = false
	}
	_ = bookingConn.Close()
	pool.Close()
	closeBooking = false
	closePool = false
	if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.New("Payment test-only listener stopped unexpectedly")
	}
	return nil
}

func requireUnusedDynamicBuyer(ctx context.Context, pool *pgxpool.Pool, rawUserID string) error {
	userID, err := uuid.Parse(rawUserID)
	if err != nil || userID == uuid.Nil || userID.String() != rawUserID {
		return errors.New("dynamic purchase buyer identity is invalid")
	}
	var hasFinancialHistory bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM payments WHERE user_id=$1)", userID).Scan(&hasFinancialHistory); err != nil {
		return errors.New("dynamic purchase restart fence could not read Payment history")
	}
	if hasFinancialHistory {
		return errors.New("dynamic purchase slot already has durable Payment history; restart is refused")
	}
	return nil
}

func groupDone(group *paymentruntime.Group) <-chan struct{} {
	if group == nil {
		return nil
	}
	return group.Done()
}

func waitBookingReady(ctx context.Context, conn *grpc.ClientConn) error {
	for conn.GetState() != connectivity.Ready {
		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
	return nil
}
