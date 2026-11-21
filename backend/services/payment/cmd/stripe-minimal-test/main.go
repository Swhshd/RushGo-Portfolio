//go:build stripe_minimal_test

// Command stripe-minimal-test is an opt-in Payment process for one bound
// Stripe TEST payment and its one full refund through normal RushGo services.
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
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	paymentgrpc "github.com/ticketbox/payment/internal/grpc"
	paymenthttp "github.com/ticketbox/payment/internal/http"
	"github.com/ticketbox/payment/internal/kafka"
	"github.com/ticketbox/payment/internal/paymentruntime"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
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

var sourceCommit = "UNSET"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Stripe TEST-only Payment process stopped:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, key, webhookSecret, err := loadTestConfig(sourceCommit)
	if err != nil {
		return err
	}
	lock, err := acquireProcessLock(os.Getenv(stateEnv) + ".lock")
	if err != nil {
		return errors.New("another process owns this Stripe TEST run")
	}
	defer lock()
	state, err := openState(os.Getenv(stateEnv), cfg)
	if err != nil {
		return err
	}
	appCfg, err := config.Load()
	if err != nil {
		return errors.New("Payment service settings unavailable")
	}
	appCfg.ServiceName = "payment-service"
	appCfg.DatabaseURL = cfg.DatabaseURL
	appCfg.KafkaBrokers = strings.Split(cfg.KafkaBrokers, ",")
	appCfg.StripeSecretKey = key
	appCfg.StripeSecretWebhook = webhookSecret
	if appCfg.ServiceName != "payment-service" || appCfg.StripeSecretKey != key {
		return errors.New("TEST Payment process identity mismatch")
	}
	logger := zap.NewNop()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		return errors.New("run-specific TEST database unavailable")
	}
	defer pool.Close()
	var db, user string
	if err := pool.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&db, &user); err != nil || db != cfg.ExpectedDatabase || user != cfg.ExpectedUser {
		return errors.New("connected TEST database identity mismatch")
	}
	backend := stripegateway.NewRefundAuditAPIBackend()
	stripeClient := stripego.NewClient(key, stripego.WithBackends(&stripego.Backends{API: backend}))
	baseGateway := stripegateway.NewStripePaymentGateway(logger, webhookSecret, nil, stripeClient)
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Second)
	account, err := baseGateway.VerifyTestAccount(verifyCtx, key)
	verifyCancel()
	if err != nil || account != approvedAccount || account != cfg.ExpectedStripeAccount {
		return errors.New("configured credential is not the approved Stripe TEST account")
	}
	baseGateway.SetWebhookScope(account, false)
	baseGateway.SetAdmissionReadBackend(backend, key)
	provider := &boundedProvider{next: baseGateway, state: state}
	bookingCredentials, testOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentityPayment)
	if err != nil || testOnly {
		return errors.New("authenticated Booking mTLS is required")
	}
	serverCredentials, testOnly, err := grpcsecurity.ServerCredentialsFromEnv(grpcsecurity.ServicePayment)
	if err != nil || testOnly {
		return errors.New("authenticated Payment mTLS is required")
	}
	bookingConn, err := grpc.NewClient(cfg.BookingAddress, grpc.WithTransportCredentials(bookingCredentials))
	if err != nil {
		return errors.New("Booking authority client unavailable")
	}
	defer bookingConn.Close()
	bookingConn.Connect()
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	err = waitReady(readyCtx, bookingConn)
	readyCancel()
	if err != nil {
		return errors.New("Booking authority not ready")
	}
	eventConn, err := grpc.NewClient(cfg.EventAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.New("Event authority client unavailable")
	}
	defer eventConn.Close()
	producer := kafka.NewPaymentProducer(appCfg.KafkaBrokers, logger)
	defer producer.Close()
	repo := repository.NewPostgresPaymentRepository(pool)
	serviceLayer := service.NewPaymentService(repo, logger, provider)
	serviceLayer.SetFinancialAuthority(bookingv1.NewBookingServiceClient(bookingConn), account, false)
	serviceLayer.SetAccountingReadiness(service.NewEventAccountingReadiness(eventv1.NewEventServiceClient(eventConn)))
	provider.approvedUnusedRefund = serviceLayer
	provider.verifyAppliedReceipt = func(ctx context.Context, state runState) error {
		return verifyAppliedPaymentReceipt(ctx, pool, state)
	}
	coordinator := service.NewAdmissionCoordinator(repo, serviceLayer, admission.NewOutcomeClient(bookingConn))
	worker := service.NewFinancialWorker(serviceLayer)
	worker.SetReceiptPublisher(producer)
	paymentRPC := paymentgrpc.NewPaymentServer(serviceLayer, logger)
	if _, err := uuid.Parse(cfg.ExpectedUserID); err != nil {
		return errors.New("expected TEST user identity invalid")
	}
	control := &testControl{cfg: cfg, token: os.Getenv(tokenEnv), state: state, booking: bookingv1.NewBookingServiceClient(bookingConn), worker: worker, payments: serviceLayer}
	if os.Getenv(receiptRecoveryEnv) != "" {
		recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 15*time.Second)
		err := recoverReceiptState(recoveryCtx, cfg, state, control, baseGateway, pool)
		recoveryCancel()
		if err != nil {
			return errors.New("bounded receipt recovery evidence did not match the original TEST run")
		}
	}
	grpcListener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return errors.New("Payment listener could not bind")
	}
	defer grpcListener.Close()
	webhookListener, err := net.Listen("tcp", cfg.WebhookAddress)
	if err != nil {
		grpcListener.Close()
		return errors.New("webhook listener could not bind")
	}
	defer webhookListener.Close()
	controlListener, err := net.Listen("tcp", cfg.ControlAddress)
	if err != nil {
		return errors.New("TEST control listener could not bind")
	}
	defer controlListener.Close()
	webhookMux := http.NewServeMux()
	wh := paymenthttp.NewPaymentHttpHandler(serviceLayer, logger, provider)
	webhookMux.HandleFunc("/webhooks/stripe", wh.WebhookHandler)
	webhookServer := &http.Server{Handler: webhookMux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second}
	controlServer := &http.Server{Handler: control.handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 15 * time.Second}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials), grpc.UnaryInterceptor(middleware.UnaryLoggingInterceptor(logger)))
	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentRPC)
	if cfg.RuntimeMode == runtimeModeDefaultWorker {
		admissionv1.RegisterPaymentAdmissionCoordinatorServer(grpcServer, paymentgrpc.NewAdmissionServer(coordinator))
	}
	reflection.Register(grpcServer)
	errs := make(chan error, 3)
	go func() { errs <- grpcServer.Serve(grpcListener) }()
	go func() { errs <- webhookServer.Serve(webhookListener) }()
	go func() { errs <- controlServer.Serve(controlListener) }()
	var workers *paymentruntime.Group
	var workersDone <-chan struct{}
	if cfg.RuntimeMode == runtimeModeDefaultWorker {
		workers = paymentruntime.Start(ctx, coordinator.Start, worker.Start, func(workerCtx context.Context) error {
			return observeDefaultWorkerState(workerCtx, pool, state, control)
		})
		workersDone = workers.Done()
	}
	ready, _ := json.Marshal(map[string]any{"run_id": cfg.RunID, "source_commit": cfg.SourceCommit, "stripe_account": account, "livemode": false, "bounded_payments": 1, "bounded_full_refunds": 1})
	fmt.Fprintln(os.Stdout, "STRIPE_MINIMAL_TEST_READY", string(ready))
	var runErr error
	select {
	case <-ctx.Done():
	case <-errs:
		runErr = errors.New("TEST service listener stopped")
	case <-workersDone:
		if workers != nil && workers.Err() != nil {
			runErr = errors.New("TEST background worker stopped")
		} else {
			runErr = errors.New("TEST background worker stopped unexpectedly")
		}
	}
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	_ = webhookServer.Shutdown(shutdown)
	_ = controlServer.Shutdown(shutdown)
	grpcStopDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopDone)
	}()
	grpcStopTimer := time.NewTimer(3 * time.Second)
	select {
	case <-grpcStopDone:
	case <-grpcStopTimer.C:
		grpcServer.Stop()
		forcedStopTimer := time.NewTimer(5 * time.Second)
		select {
		case <-grpcStopDone:
			if runErr == nil {
				runErr = errors.New("TEST gRPC graceful shutdown exceeded its deadline")
			}
		case <-forcedStopTimer.C:
			fmt.Fprintln(os.Stderr, "Stripe TEST gRPC stop did not finish; exiting without closing dependencies")
			os.Exit(1)
		}
		if !forcedStopTimer.Stop() {
			select {
			case <-forcedStopTimer.C:
			default:
			}
		}
	}
	if !grpcStopTimer.Stop() {
		select {
		case <-grpcStopTimer.C:
		default:
		}
	}
	if workers != nil {
		workerShutdown, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
		workerErr := workers.StopAndWait(workerShutdown)
		workerCancel()
		if errors.Is(workerErr, paymentruntime.ErrTasksStillRunning) {
			fmt.Fprintln(os.Stderr, "Stripe TEST background tasks exceeded shutdown deadline; exiting without closing their dependencies")
			os.Exit(1)
		}
		if workerErr != nil {
			if runErr == nil {
				runErr = errors.New("TEST background worker shutdown failed")
			}
		}
	}
	return runErr
}

func verifyAppliedPaymentReceipt(ctx context.Context, pool *pgxpool.Pool, s runState) error {
	envelopeID, err := uuid.Parse(s.WebhookEventID)
	if err != nil || envelopeID == uuid.Nil || envelopeID.String() != s.WebhookEventID {
		return errors.New("signed TEST receipt envelope identity invalid")
	}
	paymentID, err := uuid.Parse(s.PaymentID)
	if err != nil || paymentID == uuid.Nil || paymentID.String() != s.PaymentID {
		return errors.New("TEST receipt payment identity invalid")
	}
	bookingID, err := uuid.Parse(s.BookingID)
	if err != nil || bookingID == uuid.Nil || bookingID.String() != s.BookingID {
		return errors.New("TEST receipt booking identity invalid")
	}
	if s.PaymentIntentID == "" || s.VerifiedWebhookType != "payment_intent.succeeded" {
		return errors.New("successful signed TEST receipt required")
	}
	var appliedReceipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_provider_events
		WHERE provider_account=$1 AND livemode=false AND envelope_id=$2 AND event_id ~ '^evt_[A-Za-z0-9]+$' AND event_type='payment_intent.succeeded'
		AND intent_id=$3 AND payment_id=$4 AND booking_id=$5 AND payment_generation=1
		AND amount_cents=100 AND currency='usd' AND provider_status='succeeded' AND collected=true
		AND correlation_state='applied'`, approvedAccount, envelopeID, s.PaymentIntentID, paymentID, bookingID).Scan(&appliedReceipts); err != nil {
		return err
	}
	if appliedReceipts != 1 {
		return errors.New("exact signed TEST receipt is not durably applied")
	}
	return nil
}

func observeDefaultWorkerState(ctx context.Context, pool *pgxpool.Pool, state *stateStore, control *testControl) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := syncDefaultWorkerState(ctx, pool, state, control); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func syncDefaultWorkerState(ctx context.Context, pool *pgxpool.Pool, state *stateStore, control *testControl) error {
	s := state.snapshot()
	if s.RuntimeMode != runtimeModeDefaultWorker || s.Blocked || s.PaymentID == "" || s.VerifiedWebhookType != "payment_intent.succeeded" || s.WebhookEventID == "" {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	view, err := readAppliedPaymentFinancialView(readCtx, s,
		func(ctx context.Context, state runState) error { return verifyAppliedPaymentReceipt(ctx, pool, state) },
		control.financialView)
	if err != nil {
		return nil
	}
	return applyObservedDefaultWorkerView(state, s, view)
}

func applyObservedDefaultWorkerView(state *stateStore, observed runState, view *domain.FinancialView) error {
	if !observed.Collected {
		if err := state.confirmBackgroundReceiptApplied(view); err != nil {
			return errors.New("durable TEST receipt state mismatched the verified webhook")
		}
		return nil
	}
	current := state.snapshot()
	if current.Blocked || current.RuntimeMode != runtimeModeDefaultWorker ||
		current.Collected != observed.Collected || current.RefundKey != observed.RefundKey ||
		current.RefundDecisionID != observed.RefundDecisionID || current.RefundID != observed.RefundID ||
		current.RefundState != observed.RefundState || current.FinancialWorkState != observed.FinancialWorkState {
		return nil
	}
	if current.RefundKey == "" {
		return nil
	}
	if view == nil || view.RefundDecisionID == nil && view.RefundState == "not_required" {
		return nil
	}
	if err := state.confirmBackgroundRefundApplied(view); err != nil && state.snapshot().Blocked {
		return errors.New("durable default-worker refund state mismatch")
	}
	return nil
}

func readAppliedPaymentFinancialView(
	ctx context.Context,
	s runState,
	verifyReceipt func(context.Context, runState) error,
	readPayment func(context.Context, runState) (*domain.FinancialView, error),
) (*domain.FinancialView, error) {
	if verifyReceipt == nil || readPayment == nil {
		return nil, errors.New("durable payment evidence reader unavailable")
	}
	if err := verifyReceipt(ctx, s); err != nil {
		return nil, err
	}
	return readPayment(ctx, s)
}

type testIntentReader interface {
	RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error)
}

func recoverReceiptState(ctx context.Context, cfg testConfig, state *stateStore, control *testControl, stripe testIntentReader, pool *pgxpool.Pool) error {
	s := state.snapshot()
	if cfg.RunID != approvedRecoveryRunID || cfg.SourceCommit != approvedRecoverySource ||
		s.RunID != approvedRecoveryRunID || s.SourceCommit != approvedRecoverySource || s.StripeAccount != approvedAccount ||
		s.PaymentID != approvedRecoveryPaymentID || s.BookingID != approvedRecoveryBookingID || s.PaymentIntentID != approvedRecoveryIntentID || s.ExpectedUserID != cfg.ExpectedUserID ||
		s.Blocked != true || s.BlockReason != "durable_payment_state_unavailable" || s.ReceiptWorkState != "unknown" ||
		s.Collected || s.FinancialWorkAttempts != 0 || s.FinancialWorkState != "" || s.RefundID != "" || s.RefundKey != "" || s.RefundState != "" {
		return errors.New("persisted receipt failure is outside the approved recovery case")
	}
	if err := control.requireCancelled(ctx, s); err != nil {
		return errors.New("bound Booking is not terminal and cancelled")
	}
	view, err := control.financialView(ctx, s)
	if err != nil {
		return errors.New("durable collected payment state could not be verified")
	}
	if view.LiveMode == nil || *view.LiveMode || view.ProviderAccount != approvedAccount || view.ProviderOutcome != "collected" || view.Payment.Status != domain.PaymentSuccess {
		return errors.New("database does not show the bound TEST payment as collected")
	}
	envelopeID, err := uuid.Parse(s.WebhookEventID)
	if err != nil || envelopeID == uuid.Nil || envelopeID.String() != s.WebhookEventID {
		return errors.New("persisted payment receipt envelope identity is invalid")
	}
	var appliedReceipts int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM payment_provider_events
		WHERE provider_account=$1 AND livemode=false AND envelope_id=$2 AND event_id ~ '^evt_[A-Za-z0-9]+$' AND event_type='payment_intent.succeeded'
		AND intent_id=$3 AND payment_id=$4 AND booking_id=$5 AND payment_generation=1
		AND amount_cents=100 AND currency='usd' AND provider_status='succeeded' AND collected=true
		AND correlation_state='applied'`, approvedAccount, envelopeID, approvedRecoveryIntentID,
		approvedRecoveryPaymentID, approvedRecoveryBookingID).Scan(&appliedReceipts)
	if err != nil || appliedReceipts != 1 {
		return errors.New("database does not contain exactly one applied receipt for this TEST payment")
	}
	if err := verifyNoRefundAttempt(ctx, pool, s); err != nil {
		return errors.New("database refund state is outside the no-refund-attempt recovery scope")
	}
	observation, _, err := stripe.RetrieveIntent(ctx, approvedAccount, false, s.PaymentIntentID)
	if err != nil || !receiptRecoveryObservationMatches(s, observation) {
		return errors.New("Stripe does not confirm the same collected TEST PaymentIntent")
	}
	if err := state.recoverReceiptApplied(view); err != nil {
		return errors.New("persistent receipt recovery latch refused the evidence")
	}
	return nil
}

func receiptRecoveryObservationMatches(s runState, observation *domain.ProviderObservation) bool {
	if observation == nil {
		return false
	}
	return observation.IntentID == approvedRecoveryIntentID && observation.Account == approvedAccount &&
		observation.PaymentID.String() == approvedRecoveryPaymentID && observation.BookingID.String() == approvedRecoveryBookingID &&
		observation.Generation == 1 && !observation.LiveMode && observation.AmountCents == 100 && observation.Currency == "usd" &&
		observation.Status == "succeeded" && observation.Collected && s.PaymentIntentID == observation.IntentID &&
		s.PaymentID == observation.PaymentID.String() && s.BookingID == observation.BookingID.String()
}

func verifyNoRefundAttempt(ctx context.Context, pool *pgxpool.Pool, s runState) error {
	var state string
	var key, currency, reason, providerID, providerStatus *string
	var amount *int64
	var decision *uuid.UUID
	var firstAttempt *time.Time
	if err := pool.QueryRow(ctx, `SELECT refund_state,refund_key,refund_amount_cents,refund_currency,refund_reason,refund_decision_id,
		refund_provider_id,refund_provider_status,refund_first_attempt_at FROM payments
		WHERE managed AND id=$1 AND booking_id=$2 AND payment_generation=1`, approvedRecoveryPaymentID,
		approvedRecoveryBookingID).Scan(&state, &key, &amount, &currency, &reason, &decision, &providerID, &providerStatus, &firstAttempt); err != nil {
		return err
	}
	return validateReceiptRecoveryRefund(s, receiptRecoveryRefundRow{
		state: state, key: key, amount: amount, currency: currency, reason: reason, decision: decision,
		providerID: providerID, providerStatus: providerStatus, firstAttempt: firstAttempt,
	})
}

type receiptRecoveryRefundRow struct {
	state          string
	key            *string
	amount         *int64
	currency       *string
	reason         *string
	decision       *uuid.UUID
	providerID     *string
	providerStatus *string
	firstAttempt   *time.Time
}

func validateReceiptRecoveryRefund(s runState, row receiptRecoveryRefundRow) error {
	stableKey := "ticketbox:payment:" + approvedRecoveryPaymentID + ":g1:refund"
	if row.state == "not_required" {
		if row.key == nil || *row.key != stableKey || row.amount != nil || row.currency != nil || row.reason != nil || row.decision != nil ||
			row.providerID != nil || row.providerStatus != nil || row.firstAttempt != nil {
			return errors.New("unexpected refund obligation fields")
		}
		return nil
	}
	if row.state != "pending" || row.key == nil || *row.key != stableKey || row.amount == nil || *row.amount != 100 || row.currency == nil || *row.currency != "usd" ||
		row.reason == nil || *row.reason != "CANCELLED" || row.decision == nil || *row.decision != uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+s.BookingID+":g1:cancel")) ||
		row.providerID != nil || row.providerStatus != nil || row.firstAttempt != nil {
		return errors.New("pending refund obligation is not the exact untouched cancellation obligation")
	}
	return nil
}

func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	for conn.GetState() != connectivity.Ready {
		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
	return nil
}
