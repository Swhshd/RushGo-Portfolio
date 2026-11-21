package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/database"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"github.com/ticketbox/saga/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("recover-paid-unissued-booking", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	bookingIDRaw := flags.String("booking-id", "", "exact Booking UUID")
	sagaIDRaw := flags.String("saga-id", "", "exact managed Saga UUID")
	operationKeyRaw := flags.String("operation-key", "", "stable UUID reused for idempotent replay")
	operator := flags.String("operator", "", "operator identity recorded in the Saga audit")
	reason := flags.String("reason", "", "short recovery reason recorded in the Saga audit")
	expectedAccount := flags.String("expected-provider-account", "", "exact expected Stripe TEST account ID")
	environment := flags.String("environment", "", "must be test")
	execute := flags.Bool("execute", false, "atomically requeue the exact eligible Saga for the existing worker")
	confirmBookingID := flags.String("confirm-booking-id", "", "required with --execute; must exactly match --booking-id")
	inspectStalled := flags.Bool("inspect-stalled-issuance", false, "read-only diagnosis of a completed Saga whose published issuance outbox was not consumed")
	topicFlag := flags.String("topic", "", "issuance topic; defaults to the build's effective ticket-issuance identity")
	groupFlag := flags.String("group", "", "ticket-issuance consumer group; defaults to the build's effective identity")
	brokersFlag := flags.String("kafka-brokers", "", "comma-separated Kafka brokers; defaults to KAFKA_BROKERS")
	sampleInterval := flags.Duration("sample-interval", 3*time.Second, "committed-offset stability sample interval for inspection")
	maxScan := flags.Int("max-scan", 1000, "maximum records scanned per partition range")
	includeKafkaOnIssued := flags.Bool("include-kafka-on-issued", false, "include the group offset snapshot when the ticket is already issued")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	bookingID, err := uuid.Parse(strings.TrimSpace(*bookingIDRaw))
	if err != nil || bookingID == uuid.Nil {
		fmt.Fprintln(os.Stderr, "a valid --booking-id is required")
		return 2
	}
	sagaID, err := uuid.Parse(strings.TrimSpace(*sagaIDRaw))
	if err != nil || sagaID == uuid.Nil {
		fmt.Fprintln(os.Stderr, "a valid --saga-id is required")
		return 2
	}
	var operationKey uuid.UUID
	if raw := strings.TrimSpace(*operationKeyRaw); raw != "" {
		operationKey, err = uuid.Parse(raw)
		if err != nil || operationKey == uuid.Nil {
			fmt.Fprintln(os.Stderr, "a valid --operation-key is required")
			return 2
		}
	} else if !*inspectStalled {
		fmt.Fprintln(os.Stderr, "a valid --operation-key is required")
		return 2
	}
	if strings.ToLower(strings.TrimSpace(*environment)) != "test" {
		fmt.Fprintln(os.Stderr, "--environment=test is required; this command refuses other environments")
		return 2
	}
	if *inspectStalled && *execute {
		fmt.Fprintln(os.Stderr, "--inspect-stalled-issuance is read-only; the restart must be executed by the isolated recovery wrapper or an operator using the printed plan")
		return 2
	}
	if strings.TrimSpace(*expectedAccount) == "" || strings.TrimSpace(*operator) == "" || strings.TrimSpace(*reason) == "" {
		fmt.Fprintln(os.Stderr, "--expected-provider-account, --operator, and --reason are required")
		return 2
	}
	if *execute && strings.TrimSpace(*confirmBookingID) != bookingID.String() {
		fmt.Fprintln(os.Stderr, "--execute requires --confirm-booking-id to exactly match --booking-id")
		return 2
	}
	if !*execute && strings.TrimSpace(*confirmBookingID) != "" {
		fmt.Fprintln(os.Stderr, "--confirm-booking-id is only valid with --execute")
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		return 2
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot initialize recovery logger")
		return 1
	}
	defer logger.Sync()
	cfg, err := config.Load()
	if err != nil || strings.TrimSpace(cfg.DatabaseURL) == "" || strings.TrimSpace(cfg.BookingServiceAddr) == "" ||
		strings.TrimSpace(cfg.PaymentServiceAddr) == "" || strings.TrimSpace(cfg.EventServiceAddr) == "" {
		fmt.Fprintln(os.Stderr, "Saga database and Booking, Payment, and Event service endpoints are required")
		return 1
	}
	timeout := 45 * time.Second
	if *inspectStalled {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to the configured Saga database")
		return 1
	}
	defer pool.Close()

	paymentCredentials, paymentTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServicePayment, grpcsecurity.ServiceIdentitySaga)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Payment gRPC transport security initialization refused")
		return 1
	}
	bookingCredentials, bookingTestOnly, err := grpcsecurity.ClientCredentialsFromEnv(grpcsecurity.ServiceBooking, grpcsecurity.ServiceIdentitySaga)
	if err != nil || paymentTestOnly != bookingTestOnly {
		fmt.Fprintln(os.Stderr, "Booking gRPC transport security initialization refused")
		return 1
	}
	paymentConn, err := grpc.NewClient(cfg.PaymentServiceAddr, grpc.WithTransportCredentials(paymentCredentials))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot initialize Payment gRPC client")
		return 1
	}
	defer paymentConn.Close()
	bookingConn, err := grpc.NewClient(cfg.BookingServiceAddr, grpc.WithTransportCredentials(bookingCredentials))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot initialize Booking gRPC client")
		return 1
	}
	defer bookingConn.Close()
	eventTLS, err := grpcsecurity.InternalClientCredentials("event", grpcsecurity.ServiceIdentitySaga)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Event RPC transport configuration refused")
		return 1
	}
	eventConn, err := grpc.NewClient(cfg.EventServiceAddr, grpc.WithTransportCredentials(eventTLS))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot initialize Event gRPC client")
		return 1
	}
	defer eventConn.Close()
	if strings.TrimSpace(cfg.EventTicketAuthorityAddr) == "" {
		fmt.Fprintln(os.Stderr, "Event ticket authority endpoint is required; no plaintext eligibility fallback")
		return 1
	}
	eventAuthorityCredentials, err := grpcsecurity.EventRecoveryClientCredentialsFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Event recovery mTLS initialization refused")
		return 1
	}
	eventAuthorityConn, err := grpc.NewClient(cfg.EventTicketAuthorityAddr, grpc.WithTransportCredentials(eventAuthorityCredentials))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot initialize Event recovery authority client")
		return 1
	}
	defer eventAuthorityConn.Close()
	eventClient := recoveryEventClient{EventServiceClient: eventv1.NewEventServiceClient(eventConn),
		authority: eventv1.NewEventServiceClient(eventAuthorityConn)}

	sagaRepository := repository.NewSagaRepository(pool)
	sagaService := service.NewSagaService(logger, sagaRepository, registry.NewSagaStepRegistry(),
		bookingv1.NewBookingServiceClient(bookingConn), paymentv1.NewPaymentServiceClient(paymentConn), eventClient)
	if *inspectStalled {
		topic, group := topics.TicketIssuanceKafkaIdentity(grpcsecurity.TicketE2EKafkaIdentity())
		if value := strings.TrimSpace(*topicFlag); value != "" {
			topic = value
		}
		if value := strings.TrimSpace(*groupFlag); value != "" {
			group = value
		}
		brokers := cfg.KafkaBrokers
		if value := strings.TrimSpace(*brokersFlag); value != "" {
			brokers = strings.Split(value, ",")
			for i := range brokers {
				brokers[i] = strings.TrimSpace(brokers[i])
			}
		}
		if len(brokers) == 0 {
			fmt.Fprintln(os.Stderr, "Kafka brokers are required for stalled-issuance inspection")
			return 1
		}
		report, err := sagaService.InspectStalledIssuance(ctx, service.StalledIssuanceInspectRequest{
			BookingID: bookingID, SagaID: sagaID,
			ExpectedProviderAccount: strings.TrimSpace(*expectedAccount),
			Operator:                strings.TrimSpace(*operator), Reason: strings.TrimSpace(*reason),
			Topic: topic, Group: group, SampleInterval: *sampleInterval, MaxScan: *maxScan,
			IncludeKafkaOnIssued: *includeKafkaOnIssued,
		}, service.NewKafkaGroupInspector(brokers))
		if err != nil {
			fmt.Fprintln(os.Stderr, "stalled-issuance inspection failed")
			return 1
		}
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			fmt.Fprintln(os.Stderr, "cannot write stalled-issuance report")
			return 1
		}
		return 0
	}
	report, err := sagaService.RecoverPaidUnissuedBooking(ctx, service.RecoverPaidUnissuedBookingRequest{
		BookingID: bookingID, SagaID: sagaID, OperationKey: operationKey,
		Operator: strings.TrimSpace(*operator), Reason: strings.TrimSpace(*reason),
		ExpectedProviderAccount: strings.TrimSpace(*expectedAccount), Execute: *execute,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "paid-unissued recovery qualification or audit failed")
		return 1
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write recovery summary")
		return 1
	}
	return recoveryExitCode(report)
}

// Ordinary Event reads retain their existing endpoint. Only seat authority
// checks use the dedicated mTLS listener.
type recoveryEventClient struct {
	eventv1.EventServiceClient
	authority eventv1.EventServiceClient
}

func (c recoveryEventClient) VerifyBookingTicketEligibility(ctx context.Context, req *eventv1.VerifyBookingTicketEligibilityRequest, opts ...grpc.CallOption) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	return c.authority.VerifyBookingTicketEligibility(ctx, req, opts...)
}

func recoveryExitCode(report *service.PaidUnissuedRecoveryReport) int {
	if report == nil {
		return 1
	}
	if report.Eligible || report.ResultCode == "already_processing" || report.ResultCode == "issuance_already_queued" {
		return 0
	}
	return 2
}
