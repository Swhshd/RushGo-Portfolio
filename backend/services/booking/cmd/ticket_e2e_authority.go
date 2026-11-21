//go:build ticket_e2e

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/config"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const simulatedProviderAuthoritySource = "SIMULATED_PROVIDER_AUTHORITY"

var ticketE2EProjectPattern = regexp.MustCompile(`^rushgo-ticket-e2e-[0-9]{8}-[0-9a-f]{12}$`)

type simulatedPaymentEventPublisher interface {
	Publish(context.Context, string, string, pkgkafka.Event) error
	Close() error
}

type simulatedProviderPayment struct {
	id, userID, bookingID, paymentIntentID, clientSecret, resolutionDecisionID string
	amountCents                                                                int32
	currency                                                                   string
	generation                                                                 int64
	collected                                                                  bool
}

type simulatedPaymentAuthority struct {
	paymentv1.UnimplementedPaymentServiceServer
	mu           sync.RWMutex
	fixtureID    string
	payments     map[string]simulatedProviderPayment
	byBooking    map[string]string
	publisher    simulatedPaymentEventPublisher
	logger       *zap.Logger
	disputeState paymentv1.AdmissionDisputeEvidenceState
	refundState  paymentv1.AdmissionRefundEvidenceState
}

func newSimulatedPaymentAuthority(fixtureID string, publisher simulatedPaymentEventPublisher, logger *zap.Logger) (*simulatedPaymentAuthority, error) {
	if _, err := uuid.Parse(fixtureID); err != nil || publisher == nil {
		return nil, errors.New("simulated Payment fixture identity or publisher unavailable")
	}
	return &simulatedPaymentAuthority{
		fixtureID:    fixtureID,
		payments:     make(map[string]simulatedProviderPayment),
		byBooking:    make(map[string]string),
		publisher:    publisher,
		logger:       logger,
		disputeState: paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE,
		refundState:  paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND,
	}, nil
}

func validateTicketE2EStartup(cfg *config.Config) error {
	return validateTicketE2EStartupWithEnvironment(cfg, os.Getenv, os.Environ())
}

func validateTicketE2EStartupWithEnvironment(cfg *config.Config, getenv func(string) string, environment []string) error {
	if cfg == nil || getenv == nil || getenv("TICKET_E2E_MODE") != simulatedProviderAuthoritySource {
		return errors.New("explicit simulated ticket E2E mode required")
	}
	project := getenv("TICKET_E2E_PROJECT")
	if !ticketE2EProjectPattern.MatchString(project) || getenv("COMPOSE_PROJECT_NAME") != project {
		return errors.New("simulated authority requires the unique build-bound ticket E2E Compose project")
	}
	fixtureID := getenv("TICKET_E2E_FIXTURE_ID")
	if _, err := uuid.Parse(fixtureID); err != nil {
		return errors.New("synthetic ticket E2E fixture identity required")
	}
	dbURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil || dbURL.Scheme != "postgres" {
		return errors.New("simulated authority requires the isolated ticket E2E Booking database")
	}
	dbQuery, err := url.ParseQuery(dbURL.RawQuery)
	if err != nil || len(dbQuery) != 1 || len(dbQuery["sslmode"]) != 1 || dbQuery.Get("sslmode") != "disable" {
		return errors.New("simulated authority requires the isolated ticket E2E Booking database")
	}
	dbConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil || dbConfig.ConnConfig.Host != "ticket-e2e-postgres" || dbConfig.ConnConfig.Port != 5432 ||
		dbConfig.ConnConfig.Database != "rushgo_ticket_e2e_booking" || dbConfig.ConnConfig.User != "ticket_e2e" || len(dbConfig.ConnConfig.Fallbacks) != 0 {
		return errors.New("simulated authority requires the isolated ticket E2E Booking database")
	}
	if cfg.RedisURL != "redis://ticket-e2e-redis:6379" {
		return errors.New("simulated authority requires the isolated ticket E2E Redis endpoint")
	}
	if cfg.GRPCPort != "50053" || cfg.UserServiceAddr != "user-service:50051" || cfg.EventServiceAddr != "event-service:50052" || cfg.SagaServiceAddr != "saga-service:50055" ||
		cfg.PaymentServiceAddr != "booking-e2e:50053" || len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "ticket-e2e-kafka:9092" {
		return errors.New("simulated authority requires the isolated ticket E2E service network")
	}
	if cfg.OTEL.Enabled || cfg.OTEL.ExporterEndpoint != "" {
		return errors.New("simulated authority refuses external telemetry configuration")
	}
	if cfg.StripeSecretKey != "" || cfg.StripeSecretWebhook != "" || cfg.StripePublishableKey != "" {
		return errors.New("simulated authority refuses Stripe credentials in its runtime configuration")
	}
	for _, item := range environment {
		name, value, found := strings.Cut(item, "=")
		if found && strings.Contains(strings.ToUpper(name), "STRIPE") && strings.TrimSpace(value) != "" {
			return errors.New("simulated authority refuses Stripe credentials in its runtime environment")
		}
	}
	return nil
}

func registerTicketE2ESimulatedAuthority(ctx context.Context, server *grpc.Server, cfg *config.Config, logger *zap.Logger) (func(), error) {
	if err := validateTicketE2EStartup(cfg); err != nil {
		return nil, err
	}
	fixtureID := os.Getenv("TICKET_E2E_FIXTURE_ID")
	publisher := pkgkafka.NewProducer(cfg.KafkaBrokers, []string{topics.TopicPaymentEvents}, logger)
	provider, err := newSimulatedPaymentAuthority(fixtureID, publisher, logger)
	if err != nil {
		_ = publisher.Close()
		return nil, err
	}
	paymentv1.RegisterPaymentServiceServer(server, provider)
	control := http.NewServeMux()
	control.HandleFunc("/__ticket_e2e/confirm", provider.confirmPaymentHandler)
	control.HandleFunc("/__ticket_e2e/arm-consumer-ack-fault", provider.armConsumerAckFaultHandler)
	listener, err := net.Listen("tcp", ":18080")
	if err != nil {
		_ = publisher.Close()
		return nil, errors.New("simulated authority control listener unavailable")
	}
	controlServer := &http.Server{Handler: control, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if serveErr := controlServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && logger != nil {
			logger.Error("simulated authority control listener stopped", zap.Error(serveErr))
		}
	}()
	if logger != nil {
		logger.Warn("ticket E2E simulated Payment authority enabled",
			zap.String("authority_source", simulatedProviderAuthoritySource), zap.String("fixture_id", fixtureID))
	}
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = controlServer.Shutdown(shutdownCtx)
		_ = publisher.Close()
	}, nil
}

func (p *simulatedPaymentAuthority) CreatePayment(_ context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	if req == nil || req.PaymentGeneration != 1 || req.Price < 1 || req.Currency != "usd" {
		return nil, status.Error(codes.InvalidArgument, "invalid synthetic payment identity")
	}
	if _, err := uuid.Parse(req.BookingId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid synthetic booking identity")
	}
	if _, err := uuid.Parse(req.UserId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid synthetic buyer identity")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if paymentID, exists := p.byBooking[req.BookingId]; exists {
		prior := p.payments[paymentID]
		if prior.userID != req.UserId || prior.amountCents != req.Price || prior.currency != req.Currency || prior.generation != req.PaymentGeneration {
			return nil, status.Error(codes.AlreadyExists, "synthetic payment identity conflict")
		}
		return simulatedCreatePaymentResponse(prior), nil
	}
	id := uuid.NewString()
	record := simulatedProviderPayment{
		id: id, userID: req.UserId, bookingID: req.BookingId,
		paymentIntentID: "pi_simulated_" + id,
		clientSecret:    "simulated://SIMULATED_PROVIDER_AUTHORITY/" + p.fixtureID + "/" + id,
		amountCents:     req.Price, currency: req.Currency, generation: req.PaymentGeneration,
	}
	p.payments[record.id] = record
	p.byBooking[record.bookingID] = record.id
	if p.logger != nil {
		p.logger.Info("synthetic Payment fixture created", zap.String("authority_source", simulatedProviderAuthoritySource),
			zap.String("fixture_id", p.fixtureID), zap.String("payment_id", record.id), zap.String("booking_id", record.bookingID),
			zap.Int32("amount_cents", record.amountCents), zap.String("currency", record.currency))
	}
	return simulatedCreatePaymentResponse(record), nil
}

func simulatedCreatePaymentResponse(record simulatedProviderPayment) *paymentv1.CreatePaymentResponse {
	return &paymentv1.CreatePaymentResponse{Id: record.id, Status: "requires_payment_method", PaymentIntentId: record.paymentIntentID,
		PaymentIntentClientSecret: record.clientSecret, CreateState: "create_ready"}
}

func (p *simulatedPaymentAuthority) GetPaymentByPaymentIntentId(_ context.Context, req *paymentv1.GetPaymentByIntentIdReq) (*paymentv1.PaymentEntry, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "synthetic Payment lookup identity required")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, record := range p.payments {
		if record.paymentIntentID == req.PaymentIntentId {
			return simulatedPaymentEntry(record), nil
		}
	}
	return nil, status.Error(codes.NotFound, "synthetic Payment fixture not found")
}

func (p *simulatedPaymentAuthority) GetFinancialPayment(_ context.Context, req *paymentv1.FinancialPaymentRequest) (*paymentv1.PaymentEntry, error) {
	if req == nil || req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "synthetic Payment identity required")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, exists := p.byBooking[req.BookingId]
	if !exists {
		return nil, status.Error(codes.NotFound, "synthetic Payment fixture not found")
	}
	record := p.payments[id]
	if record.generation != req.Generation {
		return nil, status.Error(codes.NotFound, "synthetic Payment generation not found")
	}
	return simulatedPaymentEntry(record), nil
}

func simulatedPaymentEntry(record simulatedProviderPayment) *paymentv1.PaymentEntry {
	state, outcome, providerStatus, reconciliation := "requires_payment_method", "unknown", "requires_payment_method", "pending"
	if record.collected {
		state, outcome, providerStatus, reconciliation = "success", "collected", "succeeded", "sale_verified"
	} else if record.resolutionDecisionID != "" {
		state, outcome, providerStatus, reconciliation = "canceled", "not_collected", "canceled", "resolved"
	}
	return &paymentv1.PaymentEntry{Id: record.id, UserId: record.userID, BookingId: record.bookingID, Status: state,
		Price: record.amountCents, Currency: record.currency, PaymentIntentId: record.paymentIntentID, PaymentGeneration: record.generation,
		ProviderOutcome: outcome, ProviderStatus: providerStatus, ReconciliationState: reconciliation, RefundState: "not_required"}
}

func (p *simulatedPaymentAuthority) RequestPaymentResolution(_ context.Context, req *paymentv1.PaymentResolutionRequest) (*paymentv1.PaymentEntry, error) {
	if req == nil || req.Generation != 1 || req.Reason == "" {
		return nil, status.Error(codes.InvalidArgument, "synthetic terminal Payment identity required")
	}
	paymentID, paymentErr := uuid.Parse(req.PaymentId)
	bookingID, bookingErr := uuid.Parse(req.BookingId)
	decisionID, decisionErr := uuid.Parse(req.DecisionId)
	if paymentErr != nil || bookingErr != nil || decisionErr != nil || paymentID == uuid.Nil || bookingID == uuid.Nil || decisionID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "synthetic terminal Payment identity required")
	}
	switch req.Reason {
	case "FAILED", "CANCELLED", "EXPIRED":
	default:
		return nil, status.Error(codes.FailedPrecondition, "simulated Payment resolution requires a terminal Booking")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id, exists := p.byBooking[req.BookingId]
	if !exists || id != req.PaymentId {
		return nil, status.Error(codes.NotFound, "synthetic Payment fixture identity mismatch")
	}
	record := p.payments[id]
	if record.generation != req.Generation || record.bookingID != req.BookingId || record.id != req.PaymentId {
		return nil, status.Error(codes.NotFound, "synthetic Payment fixture identity mismatch")
	}
	if record.collected {
		return nil, status.Error(codes.FailedPrecondition, "simulated Provider does not perform refunds or compensation")
	}
	if record.resolutionDecisionID != "" && record.resolutionDecisionID != req.DecisionId {
		return nil, status.Error(codes.AlreadyExists, "synthetic Payment already resolved by another terminal decision")
	}
	record.resolutionDecisionID = req.DecisionId
	p.payments[id] = record
	return simulatedPaymentEntry(record), nil
}

func (p *simulatedPaymentAuthority) CheckAdmissionFinancialEligibility(_ context.Context, req *paymentv1.AdmissionFinancialEligibilityRequest) (*paymentv1.AdmissionFinancialEligibilityResponse, error) {
	unknown := func(reason string) *paymentv1.AdmissionFinancialEligibilityResponse {
		return &paymentv1.AdmissionFinancialEligibilityResponse{
			EligibilityState: paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN,
			DisputeState:     paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_UNKNOWN,
			RefundState:      paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_UNKNOWN, ReasonCode: reason,
		}
	}
	if req == nil || req.Generation != 1 {
		return unknown("simulated_identity_invalid"), nil
	}
	p.mu.RLock()
	id, exists := p.byBooking[req.BookingId]
	record := p.payments[id]
	p.mu.RUnlock()
	if !exists || record.id != req.PaymentId || record.bookingID != req.BookingId || record.generation != req.Generation {
		return unknown("simulated_identity_mismatch"), nil
	}
	if !record.collected {
		return unknown("simulated_payment_not_collected"), nil
	}
	if p.disputeState != paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE ||
		p.refundState != paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND {
		return unknown("simulated_refund_or_dispute_not_clear"), nil
	}
	observedAt := time.Now().UTC()
	observationID := uuid.NewString()
	if p.logger != nil {
		p.logger.Info("simulated Provider authority observed eligible fixture",
			zap.String("authority_source", simulatedProviderAuthoritySource), zap.String("fixture_id", p.fixtureID),
			zap.String("observation_id", observationID), zap.String("payment_id", record.id), zap.String("booking_id", record.bookingID),
			zap.Int32("amount_cents", record.amountCents), zap.String("currency", record.currency),
			zap.String("refund_state", "provider_observed_no_refund"), zap.String("dispute_state", "provider_observed_no_dispute"))
	}
	return &paymentv1.AdmissionFinancialEligibilityResponse{
		EligibilityState: paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE,
		DisputeState:     p.disputeState,
		RefundState:      p.refundState,
		ObservedAt:       timestamppb.New(observedAt),
		Source:           simulatedProviderAuthoritySource,
		ReasonCode:       "simulated_fixture:" + p.fixtureID + ":" + observationID,
	}, nil
}

func (p *simulatedPaymentAuthority) MakeRefundPayment(context.Context, *paymentv1.MakeRefundPaymentReq) (*emptypb.Empty, error) {
	return nil, status.Error(codes.FailedPrecondition, "simulated Provider does not perform refunds")
}

type confirmSimulatedPaymentRequest struct {
	BookingID string `json:"booking_id"`
	PaymentID string `json:"payment_id"`
}

type confirmSimulatedPaymentResponse struct {
	AuthoritySource string `json:"authority_source"`
	FixtureID       string `json:"fixture_id"`
	BookingID       string `json:"booking_id"`
	PaymentID       string `json:"payment_id"`
	PaymentState    string `json:"payment_state"`
}

type armConsumerAckFaultResponse struct {
	AuthoritySource string `json:"authority_source"`
	FixtureID       string `json:"fixture_id"`
	Armed           bool   `json:"armed"`
}

func (p *simulatedPaymentAuthority) armConsumerAckFaultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := pkgkafka.ArmTicketE2ECommitFault("/run/ticket-e2e-evidence"); err != nil {
		http.Error(w, "synthetic Consumer recovery fault refused", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(armConsumerAckFaultResponse{AuthoritySource: simulatedProviderAuthoritySource, FixtureID: p.fixtureID, Armed: true})
}

func (p *simulatedPaymentAuthority) confirmPaymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4096))
	decoder.DisallowUnknownFields()
	var req confirmSimulatedPaymentRequest
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid synthetic Payment identity", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(req.BookingID); err != nil {
		http.Error(w, "invalid synthetic Booking identity", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(req.PaymentID); err != nil {
		http.Error(w, "invalid synthetic Payment identity", http.StatusBadRequest)
		return
	}
	if err := p.confirmAndPublish(r.Context(), req.BookingID, req.PaymentID); err != nil {
		http.Error(w, "synthetic Payment confirmation refused", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(confirmSimulatedPaymentResponse{AuthoritySource: simulatedProviderAuthoritySource,
		FixtureID: p.fixtureID, BookingID: req.BookingID, PaymentID: req.PaymentID, PaymentState: "collected"})
}

func (p *simulatedPaymentAuthority) confirmAndPublish(ctx context.Context, bookingID, paymentID string) error {
	p.mu.Lock()
	id, exists := p.byBooking[bookingID]
	if !exists || id != paymentID {
		p.mu.Unlock()
		return errors.New("simulated Payment fixture identity mismatch")
	}
	record := p.payments[id]
	if record.resolutionDecisionID != "" {
		p.mu.Unlock()
		return errors.New("resolved non-collection cannot be confirmed later")
	}
	record.collected = true
	p.payments[id] = record
	p.mu.Unlock()

	object, err := json.Marshal(map[string]string{"id": record.paymentIntentID, "object": "payment_intent"})
	if err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		ID   string `json:"id"`
		Data struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}{ID: "simulated-" + p.fixtureID, Data: struct {
		Object json.RawMessage `json:"object"`
	}{Object: object}})
	if err != nil {
		return err
	}
	if _, err := simulatedPaymentIntentWakeID(data); err != nil {
		return fmt.Errorf("synthetic Saga wake identity rejected: %w", err)
	}
	event := pkgkafka.Event{ID: "simulated-" + p.fixtureID + ":" + paymentID, Type: topics.TypePaymentSucceed, Timestamp: time.Now().UTC(), Data: data}
	return p.publisher.Publish(ctx, topics.TopicPaymentEvents, paymentID, event)
}

func simulatedPaymentIntentWakeID(raw json.RawMessage) (string, error) {
	var event struct {
		ID   string `json:"id"`
		Data *struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &event) != nil || event.ID == "" || event.Data == nil {
		return "", errors.New("invalid synthetic Payment wake")
	}
	var object struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	if json.Unmarshal(event.Data.Object, &object) != nil || !strings.HasPrefix(object.ID, "pi_") || len(object.ID) < 4 || len(object.ID) > 255 || (object.Object != "" && object.Object != "payment_intent") {
		return "", errors.New("invalid synthetic PaymentIntent wake identity")
	}
	return object.ID, nil
}
