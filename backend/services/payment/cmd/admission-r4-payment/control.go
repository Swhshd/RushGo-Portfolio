//go:build admission_r4

package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serviceRuntime struct {
	cfg              bootstrapConfig
	token            string
	pool             *pgxpool.Pool
	repo             *repository.PostgresPaymentRepository
	payment          *service.PaymentService
	worker           *service.FinancialWorker
	coordinator      *service.AdmissionCoordinator
	bookingConn      *grpc.ClientConn
	provider         *localProvider
	recovery         *admissionRecovery
	prepareGate      *prepareReplyGate
	reconcileDrop    atomic.Bool
	droppedReconcile atomic.Int64
	fixtures         map[uuid.UUID]fixtureConfig
	logger           *zap.Logger
}

type controlRequest struct {
	RunID        string `json:"run_id"`
	SourceCommit string `json:"source_commit"`
	PaymentID    string `json:"payment_id,omitempty"`
}

type controlResponse struct {
	OK                    bool                `json:"ok"`
	Error                 string              `json:"error,omitempty"`
	RunID                 string              `json:"run_id,omitempty"`
	SourceCommit          string              `json:"source_commit,omitempty"`
	BuildSourceCommit     string              `json:"build_source_commit,omitempty"`
	GRPCAddress           string              `json:"grpc_address,omitempty"`
	ControlAddress        string              `json:"control_address,omitempty"`
	Database              string              `json:"database,omitempty"`
	DatabaseUser          string              `json:"database_user,omitempty"`
	Fixtures              []fixtureIdentity   `json:"fixtures,omitempty"`
	Payments              []financialSnapshot `json:"payments,omitempty"`
	Provider              *providerStats      `json:"provider,omitempty"`
	RecoveryPaused        bool                `json:"recovery_paused,omitempty"`
	ReconcileDropArmed    bool                `json:"reconcile_drop_armed,omitempty"`
	DroppedReconcile      int64               `json:"dropped_reconcile_calls,omitempty"`
	PrepareGate           prepareGateSnapshot `json:"prepare_gate,omitempty"`
	PaymentID             string              `json:"payment_id,omitempty"`
	PaymentIntentID       string              `json:"payment_intent_id,omitempty"`
	DynamicPurchaseUserID string              `json:"dynamic_purchase_user_id,omitempty"`
	DynamicPurchaseState  string              `json:"dynamic_purchase_state,omitempty"`
	RefundKey             string              `json:"refund_key,omitempty"`
	RefundState           string              `json:"refund_state,omitempty"`
	Processed             int                 `json:"processed,omitempty"`
	FinancialWorkerMode   string              `json:"financial_worker_mode,omitempty"`
}

type fixtureIdentity struct {
	PaymentID string `json:"payment_id"`
	BookingID string `json:"booking_id"`
	UserID    string `json:"user_id"`
}

type financialSnapshot struct {
	PaymentID           string `json:"payment_id"`
	BookingID           string `json:"booking_id"`
	Generation          int64  `json:"generation"`
	BookingDisposition  string `json:"booking_disposition"`
	ReconciliationState string `json:"reconciliation_state"`
	RefundState         string `json:"refund_state"`
	CreateState         string `json:"create_state"`
}

type prepareGateRun struct {
	id      uint64
	release chan struct{}
	state   string
}

type prepareGateSnapshot struct {
	ID    uint64 `json:"id,omitempty"`
	State string `json:"state"`
}

type prepareReplyGate struct {
	mu      sync.Mutex
	wait    time.Duration
	nextID  uint64
	current *prepareGateRun
}

func newPrepareReplyGate(wait time.Duration) *prepareReplyGate {
	return &prepareReplyGate{wait: wait}
}

func (g *prepareReplyGate) arm() (prepareGateSnapshot, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current != nil && (g.current.state == "armed" || g.current.state == "blocked" || g.current.state == "release_requested") {
		return g.snapshotLocked(), false
	}
	g.nextID++
	g.current = &prepareGateRun{id: g.nextID, release: make(chan struct{}), state: "armed"}
	return g.snapshotLocked(), true
}

func (g *prepareReplyGate) release() (prepareGateSnapshot, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == nil || (g.current.state != "armed" && g.current.state != "blocked") {
		return g.snapshotLocked(), false
	}
	g.current.state = "release_requested"
	close(g.current.release)
	return g.snapshotLocked(), true
}

func (g *prepareReplyGate) waitForRelease(ctx context.Context) error {
	g.mu.Lock()
	gate := g.current
	if gate == nil {
		g.mu.Unlock()
		return nil
	}
	if gate.state == "release_requested" {
		g.mu.Unlock()
		g.finish(gate, "released")
		return nil
	}
	if gate.state != "armed" {
		g.mu.Unlock()
		return nil
	}
	gate.state = "blocked"
	released := gate.release
	g.mu.Unlock()

	timer := time.NewTimer(g.wait)
	defer timer.Stop()
	select {
	case <-released:
		g.finish(gate, "released")
		return nil
	case <-ctx.Done():
		g.finish(gate, "context_canceled")
		return ctx.Err()
	case <-timer.C:
		g.finish(gate, "timed_out")
		return status.Error(codes.Unavailable, "test-only PrepareAdmission reply gate timed out")
	}
}

func (g *prepareReplyGate) finish(gate *prepareGateRun, state string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == gate {
		g.current.state = state
	}
}

func (g *prepareReplyGate) snapshotLocked() prepareGateSnapshot {
	if g.current == nil {
		return prepareGateSnapshot{State: "idle"}
	}
	return prepareGateSnapshot{ID: g.current.id, State: g.current.state}
}

func (g *prepareReplyGate) snapshot() prepareGateSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshotLocked()
}

func (s *serviceRuntime) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == admissionv1.PaymentAdmissionCoordinator_ReconcileAdmission_FullMethodName && s.reconcileDrop.Load() {
			if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
				return nil, err
			}
			if s.reconcileDrop.CompareAndSwap(true, false) {
				s.droppedReconcile.Add(1)
				return nil, status.Error(codes.Unavailable, "test-only ReconcileAdmission call was withheld before Payment settlement")
			}
		}
		response, err := handler(ctx, request)
		if err != nil || info.FullMethod != admissionv1.PaymentAdmissionCoordinator_PrepareAdmission_FullMethodName {
			return response, err
		}
		permit, ok := response.(*admissionv1.Permit)
		if !ok || permit.GetState() != string(admission.Pending) {
			return response, nil
		}
		if err := s.prepareGate.waitForRelease(ctx); err != nil {
			return nil, err
		}
		return response, nil
	}
}

func (s *serviceRuntime) controlHandler() http.Handler {
	mux := http.NewServeMux()
	routes := []string{"/v1/health", "/v1/identity", "/v1/stats"}
	if s.cfg.RuntimeMode != "default_worker" {
		routes = append(routes,
			"/v1/require-refund", "/v1/refund-once", "/v1/recover-once",
			"/v1/prepare-gate/arm", "/v1/prepare-gate/release",
			"/v1/reconcile-drop/arm", "/v1/reconcile-drop/clear", "/v1/recovery/resume",
		)
	}
	if s.cfg.DynamicPurchase != nil && s.cfg.DynamicPurchase.Enabled {
		routes = append(routes, "/v1/confirm-purchase")
	}
	for _, route := range routes {
		mux.HandleFunc("POST "+route, s.handleControl)
	}
	return mux
}

func (s *serviceRuntime) handleControl(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeControlJSON(w, http.StatusUnauthorized, controlResponse{Error: "control_authentication_failed"})
		return
	}
	var request controlRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF {
		writeControlJSON(w, http.StatusBadRequest, controlResponse{Error: "invalid_control_request"})
		return
	}
	if request.RunID != s.cfg.RunID || request.SourceCommit != s.cfg.SourceCommit {
		writeControlJSON(w, http.StatusForbidden, controlResponse{Error: "run_or_source_identity_mismatch"})
		return
	}

	switch r.URL.Path {
	case "/v1/health", "/v1/identity":
		s.handleHealth(w, r)
	case "/v1/stats":
		s.handleStats(w, r)
	case "/v1/confirm-purchase":
		s.handleConfirmPurchase(w, r, request)
	case "/v1/require-refund":
		s.handleRequireRefund(w, r, request)
	case "/v1/refund-once":
		s.handleRefundOnce(w, r, request)
	case "/v1/recover-once":
		s.handleRecoverOnce(w, r)
	case "/v1/prepare-gate/arm":
		pauseCtx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		pauseErr := s.recovery.pause(pauseCtx)
		cancel()
		if pauseErr != nil {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "admission_recovery_did_not_quiesce"})
			return
		}
		gate, ok := s.prepareGate.arm()
		if !ok {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "prepare_gate_already_armed", PrepareGate: gate})
			return
		}
		writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true, PrepareGate: gate}))
	case "/v1/prepare-gate/release":
		gate, ok := s.prepareGate.release()
		if !ok {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "prepare_gate_not_waiting", PrepareGate: gate})
			return
		}
		writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true, PrepareGate: gate}))
	case "/v1/reconcile-drop/arm":
		s.handleArmReconcileDrop(w, r)
	case "/v1/reconcile-drop/clear":
		s.reconcileDrop.Store(false)
		s.recovery.unpause()
		writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true}))
	case "/v1/recovery/resume":
		s.recovery.unpause()
		writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true}))
	default:
		writeControlJSON(w, http.StatusNotFound, controlResponse{Error: "unknown_control_operation"})
	}
}

func (s *serviceRuntime) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := strings.TrimPrefix(header, prefix)
	return len(provided) == len(s.token) && subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) == 1
}

func (s *serviceRuntime) baseResponse(response controlResponse) controlResponse {
	response.RunID = s.cfg.RunID
	response.SourceCommit = s.cfg.SourceCommit
	response.BuildSourceCommit = buildSourceCommit
	response.GRPCAddress = s.cfg.GRPCAdvertiseAddress
	response.ControlAddress = s.cfg.ControlListenAddress
	response.RecoveryPaused = s.recovery.isPaused()
	response.ReconcileDropArmed = s.reconcileDrop.Load()
	response.DroppedReconcile = s.droppedReconcile.Load()
	response.PrepareGate = s.prepareGate.snapshot()
	if s.cfg.RuntimeMode == "default_worker" {
		response.FinancialWorkerMode = "background_default_worker"
	}
	return response
}

func (s *serviceRuntime) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		writeControlJSON(w, http.StatusServiceUnavailable, controlResponse{Error: "payment_database_unavailable"})
		return
	}
	var database, databaseUser string
	if err := s.pool.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&database, &databaseUser); err != nil ||
		database != s.cfg.ExpectedDatabase || databaseUser != s.cfg.ExpectedUser {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_database_identity_mismatch"})
		return
	}
	fixtures := make([]fixtureIdentity, 0, len(s.cfg.Fixtures))
	for _, fixture := range s.cfg.Fixtures {
		fixtures = append(fixtures, fixtureIdentity{PaymentID: fixture.PaymentID, BookingID: fixture.BookingID, UserID: fixture.UserID})
	}
	response := s.baseResponse(controlResponse{OK: true, Database: database, DatabaseUser: databaseUser, Fixtures: fixtures})
	if s.cfg.DynamicPurchase != nil && s.cfg.DynamicPurchase.Enabled {
		response.DynamicPurchaseUserID = s.cfg.DynamicPurchase.UserID
	}
	writeControlJSON(w, http.StatusOK, response)
}

func (s *serviceRuntime) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	payments := make([]financialSnapshot, 0, len(s.cfg.Fixtures))
	for _, fixture := range s.cfg.Fixtures {
		paymentID, _ := uuid.Parse(fixture.PaymentID)
		view, err := s.repo.GetFinancial(ctx, paymentID)
		if err != nil || view == nil || view.Payment == nil || view.Payment.ID != paymentID {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_fixture_identity_unavailable"})
			return
		}
		payments = append(payments, financialSnapshot{
			PaymentID: fixture.PaymentID, BookingID: fixture.BookingID, Generation: view.Generation,
			BookingDisposition:  view.BookingDisposition,
			ReconciliationState: view.ReconciliationState, RefundState: view.RefundState, CreateState: view.CreateState,
		})
	}
	if s.provider != nil && s.cfg.DynamicPurchase != nil && s.cfg.DynamicPurchase.Enabled {
		dynamic := s.provider.dynamicStatsSnapshot()
		if dynamic.Created {
			paymentID, parseErr := uuid.Parse(dynamic.PaymentID)
			view, err := s.repo.GetFinancial(ctx, paymentID)
			op, intentID, owned := s.provider.dynamicOperationFor(paymentID)
			if parseErr != nil || err != nil || !owned || !matchesDynamicFinancial(view, op, intentID) {
				writeControlJSON(w, http.StatusConflict, controlResponse{Error: "dynamic_purchase_identity_unavailable"})
				return
			}
			payments = append(payments, financialSnapshot{
				PaymentID: paymentID.String(), BookingID: op.Identity.BookingID.String(), Generation: view.Generation,
				BookingDisposition: view.BookingDisposition, ReconciliationState: view.ReconciliationState,
				RefundState: view.RefundState, CreateState: view.CreateState,
			})
		}
	}
	provider := s.provider.stats()
	response := s.baseResponse(controlResponse{OK: true, Payments: payments, Provider: &provider})
	writeControlJSON(w, http.StatusOK, response)
}

func (s *serviceRuntime) handleConfirmPurchase(w http.ResponseWriter, r *http.Request, request controlRequest) {
	if s.cfg.DynamicPurchase == nil || !s.cfg.DynamicPurchase.Enabled || s.payment == nil || s.repo == nil || s.provider == nil {
		writeControlJSON(w, http.StatusNotFound, controlResponse{Error: "dynamic_purchase_control_unavailable"})
		return
	}
	paymentID, err := uuid.Parse(request.PaymentID)
	if err != nil || paymentID == uuid.Nil || request.PaymentID != paymentID.String() {
		writeControlJSON(w, http.StatusBadRequest, controlResponse{Error: "dynamic_purchase_payment_id_invalid"})
		return
	}
	op, intentID, owned := s.provider.dynamicOperationFor(paymentID)
	if !owned {
		writeControlJSON(w, http.StatusNotFound, controlResponse{Error: "dynamic_purchase_payment_not_found"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	view, err := s.repo.GetFinancial(ctx, paymentID)
	if err != nil || !matchesDynamicFinancial(view, op, intentID) {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "dynamic_purchase_payment_identity_mismatch"})
		return
	}
	receipt, err := s.provider.dynamicConfirmationReceipt(paymentID)
	if err != nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "dynamic_purchase_confirmation_unavailable"})
		return
	}
	if err := s.payment.AcceptProviderReceipt(ctx, receipt); err != nil {
		writeControlJSON(w, http.StatusServiceUnavailable, controlResponse{Error: "dynamic_purchase_receipt_persistence_failed"})
		return
	}
	if err := s.provider.markDynamicReceiptAccepted(paymentID, receipt); err != nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "dynamic_purchase_receipt_identity_mismatch"})
		return
	}
	writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{
		OK: true, PaymentID: paymentID.String(), PaymentIntentID: intentID, DynamicPurchaseState: "receipt_accepted",
	}))
}

func (s *serviceRuntime) handleRequireRefund(w http.ResponseWriter, r *http.Request, request controlRequest) {
	paymentID, fixture, ok := s.fixtureFor(request.PaymentID)
	if !ok {
		writeControlJSON(w, http.StatusBadRequest, controlResponse{Error: "payment_fixture_not_allowlisted"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	view, err := s.repo.GetFinancial(ctx, paymentID)
	if err != nil || !matchesFixtureFinancial(view, fixture) || view.RefundState != "not_required" || view.ReconciliationState != "sale_verified" {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_financial_identity_not_refundable"})
		return
	}
	bookingID, _ := uuid.Parse(fixture.BookingID)
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:expire"))
	refundKey := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	obligation := domain.RefundObligation{
		PaymentID: paymentID, DecisionID: decisionID, Generation: fixture.Generation,
		AmountCents: int64(view.Payment.Price), Currency: view.Payment.Currency,
		Reason: "EXPIRED", Key: refundKey,
	}
	updated, err := s.repo.RequireRefund(ctx, obligation)
	if err != nil || updated == nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "refund_obligation_rejected"})
		return
	}
	writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{
		OK: true, PaymentID: fixture.PaymentID, RefundKey: refundKey, RefundState: updated.RefundState,
	}))
}

func (s *serviceRuntime) handleRefundOnce(w http.ResponseWriter, r *http.Request, request controlRequest) {
	paymentID, fixture, ok := s.fixtureFor(request.PaymentID)
	if !ok {
		writeControlJSON(w, http.StatusBadRequest, controlResponse{Error: "payment_fixture_not_allowlisted"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	for _, candidate := range s.cfg.Fixtures {
		candidateID, _ := uuid.Parse(candidate.PaymentID)
		view, err := s.repo.GetFinancial(ctx, candidateID)
		if err != nil || view == nil || view.Payment == nil || view.Payment.ID != candidateID {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_fixture_identity_unavailable"})
			return
		}
		if candidateID == paymentID {
			if view.RefundState != "pending" || !matchesFixtureFinancial(view, candidate) {
				writeControlJSON(w, http.StatusConflict, controlResponse{Error: "refund_obligation_not_ready"})
				return
			}
			continue
		}
		if view.CreateState == "create_pending" || view.CreateState == "create_unknown" || view.CreateState == "create_claimed" ||
			view.RefundState == "pending" || view.RefundState == "claimed" || view.RefundState == "retryable" || view.RefundState == "provider_pending" || view.RefundState == "unknown" ||
			(view.ProviderOutcome != "collected" && (view.CancelState == "cancel_pending" || view.CancelState == "cancel_claimed" || view.CancelState == "cancel_unknown")) ||
			((view.CreateState == "create_ready" || view.CreateState == "create_attention") && (view.ReconciliationState == "pending" || view.ReconciliationState == "claimed")) {
			writeControlJSON(w, http.StatusConflict, controlResponse{Error: "another_payment_has_due_financial_work"})
			return
		}
	}
	processed, err := s.worker.RunOnce(ctx, 1)
	if err != nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_financial_worker_failed", Processed: processed})
		return
	}
	view, viewErr := s.repo.GetFinancial(ctx, paymentID)
	if viewErr != nil || view == nil || !matchesFixtureFinancial(view, fixture) {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_financial_identity_unavailable", Processed: processed})
		return
	}
	writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{
		OK: true, PaymentID: fixture.PaymentID, RefundState: view.RefundState, Processed: processed,
	}))
}

func (s *serviceRuntime) handleRecoverOnce(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	processed, err := s.recovery.run(ctx, true)
	if err != nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "payment_admission_recovery_failed", Processed: processed})
		return
	}
	writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true, Processed: processed}))
}

func (s *serviceRuntime) handleArmReconcileDrop(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	if err := s.recovery.pause(ctx); err != nil {
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "admission_recovery_did_not_quiesce"})
		return
	}
	if !s.reconcileDrop.CompareAndSwap(false, true) {
		s.recovery.unpause()
		writeControlJSON(w, http.StatusConflict, controlResponse{Error: "reconcile_drop_already_armed"})
		return
	}
	writeControlJSON(w, http.StatusOK, s.baseResponse(controlResponse{OK: true}))
}

func (s *serviceRuntime) fixtureFor(rawID string) (uuid.UUID, fixtureConfig, bool) {
	id, err := uuid.Parse(rawID)
	if err != nil || id == uuid.Nil || rawID != id.String() {
		return uuid.Nil, fixtureConfig{}, false
	}
	fixture, ok := s.fixtures[id]
	return id, fixture, ok
}

func matchesFixtureFinancial(view *domain.FinancialView, fixture fixtureConfig) bool {
	if view == nil || view.Payment == nil || view.Payment.BookingId == nil || view.LiveMode == nil {
		return false
	}
	paymentID, paymentErr := uuid.Parse(fixture.PaymentID)
	bookingID, bookingErr := uuid.Parse(fixture.BookingID)
	userID, userErr := uuid.Parse(fixture.UserID)
	return paymentErr == nil && bookingErr == nil && userErr == nil && view.Payment.ID == paymentID &&
		*view.Payment.BookingId == bookingID && view.Payment.UserId == userID && view.Generation == fixture.Generation &&
		view.Managed && view.ProviderAccount == fixture.ProviderAccount && !*view.LiveMode &&
		view.Payment.PaymentIntentId == fixture.PaymentIntentID && int64(view.Payment.Price) == fixture.AmountCents &&
		view.Payment.Currency == fixture.Currency && view.ProviderOutcome == "collected" &&
		view.CreateState == "create_ready" && view.CancelState == "not_requested"
}

func matchesDynamicFinancial(view *domain.FinancialView, op domain.CreateOperation, intentID string) bool {
	if view == nil || view.Payment == nil || view.Payment.BookingId == nil || view.LiveMode == nil {
		return false
	}
	i := op.Identity
	return view.Payment.ID == op.PaymentID && *view.Payment.BookingId == i.BookingID && view.Payment.UserId == i.UserID &&
		view.Generation == i.Generation && view.Managed && view.ProviderAccount == i.ProviderAccount && !*view.LiveMode &&
		view.Payment.PaymentIntentId == intentID && intentID == "pi_r4_"+strings.ReplaceAll(op.PaymentID.String(), "-", "") &&
		int64(view.Payment.Price) == 100 && view.Payment.Currency == "usd" && i.UserID != uuid.Nil &&
		i.Generation == 1 && i.AmountCents == 100 && i.Currency == "usd" && i.ProviderAccount == "acct_r4_local" &&
		!i.LiveMode && view.CreateState == "create_ready" && view.CancelState == "not_requested" &&
		(view.ProviderOutcome == "unknown" || view.ProviderOutcome == "collected")
}

func writeControlJSON(w http.ResponseWriter, statusCode int, response controlResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}
