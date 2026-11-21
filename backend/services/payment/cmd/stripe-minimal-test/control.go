//go:build stripe_minimal_test

package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
)

type testControl struct {
	cfg      testConfig
	token    string
	state    *stateStore
	booking  bookingv1.BookingServiceClient
	worker   *service.FinancialWorker
	payments financialPaymentReader
}

type financialPaymentReader interface {
	GetFinancialPayment(context.Context, uuid.UUID, uuid.UUID, int64) (*domain.FinancialView, error)
}

type controlRequest struct {
	RunID        string `json:"run_id"`
	SourceCommit string `json:"source_commit"`
}

func (c *testControl) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", c.health)
	if mode, ok := configuredRuntimeMode(c.cfg.RuntimeMode); ok && mode != runtimeModeDefaultWorker {
		mux.HandleFunc("/v1/receipts-once", c.receiptsOnce)
		mux.HandleFunc("/v1/run-once", c.runOnce)
	}
	return c.authorize(mux)
}

func (c *testControl) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(c.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (c *testControl) decode(w http.ResponseWriter, r *http.Request) (controlRequest, bool) {
	var q controlRequest
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return q, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(&q) != nil || d.Decode(new(any)) != io.EOF || q.RunID != c.cfg.RunID || q.SourceCommit != c.cfg.SourceCommit {
		http.Error(w, "run identity mismatch", http.StatusConflict)
		return q, false
	}
	return q, true
}

func (c *testControl) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	view := stateView(c.state.snapshot())
	mode, ok := configuredRuntimeMode(c.cfg.RuntimeMode)
	if !ok {
		http.Error(w, "runtime mode unavailable", http.StatusServiceUnavailable)
		return
	}
	view["runtime_mode"] = mode
	if mode == runtimeModeDefaultWorker {
		view["financial_worker_mode"] = financialWorkerModeDefault
	} else {
		view["financial_worker_mode"] = "manual_control"
	}
	writeJSON(w, http.StatusOK, view)
}

func (c *testControl) receiptsOnce(w http.ResponseWriter, r *http.Request) {
	if _, ok := c.decode(w, r); !ok {
		return
	}
	s := c.state.snapshot()
	if s.Blocked {
		http.Error(w, "run stopped after an unknown provider outcome", http.StatusLocked)
		return
	}
	if !c.paymentEventReady(s) {
		http.Error(w, "verified collected TEST webhook required", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	err := c.requireCancelled(ctx, s)
	cancel()
	if err != nil {
		http.Error(w, "winning Booking cancellation required before receipt worker", http.StatusConflict)
		return
	}
	alreadyDone, err := c.state.beginReceiptWork()
	if err != nil {
		http.Error(w, "receipt worker is not available for this run", http.StatusConflict)
		return
	}
	if alreadyDone {
		writeJSON(w, http.StatusOK, stateView(c.state.snapshot()))
		return
	}
	ctx, cancel = context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()
	processed, err := c.worker.RunReceiptsOnce(ctx, 1)
	if err != nil || processed != 1 {
		_ = c.state.block("receipt", "receipt_worker_failed_or_outcome_unknown")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "receipt worker failed or did not process the bound receipt", "processed": processed})
		return
	}
	view, err := c.financialView(ctx, s)
	if err != nil {
		_ = c.state.block("receipt", "durable_payment_state_unavailable")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "durable payment state unavailable"})
		return
	}
	if err := c.state.confirmReceiptApplied(view); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "durable payment receipt identity mismatch", "state": stateView(c.state.snapshot())})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"processed": processed, "state": stateView(c.state.snapshot())})
}

func (c *testControl) runOnce(w http.ResponseWriter, r *http.Request) {
	if _, ok := c.decode(w, r); !ok {
		return
	}
	s := c.state.snapshot()
	if s.Blocked {
		http.Error(w, "run stopped after an unknown provider outcome", http.StatusLocked)
		return
	}
	if !s.Collected || s.ReceiptWorkState != "done" || s.PaymentID == "" || s.BookingID == "" {
		http.Error(w, "durable successful TEST receipt required", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	err := c.requireCancelled(ctx, s)
	cancel()
	if err != nil {
		http.Error(w, "winning Booking cancellation required before financial worker", http.StatusConflict)
		return
	}
	alreadyDone, err := c.state.beginFinancialWork()
	if err != nil {
		if c.state.snapshot().Blocked {
			http.Error(w, "run stopped after an unknown provider outcome", http.StatusLocked)
		} else {
			http.Error(w, "financial worker is not available for this run", http.StatusConflict)
		}
		return
	}
	if alreadyDone {
		writeJSON(w, http.StatusOK, stateView(c.state.snapshot()))
		return
	}
	ctx, cancel = context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	worked, err := c.worker.RunOnce(ctx, 1)
	if err != nil {
		_ = c.state.block("financial", "financial_worker_error_or_outcome_unknown")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "financial worker failed or outcome is unknown", "worked": worked})
		return
	}
	view, err := c.financialView(ctx, s)
	if err != nil {
		_ = c.state.block("financial", "durable_financial_state_unavailable")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "durable financial state unavailable", "worked": worked})
		return
	}
	if err := c.state.confirmRefundApplied(view); err != nil {
		if c.state.snapshot().Blocked {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "financial identity or provider outcome is unknown", "worked": worked, "state": stateView(c.state.snapshot())})
			return
		}
		writeJSON(w, http.StatusConflict, map[string]any{"error": "financial worker reached a known terminal state", "worked": worked, "state": stateView(c.state.snapshot())})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"worked": worked, "state": stateView(c.state.snapshot())})
}

func (c *testControl) paymentEventReady(s runState) bool {
	return s.VerifiedWebhookType == "payment_intent.succeeded" && s.WebhookEventID != "" && s.PaymentID != "" && s.PaymentIntentID != ""
}

func (c *testControl) financialView(ctx context.Context, s runState) (*domain.FinancialView, error) {
	paymentID, err := uuid.Parse(s.PaymentID)
	if err != nil || paymentID == uuid.Nil || paymentID.String() != s.PaymentID {
		return nil, errors.New("bound payment ID invalid")
	}
	bookingID, err := uuid.Parse(s.BookingID)
	if err != nil || bookingID == uuid.Nil || bookingID.String() != s.BookingID {
		return nil, errors.New("bound booking ID invalid")
	}
	userID, err := uuid.Parse(s.ExpectedUserID)
	if err != nil || userID == uuid.Nil || userID.String() != s.ExpectedUserID {
		return nil, errors.New("bound user ID invalid")
	}
	// The service API accepts exactly one selector: payment ID or booking ID.
	view, err := c.payments.GetFinancialPayment(ctx, paymentID, uuid.Nil, 1)
	if err != nil {
		return nil, err
	}
	if !boundFinancialView(view, paymentID, bookingID, userID, s.PaymentIntentID) {
		return nil, errors.New("durable payment identity mismatch")
	}
	return view, nil
}

func boundFinancialView(view *domain.FinancialView, paymentID, bookingID, userID uuid.UUID, intentID string) bool {
	return view != nil && view.Payment != nil && view.Generation == 1 && view.Payment.ID == paymentID &&
		view.Payment.BookingId != nil && *view.Payment.BookingId == bookingID && view.Payment.UserId == userID &&
		view.Payment.Price == 100 && view.Payment.Currency == "usd" && view.Payment.PaymentIntentId == intentID
}

func (c *testControl) requireCancelled(ctx context.Context, s runState) error {
	bookingID, err := uuid.Parse(s.BookingID)
	if err != nil || bookingID == uuid.Nil {
		return errors.New("bound Booking ID invalid")
	}
	life, err := c.booking.GetLifecycleDisposition(ctx, &bookingv1.LifecycleRequest{BookingId: bookingID.String(), Generation: 1})
	if err != nil || life == nil || life.Phase != "TERMINAL" || life.Booking == nil || life.Booking.Id != bookingID.String() ||
		life.Booking.UserId != c.cfg.ExpectedUserID || life.Booking.TotalAmountCents != 100 || life.Currency != "usd" || life.Booking.Status != "CANCELLED" {
		return errors.New("Booking is not the bound terminal cancellation")
	}
	return nil
}

func stateView(s runState) map[string]any {
	mode, ok := configuredRuntimeMode(s.RuntimeMode)
	if !ok {
		mode = runtimeModeManual
	}
	workerMode := "manual_control"
	if mode == runtimeModeDefaultWorker {
		workerMode = financialWorkerModeDefault
	}
	return map[string]any{
		"ok": !s.Blocked, "run_id": s.RunID, "source_commit": s.SourceCommit,
		"runtime_mode": mode, "financial_worker_mode": workerMode,
		"stripe_account": s.StripeAccount, "livemode": false,
		"payment_id": s.PaymentID, "booking_id": s.BookingID, "payment_intent_id": s.PaymentIntentID,
		"create_state": s.CreateState, "webhook_event_id": s.WebhookEventID,
		"collected": s.Collected, "receipt_work_state": s.ReceiptWorkState,
		"refund_id": s.RefundID, "refund_state": s.RefundState,
		"financial_work_state": s.FinancialWorkState, "financial_work_attempts": s.FinancialWorkAttempts,
		"blocked": s.Blocked, "block_reason": s.BlockReason,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
