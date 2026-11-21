//go:build stripe_minimal_test

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

type runState struct {
	Version               int    `json:"version"`
	RuntimeMode           string `json:"runtime_mode,omitempty"`
	RunID                 string `json:"run_id"`
	SourceCommit          string `json:"source_commit"`
	StripeAccount         string `json:"stripe_account"`
	ExpectedUserID        string `json:"expected_user_id"`
	BookingID             string `json:"booking_id,omitempty"`
	PaymentID             string `json:"payment_id,omitempty"`
	PaymentIntentID       string `json:"payment_intent_id,omitempty"`
	CreateKey             string `json:"create_key,omitempty"`
	CreateState           string `json:"create_state"`
	RefundID              string `json:"refund_id,omitempty"`
	RefundDecisionID      string `json:"refund_decision_id,omitempty"`
	RefundKey             string `json:"refund_key,omitempty"`
	RefundState           string `json:"refund_state"`
	WebhookEventID        string `json:"webhook_event_id,omitempty"`
	VerifiedWebhookType   string `json:"verified_webhook_type,omitempty"`
	ReceiptWorkState      string `json:"receipt_work_state,omitempty"`
	FinancialWorkState    string `json:"financial_work_state,omitempty"`
	FinancialWorkAttempts int    `json:"financial_work_attempts"`
	Collected             bool   `json:"collected"`
	Blocked               bool   `json:"blocked"`
	BlockReason           string `json:"block_reason,omitempty"`
}

type stateStore struct {
	mu    sync.Mutex
	path  string
	state runState
}

func openState(path string, cfg testConfig) (*stateStore, error) {
	mode, ok := configuredRuntimeMode(cfg.RuntimeMode)
	if strings.TrimSpace(path) == "" || cfg.ExpectedStripeAccount != approvedAccount || !uuidPattern(cfg.ExpectedUserID) || !ok {
		return nil, errors.New("persistent state identity is outside the approved TEST scope")
	}
	if info, err := os.Stat(path); err == nil {
		if !privateRegularFile(info) {
			return nil, errors.New("persistent state file must be private")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.New("persistent state file cannot be read")
		}
		var state runState
		if json.Unmarshal(data, &state) != nil {
			return nil, errors.New("persistent state identity mismatch")
		}
		persistedMode, persistedModeValid := configuredRuntimeMode(state.RuntimeMode)
		if state.Version != 1 || state.RunID != cfg.RunID || state.SourceCommit != cfg.SourceCommit || state.StripeAccount != cfg.ExpectedStripeAccount || state.ExpectedUserID != cfg.ExpectedUserID ||
			!persistedModeValid || persistedMode != mode {
			return nil, errors.New("persistent state identity mismatch")
		}
		state.RuntimeMode = mode
		if state.CreateState == "creating" || state.RefundState == "creating" || state.ReceiptWorkState == "running" || state.FinancialWorkState == "running" {
			if !state.Blocked {
				state.Blocked = true
				state.BlockReason = "prior_external_write_outcome_unknown"
			}
			if state.CreateState == "creating" {
				state.CreateState = "unknown"
			}
			if state.RefundState == "creating" {
				state.RefundState = "unknown"
			}
			if state.ReceiptWorkState == "running" {
				state.ReceiptWorkState = "unknown"
			}
			if state.FinancialWorkState == "running" {
				state.FinancialWorkState = "unknown"
			}
			if err := atomicState(path, state); err != nil {
				return nil, errors.New("unknown-state latch could not be persisted")
			}
		}
		return &stateStore{path: path, state: state}, nil
	} else if !os.IsNotExist(err) {
		return nil, errors.New("persistent state path unavailable")
	}
	s := runState{Version: 1, RuntimeMode: mode, RunID: cfg.RunID, SourceCommit: cfg.SourceCommit, StripeAccount: cfg.ExpectedStripeAccount, ExpectedUserID: cfg.ExpectedUserID}
	if err := atomicState(path, s); err != nil {
		return nil, errors.New("persistent state could not be initialized")
	}
	return &stateStore{path: path, state: s}, nil
}

func configuredRuntimeMode(mode string) (string, bool) {
	switch mode {
	case "", runtimeModeManual:
		return runtimeModeManual, true
	case runtimeModeDefaultWorker:
		return runtimeModeDefaultWorker, true
	default:
		return "", false
	}
}

func atomicState(path string, s runState) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".stripe-test-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	return syncParentDirectory(filepath.Dir(path))
}

func (s *stateStore) snapshot() runState { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *stateStore) update(fn func(*runState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state
	if err := fn(&next); err != nil {
		return err
	}
	if err := atomicState(s.path, next); err != nil {
		s.state.Blocked = true
		s.state.BlockReason = "persistent_state_write_failed"
		return err
	}
	s.state = next
	return nil
}

func (s *stateStore) reserve(op domain.CreateOperation) error {
	return s.update(func(st *runState) error {
		if st.Blocked {
			return errors.New("run is stopped after an unknown provider outcome")
		}
		if op.PaymentID == uuid.Nil || op.Identity.BookingID == uuid.Nil || op.Identity.UserID.String() != st.ExpectedUserID || op.Identity.AmountCents != 100 || op.Identity.Currency != "usd" || op.Identity.Generation != 1 || op.Identity.LiveMode || op.Identity.ProviderAccount != st.StripeAccount || op.Key != "ticketbox:payment:"+op.PaymentID.String()+":g1:create" {
			return errors.New("payment operation is outside approved TEST scope")
		}
		if st.PaymentID != "" {
			return errors.New("one-payment TEST limit already bound")
		}
		st.BookingID = op.Identity.BookingID.String()
		st.PaymentID = op.PaymentID.String()
		st.CreateKey = op.Key
		st.CreateState = "creating"
		return nil
	})
}

func (s *stateStore) created(o *domain.ProviderObservation) error {
	invalid := false
	err := s.update(func(st *runState) error {
		if o == nil || st.PaymentID != o.PaymentID.String() || st.BookingID != o.BookingID.String() || o.Generation != 1 || o.AmountCents != 100 || o.Currency != "usd" || o.LiveMode || o.Account != st.StripeAccount || o.IntentID == "" || o.Status == "" || o.Collected {
			st.Blocked = true
			st.BlockReason = "provider_create_identity_mismatch"
			st.CreateState = "unknown"
			invalid = true
			return nil
		}
		st.PaymentIntentID = o.IntentID
		st.CreateState = "created"
		return nil
	})
	if err != nil {
		return err
	}
	if invalid {
		return errors.New("provider create identity mismatch")
	}
	return nil
}

func (s *stateStore) unknownCreate() error {
	return s.update(func(st *runState) error {
		st.CreateState = "unknown"
		st.Blocked = true
		st.BlockReason = "provider_create_outcome_unknown"
		return nil
	})
}
func (s *stateStore) webhook(r domain.ProviderReceipt) error {
	return s.update(func(st *runState) error {
		o := r.Observation
		if st.Blocked {
			return errors.New("run is stopped after an unknown provider outcome")
		}
		if st.PaymentID == "" || o.PaymentID.String() != st.PaymentID || o.BookingID.String() != st.BookingID || o.Generation != 1 || o.AmountCents != 100 || o.Currency != "usd" || o.LiveMode || o.Account != st.StripeAccount || o.IntentID == "" || r.EnvelopeID == "" {
			return errors.New("verified event is outside the one-payment TEST scope")
		}
		if st.PaymentIntentID != "" && st.PaymentIntentID != o.IntentID {
			return errors.New("payment intent identity mismatch")
		}
		switch r.EventType {
		case "payment_intent.succeeded":
			if o.Status != "succeeded" || !o.Collected {
				return errors.New("TEST payment event is not collected")
			}
		case "payment_intent.payment_failed", "payment_intent.canceled":
			return errors.New("failed or canceled payment cannot proceed in this one-payment run")
		case "refund.created", "refund.updated", "refund.failed":
			if st.RefundKey == "" || st.RefundState != "creating" && st.RefundID == "" {
				return errors.New("refund event is not bound to a reserved refund")
			}
			if o.RefundID == "" || st.RefundID != "" && o.RefundID != st.RefundID {
				return errors.New("refund event identity mismatch")
			}
			if st.RefundID == "" {
				st.RefundID = o.RefundID
			}
		default:
			return errors.New("event type is outside approved TEST scope")
		}
		if strings.HasPrefix(r.EventType, "payment_intent.") {
			if st.WebhookEventID != "" && st.WebhookEventID != r.EnvelopeID {
				return errors.New("another payment event is already bound to this run")
			}
			st.PaymentIntentID = o.IntentID
			st.WebhookEventID = r.EnvelopeID
			st.VerifiedWebhookType = r.EventType
		}
		return nil
	})
}

func (s *stateStore) reserveRefund(c domain.FinancialClaim, id string, authorityValidated ...bool) error {
	return s.update(func(st *runState) error {
		if st.Blocked {
			return errors.New("run is stopped after an unknown provider outcome")
		}
		mode, ok := configuredRuntimeMode(st.RuntimeMode)
		if !ok {
			return errors.New("refund runtime mode is invalid")
		}
		if (mode == runtimeModeDefaultWorker && (len(authorityValidated) == 0 || !authorityValidated[0] || validateDefaultWorkerRefundClaim(*st, c, id) != nil || !st.Collected || (st.FinancialWorkState != "" && st.FinancialWorkState != "running" && st.FinancialWorkState != "pending"))) ||
			(mode != runtimeModeDefaultWorker && st.FinancialWorkState != "running") ||
			c.PaymentID.String() != st.PaymentID || c.Operation.Identity.BookingID.String() != st.BookingID || c.Operation.Identity.UserID.String() != st.ExpectedUserID || c.Operation.Identity.ProviderAccount != st.StripeAccount || c.Operation.Identity.LiveMode || c.Operation.Identity.Generation != 1 || c.Operation.Identity.AmountCents != 100 || c.Operation.Identity.Currency != "usd" || id != st.PaymentIntentID || c.Kind != "refund" || c.Refund == nil || c.Refund.PaymentID != c.PaymentID || c.Refund.Generation != 1 || c.Refund.AmountCents != 100 || c.Refund.Currency != "usd" || c.Key != "ticketbox:payment:"+st.PaymentID+":g1:refund" || c.Key != c.Refund.Key {
			return errors.New("refund operation is outside approved TEST scope")
		}
		if st.RefundKey != "" || st.RefundID != "" || st.RefundState != "" {
			return errors.New("one-refund TEST limit already reserved")
		}
		if mode == runtimeModeDefaultWorker {
			decisionID := c.Refund.DecisionID.String()
			if st.RefundDecisionID != "" && st.RefundDecisionID != decisionID {
				return errors.New("approved-unused refund decision changed")
			}
			st.RefundDecisionID = decisionID
			if st.FinancialWorkState == "" || st.FinancialWorkState == "pending" {
				st.FinancialWorkAttempts++
			}
			st.FinancialWorkState = "running"
		}
		st.RefundKey = c.Key
		st.RefundState = "creating"
		return nil
	})
}

func validateDefaultWorkerRefundClaim(st runState, c domain.FinancialClaim, intentID string) error {
	decisionID := uuid.Nil
	if c.Refund != nil {
		decisionID = c.Refund.DecisionID
	}
	key := "ticketbox:payment:" + st.PaymentID + ":g1:refund"
	if st.RuntimeMode != runtimeModeDefaultWorker || st.Blocked || st.PaymentID == "" || st.BookingID == "" ||
		st.CreateState != "created" || st.VerifiedWebhookType != "payment_intent.succeeded" || st.WebhookEventID == "" ||
		st.PaymentIntentID == "" || intentID != st.PaymentIntentID || c.Kind != "refund" || c.PaymentID.String() != st.PaymentID ||
		c.Operation.PaymentID != c.PaymentID || c.Operation.Identity.BookingID.String() != st.BookingID ||
		c.Operation.Identity.UserID.String() != st.ExpectedUserID || c.Operation.Identity.ProviderAccount != st.StripeAccount ||
		c.Operation.Identity.LiveMode || c.Operation.Identity.Generation != 1 || c.Operation.Identity.AmountCents != 100 ||
		c.Operation.Identity.Currency != "usd" || c.Key != key || c.Refund == nil || decisionID == uuid.Nil ||
		c.Refund.PaymentID != c.PaymentID || c.Refund.Generation != 1 || c.Refund.AmountCents != 100 ||
		c.Refund.Currency != "usd" || c.Refund.Reason != domain.RefundReasonConfirmedUnusedTicket || c.Refund.Key != key ||
		c.Refund.Key != c.Key || c.View == nil || c.View.Payment == nil || c.View.Payment.ID.String() != st.PaymentID ||
		c.View.Payment.BookingId == nil || c.View.Payment.BookingId.String() != st.BookingID ||
		c.View.Payment.UserId.String() != st.ExpectedUserID || c.View.Payment.Status != domain.PaymentSuccess ||
		c.View.Payment.Price != 100 || c.View.Payment.Currency != "usd" || c.View.Payment.PaymentIntentId != st.PaymentIntentID ||
		c.View.Generation != 1 || !c.View.Managed || c.View.ProviderAccount != st.StripeAccount || c.View.LiveMode == nil || *c.View.LiveMode ||
		c.View.ProviderOutcome != "collected" || c.View.BookingDisposition != "CONFIRMED" || c.View.ReconciliationState != "refund_required" ||
		c.View.RefundState != "claimed" || c.View.RefundAmountCents != 100 || c.View.RefundReason != domain.RefundReasonConfirmedUnusedTicket ||
		c.View.RefundDecisionID == nil || *c.View.RefundDecisionID != decisionID || c.View.RefundID != "" || c.View.RefundStatus != "" ||
		(st.RefundDecisionID != "" && st.RefundDecisionID != decisionID.String()) || st.RefundKey != "" || st.RefundID != "" || st.RefundState != "" {
		return errors.New("approved-unused refund claim is outside the one-payment TEST scope")
	}
	return nil
}
func (s *stateStore) refundCreated(o *domain.ProviderObservation) error {
	invalid := false
	err := s.update(func(st *runState) error {
		if o == nil || o.PaymentID.String() != st.PaymentID || o.BookingID.String() != st.BookingID || o.Generation != 1 || o.IntentID != st.PaymentIntentID || o.AmountCents != 100 || o.Currency != "usd" || o.LiveMode || o.Account != st.StripeAccount || o.RefundID == "" || o.Status == "" || st.RefundID != "" && st.RefundID != o.RefundID {
			st.Blocked = true
			st.BlockReason = "provider_refund_identity_mismatch"
			st.RefundState = "unknown"
			invalid = true
			return nil
		}
		st.RefundID = o.RefundID
		st.RefundState = o.Status
		return nil
	})
	if err != nil {
		return err
	}
	if invalid {
		return errors.New("provider refund identity mismatch")
	}
	return nil
}
func (s *stateStore) unknownRefund() error {
	return s.update(func(st *runState) error {
		st.RefundState = "unknown"
		st.Blocked = true
		st.BlockReason = "provider_refund_outcome_unknown"
		return nil
	})
}

func (s *stateStore) permitRead(paymentID string) error {
	st := s.snapshot()
	if st.Blocked || (st.PaymentID != "" && paymentID != st.PaymentID) {
		return errors.New("payment outside active one-payment scope")
	}
	return nil
}
func (s *stateStore) permitIntent(id string) error {
	st := s.snapshot()
	if st.Blocked || id == "" || st.PaymentIntentID == "" || id != st.PaymentIntentID {
		return errors.New("intent outside active one-payment scope")
	}
	return nil
}
func (s *stateStore) permitRefund(id string) error {
	st := s.snapshot()
	if st.Blocked || id == "" || st.RefundID == "" || id != st.RefundID {
		return errors.New("refund outside active one-refund scope")
	}
	return nil
}
func (s *stateStore) permitOperation(op domain.CreateOperation) error {
	st := s.snapshot()
	if st.Blocked || st.PaymentID == "" || op.PaymentID.String() != st.PaymentID || op.Identity.BookingID.String() != st.BookingID || op.Identity.UserID.String() != st.ExpectedUserID || op.Key != st.CreateKey || op.Identity.ProviderAccount != st.StripeAccount || op.Identity.AmountCents != 100 || op.Identity.Currency != "usd" || op.Identity.Generation != 1 || op.Identity.LiveMode {
		return errors.New("operation outside one-payment scope")
	}
	return nil
}

func (s *stateStore) permitAdmission(i domain.AdmissionFinancialIdentity) error {
	st := s.snapshot()
	if st.Blocked || st.PaymentID == "" || i.PaymentID.String() != st.PaymentID || i.BookingID.String() != st.BookingID ||
		i.PaymentIntentID != st.PaymentIntentID || i.ProviderAccount != st.StripeAccount || i.LiveMode || i.Generation != 1 || i.AmountCents != 100 || i.Currency != "usd" {
		return errors.New("admission identity outside one-payment scope")
	}
	return nil
}

func (s *stateStore) beginReceiptWork() (bool, error) {
	alreadyDone := false
	err := s.update(func(st *runState) error {
		if st.Blocked {
			return errors.New("run is stopped after an unknown provider outcome")
		}
		if st.ReceiptWorkState == "done" {
			alreadyDone = true
			return nil
		}
		if st.ReceiptWorkState != "" {
			return errors.New("receipt worker already attempted")
		}
		if st.VerifiedWebhookType != "payment_intent.succeeded" || st.WebhookEventID == "" || st.PaymentID == "" || st.PaymentIntentID == "" {
			return errors.New("verified collected TEST webhook required")
		}
		st.ReceiptWorkState = "running"
		return nil
	})
	return alreadyDone, err
}

func (s *stateStore) confirmReceiptApplied(view *domain.FinancialView) error {
	invalid := false
	err := s.update(func(st *runState) error {
		if st.Blocked || st.ReceiptWorkState != "running" || view == nil || view.Payment == nil || view.LiveMode == nil ||
			!view.Managed || view.Payment.ID.String() != st.PaymentID || view.Payment.BookingId == nil || view.Payment.BookingId.String() != st.BookingID ||
			view.Payment.Price != 100 || view.Payment.Currency != "usd" || view.Payment.Status != domain.PaymentSuccess ||
			view.ProviderAccount != st.StripeAccount || *view.LiveMode || view.ProviderOutcome != "collected" || view.Payment.PaymentIntentId != st.PaymentIntentID {
			st.Blocked = true
			st.BlockReason = "durable_payment_receipt_state_mismatch"
			st.ReceiptWorkState = "unknown"
			invalid = true
			return nil
		}
		st.Collected = true
		st.ReceiptWorkState = "done"
		return nil
	})
	if err != nil {
		return err
	}
	if invalid {
		return errors.New("durable payment receipt state mismatch")
	}
	return nil
}

func (s *stateStore) confirmBackgroundReceiptApplied(view *domain.FinancialView) error {
	return s.update(func(st *runState) error {
		paymentID, paymentErr := uuid.Parse(st.PaymentID)
		bookingID, bookingErr := uuid.Parse(st.BookingID)
		userID, userErr := uuid.Parse(st.ExpectedUserID)
		if st.RuntimeMode != runtimeModeDefaultWorker || st.Blocked || st.CreateState != "created" ||
			st.VerifiedWebhookType != "payment_intent.succeeded" || st.WebhookEventID == "" || st.PaymentIntentID == "" ||
			paymentErr != nil || paymentID == uuid.Nil || paymentID.String() != st.PaymentID ||
			bookingErr != nil || userErr != nil || bookingID == uuid.Nil || userID == uuid.Nil ||
			(st.ReceiptWorkState != "" && st.ReceiptWorkState != "done") ||
			!boundFinancialView(view, paymentID, bookingID, userID, st.PaymentIntentID) || view.LiveMode == nil || *view.LiveMode ||
			!view.Managed || view.ProviderAccount != st.StripeAccount || view.ProviderOutcome != "collected" ||
			view.Payment.Status != domain.PaymentSuccess {
			return errors.New("background receipt evidence is outside the one-payment TEST scope")
		}
		st.Collected = true
		st.ReceiptWorkState = "done"
		return nil
	})
}

func (s *stateStore) confirmBackgroundRefundApplied(view *domain.FinancialView) error {
	invalid := false
	err := s.update(func(st *runState) error {
		decisionID, decisionErr := uuid.Parse(st.RefundDecisionID)
		paymentID, paymentErr := uuid.Parse(st.PaymentID)
		bookingID, bookingErr := uuid.Parse(st.BookingID)
		userID, userErr := uuid.Parse(st.ExpectedUserID)
		if st.RuntimeMode != runtimeModeDefaultWorker || st.Blocked || !st.Collected || st.RefundKey != "ticketbox:payment:"+st.PaymentID+":g1:refund" ||
			decisionErr != nil || decisionID == uuid.Nil || paymentErr != nil || bookingErr != nil || userErr != nil ||
			!boundFinancialView(view, paymentID, bookingID, userID, st.PaymentIntentID) || view.LiveMode == nil || *view.LiveMode ||
			!view.Managed || view.ProviderAccount != st.StripeAccount || view.ProviderOutcome != "collected" ||
			view.Payment.Status != domain.PaymentSuccess || view.BookingDisposition != "CONFIRMED" ||
			view.RefundDecisionID == nil || *view.RefundDecisionID != decisionID || view.RefundAmountCents != 100 ||
			view.RefundReason != domain.RefundReasonConfirmedUnusedTicket || view.RefundState == "not_required" {
			st.Blocked = true
			st.BlockReason = "durable_refund_decision_mismatch"
			st.FinancialWorkState = "unknown"
			invalid = true
			return nil
		}
		if st.RefundID != "" && view.RefundID != "" && st.RefundID != view.RefundID {
			st.Blocked = true
			st.BlockReason = "durable_refund_identity_mismatch"
			st.FinancialWorkState = "unknown"
			invalid = true
			return nil
		}
		if st.RefundState == "creating" && st.RefundID == "" {
			return nil
		}
		if view.RefundID != "" {
			st.RefundID = view.RefundID
		}
		switch view.RefundState {
		case "succeeded":
			if st.RefundID == "" || view.RefundID != st.RefundID || view.RefundAmountCents != 100 || view.RefundStatus != "succeeded" {
				st.Blocked = true
				st.BlockReason = "durable_refund_identity_mismatch"
				st.FinancialWorkState = "unknown"
				invalid = true
				return nil
			}
			st.RefundState = "succeeded"
			st.FinancialWorkState = "done"
		case "claimed", "pending", "retryable", "provider_pending", "unknown":
			st.RefundState = view.RefundState
			st.FinancialWorkState = "pending"
		case "attention":
			st.RefundState = view.RefundState
			st.FinancialWorkState = "attention"
		default:
			st.Blocked = true
			st.BlockReason = "durable_refund_state_mismatch"
			st.FinancialWorkState = "unknown"
			invalid = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if invalid {
		return errors.New("background financial state mismatch")
	}
	return nil
}

// recoverReceiptApplied is available only for the known failure where the
// real receipt worker completed but the harness failed while reading the
// resulting durable payment state. The caller must first re-read that state,
// verify the bound Booking is terminal/cancelled, and retrieve the same TEST
// PaymentIntent from Stripe. It never calls the receipt worker or provider.
func (s *stateStore) recoverReceiptApplied(view *domain.FinancialView) error {
	return s.update(func(st *runState) error {
		paymentID, paymentErr := uuid.Parse(st.PaymentID)
		bookingID, bookingErr := uuid.Parse(st.BookingID)
		userID, userErr := uuid.Parse(st.ExpectedUserID)
		if !st.Blocked || st.BlockReason != "durable_payment_state_unavailable" || st.ReceiptWorkState != "unknown" || st.Collected ||
			st.FinancialWorkAttempts != 0 || st.FinancialWorkState != "" || st.RefundID != "" || st.RefundKey != "" || st.RefundState != "" ||
			st.CreateState != "created" || st.VerifiedWebhookType != "payment_intent.succeeded" || st.WebhookEventID == "" || st.PaymentIntentID == "" ||
			paymentErr != nil || bookingErr != nil || userErr != nil || paymentID == uuid.Nil || bookingID == uuid.Nil || userID == uuid.Nil ||
			!boundFinancialView(view, paymentID, bookingID, userID, st.PaymentIntentID) || view.LiveMode == nil || *view.LiveMode || !view.Managed ||
			view.ProviderAccount != st.StripeAccount || view.ProviderOutcome != "collected" || view.Payment.Status != domain.PaymentSuccess ||
			(view.RefundState != "not_required" && view.RefundState != "pending") || view.RefundID != "" || view.RefundStatus != "" ||
			(view.RefundState == "not_required" && view.RefundAmountCents != 0) ||
			(view.RefundState == "pending" && view.RefundAmountCents != 100) {
			return errors.New("receipt recovery evidence outside the one-payment failure scope")
		}
		st.Collected = true
		st.ReceiptWorkState = "done"
		st.Blocked = false
		st.BlockReason = ""
		return nil
	})
}

func (s *stateStore) beginFinancialWork() (bool, error) {
	alreadyDone := false
	passLimit := false
	err := s.update(func(st *runState) error {
		if st.Blocked {
			return errors.New("run is stopped after an unknown provider outcome")
		}
		if st.FinancialWorkState == "done" {
			alreadyDone = true
			return nil
		}
		if st.FinancialWorkState == "attention" {
			return errors.New("financial worker stopped at a known terminal state")
		}
		if st.FinancialWorkState == "running" || st.FinancialWorkState == "unknown" {
			return errors.New("financial worker outcome is unknown")
		}
		if !st.Collected || st.ReceiptWorkState != "done" || st.PaymentID == "" {
			return errors.New("durable payment receipt required")
		}
		if st.FinancialWorkAttempts >= 90 {
			st.FinancialWorkState = "attention"
			st.BlockReason = "financial_worker_pass_limit_reached"
			passLimit = true
			return nil
		}
		st.FinancialWorkAttempts++
		st.FinancialWorkState = "running"
		return nil
	})
	if err == nil && passLimit {
		return false, errors.New("financial worker pass limit reached")
	}
	return alreadyDone, err
}

func (s *stateStore) confirmRefundApplied(view *domain.FinancialView) error {
	invalid := false
	attention := false
	err := s.update(func(st *runState) error {
		if st.Blocked || st.FinancialWorkState != "running" || view == nil || view.Payment == nil || view.LiveMode == nil ||
			!view.Managed || view.Payment.ID.String() != st.PaymentID || view.Payment.BookingId == nil || view.Payment.BookingId.String() != st.BookingID ||
			view.Payment.Price != 100 || view.Payment.Currency != "usd" || view.ProviderAccount != st.StripeAccount || *view.LiveMode ||
			view.Payment.PaymentIntentId != st.PaymentIntentID || view.ProviderOutcome != "collected" {
			st.Blocked = true
			st.BlockReason = "durable_refund_state_mismatch"
			st.FinancialWorkState = "unknown"
			invalid = true
			return nil
		}
		if view.RefundState == "succeeded" {
			if st.RefundID == "" || view.RefundID != st.RefundID || view.RefundAmountCents != 100 || view.RefundStatus != "succeeded" || st.RefundState == "unknown" {
				st.Blocked = true
				st.BlockReason = "durable_refund_identity_mismatch"
				st.FinancialWorkState = "unknown"
				invalid = true
				return nil
			}
			st.RefundState = "succeeded"
			st.FinancialWorkState = "done"
			return nil
		}
		if (view.RefundState == "pending" || view.RefundState == "not_required") &&
			(view.ReconciliationState == "pending" || view.ReconciliationState == "claimed" || view.ReconciliationState == "refund_required") {
			if view.RefundID != "" && st.RefundID != view.RefundID {
				st.Blocked = true
				st.BlockReason = "durable_refund_identity_mismatch"
				st.FinancialWorkState = "unknown"
				invalid = true
				return nil
			}
			st.FinancialWorkState = "pending"
			return nil
		}
		st.FinancialWorkState = "attention"
		st.BlockReason = "financial_worker_reached_terminal_attention"
		attention = true
		return nil
	})
	if err != nil {
		return err
	}
	if invalid {
		return errors.New("durable refund state mismatch")
	}
	if attention {
		return errors.New("financial worker reached a known terminal attention state")
	}
	return nil
}

func (s *stateStore) block(state, reason string) error {
	return s.update(func(st *runState) error {
		st.Blocked = true
		st.BlockReason = reason
		switch state {
		case "receipt":
			st.ReceiptWorkState = "unknown"
		case "financial":
			st.FinancialWorkState = "unknown"
		case "refund":
			st.RefundState = "unknown"
		}
		return nil
	})
}
