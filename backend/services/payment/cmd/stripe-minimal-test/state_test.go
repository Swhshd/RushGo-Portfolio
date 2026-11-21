//go:build stripe_minimal_test

package main

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func testStateConfig() testConfig {
	return testConfig{
		RunID:                 "00000000-0000-4000-8000-000000000001",
		SourceCommit:          "0123456789abcdef0123456789abcdef01234567",
		ExpectedStripeAccount: approvedAccount,
		ExpectedUserID:        "00000000-0000-4000-8000-000000000002",
	}
}

func testCreateOperation() domain.CreateOperation {
	booking := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	user := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	payment := uuid.MustParse("00000000-0000-4000-8000-000000000004")
	return domain.CreateOperation{
		PaymentID: payment,
		Key:       "ticketbox:payment:" + payment.String() + ":g1:create",
		Identity: domain.FinancialIdentity{
			BookingID: booking, UserID: user, Generation: 1,
			ProviderAccount: approvedAccount, AmountCents: 100,
			Currency: "usd",
		},
	}
}

func TestStateReserveAllowsOnlyApproved100CentTestIdentity(t *testing.T) {
	tests := []struct {
		name   string
		change func(*domain.CreateOperation)
	}{
		{name: "wrong user", change: func(op *domain.CreateOperation) { op.Identity.UserID = uuid.New() }},
		{name: "wrong account", change: func(op *domain.CreateOperation) { op.Identity.ProviderAccount = "acct_other" }},
		{name: "live mode", change: func(op *domain.CreateOperation) { op.Identity.LiveMode = true }},
		{name: "wrong amount", change: func(op *domain.CreateOperation) { op.Identity.AmountCents = 101 }},
		{name: "wrong currency", change: func(op *domain.CreateOperation) { op.Identity.Currency = "eur" }},
		{name: "wrong generation", change: func(op *domain.CreateOperation) { op.Identity.Generation = 2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := openState(filepath.Join(t.TempDir(), "state.json"), testStateConfig())
			if err != nil {
				t.Fatal(err)
			}
			op := testCreateOperation()
			tt.change(&op)
			if err := store.reserve(op); err == nil {
				t.Fatal("out-of-scope create operation was accepted")
			}
		})
	}
}

func TestStateBindsOneCreateAndUnknownOutcomeStopsRunAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	cfg := testStateConfig()
	store, err := openState(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := testCreateOperation()
	if err := store.reserve(first); err != nil {
		t.Fatal(err)
	}
	second := testCreateOperation()
	second.PaymentID = uuid.New()
	second.Key = "ticketbox:payment:" + second.PaymentID.String() + ":g1:create"
	second.Identity.BookingID = uuid.New()
	if err := store.reserve(second); err == nil {
		t.Fatal("second payment identity was accepted")
	}
	if err := store.unknownCreate(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openState(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.snapshot(); !got.Blocked || got.CreateState != "unknown" {
		t.Fatalf("unknown outcome did not persist a stop latch: %+v", got)
	}
	if err := reopened.reserve(first); err == nil {
		t.Fatal("write after an unknown provider outcome was accepted")
	}
}

func TestVerifiedWebhookDoesNotMarkPaymentCollectedBeforeDurableReceiptProcessing(t *testing.T) {
	store, err := openState(filepath.Join(t.TempDir(), "state.json"), testStateConfig())
	if err != nil {
		t.Fatal(err)
	}
	op := testCreateOperation()
	if err := store.reserve(op); err != nil {
		t.Fatal(err)
	}
	pi := "pi_test_approved"
	if err := store.created(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: 1, IntentID: pi, AmountCents: 100, Currency: "usd",
		Account: approvedAccount, Status: "requires_payment_method",
	}); err != nil {
		t.Fatal(err)
	}
	receipt := domain.ProviderReceipt{
		EnvelopeID: "evt_test_approved", EventType: "payment_intent.succeeded",
		Observation: domain.ProviderObservation{
			PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
			Generation: 1, IntentID: pi, Status: "succeeded", AmountCents: 100,
			Currency: "usd", Account: approvedAccount, Collected: true,
		},
	}
	if err := store.webhook(receipt); err != nil {
		t.Fatal(err)
	}
	if got := store.snapshot(); got.Collected {
		t.Fatal("signature verification alone marked the payment collected before the receipt worker persisted it")
	}
}

func TestRefundWebhookCanBindTheReservedRefundBeforeProviderResponse(t *testing.T) {
	store, op := prepareCollectedPayment(t)
	if _, err := store.beginFinancialWork(); err != nil {
		t.Fatal(err)
	}
	claim := testRefundClaim(op)
	if err := store.reserveRefund(claim, "pi_test_approved"); err != nil {
		t.Fatal(err)
	}
	if err := store.webhook(domain.ProviderReceipt{
		EnvelopeID: "evt_refund_created", EventType: "refund.created",
		Observation: domain.ProviderObservation{
			PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
			Generation: 1, IntentID: "pi_test_approved", RefundID: "re_test_approved",
			Status: "pending", AmountCents: 100, Currency: "usd", Account: approvedAccount,
		},
	}); err != nil {
		t.Fatalf("valid reserved refund webhook was rejected before API response: %v", err)
	}
	if err := store.refundCreated(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: 1, IntentID: "pi_test_approved", RefundID: "re_test_approved",
		Status: "succeeded", AmountCents: 100, Currency: "usd", Account: approvedAccount,
	}); err != nil {
		t.Fatal(err)
	}
	if got := store.snapshot(); got.Blocked || got.RefundID != "re_test_approved" || got.RefundState != "succeeded" {
		t.Fatalf("reserved refund identity was not preserved: %+v", got)
	}
}

func TestRefundIdentityMismatchPersistsStopLatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	cfg := testStateConfig()
	store, op := prepareCollectedPaymentAt(t, path, cfg)
	if _, err := store.beginFinancialWork(); err != nil {
		t.Fatal(err)
	}
	if err := store.reserveRefund(testRefundClaim(op), "pi_test_approved"); err != nil {
		t.Fatal(err)
	}
	if err := store.webhook(domain.ProviderReceipt{
		EnvelopeID: "evt_refund_created", EventType: "refund.created",
		Observation: domain.ProviderObservation{
			PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
			Generation: 1, IntentID: "pi_test_approved", RefundID: "re_test_approved",
			Status: "pending", AmountCents: 100, Currency: "usd", Account: approvedAccount,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.refundCreated(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: 1, IntentID: "pi_test_approved", RefundID: "re_wrong",
		Status: "succeeded", AmountCents: 100, Currency: "usd", Account: approvedAccount,
	}); err == nil {
		t.Fatal("provider refund identity mismatch was accepted")
	}
	reopened, err := openState(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.snapshot(); !got.Blocked || got.BlockReason != "provider_refund_identity_mismatch" {
		t.Fatalf("refund identity mismatch stop latch was not durable: %+v", got)
	}
}

func TestFinancialWorkerKnownProgressAllowsNextSinglePass(t *testing.T) {
	store, op := prepareCollectedPayment(t)
	if alreadyDone, err := store.beginFinancialWork(); err != nil || alreadyDone {
		t.Fatalf("first worker pass start alreadyDone=%v err=%v", alreadyDone, err)
	}
	view := &domain.FinancialView{
		Payment: &domain.Payment{ID: op.PaymentID, BookingId: &op.Identity.BookingID, Price: 100, Currency: "usd", PaymentIntentId: "pi_test_approved", Status: domain.PaymentSuccess},
		Managed: true, ProviderAccount: approvedAccount, LiveMode: boolPointer(false), ProviderOutcome: "collected",
		RefundState: "pending", ReconciliationState: "refund_required",
	}
	if err := store.confirmRefundApplied(view); err != nil {
		t.Fatalf("known intermediate financial state was rejected: %v", err)
	}
	if alreadyDone, err := store.beginFinancialWork(); err != nil || alreadyDone {
		t.Fatalf("next bounded worker pass not allowed alreadyDone=%v err=%v", alreadyDone, err)
	}
	if got := store.snapshot(); got.Blocked || got.FinancialWorkState != "running" || got.FinancialWorkAttempts != 2 {
		t.Fatalf("known progress did not preserve the run for the next pass: %+v", got)
	}
}

func TestReceiptRecoveryClearsOnlyDurableStateReadFailure(t *testing.T) {
	store, op := prepareBlockedReceipt(t)
	view := receiptRecoveryView(op)
	if err := store.recoverReceiptApplied(view); err != nil {
		t.Fatalf("matching durable TEST evidence did not recover receipt state: %v", err)
	}
	got := store.snapshot()
	if got.Blocked || got.BlockReason != "" || !got.Collected || got.ReceiptWorkState != "done" || got.FinancialWorkAttempts != 0 {
		t.Fatalf("receipt recovery changed unexpected state: %+v", got)
	}
	if alreadyDone, err := store.beginFinancialWork(); err != nil || alreadyDone {
		t.Fatalf("recovered receipt did not permit the next bounded financial pass: alreadyDone=%v err=%v", alreadyDone, err)
	}

	store, op = prepareBlockedReceipt(t)
	view = receiptRecoveryView(op)
	view.RefundState = "pending"
	view.RefundAmountCents = 100
	if err := store.recoverReceiptApplied(view); err != nil {
		t.Fatalf("exact cancellation refund obligation was not compatible with receipt recovery: %v", err)
	}
}

func TestReceiptRecoveryRejectsOtherFailuresAndMismatchedEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runState, *domain.FinancialView)
	}{
		{name: "different failure", mutate: func(s *runState, _ *domain.FinancialView) { s.BlockReason = "provider_create_outcome_unknown" }},
		{name: "financial work attempted", mutate: func(s *runState, _ *domain.FinancialView) { s.FinancialWorkAttempts = 1 }},
		{name: "refund identity exists", mutate: func(s *runState, _ *domain.FinancialView) { s.RefundKey = "ticketbox:payment:unexpected:g1:refund" }},
		{name: "wrong generation", mutate: func(_ *runState, v *domain.FinancialView) { v.Generation = 2 }},
		{name: "wrong user", mutate: func(_ *runState, v *domain.FinancialView) { v.Payment.UserId = uuid.New() }},
		{name: "wrong booking", mutate: func(_ *runState, v *domain.FinancialView) { id := uuid.New(); v.Payment.BookingId = &id }},
		{name: "live evidence", mutate: func(_ *runState, v *domain.FinancialView) { v.LiveMode = boolPointer(true) }},
		{name: "refund already exists", mutate: func(_ *runState, v *domain.FinancialView) { v.RefundID = "re_existing" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, op := prepareBlockedReceipt(t)
			view := receiptRecoveryView(op)
			if err := store.update(func(s *runState) error { tt.mutate(s, view); return nil }); err != nil {
				t.Fatal(err)
			}
			if err := store.recoverReceiptApplied(view); err == nil {
				t.Fatal("out-of-scope recovery evidence was accepted")
			}
			if got := store.snapshot(); !got.Blocked || got.Collected || got.ReceiptWorkState != "unknown" {
				t.Fatalf("rejected recovery changed the stop latch: %+v", got)
			}
		})
	}
}

func prepareBlockedReceipt(t *testing.T) (*stateStore, domain.CreateOperation) {
	t.Helper()
	store, err := openState(filepath.Join(t.TempDir(), "state.json"), testStateConfig())
	if err != nil {
		t.Fatal(err)
	}
	op := testCreateOperation()
	if err := store.reserve(op); err != nil {
		t.Fatal(err)
	}
	if err := store.created(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, IntentID: "pi_test_approved",
		AmountCents: 100, Currency: "usd", Account: approvedAccount, Status: "requires_payment_method",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.webhook(domain.ProviderReceipt{EnvelopeID: "evt_test_approved", EventType: "payment_intent.succeeded", Observation: domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, IntentID: "pi_test_approved",
		Status: "succeeded", AmountCents: 100, Currency: "usd", Account: approvedAccount, Collected: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.beginReceiptWork(); err != nil {
		t.Fatal(err)
	}
	if err := store.block("receipt", "durable_payment_state_unavailable"); err != nil {
		t.Fatal(err)
	}
	return store, op
}

func receiptRecoveryView(op domain.CreateOperation) *domain.FinancialView {
	return &domain.FinancialView{
		Payment: &domain.Payment{ID: op.PaymentID, BookingId: &op.Identity.BookingID, UserId: op.Identity.UserID,
			Price: 100, Currency: "usd", PaymentIntentId: "pi_test_approved", Status: domain.PaymentSuccess},
		Generation: 1, Managed: true, ProviderAccount: approvedAccount, LiveMode: boolPointer(false), ProviderOutcome: "collected", RefundState: "not_required",
	}
}

func prepareCollectedPayment(t *testing.T) (*stateStore, domain.CreateOperation) {
	t.Helper()
	return prepareCollectedPaymentAt(t, filepath.Join(t.TempDir(), "state.json"), testStateConfig())
}

func prepareCollectedPaymentAt(t *testing.T, path string, cfg testConfig) (*stateStore, domain.CreateOperation) {
	t.Helper()
	store, err := openState(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	op := testCreateOperation()
	if err := store.reserve(op); err != nil {
		t.Fatal(err)
	}
	if err := store.created(&domain.ProviderObservation{
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: 1, IntentID: "pi_test_approved", AmountCents: 100, Currency: "usd", Account: approvedAccount, Status: "requires_payment_method",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.webhook(domain.ProviderReceipt{
		EnvelopeID: "evt_test_approved", EventType: "payment_intent.succeeded",
		Observation: domain.ProviderObservation{
			PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
			Generation: 1, IntentID: "pi_test_approved", Status: "succeeded", AmountCents: 100,
			Currency: "usd", Account: approvedAccount, Collected: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.beginReceiptWork(); err != nil {
		t.Fatal(err)
	}
	view := &domain.FinancialView{
		Payment: &domain.Payment{ID: op.PaymentID, BookingId: &op.Identity.BookingID, Price: 100, Currency: "usd", PaymentIntentId: "pi_test_approved", Status: domain.PaymentSuccess},
		Managed: true, ProviderAccount: approvedAccount, LiveMode: boolPointer(false), ProviderOutcome: "collected",
	}
	if err := store.confirmReceiptApplied(view); err != nil {
		t.Fatal(err)
	}
	return store, op
}

func testRefundClaim(op domain.CreateOperation) domain.FinancialClaim {
	key := "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"
	return domain.FinancialClaim{
		PaymentID: op.PaymentID,
		Kind:      "refund",
		Key:       key,
		Operation: op,
		Refund: &domain.RefundObligation{
			PaymentID: op.PaymentID, Generation: 1, AmountCents: 100,
			Currency: "usd", Key: key,
		},
	}
}

func boolPointer(v bool) *bool { return &v }
