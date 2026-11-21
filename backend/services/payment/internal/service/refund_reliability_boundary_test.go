package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/reliabilityhook"
)

type refundBoundaryRecorder struct {
	enabled bool
	calls   []reliabilityhook.Boundary
}

func (r *refundBoundaryRecorder) Enabled() bool { return r.enabled }

func (r *refundBoundaryRecorder) Await(_ context.Context, boundary reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error) {
	r.calls = append(r.calls, boundary)
	return reliabilityhook.DecisionContinue, true, nil
}

func (r *refundBoundaryRecorder) AwaitW28Refund(ctx context.Context, boundary reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error) {
	return r.Await(ctx, boundary)
}

func refundBoundaryFixture() (domain.FinancialClaim, domain.FinancialResult) {
	paymentID, bookingID := uuid.New(), uuid.New()
	token := uuid.New()
	claim := domain.FinancialClaim{
		PaymentID:  paymentID,
		Token:      token,
		Fence:      7,
		LeaseUntil: time.Now().Add(time.Minute),
		Kind:       "refund",
		Operation: domain.CreateOperation{
			PaymentID: paymentID,
			Identity: domain.FinancialIdentity{
				BookingID:       bookingID,
				Generation:      1,
				AmountCents:     725,
				Currency:        "usd",
				ProviderAccount: "acct_fixture",
				LiveMode:        false,
			},
		},
		Refund: &domain.RefundObligation{
			PaymentID:   paymentID,
			Generation:  1,
			AmountCents: 725,
			Currency:    "usd",
			Key:         "ticketbox:payment:" + paymentID.String() + ":g1:refund",
		},
		View: &domain.FinancialView{Payment: &domain.Payment{
			PaymentIntentId: "pi_fixture",
		}},
	}
	result := domain.FinancialResult{
		State: "succeeded",
		Observation: &domain.ProviderObservation{
			PaymentID:            paymentID,
			BookingID:            bookingID,
			Generation:           1,
			IntentID:             "pi_fixture",
			RefundID:             "re_fixture",
			Status:               "succeeded",
			AmountCents:          725,
			SucceededRefundCents: 725,
			Currency:             "usd",
			Account:              "acct_fixture",
			LiveMode:             false,
		},
	}
	return claim, result
}

func TestRefundReliabilityBoundaryIsDisabledWithoutController(t *testing.T) {
	claim, result := refundBoundaryFixture()
	worker := &FinancialWorker{}
	if err := worker.awaitVerifiedFullRefundBoundary(context.Background(), claim, result); err != nil {
		t.Fatalf("disabled refund boundary returned %v", err)
	}

	recorder := &refundBoundaryRecorder{enabled: false}
	worker.reliability = recorder
	if err := worker.awaitVerifiedFullRefundBoundary(context.Background(), claim, result); err != nil {
		t.Fatalf("disabled controller returned %v", err)
	}
	if len(recorder.calls) != 0 {
		t.Fatalf("disabled controller observed %d boundaries", len(recorder.calls))
	}
}

func TestRefundReliabilityBoundaryBindsThePreMarkerClaim(t *testing.T) {
	claim, result := refundBoundaryFixture()
	recorder := &refundBoundaryRecorder{enabled: true}
	worker := &FinancialWorker{reliability: recorder}
	if err := worker.awaitVerifiedFullRefundBoundary(context.Background(), claim, result); err != nil {
		t.Fatalf("refund boundary returned %v", err)
	}
	if len(recorder.calls) != 1 {
		t.Fatalf("refund boundary calls=%d", len(recorder.calls))
	}
	boundary := recorder.calls[0]
	if boundary.WindowID != "saga.late_collection.compensation_verified_full_refund" ||
		boundary.OperationID != claim.PaymentID.String() ||
		boundary.Checkpoint != "payment_verified_full_refund_before_marker" ||
		boundary.Action != reliabilityhook.ActionPause {
		t.Fatalf("unexpected refund boundary: %+v", boundary)
	}
	want := map[string]string{
		"payment_id":          claim.PaymentID.String(),
		"booking_id":          claim.Operation.Identity.BookingID.String(),
		"payment_intent_id":   "pi_fixture",
		"refund_id":           "re_fixture",
		"refund_amount_cents": "725",
		"refund_currency":     "usd",
		"claim_token":         claim.Token.String(),
		"claim_fence":         "7",
		"provider_account":    "acct_fixture",
		"livemode":            "false",
	}
	for key, value := range want {
		if boundary.Facts[key] != value {
			t.Fatalf("fact %s=%q want %q", key, boundary.Facts[key], value)
		}
	}
	if _, ok := boundary.Facts["lease_until"]; !ok {
		t.Fatal("lease_until fact is missing")
	}
}

func TestRefundReliabilityBoundaryRejectsAnInactiveClaimBeforeCallingController(t *testing.T) {
	claim, result := refundBoundaryFixture()
	claim.LeaseUntil = time.Now().Add(-time.Second)
	recorder := &refundBoundaryRecorder{enabled: true}
	worker := &FinancialWorker{reliability: recorder}
	if err := worker.awaitVerifiedFullRefundBoundary(context.Background(), claim, result); err == nil {
		t.Fatal("inactive refund claim was accepted")
	}
	if len(recorder.calls) != 0 {
		t.Fatal("inactive refund claim reached the controller")
	}
}

func TestW28RefundEvidenceIsAvailableOnTheNonPrimaryWorker(t *testing.T) {
	claim, result := refundBoundaryFixture()
	recorder := &refundBoundaryRecorder{enabled: true}
	worker := &FinancialWorker{w28RefundHook: recorder}
	if err := worker.awaitVerifiedFullRefundBoundary(context.Background(), claim, result); err != nil {
		t.Fatal(err)
	}
	if len(recorder.calls) != 1 || recorder.calls[0].Checkpoint != "payment_w28_verified_refund_before_marker" ||
		recorder.calls[0].Facts["claim_token"] != claim.Token.String() {
		t.Fatalf("expected the W28-specific checkpoint for the actual refund claim")
	}
}
