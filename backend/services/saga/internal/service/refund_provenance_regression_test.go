package service

import (
	"context"
	"errors"
	"testing"
)

// Exercise the actual S1 compensation reader with local clients only. Payment
// audit records are intentionally not an input to the public financial contract.
func TestRefundProvenanceDoesNotChangeSagaFinancialCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, refund, reconciliation string
		amount                                int64
		wantWait                              bool
	}{
		{"full succeeded", "collected", "succeeded", "resolved", 725, false},
		{"partial succeeded stays pending", "collected", "succeeded", "resolved", 724, true},
		{"pending is not refunded", "collected", "pending", "resolved", 725, true},
		{"unknown is not refunded", "collected", "unknown", "resolved", 725, true},
		{"not collected resolved", "not_collected", "not_required", "resolved", 0, false},
		{"unknown collection cannot complete", "unknown", "succeeded", "resolved", 725, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, saga, booking, payment, _ := replayLiteralFixture()
			booking.view.Booking.Status = "EXPIRED"
			booking.view.Phase = "TERMINAL"
			booking.view.DecisionId = decisionID(saga.BookingID.String(), "expire")
			payment.entry.ProviderOutcome = tc.outcome
			payment.entry.RefundState = tc.refund
			payment.entry.RefundAmountCents = tc.amount
			payment.entry.ReconciliationState = tc.reconciliation
			err := s.compensateStoredStep(context.Background(), saga, 1)
			if tc.wantWait && !errors.Is(err, errFinancialWait) || !tc.wantWait && err != nil {
				t.Fatalf("financial completion: err=%v wantWait=%v", err, tc.wantWait)
			}
			if payment.resolutions != 1 || payment.legacyWrites != 0 {
				t.Fatal("audit work changed resolution or invoked legacy financial mutation")
			}
		})
	}
}
