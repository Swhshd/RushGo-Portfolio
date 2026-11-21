package domain

import (
	"testing"

	"github.com/google/uuid"
)

func TestUnusedRefundPaymentSnapshotRequiresExactCollectedFullUSDPayment(t *testing.T) {
	candidate := RefundApprovalCandidate{
		BookingID: uuid.New(), BuyerID: uuid.New(), EventID: uuid.New(), PaymentID: uuid.New(),
		Generation: 1, AmountCents: 725, Currency: "usd",
	}
	snapshot := UnusedRefundPaymentSnapshot{
		BookingID: candidate.BookingID, BuyerID: candidate.BuyerID, PaymentID: candidate.PaymentID,
		PaymentIntentID: "pi_test_123", ProviderAccount: "acct_test_123", Generation: candidate.Generation,
		AmountCents: candidate.AmountCents, Currency: candidate.Currency, Collected: true, EligibleForApproval: true,
	}
	if err := ValidateUnusedRefundPaymentSnapshot(candidate, snapshot); err != nil {
		t.Fatalf("valid Payment snapshot rejected: %v", err)
	}

	cases := []struct {
		name   string
		change func(*UnusedRefundPaymentSnapshot)
	}{
		{name: "wrong buyer", change: func(s *UnusedRefundPaymentSnapshot) { s.BuyerID = uuid.New() }},
		{name: "wrong payment", change: func(s *UnusedRefundPaymentSnapshot) { s.PaymentID = uuid.New() }},
		{name: "wrong generation", change: func(s *UnusedRefundPaymentSnapshot) { s.Generation++ }},
		{name: "wrong amount", change: func(s *UnusedRefundPaymentSnapshot) { s.AmountCents++ }},
		{name: "wrong currency", change: func(s *UnusedRefundPaymentSnapshot) { s.Currency = "eur" }},
		{name: "not collected", change: func(s *UnusedRefundPaymentSnapshot) { s.Collected = false }},
		{name: "live mode payment", change: func(s *UnusedRefundPaymentSnapshot) { s.Livemode = true }},
		{name: "not eligible", change: func(s *UnusedRefundPaymentSnapshot) { s.EligibleForApproval = false }},
		{name: "existing refund decision", change: func(s *UnusedRefundPaymentSnapshot) { s.RefundDecisionID = uuid.New() }},
		{name: "missing payment intent", change: func(s *UnusedRefundPaymentSnapshot) { s.PaymentIntentID = "" }},
		{name: "missing provider account", change: func(s *UnusedRefundPaymentSnapshot) { s.ProviderAccount = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := snapshot
			tc.change(&changed)
			if err := ValidateUnusedRefundPaymentSnapshot(candidate, changed); err == nil {
				t.Fatal("invalid Payment snapshot was accepted")
			}
		})
	}
}
