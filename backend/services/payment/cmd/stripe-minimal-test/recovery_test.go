//go:build stripe_minimal_test

package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestReceiptRecoveryStripeObservationMustMatchExactOriginalObjects(t *testing.T) {
	s := runState{PaymentID: approvedRecoveryPaymentID, BookingID: approvedRecoveryBookingID, PaymentIntentID: approvedRecoveryIntentID}
	valid := &domain.ProviderObservation{
		Account: approvedAccount, IntentID: approvedRecoveryIntentID,
		PaymentID: uuid.MustParse(approvedRecoveryPaymentID), BookingID: uuid.MustParse(approvedRecoveryBookingID),
		Generation: 1, AmountCents: 100, Currency: "usd", Status: "succeeded", Collected: true,
	}
	if !receiptRecoveryObservationMatches(s, valid) {
		t.Fatal("exact TEST PaymentIntent evidence was rejected")
	}
	tests := []struct {
		name   string
		change func(*domain.ProviderObservation)
	}{
		{name: "wrong payment metadata", change: func(o *domain.ProviderObservation) { o.PaymentID = uuid.New() }},
		{name: "wrong booking metadata", change: func(o *domain.ProviderObservation) { o.BookingID = uuid.New() }},
		{name: "wrong generation", change: func(o *domain.ProviderObservation) { o.Generation = 2 }},
		{name: "live", change: func(o *domain.ProviderObservation) { o.LiveMode = true }},
		{name: "not succeeded", change: func(o *domain.ProviderObservation) { o.Status = "processing"; o.Collected = false }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observation := *valid
			tt.change(&observation)
			if receiptRecoveryObservationMatches(s, &observation) {
				t.Fatal("mismatched provider metadata was accepted")
			}
		})
	}
}

func TestReceiptRecoveryRefundRowAllowsOnlyUnattemptedExactCancellation(t *testing.T) {
	s := runState{BookingID: approvedRecoveryBookingID}
	key := "ticketbox:payment:" + approvedRecoveryPaymentID + ":g1:refund"
	amount, currency, reason := int64(100), "usd", "CANCELLED"
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+approvedRecoveryBookingID+":g1:cancel"))
	if err := validateReceiptRecoveryRefund(s, receiptRecoveryRefundRow{state: "not_required", key: &key}); err != nil {
		t.Fatalf("no-obligation state rejected: %v", err)
	}
	if err := validateReceiptRecoveryRefund(s, receiptRecoveryRefundRow{
		state: "pending", key: &key, amount: &amount, currency: &currency, reason: &reason, decision: &decision,
	}); err != nil {
		t.Fatalf("exact untouched cancellation obligation rejected: %v", err)
	}
	providerID := "re_must_not_exist"
	for _, row := range []receiptRecoveryRefundRow{
		{state: "succeeded", key: &key, amount: &amount, currency: &currency, reason: &reason, decision: &decision},
		{state: "pending", key: &key, amount: &amount, currency: &currency, reason: &reason, decision: &decision, providerID: &providerID},
		{state: "pending", key: &key, amount: &amount, currency: &currency, reason: &reason, decision: &decision, firstAttempt: new(time.Time)},
	} {
		if err := validateReceiptRecoveryRefund(s, row); err == nil {
			t.Fatal("refund with an existing provider identity or attempt was accepted")
		}
	}
}
