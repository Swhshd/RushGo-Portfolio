//go:build stripe_minimal_test

package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

type financialReaderStub struct {
	gotPayment uuid.UUID
	gotBooking uuid.UUID
	gotGen     int64
	view       *domain.FinancialView
}

func (s *financialReaderStub) GetFinancialPayment(_ context.Context, payment, booking uuid.UUID, generation int64) (*domain.FinancialView, error) {
	s.gotPayment, s.gotBooking, s.gotGen = payment, booking, generation
	return s.view, nil
}

func TestFinancialViewLooksUpByPaymentOnlyAndRechecksBoundIdentity(t *testing.T) {
	paymentID := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	bookingID := uuid.MustParse("00000000-0000-4000-8000-000000000004")
	userID := uuid.MustParse("00000000-0000-4000-8000-000000000005")
	intentID := "pi_test_bound"
	reader := &financialReaderStub{view: &domain.FinancialView{
		Generation: 1,
		Payment:    &domain.Payment{ID: paymentID, BookingId: &bookingID, UserId: userID, Price: 100, Currency: "usd", PaymentIntentId: intentID},
	}}
	control := &testControl{cfg: testConfig{ExpectedUserID: userID.String()}, payments: reader}
	state := runState{PaymentID: paymentID.String(), BookingID: bookingID.String(), ExpectedUserID: userID.String(), PaymentIntentID: intentID}

	if _, err := control.financialView(context.Background(), state); err != nil {
		t.Fatalf("valid bound view rejected: %v", err)
	}
	if reader.gotPayment != paymentID || reader.gotBooking != uuid.Nil || reader.gotGen != 1 {
		t.Fatalf("lookup selectors = (%s,%s,%d), want payment-only selector", reader.gotPayment, reader.gotBooking, reader.gotGen)
	}

	wrongBooking := uuid.New()
	reader.view.Payment.BookingId = &wrongBooking
	if _, err := control.financialView(context.Background(), state); err == nil {
		t.Fatal("view for a different booking was accepted")
	}
}
