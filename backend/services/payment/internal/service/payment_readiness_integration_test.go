package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreatePaymentRejectsUnreadyTierBeforeExistingIntentReuseIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user, tier := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{
		PaymentID:    id,
		Key:          "ticketbox:payment:" + id.String() + ":g1:create",
		FrozenParams: frozen,
		Identity:     domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"},
	}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture",
		IntentID: "pi_existing_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded",
		Collected: true, ObservedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	view := &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id:               booking.String(),
			UserId:           user.String(),
			TotalAmountCents: 725,
			Items:            []*bookingv1.BookingItem{{TicketTierId: tier.String(), Quantity: 1}},
		},
		Generation:          1,
		Phase:               "OPEN",
		Currency:            "usd",
		ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour)),
	}
	s := &PaymentService{
		repo:       repo,
		logger:     zap.NewNop(),
		booking:    &readinessBooking{view: view},
		accounting: denyAccounting{err: ErrAccountingNotReady},
		account:    "acct_fixture",
		provider:   noProvider{},
	}
	_, err := s.CreatePayment(ctx, &domain.CreatePaymentRequest{
		Payment:           domain.Payment{UserId: user, BookingId: &booking, Price: 725, Currency: "usd"},
		PaymentGeneration: 1,
	})
	if !errors.Is(err, ErrAccountingNotReady) {
		t.Fatalf("unready existing-intent path returned %v, want ErrAccountingNotReady", err)
	}
}

func TestFinancialWorkerKeepsTransientReadinessFailureRetryableIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user, tier := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{
		PaymentID:    id,
		Key:          "ticketbox:payment:" + id.String() + ":g1:create",
		FrozenParams: frozen,
		Identity:     domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"},
	}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	claim, err := repo.ClaimCreatePayment(ctx, id, 30*time.Second)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}

	view := &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id:               booking.String(),
			UserId:           user.String(),
			TotalAmountCents: 725,
			Items:            []*bookingv1.BookingItem{{TicketTierId: tier.String(), Quantity: 1}},
		},
		Generation:          1,
		Phase:               "OPEN",
		Currency:            "usd",
		ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour)),
	}
	s := &PaymentService{
		repo:       repo,
		logger:     zap.NewNop(),
		booking:    &readinessBooking{view: view},
		accounting: denyAccounting{err: ErrAccountingUnavailable},
		account:    "acct_fixture",
		provider:   noProvider{},
	}
	if _, err := NewFinancialWorker(s).executeCreate(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetFinancial(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.CreateState != "create_unknown" || got.ErrorCategory != "accounting_readiness_unavailable" || got.Payment.PaymentIntentId != "" {
		t.Fatalf("transient authority failure was not left retryable: %+v", got)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		retry, err := repo.ClaimCreatePayment(ctx, id, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if retry != nil {
			if retry.Kind != "create" || retry.PaymentID != id {
				t.Fatalf("retry claim = %+v", retry)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("transient readiness failure was not retryable")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestFinancialWorkerRejectsUnreadyTierBeforeProviderIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user, tier := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{
		PaymentID:    id,
		Key:          "ticketbox:payment:" + id.String() + ":g1:create",
		FrozenParams: frozen,
		Identity:     domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"},
	}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	claim, err := repo.ClaimCreatePayment(ctx, id, 30*time.Second)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}

	view := &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id:               booking.String(),
			UserId:           user.String(),
			TotalAmountCents: 725,
			Items:            []*bookingv1.BookingItem{{TicketTierId: tier.String(), Quantity: 1}},
		},
		Generation:          1,
		Phase:               "OPEN",
		Currency:            "usd",
		ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour)),
	}
	s := &PaymentService{
		repo:       repo,
		logger:     zap.NewNop(),
		booking:    &readinessBooking{view: view},
		accounting: denyAccounting{err: ErrAccountingNotReady},
		account:    "acct_fixture",
		provider:   noProvider{},
	}
	if _, err := NewFinancialWorker(s).executeCreate(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetFinancial(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.CreateState != "create_attention" || got.ErrorCategory != "accounting_not_ready" || got.Payment.PaymentIntentId != "" {
		t.Fatalf("worker did not fail closed before provider: %+v", got)
	}
}
