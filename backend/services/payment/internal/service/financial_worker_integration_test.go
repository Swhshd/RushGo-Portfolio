package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type resolutionBookingReader struct {
	bookingv1.BookingServiceClient
	view             *bookingv1.LifecycleDisposition
	err              error
	entered, release chan struct{}
}

func (b *resolutionBookingReader) GetLifecycleDisposition(ctx context.Context, _ *bookingv1.LifecycleRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if b.entered != nil {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.view, b.err
}

func TestUnconfirmableCollectionCreatesDurableRefundIntegration(t *testing.T) {
	for _, status := range []string{"EXPIRED", "CANCELLED", "FAILED", "CONFIRMED", "OPEN", "CONFIRMING", "read_timeout"} {
		t.Run(status, func(t *testing.T) {
			pool := retryPaymentFixture(t)
			repo := repository.NewPostgresPaymentRepository(pool)
			ctx := context.Background()
			id, booking, user := uuid.New(), uuid.New(), uuid.New()
			frozen := []byte(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
			op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, AmountCents: 725, Currency: "usd", ProviderAccount: "acct_fixture"}}
			op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
			if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
				t.Fatal(err)
			}
			if err := repo.RecordObservation(ctx, domain.ProviderObservation{PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_disposition_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			phase, publicStatus, kind := "TERMINAL", status, "expire"
			switch status {
			case "CANCELLED":
				kind = "cancel"
			case "FAILED":
				kind = "fail"
			case "CONFIRMED":
				kind = "confirm"
			case "OPEN", "CONFIRMING":
				phase, publicStatus = status, "PENDING"
			}
			decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:"+kind))
			reader := &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: publicStatus}, Generation: 1, Phase: phase, Currency: "usd", DecisionId: decision.String()}}
			if status == "read_timeout" {
				reader.err = errors.New("fixture Booking deadline exceeded")
			}
			s := NewPaymentService(repo, zap.NewNop(), nil)
			s.SetFinancialAuthority(reader, "acct_fixture", false)
			worker := NewFinancialWorker(s)
			if _, err := worker.RunOnce(ctx, 1); err != nil {
				t.Fatal(err)
			}
			view, err := repo.GetFinancial(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if view.ProviderOutcome != "collected" || view.Payment.Status != domain.PaymentSuccess {
				t.Fatal("disposition erased collection")
			}
			switch status {
			case "EXPIRED", "CANCELLED", "FAILED":
				if view.RefundState != "pending" || view.ReconciliationState != "refund_required" || view.RefundAmountCents != 725 || view.BookingDisposition != status {
					t.Errorf("unconfirmable collection has no full durable obligation: %+v", view)
				}
				var key, currency string
				var storedDecision *uuid.UUID
				if err := pool.QueryRow(ctx, `SELECT refund_key,COALESCE(refund_currency,''),refund_decision_id FROM payments WHERE id=$1`, id).Scan(&key, &currency, &storedDecision); err != nil {
					t.Fatal(err)
				}
				if key != "ticketbox:payment:"+id.String()+":g1:refund" || currency != "usd" || storedDecision == nil || *storedDecision != decision {
					t.Error("refund target/key/decision not retained")
				}
			case "CONFIRMED":
				if view.ReconciliationState != "sale_verified" || view.RefundState != "not_required" {
					t.Error("confirmed legal sale not verified")
				}
			default:
				if view.ReconciliationState != "pending" || view.RefundState != "not_required" {
					t.Error("uncertain Booking incorrectly inferred refund")
				}
			}
		})
	}
}

func TestBookingResolutionReadIsOutsidePaymentTransactionIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := []byte(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	observation := domain.ProviderObservation{PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_read_barrier", AmountCents: 725, Currency: "usd", Status: "requires_payment_method", ObservedAt: time.Now()}
	if err := repo.RecordObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:cancel"))
	reader := &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: "CANCELLED"}, Generation: 1, Phase: "TERMINAL", Currency: "usd", DecisionId: decision.String()}, entered: make(chan struct{}), release: make(chan struct{})}
	s := NewPaymentService(repo, zap.NewNop(), nil)
	s.SetFinancialAuthority(reader, "acct_fixture", false)
	done := make(chan error, 1)
	go func() { _, err := NewFinancialWorker(s).RunOnce(ctx, 1); done <- err }()
	select {
	case <-reader.entered:
	case <-time.After(5 * time.Second):
		close(reader.release)
		t.Fatal("Booking RPC barrier not observed")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		close(reader.release)
		t.Fatal(err)
	}
	_, lockErr := tx.Exec(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE NOWAIT`, id)
	_ = tx.Rollback(ctx)
	// Collection arrives while the old claim still holds an uncollected view.
	observation.Collected = true
	observation.Status = "succeeded"
	observation.ObservedAt = time.Now()
	if err := repo.RecordObservation(ctx, observation); err != nil {
		close(reader.release)
		t.Fatal(err)
	}
	close(reader.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if lockErr != nil {
		t.Fatalf("Payment row locked during Booking RPC: %v", lockErr)
	}
	view, err := repo.GetFinancial(ctx, id)
	if err != nil || view.ProviderOutcome != "collected" || view.ReconciliationState != "pending" {
		t.Fatalf("old uncollected view suppressed late collection: %+v %v", view, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	reader.entered = nil
	reader.release = nil
	if _, err := NewFinancialWorker(s).RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	view, err = repo.GetFinancial(ctx, id)
	if err != nil || view.RefundState != "pending" {
		t.Fatal("late collection not discoverable after old reconciliation")
	}
}

func TestResolutionRequestVerifiesWinningBookingDecisionIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := []byte(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_resolution_request", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:expire"))
	reader := &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: "EXPIRED"}, Generation: 1, Phase: "TERMINAL", Currency: "usd", DecisionId: decision.String()}}
	s := NewPaymentService(repo, zap.NewNop(), nil)
	s.SetFinancialAuthority(reader, "acct_fixture", false)
	for _, change := range []string{"decision", "user", "amount", "phase", "currency"} {
		t.Run(change, func(t *testing.T) {
			view := reader.view
			oldDecision, oldUser, oldAmount, oldPhase, oldCurrency := view.DecisionId, view.Booking.UserId, view.Booking.TotalAmountCents, view.Phase, view.Currency
			switch change {
			case "decision":
				view.DecisionId = uuid.NewString()
			case "user":
				view.Booking.UserId = uuid.NewString()
			case "amount":
				view.Booking.TotalAmountCents = 800
			case "phase":
				view.Phase = "OPEN"
			case "currency":
				view.Currency = "eur"
			}
			if _, err := s.RequestPaymentResolution(ctx, id, booking, 1, decision, "caller reason"); !errors.Is(err, repository.ErrFinancialConflict) {
				t.Errorf("changed authority accepted: %v", err)
			}
			view.DecisionId, view.Booking.UserId, view.Booking.TotalAmountCents, view.Phase, view.Currency = oldDecision, oldUser, oldAmount, oldPhase, oldCurrency
		})
	}
	for range 2 {
		view, err := s.RequestPaymentResolution(ctx, id, booking, 1, decision, "untrusted partial-refund reason")
		if err != nil || view.RefundState != "pending" || view.RefundAmountCents != 725 {
			t.Fatalf("full obligation acceptance: %+v %v", view, err)
		}
	}
	for _, selectors := range []struct {
		payment, booking uuid.UUID
		generation       int64
	}{{id, uuid.Nil, 1}, {uuid.Nil, booking, 1}} {
		view, err := s.GetFinancialPayment(ctx, selectors.payment, selectors.booking, selectors.generation)
		if err != nil || view.Payment.ID != id {
			t.Fatal("financial read selectors lost original identity")
		}
	}
	if _, err := s.GetFinancialPayment(ctx, id, booking, 1); !errors.Is(err, repository.ErrInvalidFinancialIdentity) {
		t.Fatal("ambiguous selectors accepted")
	}
}
