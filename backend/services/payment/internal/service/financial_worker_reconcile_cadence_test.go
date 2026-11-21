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
	"google.golang.org/grpc"
)

type reconcileCadenceRepository struct {
	repository.FinancialRepository
	finished *domain.FinancialResult
}

func (r *reconcileCadenceRepository) FinishFinancialWork(_ context.Context, _ domain.FinancialClaim, result domain.FinancialResult) (bool, error) {
	r.finished = &result
	return true, nil
}

type reconcileCadenceBookingClient struct {
	bookingv1.BookingServiceClient
	phase    string
	userID   uuid.UUID
	amount   int64
	currency string
	err      error
}

func (c *reconcileCadenceBookingClient) GetLifecycleDisposition(_ context.Context, req *bookingv1.LifecycleRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id:               req.BookingId,
			UserId:           c.userID.String(),
			Status:           "PENDING",
			TotalAmountCents: c.amount,
		},
		Generation: req.Generation,
		Phase:      c.phase,
		Currency:   c.currency,
	}, nil
}

func reconcileClaim(bookingID, userID, paymentID uuid.UUID, outcome string) domain.FinancialClaim {
	return domain.FinancialClaim{
		PaymentID: paymentID,
		Kind:      "reconcile",
		Operation: domain.CreateOperation{PaymentID: paymentID, Identity: domain.FinancialIdentity{
			BookingID: bookingID, UserID: userID, Generation: 1, AmountCents: 100, Currency: "usd",
		}},
		View: &domain.FinancialView{ProviderOutcome: outcome, RefundState: "not_required"},
	}
}

func assertReconcileCadence(t *testing.T, repo *reconcileCadenceRepository, started time.Time, prompt bool) {
	t.Helper()
	if repo.finished == nil {
		t.Fatal("reconcile result was not persisted")
	}
	delay := repo.finished.NextAttemptAt.Sub(started)
	if prompt {
		if delay < 0 || delay > 2*time.Second {
			t.Fatalf("collected payment retry delay = %v; want a prompt follow-up within the issuance retry window", delay)
		}
		return
	}
	if delay < 3*time.Second {
		t.Fatalf("uncollected checkout retry delay = %v; want the default backoff instead of per-tick load", delay)
	}
}

// A collected payment is reconciled once before the Saga confirms its booking.
// That observation must not park the next attempt behind a multi-second
// backoff; the ticket issuance consumer only retries for about 1.5s. An
// uncollected open checkout must keep the default backoff, or every abandoned
// checkout would occupy the payment worker's claim budget on every tick.
func TestReconcileCadenceDependsOnCollectedProviderOutcome(t *testing.T) {
	cases := []struct {
		name    string
		phase   string
		outcome string
		prompt  bool
	}{
		{"open_collected", "OPEN", "collected", true},
		{"confirming_collected", "CONFIRMING", "collected", true},
		{"open_uncollected", "OPEN", "not_collected", false},
		{"confirming_uncollected", "CONFIRMING", "not_collected", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &reconcileCadenceRepository{}
			bookingID, userID, paymentID := uuid.New(), uuid.New(), uuid.New()
			client := &reconcileCadenceBookingClient{phase: tc.phase, userID: userID, amount: 100, currency: "usd"}
			worker := &FinancialWorker{repo: repo, booking: client}
			started := time.Now()
			if err := worker.executeReconcile(context.Background(), reconcileClaim(bookingID, userID, paymentID, tc.outcome)); err != nil {
				t.Fatalf("execute reconcile: %v", err)
			}
			if repo.finished.State != "pending" || repo.finished.ErrorCategory != "" {
				t.Fatalf("settling reconcile result = %q/%q; want pending with no error", repo.finished.State, repo.finished.ErrorCategory)
			}
			assertReconcileCadence(t, repo, started, tc.prompt)
		})
	}
}

// A transient Booking read failure during the settle window must also re-check
// promptly for a collected payment; otherwise one slow RPC reintroduces the
// five-second park. Uncollected checkouts keep the default backoff.
func TestReconcileReadFailureCadenceDependsOnCollectedOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome string
		prompt  bool
	}{
		{"collected", "collected", true},
		{"uncollected", "not_collected", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &reconcileCadenceRepository{}
			bookingID, userID, paymentID := uuid.New(), uuid.New(), uuid.New()
			client := &reconcileCadenceBookingClient{err: errors.New("booking authority unavailable"), userID: userID, amount: 100, currency: "usd"}
			worker := &FinancialWorker{repo: repo, booking: client}
			started := time.Now()
			if err := worker.executeReconcile(context.Background(), reconcileClaim(bookingID, userID, paymentID, tc.outcome)); err != nil {
				t.Fatalf("execute reconcile: %v", err)
			}
			if repo.finished.State != "pending" || repo.finished.ErrorCategory != "booking_disposition_unavailable" {
				t.Fatalf("failed booking read result = %+v; want pending/booking_disposition_unavailable", repo.finished)
			}
			assertReconcileCadence(t, repo, started, tc.prompt)
		})
	}
}
