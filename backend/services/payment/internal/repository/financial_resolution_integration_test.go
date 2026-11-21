package repository

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

func TestRefundObligationSurvivesRestartAndDuplicateRequestsIntegration(t *testing.T) {
	pool, dsn := paymentFinancialFixture(t)
	repo := NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, IntentID: "pi_refund_obligation_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}
	if err := repo.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	obligation := domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}
	address, committed := paymentCommitACKProxy(t, dsn)
	parsed, _ := url.Parse(dsn)
	parsed.Host = address
	proxy, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if _, err := NewPostgresPaymentRepository(proxy).RequireRefund(ctx, obligation); err == nil {
		t.Fatal("obligation COMMIT ACK not lost")
	}
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("committed obligation boundary not observed")
	}
	restarted := NewPostgresPaymentRepository(pool)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, err := restarted.RequireRefund(ctx, obligation)
			if err != nil || view.RefundState != "pending" || view.RefundAmountCents != 725 {
				t.Errorf("obligation replay: %+v %v", view, err)
			}
		}()
	}
	wg.Wait()
	var key, reason, currency string
	var amount int64
	var saved uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT refund_key,refund_reason,refund_currency,refund_amount_cents,refund_decision_id FROM payments WHERE id=$1`, op.PaymentID).Scan(&key, &reason, &currency, &amount, &saved); err != nil {
		t.Fatal(err)
	}
	if key != obligation.Key || amount != 725 || reason != "EXPIRED" || currency != "usd" || saved != decision {
		t.Fatal("full target changed after ACK loss/restart")
	}
	for _, field := range []string{"amount", "currency", "decision", "reason", "key"} {
		t.Run(field, func(t *testing.T) {
			changed := obligation
			switch field {
			case "amount":
				changed.AmountCents = 724
			case "currency":
				changed.Currency = "eur"
			case "decision":
				changed.DecisionID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:cancel"))
				changed.Reason = "CANCELLED"
			case "reason":
				changed.Reason = "client says refund"
			case "key":
				changed.Key = "different"
			}
			if _, err := restarted.RequireRefund(ctx, changed); err == nil {
				t.Fatal("changed obligation accepted")
			}
		})
	}
	o.Collected = false
	o.Status = "requires_payment_method"
	o.ObservedAt = time.Now().Add(-time.Hour)
	if err := repo.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	view, err := repo.GetFinancial(ctx, op.PaymentID)
	if err != nil || view.ProviderOutcome != "collected" || view.RefundState != "pending" || view.RefundAmountCents != 725 {
		t.Fatal("stale failure erased obligation")
	}
	if _, err := repo.RequireCancel(ctx, op.PaymentID, decision); !errors.Is(err, ErrFinancialConflict) {
		t.Fatal("collected Payment accepted cancel instead of refund")
	}
}

func TestReconciliationResultCannotEraseNewCollectionIntegration(t *testing.T) {
	pool, _ := paymentFinancialFixture(t)
	repo := NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, IntentID: "pi_reconcile_race", AmountCents: 725, Currency: "usd", Status: "canceled", ObservedAt: time.Now()}
	if err := repo.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	claims, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal("reconciliation claim missing")
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:cancel"))
	if _, err := repo.RequireCancel(ctx, op.PaymentID, decision); err != nil {
		t.Fatal(err)
	}
	o.Collected = true
	o.Status = "succeeded"
	o.ObservedAt = time.Now()
	if err := repo.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishFinancialWork(ctx, claims[0], domain.FinancialResult{State: "resolved", BookingDisposition: "CANCELLED", DecisionID: decision}); err != nil {
		t.Fatal(err)
	}
	view, err := repo.GetFinancial(ctx, op.PaymentID)
	if err != nil || view.ProviderOutcome != "collected" || view.ReconciliationState != "pending" {
		t.Fatalf("old settlement erased new collection duty: %+v %v", view, err)
	}
}
