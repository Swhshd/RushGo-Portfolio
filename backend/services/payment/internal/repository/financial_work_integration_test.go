package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestConcurrentCreateClaimsOnePaymentIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	claims := make(chan *domain.FinancialClaim, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, err := r.ClaimCreatePayment(ctx, op.PaymentID, 30*time.Second)
			if err != nil {
				t.Error(err)
			}
			claims <- c
		}()
	}
	close(start)
	wg.Wait()
	close(claims)
	var winner *domain.FinancialClaim
	count := 0
	for c := range claims {
		if c != nil {
			winner = c
			count++
		}
	}
	if count != 1 || winner.Key != op.Key || winner.FirstAttemptAt == nil {
		t.Fatalf("claims=%d winner=%+v", count, winner)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	if owned, err := r.FinishFinancialWork(ctx, *winner, domain.FinancialResult{State: "create_attention", ErrorCategory: "expired_worker"}); err != nil || owned {
		t.Fatalf("expired execution claim still owned completion: %v %v", owned, err)
	}
	// A worker death leaves claim evidence persisted; another process claims
	// after lease expiry with the same operation but a greater execution fence.
	if _, err := p.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	newer, err := r.ClaimCreatePayment(ctx, op.PaymentID, 30*time.Second)
	if err != nil || newer == nil || newer.Token == winner.Token || newer.Fence <= winner.Fence || !newer.FirstAttemptAt.Equal(*winner.FirstAttemptAt) || newer.Key != winner.Key {
		t.Fatalf("reclaim=%+v %v", newer, err)
	}
	if owned, err := r.FinishFinancialWork(ctx, *winner, domain.FinancialResult{State: "create_attention", ErrorCategory: "old_worker"}); err != nil || owned {
		t.Fatalf("stale result won: %v %v", owned, err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, AmountCents: 725, Currency: "usd", IntentID: "pi_claim_fixture", Status: "requires_payment_method", ObservedAt: time.Now()}
	if owned, err := r.FinishFinancialWork(ctx, *winner, domain.FinancialResult{State: "create_ready", Observation: &o}); err != nil || owned {
		t.Fatalf("late evidence: %v %v", owned, err)
	}
	var token uuid.UUID
	if err := p.QueryRow(ctx, `SELECT claim_token FROM payments WHERE id=$1`, op.PaymentID).Scan(&token); err != nil || token != newer.Token {
		t.Fatal("late evidence stole live claim")
	}
	if owned, err := r.FinishFinancialWork(ctx, *newer, domain.FinancialResult{State: "create_unknown"}); err != nil || !owned {
		t.Fatalf("current completion: %v %v", owned, err)
	}
	view, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || view.CreateState != "create_ready" || view.Payment.PaymentIntentId != o.IntentID {
		t.Fatalf("late evidence lost: %+v %v", view, err)
	}
}

func TestFinancialClaimsFenceAndRetainLateEvidenceIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, AmountCents: 725, Currency: "usd", IntentID: "pi_fence_refund", Status: "succeeded", Collected: true, ObservedAt: time.Now()}
	if err := r.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	if _, err := r.RequireRefund(ctx, domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}); err != nil {
		t.Fatal(err)
	}
	old, err := r.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(old) != 1 {
		t.Fatal("refund claim missing", err)
	}
	if old[0].Kind != "refund" || old[0].Refund == nil || old[0].Refund.AmountCents != 725 || old[0].Key != "ticketbox:payment:"+op.PaymentID.String()+":g1:refund" {
		t.Fatal("frozen refund claim lost")
	}
	if _, err = p.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	current, err := r.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(current) != 1 {
		t.Fatal("refund reclaim missing", err)
	}
	o.RefundID = "re_fence_refund"
	o.SucceededRefundCents = 725
	if owned, err := r.FinishFinancialWork(ctx, old[0], domain.FinancialResult{State: "succeeded", Observation: &o}); err != nil || owned {
		t.Fatalf("stale result ownership: %v %v", owned, err)
	}
	var token uuid.UUID
	if err := p.QueryRow(ctx, `SELECT claim_token FROM payments WHERE id=$1`, op.PaymentID).Scan(&token); err != nil || token != current[0].Token {
		t.Fatal("trusted observation stole current lease", err)
	}
	o.SucceededRefundCents = 0
	o.Status = "pending"
	o.ObservedAt = time.Now().Add(-time.Hour)
	if _, err := r.FinishFinancialWork(ctx, current[0], domain.FinancialResult{State: "provider_pending", Observation: &o}); err != nil {
		t.Fatal(err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "succeeded" || v.ReconciliationState != "resolved" || v.RefundStatus != "succeeded" {
		t.Fatalf("stale pending regressed full refund: %+v %v", v, err)
	}
}

func TestRefundEvidenceCannotInventCollectionIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, AmountCents: 725, Currency: "usd", IntentID: "pi_invalid_refund", Status: "succeeded", Collected: true, ObservedAt: time.Now()}
	if err := r.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	if _, err := r.RequireRefund(ctx, domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}); err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal(err)
	}
	o.Collected = false
	o.SucceededRefundCents = 725
	o.RefundID = "re_invalid"
	if _, err := r.FinishFinancialWork(ctx, claims[0], domain.FinancialResult{State: "succeeded", Observation: &o}); !errors.Is(err, ErrProviderEvidenceConflict) {
		t.Fatalf("uncollected evidence fabricated full refund: %v", err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState == "succeeded" {
		t.Fatal("invalid refund persisted", err)
	}
}

func TestFinancialBacklogSummaryIncludesDueAgeAndAttentionIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET created_at=NOW()-INTERVAL '2 hours' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	summary, ok := any(r).(interface {
		FinancialBacklog(context.Context) (int64, int64, float64, error)
	})
	if !ok {
		t.Fatal("financial due/attention backlog is not observable")
	}
	due, attention, age, err := summary.FinancialBacklog(ctx)
	if err != nil || due != 1 || attention != 0 || age < 7190 {
		t.Fatalf("due financial work lost from summary: due=%d attention=%d age=%f %v", due, attention, age, err)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET create_state='create_attention',reconciliation_state='attention' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	due, attention, age, err = summary.FinancialBacklog(ctx)
	if err != nil || due != 0 || attention != 1 || age < 7190 {
		t.Fatalf("financial attention lost from summary: due=%d attention=%d age=%f %v", due, attention, age, err)
	}
}

func TestStalePendingRefundCannotOverwriteNewerAttentionScheduleIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, AmountCents: 725, Currency: "usd", IntentID: "pi_stale_refund", Status: "succeeded", Collected: true, ObservedAt: time.Now()}
	if err := r.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	if _, err := r.RequireRefund(ctx, domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}); err != nil {
		t.Fatal(err)
	}
	old, err := r.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(old) != 1 {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	current, err := r.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(current) != 1 {
		t.Fatal(err)
	}
	o.RefundID = "re_stale"
	o.Status = "failed"
	o.ObservedAt = time.Now()
	next := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	if _, err := r.FinishFinancialWork(ctx, current[0], domain.FinancialResult{State: "attention", Observation: &o, NextAttemptAt: next}); err != nil {
		t.Fatal(err)
	}
	o.Status = "pending"
	o.ObservedAt = o.ObservedAt.Add(-time.Minute)
	if owned, err := r.FinishFinancialWork(ctx, old[0], domain.FinancialResult{State: "provider_pending", Observation: &o}); err != nil || owned {
		t.Fatal("stale completion owned", err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundStatus != "failed" || v.ReconciliationState != "attention" || v.RefundState != "attention" || v.NextAttemptAt == nil || !v.NextAttemptAt.Equal(next) {
		t.Fatalf("stale evidence overwrote current status/schedule: %+v %v", v, err)
	}
}
