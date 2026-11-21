//go:build recoverycomponents

package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

func TestRefundPreSendGuardChecksCurrentAuthorityIntegration(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)

	t.Run("allow_exact_current_command", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		result, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || result.Decision != RefundPreSendAllow {
			t.Fatalf("current refund command = %+v err=%v; want allow", result, err)
		}
		recordRefundPreSendClaimUnchanged(t, ctx, pool, claim, op.PaymentID)
	})

	t.Run("changed_owner_suppresses_and_preserves_successor", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		old := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		current := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		result, err := repo.CheckRefundPreSend(ctx, old)
		if err != nil || result.Decision != RefundPreSendStaleOwner {
			t.Fatalf("old owner check = %+v err=%v; want stale_owner", result, err)
		}
		owned, err := repo.FinishFinancialWork(ctx, old, domain.FinancialResult{State: "retryable", ErrorCategory: "pre_send_owner_lost", NextAttemptAt: time.Now().UTC().Add(time.Second)})
		if err != nil || owned {
			t.Fatalf("old owner finish = owned:%v err=%v; want fenced no-op", owned, err)
		}
		recordRefundPreSendClaimUnchanged(t, ctx, pool, current, op.PaymentID)
	})

	t.Run("database_expired_lease_suppresses", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		result, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || result.Decision != RefundPreSendStaleOwner {
			t.Fatalf("expired lease check = %+v err=%v; want stale_owner", result, err)
		}
		owned, err := repo.FinishFinancialWork(ctx, claim, domain.FinancialResult{State: "retryable", ErrorCategory: "pre_send_expired", NextAttemptAt: time.Now().UTC().Add(time.Second)})
		if err != nil || owned {
			t.Fatalf("expired owner finish = owned:%v err=%v; want fenced no-op", owned, err)
		}
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='attention' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("lease_expiring_while_waiting_uses_post_lock_database_time", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()+INTERVAL '600 milliseconds' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		lock, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback(ctx)
		var lockedID uuid.UUID
		if err := lock.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE`, op.PaymentID).Scan(&lockedID); err != nil {
			t.Fatal(err)
		}

		type checkResult struct {
			result RefundPreSendResult
			err    error
		}
		checked := make(chan checkResult, 1)
		started := make(chan struct{})
		go func() {
			close(started)
			result, err := repo.CheckRefundPreSend(ctx, claim)
			checked <- checkResult{result: result, err: err}
		}()
		<-started

		deadline := time.Now().Add(4 * time.Second)
		waiting := false
		for time.Now().Before(deadline) {
			if err := pool.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE wait_event_type='Lock' AND query LIKE 'SELECT id FROM payments WHERE id=$1 AND managed FOR UPDATE%'
			)`).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !waiting {
			t.Fatal("pre-send check did not block on the held Payment row lock")
		}

		for time.Now().Before(deadline) {
			var expired bool
			if err := pool.QueryRow(ctx, `SELECT lease_until<=clock_timestamp() FROM payments WHERE id=$1`, op.PaymentID).Scan(&expired); err != nil {
				t.Fatal(err)
			}
			if expired {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT lease_until<=clock_timestamp() FROM payments WHERE id=$1`, op.PaymentID).Scan(&expired); err != nil || !expired {
			t.Fatalf("database lease did not expire while waiting: expired=%v err=%v", expired, err)
		}
		if err := lock.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case result := <-checked:
			if result.err != nil || result.result.Decision != RefundPreSendStaleOwner {
				t.Fatalf("post-lock lease decision = %+v err=%v; want stale_owner", result.result, result.err)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("pre-send check did not return after row lock release")
		}
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='attention' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("committed_success_is_preserved", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='succeeded',refund_provider_id='re_late_success',refund_provider_status='succeeded',refund_completed_at=clock_timestamp(),reconciliation_state='resolved' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		var state string
		if err := pool.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, op.PaymentID).Scan(&state); err != nil || state != "succeeded" {
			t.Fatalf("success fixture state=%q err=%v", state, err)
		}
		result, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || result.Decision != RefundPreSendSucceeded {
			t.Fatalf("committed success check = %+v err=%v; want succeeded", result, err)
		}
		recordRefundPreSendClaimUnchanged(t, ctx, pool, claim, op.PaymentID)
	})

	t.Run("current_refund_id_requires_retrieval", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_provider_id='re_current_result',refund_provider_status='pending' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		result, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || result.Decision != RefundPreSendExistingResult || result.RefundID != "re_current_result" {
			t.Fatalf("current provider result check = %+v err=%v; want retrieval of current id", result, err)
		}
	})

	t.Run("changed_command_identity_is_rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			mutate func(*domain.FinancialClaim)
		}{
			{name: "booking", mutate: func(c *domain.FinancialClaim) { c.Operation.Identity.BookingID = uuid.New() }},
			{name: "user", mutate: func(c *domain.FinancialClaim) { c.Operation.Identity.UserID = uuid.New() }},
			{name: "generation", mutate: func(c *domain.FinancialClaim) { c.Operation.Identity.Generation = 2 }},
			{name: "account", mutate: func(c *domain.FinancialClaim) { c.Operation.Identity.ProviderAccount = "acct_changed" }},
			{name: "environment", mutate: func(c *domain.FinancialClaim) { c.Operation.Identity.LiveMode = true }},
			{name: "payment_intent", mutate: func(c *domain.FinancialClaim) { c.View.Payment.PaymentIntentId = "pi_changed" }},
			{name: "refund_decision", mutate: func(c *domain.FinancialClaim) { c.Refund.DecisionID = uuid.New() }},
			{name: "refund_amount", mutate: func(c *domain.FinancialClaim) { c.Refund.AmountCents-- }},
			{name: "refund_currency", mutate: func(c *domain.FinancialClaim) { c.Refund.Currency = "eur" }},
			{name: "refund_reason", mutate: func(c *domain.FinancialClaim) { c.Refund.Reason = "CANCELLED" }},
			{name: "claim_key", mutate: func(c *domain.FinancialClaim) { c.Key += ":changed" }},
			{name: "operation_key", mutate: func(c *domain.FinancialClaim) { c.Operation.Key += ":changed" }},
			{name: "refund_key", mutate: func(c *domain.FinancialClaim) { c.Refund.Key += ":changed" }},
			{name: "frozen_parameters", mutate: func(c *domain.FinancialClaim) {
				c.Operation.FrozenParams = []byte(`{"version":1,"amount":700,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				op := seedRefundPreSendPayment(t, ctx, repo)
				claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
				tc.mutate(&claim)
				result, err := repo.CheckRefundPreSend(ctx, claim)
				if err != nil || result.Decision != RefundPreSendCommandMismatch || result.Reason == "" {
					t.Fatalf("changed %s command = %+v err=%v; want a stable mismatch reason", tc.name, result, err)
				}
				recordRefundPreSendClaimUnchanged(t, ctx, pool, claim, op.PaymentID)
			})
		}
	})

	t.Run("current_state_no_longer_allows_create", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='attention' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		result, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || result.Decision != RefundPreSendNotAllowed {
			t.Fatalf("non-claimable current state = %+v err=%v; want not_allowed", result, err)
		}
	})

	t.Run("database_read_error_is_unknown", func(t *testing.T) {
		op := seedRefundPreSendPayment(t, ctx, repo)
		claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		result, err := repo.CheckRefundPreSend(canceled, claim)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled database read = %+v err=%v; want unknown context error", result, err)
		}
	})
}

func seedRefundPreSendPayment(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := financialFixtureOperation()
	op.PaymentID, op.Identity.BookingID, op.Identity.UserID = uuid.New(), uuid.New(), uuid.New()
	if supplied := strings.TrimSpace(os.Getenv("REFUND_COMPONENT_BOOKING_ID")); supplied != "" {
		bookingID, err := uuid.Parse(supplied)
		if err != nil || bookingID == uuid.Nil {
			t.Fatal("REFUND_COMPONENT_BOOKING_ID must be a non-nil UUID")
		}
		op.Identity.BookingID = bookingID
	}
	op.Key = "ticketbox:payment:" + op.PaymentID.String() + ":g1:create"
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, op.FrozenParams)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_presend_fixture_" + uuid.NewString()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded", IntentID: intentID,
		PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	if _, err := repo.RequireRefund(ctx, domain.RefundObligation{
		PaymentID: op.PaymentID, DecisionID: decisionID, Generation: op.Identity.Generation,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Reason: "EXPIRED",
		Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund",
	}); err != nil {
		t.Fatal(err)
	}
	return op
}

func claimRefundPreSendPayment(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository, paymentID uuid.UUID) domain.FinancialClaim {
	t.Helper()
	claims, err := repo.ClaimFinancialWork(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Kind != "refund" || claims[0].PaymentID != paymentID || claims[0].Refund == nil {
		t.Fatalf("claimed %+v err=%v; want refund for payment %s", claims, err, paymentID)
	}
	return claims[0]
}

func recordRefundPreSendClaimUnchanged(t *testing.T, ctx context.Context, pool *pgxpool.Pool, claim domain.FinancialClaim, paymentID uuid.UUID) {
	t.Helper()
	var token *uuid.UUID
	var fence int64
	err := pool.QueryRow(ctx, `SELECT claim_token,claim_fence FROM payments WHERE id=$1`, paymentID).Scan(&token, &fence)
	if err != nil || token == nil || *token != claim.Token || fence != claim.Fence {
		t.Fatalf("pre-send check changed the current claim: token=%v fence=%d err=%v", token, fence, err)
	}
}
