//go:build recoverycomponents

package repository

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

func TestRecoveryComponentRefundLateFullFieldMatrix(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)

	t.Run("stale_full_success_updates_facts_but_preserves_B_claim_and_schedule", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, current := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
		if current.View == nil || current.View.RefundState != "claimed" {
			t.Fatal("B's claim must retain its pre-observation snapshot")
		}
		before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)

		owned, err := repo.FinishFinancialWork(ctx, stale, refundLateSucceededResult(op, "re_late_full", "succeeded", op.Identity.AmountCents))
		if err != nil || owned {
			t.Fatalf("late A full result = owned:%v err:%v; want committed observation and owned=false", owned, err)
		}
		after := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		assertRefundLateControlFieldsUnchanged(t, after, before)
		assertRefundLateFullFields(t, after, "re_late_full")
	})

	for _, tc := range []struct {
		name, state, errorCategory string
	}{
		{"B_provider_pending_finish", "provider_pending", "B_provider_pending_after_A_full"},
		{"B_attention_finish", "attention", "B_attention_after_A_full"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := seedRefundLateMatrixPayment(t, ctx, repo)
			stale, current := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
			beforeA := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
			owned, err := repo.FinishFinancialWork(ctx, stale, refundLateSucceededResult(op, "re_late_current_B", "succeeded", op.Identity.AmountCents))
			if err != nil || owned {
				t.Fatalf("A late full result = owned:%v err:%v; want stale fact retained", owned, err)
			}
			afterA := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
			assertRefundLateControlFieldsUnchanged(t, afterA, beforeA)
			assertRefundLateFullFields(t, afterA, "re_late_current_B")

			nextAttempt := time.Now().UTC().Add(3 * time.Minute).Truncate(time.Microsecond)
			owned, err = repo.FinishFinancialWork(ctx, current, domain.FinancialResult{
				State: tc.state, ErrorCategory: tc.errorCategory, NextAttemptAt: nextAttempt,
			})
			if err != nil || !owned {
				t.Fatalf("B %s finish = owned:%v err:%v; want current owner accepted", tc.state, owned, err)
			}
			afterB := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
			assertRefundLateFullFields(t, afterB, "re_late_current_B")
			if afterB.claimToken != nil || afterB.leaseUntil != nil || afterB.operationKind != nil || afterB.claimFence != current.Fence {
				t.Fatalf("B finish did not release only its own claim: %+v", afterB)
			}
			if afterB.lastError == nil || *afterB.lastError != tc.errorCategory || !afterB.dueAt.Equal(nextAttempt) || !afterB.updatedAt.After(afterA.updatedAt) || afterB.attempts != afterA.attempts {
				t.Fatalf("B finish due/error/update fields = due:%s error:%v updated:%s attempts:%d; want due:%s error:%s, updated after A, attempts unchanged",
					afterB.dueAt, afterB.lastError, afterB.updatedAt, afterB.attempts, nextAttempt, tc.errorCategory)
			}
		})
	}

	t.Run("repeat_same_full_observation_is_idempotent", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, current := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
		result := refundLateSucceededResult(op, "re_repeat_full", "succeeded", op.Identity.AmountCents)
		before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		var afterFirst refundLateFieldSnapshot

		for i := 0; i < 2; i++ {
			owned, err := repo.FinishFinancialWork(ctx, stale, result)
			if err != nil || owned {
				t.Fatalf("repeat %d = owned:%v err:%v; want stale observation retained", i+1, owned, err)
			}
			if i == 0 {
				afterFirst = readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
			}
		}
		got := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		assertRefundLateControlFieldsUnchanged(t, got, before)
		assertRefundLateFullFields(t, got, "re_repeat_full")
		if afterFirst.refundCompletedAt == nil || !afterFirst.refundCompletedAt.Equal(*got.refundCompletedAt) {
			t.Fatal("repeated identical full observation changed refund_completed_at")
		}
		if current.Token == stale.Token || current.Fence <= stale.Fence {
			t.Fatal("expected B's later claim to fence A")
		}
	})

	t.Run("empty_refund_id_with_full_amount_is_accepted", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, _ := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
		result := refundLateSucceededResult(op, "", "succeeded", op.Identity.AmountCents)

		owned, err := repo.FinishFinancialWork(ctx, stale, result)
		if err != nil || owned {
			t.Fatalf("empty-ID full observation = owned:%v err:%v; want accepted as a stale fact", owned, err)
		}
		got := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		if got.refundProviderID != nil {
			t.Fatalf("empty RefundID populated refund_provider_id: %q", *got.refundProviderID)
		}
		if got.refundProviderStatus == nil || *got.refundProviderStatus != "succeeded" || got.refundState != "succeeded" || got.refundCompletedAt == nil || got.reconciliationState != "resolved" {
			t.Fatalf("full amount without RefundID did not set current full fields: %+v", got)
		}
	})

	t.Run("contradictory_status_with_full_amount_is_accepted_and_normalized", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, _ := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
		result := refundLateSucceededResult(op, "re_contradictory", "failed", op.Identity.AmountCents)

		owned, err := repo.FinishFinancialWork(ctx, stale, result)
		if err != nil || owned {
			t.Fatalf("contradictory status full observation = owned:%v err:%v; want accepted by current source", owned, err)
		}
		got := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		assertRefundLateFullFields(t, got, "re_contradictory")
	})

	t.Run("conflicting_full_ID_is_accepted_but_first_ID_is_preserved", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, current := takeoverRefundComponentClaimWithExistingID(t, ctx, pool, repo, op)
		before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		if before.refundProviderID == nil || *before.refundProviderID != "re_first_observation" {
			t.Fatal("fixture lacks its first persisted refund ID")
		}

		owned, err := repo.FinishFinancialWork(ctx, stale, refundLateSucceededResult(op, "re_different_full", "succeeded", op.Identity.AmountCents))
		if err != nil || owned {
			t.Fatalf("different full RefundID = owned:%v err:%v; want full cumulative observation accepted", owned, err)
		}
		got := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		assertRefundLateControlFieldsUnchanged(t, got, before)
		if got.refundProviderID == nil || *got.refundProviderID != "re_first_observation" {
			t.Fatalf("COALESCE did not preserve first RefundID: %+v", got.refundProviderID)
		}
		assertRefundLateFullFields(t, got, "re_first_observation")
		if current.Token == stale.Token || current.Fence <= stale.Fence {
			t.Fatal("expected B's later claim to fence A")
		}
	})

	t.Run("conflicting_nonfull_ID_is_rejected_without_writes", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, _ := takeoverRefundComponentClaimWithExistingID(t, ctx, pool, repo, op)
		before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		result := refundLateSucceededResult(op, "re_different_partial", "pending", op.Identity.AmountCents/2)

		owned, err := repo.FinishFinancialWork(ctx, stale, result)
		if owned || !errors.Is(err, ErrProviderEvidenceConflict) {
			t.Fatalf("different nonfull RefundID = owned:%v err:%v; want evidence conflict", owned, err)
		}
		if after := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID); !reflect.DeepEqual(after, before) {
			t.Fatalf("rejected conflicting nonfull observation wrote fields:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("wrong_claim_payment_is_not_found", func(t *testing.T) {
		op := seedRefundLateMatrixPayment(t, ctx, repo)
		stale, _ := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
		before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
		wrongPayment := stale
		wrongPayment.PaymentID = uuid.New()

		owned, err := repo.FinishFinancialWork(ctx, wrongPayment, refundLateSucceededResult(op, "re_wrong_claim", "succeeded", op.Identity.AmountCents))
		if owned || !errors.Is(err, RecordNotFound) {
			t.Fatalf("wrong claim Payment ID = owned:%v err:%v; want not found", owned, err)
		}
		if after := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID); !reflect.DeepEqual(after, before) {
			t.Fatalf("wrong claim Payment ID wrote target row:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*domain.ProviderObservation)
	}{
		{"observation_payment", func(o *domain.ProviderObservation) { o.PaymentID = uuid.New() }},
		{"booking", func(o *domain.ProviderObservation) { o.BookingID = uuid.New() }},
		{"generation", func(o *domain.ProviderObservation) { o.Generation++ }},
		{"provider_account", func(o *domain.ProviderObservation) { o.Account = "acct_wrong" }},
		{"live_mode", func(o *domain.ProviderObservation) { o.LiveMode = !o.LiveMode }},
		{"payment_intent", func(o *domain.ProviderObservation) { o.IntentID = "pi_wrong" }},
		{"amount", func(o *domain.ProviderObservation) { o.AmountCents++ }},
		{"currency", func(o *domain.ProviderObservation) { o.Currency = "cad" }},
		{"observed_at", func(o *domain.ProviderObservation) { o.ObservedAt = time.Time{} }},
		{"not_collected", func(o *domain.ProviderObservation) { o.Collected = false }},
		{"negative_succeeded_sum", func(o *domain.ProviderObservation) { o.SucceededRefundCents = -1 }},
		{"over_target_succeeded_sum", func(o *domain.ProviderObservation) { o.SucceededRefundCents++ }},
	} {
		t.Run("identity_"+tc.name, func(t *testing.T) {
			op := seedRefundLateMatrixPayment(t, ctx, repo)
			stale, _ := takeoverRefundComponentClaim(t, ctx, pool, repo, op)
			before := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID)
			result := refundLateSucceededResult(op, "re_wrong_identity", "succeeded", op.Identity.AmountCents)
			tc.mutate(result.Observation)

			owned, err := repo.FinishFinancialWork(ctx, stale, result)
			if owned || !errors.Is(err, ErrProviderEvidenceConflict) {
				t.Fatalf("wrong %s = owned:%v err:%v; want evidence conflict", tc.name, owned, err)
			}
			if after := readRefundLateFieldSnapshot(t, ctx, pool, op.PaymentID); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected %s observation wrote fields:\nbefore=%+v\nafter=%+v", tc.name, before, after)
			}
		})
	}
}

func takeoverRefundComponentClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *PostgresPaymentRepository, op domain.CreateOperation) (domain.FinancialClaim, domain.FinancialClaim) {
	t.Helper()
	stale := claimRefundComponent(t, ctx, repo)
	if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	current := claimRefundComponent(t, ctx, repo)
	assertRefundComponentTakeover(t, stale, current)
	return stale, current
}

func seedRefundLateMatrixPayment(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_component_" + op.PaymentID.String()
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

func refundLateIntentID(op domain.CreateOperation) string {
	return "pi_component_" + op.PaymentID.String()
}

func takeoverRefundComponentClaimWithExistingID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *PostgresPaymentRepository, op domain.CreateOperation) (domain.FinancialClaim, domain.FinancialClaim) {
	t.Helper()
	first := claimRefundComponent(t, ctx, repo)
	partial := refundLateSucceededResult(op, "re_first_observation", "pending", 0)
	partial.State = "provider_pending"
	owned, err := repo.FinishFinancialWork(ctx, first, partial)
	if err != nil || !owned {
		t.Fatalf("seed initial provider refund identity = owned:%v err:%v", owned, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	stale := claimRefundComponent(t, ctx, repo)
	if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=clock_timestamp()-INTERVAL '1 second',due_at=clock_timestamp() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	current := claimRefundComponent(t, ctx, repo)
	assertRefundComponentTakeover(t, stale, current)
	return stale, current
}

func assertRefundComponentTakeover(t *testing.T, stale, current domain.FinancialClaim) {
	t.Helper()
	if stale.Token == current.Token || current.Fence <= stale.Fence {
		t.Fatal("takeover did not issue B a new token and higher fence")
	}
	if current.View == nil || current.View.RefundState != "claimed" {
		t.Fatal("B's claim snapshot does not show the pre-late-success claimed state")
	}
}

func refundLateSucceededResult(op domain.CreateOperation, refundID, status string, succeededCents int64) domain.FinancialResult {
	observation := &domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, LiveMode: op.Identity.LiveMode, Status: status,
		IntentID: refundLateIntentID(op), RefundID: refundID, PaymentID: op.PaymentID, BookingID: op.Identity.BookingID,
		Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents, SucceededRefundCents: succeededCents,
		Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
	}
	return domain.FinancialResult{State: "succeeded", Observation: observation, NextAttemptAt: time.Now().UTC().Add(time.Second)}
}

type refundLateFieldSnapshot struct {
	paymentID, bookingID   uuid.UUID
	generation             int64
	providerAccount        string
	liveMode               bool
	intentID               *string
	refundProviderID       *string
	refundProviderStatus   *string
	refundState            string
	refundCompletedAt      *time.Time
	reconciliationState    string
	refundDecisionID       *uuid.UUID
	refundAmountCents      *int64
	refundCurrency, reason *string
	refundKey              string
	claimToken             *uuid.UUID
	claimFence             int64
	leaseUntil             *time.Time
	operationKind          *string
	dueAt, updatedAt       time.Time
	lastError              *string
	attempts               int
}

func readRefundLateFieldSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) refundLateFieldSnapshot {
	t.Helper()
	var row refundLateFieldSnapshot
	err := pool.QueryRow(ctx, `SELECT id,booking_id,payment_generation,provider_account,livemode,payment_intent_id,
		refund_provider_id,refund_provider_status,refund_state,refund_completed_at,reconciliation_state,
		refund_decision_id,refund_amount_cents,refund_currency,refund_reason,refund_key,
		claim_token,claim_fence,lease_until,operation_kind,due_at,updated_at,last_error,attempts
		FROM payments WHERE id=$1`, paymentID).Scan(
		&row.paymentID, &row.bookingID, &row.generation, &row.providerAccount, &row.liveMode, &row.intentID,
		&row.refundProviderID, &row.refundProviderStatus, &row.refundState, &row.refundCompletedAt, &row.reconciliationState,
		&row.refundDecisionID, &row.refundAmountCents, &row.refundCurrency, &row.reason, &row.refundKey,
		&row.claimToken, &row.claimFence, &row.leaseUntil, &row.operationKind, &row.dueAt, &row.updatedAt, &row.lastError, &row.attempts)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func assertRefundLateControlFieldsUnchanged(t *testing.T, got, want refundLateFieldSnapshot) {
	t.Helper()
	got.refundProviderID, got.refundProviderStatus, got.refundState, got.refundCompletedAt, got.reconciliationState = nil, nil, "", nil, ""
	want.refundProviderID, want.refundProviderStatus, want.refundState, want.refundCompletedAt, want.reconciliationState = nil, nil, "", nil, ""
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("late observation changed claim, command, schedule, or unrelated identity fields:\nbefore=%+v\nafter=%+v", want, got)
	}
}

func assertRefundLateFullFields(t *testing.T, got refundLateFieldSnapshot, wantRefundID string) {
	t.Helper()
	if got.refundProviderID == nil || *got.refundProviderID != wantRefundID || got.refundProviderStatus == nil || *got.refundProviderStatus != "succeeded" ||
		got.refundState != "succeeded" || got.refundCompletedAt == nil || got.reconciliationState != "resolved" {
		t.Fatalf("late full observation fields = id:%v provider_status:%v refund_state:%q completed_at:%v reconciliation:%q",
			got.refundProviderID, got.refundProviderStatus, got.refundState, got.refundCompletedAt, got.reconciliationState)
	}
}
