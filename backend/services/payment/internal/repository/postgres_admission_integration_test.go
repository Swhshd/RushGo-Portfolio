//go:build recoverycomponents

package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

func TestPaymentAdmissionFenceIntegration(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)
	inactivePayment := seedPaymentAdmissionEligible(t, ctx, pool, repo)
	if _, err := repo.GrantAdmission(ctx, paymentAdmissionIdentity(inactivePayment), paymentAdmissionEvidence()); !errors.Is(err, admission.ErrUnavailable) {
		t.Fatalf("inactive Payment admission mode grant error=%v; want fail-closed unavailable", err)
	}
	var inactivePermitCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE payment_id=$1`, inactivePayment.PaymentID).Scan(&inactivePermitCount); err != nil || inactivePermitCount != 0 {
		t.Fatalf("inactive mode persisted %d permits; err=%v", inactivePermitCount, err)
	}
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("permit_identity_and_terminal_fence", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		identity := paymentAdmissionIdentity(op)
		evidence := paymentAdmissionEvidence()

		first, err := repo.GrantAdmission(ctx, identity, evidence)
		if err != nil || first.State != admission.Pending || first.Token == uuid.Nil || first.Fence <= 0 {
			t.Fatalf("grant = %+v err=%v; want immutable pending permit", first, err)
		}
		retry, err := repo.GrantAdmission(ctx, identity, evidence)
		if err != nil || retry != first {
			t.Fatalf("same-identity grant retry = %+v err=%v; want original %+v", retry, err, first)
		}
		conflict := identity
		conflict.Fingerprint = sha256.Sum256([]byte("same id, changed payload"))
		if _, err := repo.GetAdmission(ctx, conflict); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("same RequestID with changed binding error=%v; want conflict", err)
		}

		committed, err := repo.SettleAdmission(ctx, identity, first.Token, first.Fence, admission.Committed)
		if err != nil || committed.State != admission.Committed {
			t.Fatalf("settle committed = %+v err=%v", committed, err)
		}
		if _, err := repo.SettleAdmission(ctx, identity, first.Token, first.Fence, admission.Aborted); !errors.Is(err, admission.ErrStale) {
			t.Fatalf("terminal permit reversal error=%v; want stale rejection", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE payment_admission_mode SET active=FALSE WHERE singleton=TRUE`); err == nil {
			t.Fatal("activated Payment admission latch was reset")
		}
	})

	t.Run("prepared_trigger_and_refund_first_history", func(t *testing.T) {
		preparedFirstPayment := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		preparedFirstIdentity := paymentAdmissionIdentity(preparedFirstPayment)
		permit, err := repo.GrantAdmission(ctx, preparedFirstIdentity, paymentAdmissionEvidence())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.SettleAdmission(ctx, preparedFirstIdentity, permit.Token, permit.Fence, admission.Aborted); err != nil {
			t.Fatal(err)
		}
		prepared := paymentAdmissionPreparedAudit(t, ctx, repo, preparedFirstPayment)
		if err := repo.AppendRefundAudit(ctx, prepared); err != nil {
			t.Fatalf("PREPARED after a settled permit should become the durable refund-first marker: %v", err)
		}
		newRequest := paymentAdmissionIdentity(preparedFirstPayment)
		newRequest.TicketID = uuid.New()
		newRequest.ActorID = uuid.New()
		newRequest.KeyDigest = sha256.Sum256([]byte("new scan key"))
		newRequest.RequestID = admission.RequestIDFor(newRequest.ActorID, newRequest.KeyDigest)
		newRequest.Fingerprint = sha256.Sum256([]byte("new scan payload"))
		if _, err := repo.GrantAdmission(ctx, newRequest, paymentAdmissionEvidence()); !errors.Is(err, admission.ErrBlocked) {
			t.Fatalf("new permit after retained PREPARED history error=%v; want blocked", err)
		}

		pendingPayment := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		pendingIdentity := paymentAdmissionIdentity(pendingPayment)
		if _, err := repo.GrantAdmission(ctx, pendingIdentity, paymentAdmissionEvidence()); err != nil {
			t.Fatal(err)
		}
		pendingPrepared := paymentAdmissionPreparedAudit(t, ctx, repo, pendingPayment)
		if err := repo.AppendRefundAudit(ctx, pendingPrepared); err == nil {
			t.Fatal("legacy direct PREPARED insert bypassed the pending admission database guard")
		}
	})

	t.Run("claim_skip_and_synthetic_neutral_defer_branch_coverage", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
		if _, err := repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decisionID,
			Generation: 1, AmountCents: op.Identity.AmountCents, Currency: "usd", Reason: "EXPIRED",
			Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}); err != nil {
			t.Fatal(err)
		}
		claims, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
		if err != nil || len(claims) != 1 || claims[0].PaymentID != op.PaymentID || claims[0].Kind != "refund" {
			t.Fatalf("pre-admission refund claim = %+v err=%v", claims, err)
		}
		claim := claims[0]
		if claim.PriorRefundState != "pending" || claim.PriorAttempts != 0 || claim.PriorRefundFirstAttemptAt != nil {
			t.Fatalf("preclaim baseline = state:%q attempts:%d first:%v", claim.PriorRefundState, claim.PriorAttempts, claim.PriorRefundFirstAttemptAt)
		}

		// Synthetic branch-coverage setup only: directly reset the state after
		// claim so the final guard can produce its positive wait proof. The real
		// grant path rejects a normal claimed refund; this test does not claim
		// that resetting the obligation is a supported business interleaving.
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='not_required',refund_amount_cents=NULL,
			reconciliation_state='sale_verified' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		identity := paymentAdmissionIdentity(op)
		permit, err := repo.GrantAdmission(ctx, identity, paymentAdmissionEvidence())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='claimed',refund_amount_cents=$2,
			reconciliation_state='refund_required' WHERE id=$1`, op.PaymentID, op.Identity.AmountCents); err != nil {
			t.Fatal(err)
		}
		guard, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || guard.Decision != RefundPreSendCoordinationWait || !guard.coordinationWaitProof {
			t.Fatalf("final pre-send coordination result = %+v err=%v; want positive committed wait proof", guard, err)
		}
		if count, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{PaymentID: op.PaymentID, Kind: domain.RefundAuditKindPrepared}); err != nil || len(count) != 0 {
			t.Fatalf("coordination wait wrote PREPARED: rows=%d err=%v", len(count), err)
		}
		deferred, err := repo.DeferRefundForAdmission(ctx, claim, guard, time.Now().UTC().Add(15*time.Second))
		if err != nil || !deferred {
			t.Fatalf("neutral defer = owned:%v err=%v", deferred, err)
		}
		var state string
		var attempts int
		var first *time.Time
		var token *uuid.UUID
		var lease *time.Time
		var due time.Time
		if err := pool.QueryRow(ctx, `SELECT refund_state,attempts,refund_first_attempt_at,claim_token,lease_until,due_at FROM payments WHERE id=$1`, op.PaymentID).
			Scan(&state, &attempts, &first, &token, &lease, &due); err != nil {
			t.Fatal(err)
		}
		if state != "pending" || attempts != claim.PriorAttempts || first != nil || token != nil || lease != nil || !due.After(time.Now()) {
			t.Fatalf("neutral defer did not restore baseline: state=%q attempts=%d first=%v token=%v lease=%v due=%v", state, attempts, first, token, lease, due)
		}

		if _, err := pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
		retry, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
		if err != nil || len(retry) != 0 {
			t.Fatalf("pending admission consumed another refund claim: claims=%+v err=%v permit=%+v", retry, err, permit)
		}
	})

	t.Run("neutral_defer_restores_existing_horizon_synthetic_branch_coverage", func(t *testing.T) {
		priorFirst := time.Now().UTC().Add(-time.Hour)
		_, claim, guard := prepareSyntheticAdmissionWaitForBranchCoverage(t, ctx, pool, repo, 9, &priorFirst)
		deferred, err := repo.DeferRefundForAdmission(ctx, claim, guard, time.Now().UTC().Add(15*time.Second))
		if err != nil || !deferred {
			t.Fatalf("neutral defer with existing horizon = owned:%v err=%v", deferred, err)
		}
		var state string
		var attempts int
		var first *time.Time
		if err := pool.QueryRow(ctx, `SELECT refund_state,attempts,refund_first_attempt_at FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&state, &attempts, &first); err != nil {
			t.Fatal(err)
		}
		if state != "pending" || attempts != claim.PriorAttempts || first == nil || claim.PriorRefundFirstAttemptAt == nil ||
			!first.Equal(*claim.PriorRefundFirstAttemptAt) {
			t.Fatalf("existing refund baseline changed: state=%q attempts=%d first=%v prior_attempts=%d prior_first=%v",
				state, attempts, first, claim.PriorAttempts, claim.PriorRefundFirstAttemptAt)
		}
	})

	t.Run("neutral_defer_preserves_successor_synthetic_branch_coverage", func(t *testing.T) {
		_, claim, guard := prepareSyntheticAdmissionWaitForBranchCoverage(t, ctx, pool, repo, 4, nil)
		successorToken := uuid.New()
		successorFirst := time.Now().UTC().Add(-2 * time.Hour)
		if _, err := pool.Exec(ctx, `UPDATE payments SET claim_token=$2,claim_fence=claim_fence+1,
			attempts=71,refund_first_attempt_at=$3,lease_until=clock_timestamp()+INTERVAL '1 minute',
			operation_kind='refund',refund_state='claimed' WHERE id=$1`, claim.PaymentID, successorToken, successorFirst); err != nil {
			t.Fatal(err)
		}
		var storedSuccessorFirst *time.Time
		var successorFence int64
		if err := pool.QueryRow(ctx, `SELECT refund_first_attempt_at,claim_fence FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&storedSuccessorFirst, &successorFence); err != nil {
			t.Fatal(err)
		}
		deferred, err := repo.DeferRefundForAdmission(ctx, claim, guard, time.Now().UTC().Add(15*time.Second))
		if err != nil || deferred {
			t.Fatalf("stale neutral defer = owned:%v err=%v; want untouched successor", deferred, err)
		}
		var token *uuid.UUID
		var fence int64
		var attempts int
		var state string
		var first *time.Time
		if err := pool.QueryRow(ctx, `SELECT claim_token,claim_fence,attempts,refund_state,refund_first_attempt_at FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&token, &fence, &attempts, &state, &first); err != nil {
			t.Fatal(err)
		}
		if token == nil || *token != successorToken || fence != successorFence || attempts != 71 || state != "claimed" ||
			first == nil || storedSuccessorFirst == nil || !first.Equal(*storedSuccessorFirst) {
			t.Fatalf("stale defer restored over successor: token=%v fence=%d attempts=%d state=%q first=%v", token, fence, attempts, state, first)
		}
	})

	t.Run("neutral_defer_preserves_expired_owner_synthetic_branch_coverage", func(t *testing.T) {
		_, claim, guard := prepareSyntheticAdmissionWaitForBranchCoverage(t, ctx, pool, repo, 5, nil)
		expiredFirst := time.Now().UTC().Add(-3 * time.Hour)
		if _, err := pool.Exec(ctx, `UPDATE payments SET attempts=83,refund_first_attempt_at=$2,
			lease_until=clock_timestamp()-INTERVAL '1 second',refund_state='claimed' WHERE id=$1`, claim.PaymentID, expiredFirst); err != nil {
			t.Fatal(err)
		}
		var expectedFirst *time.Time
		if err := pool.QueryRow(ctx, `SELECT refund_first_attempt_at FROM payments WHERE id=$1`, claim.PaymentID).Scan(&expectedFirst); err != nil {
			t.Fatal(err)
		}
		deferred, err := repo.DeferRefundForAdmission(ctx, claim, guard, time.Now().UTC().Add(15*time.Second))
		if err != nil || deferred {
			t.Fatalf("expired-owner neutral defer = owned:%v err=%v; want no restoration", deferred, err)
		}
		var token *uuid.UUID
		var attempts int
		var state string
		var first *time.Time
		var leaseValid bool
		if err := pool.QueryRow(ctx, `SELECT claim_token,attempts,refund_state,refund_first_attempt_at,
			COALESCE(lease_until>clock_timestamp(),false) FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&token, &attempts, &state, &first, &leaseValid); err != nil {
			t.Fatal(err)
		}
		if token == nil || *token != claim.Token || attempts != 83 || state != "claimed" || leaseValid ||
			first == nil || expectedFirst == nil || !first.Equal(*expectedFirst) {
			t.Fatalf("expired-owner defer restored bookkeeping: token=%v attempts=%d state=%q lease_valid=%v first=%v", token, attempts, state, leaseValid, first)
		}
	})

	t.Run("neutral_defer_preserves_late_success_synthetic_branch_coverage", func(t *testing.T) {
		_, claim, guard := prepareSyntheticAdmissionWaitForBranchCoverage(t, ctx, pool, repo, 6, nil)
		if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='succeeded',refund_provider_id='re_branch_success',
			refund_provider_status='succeeded',refund_completed_at=clock_timestamp(),reconciliation_state='resolved'
			WHERE id=$1`, claim.PaymentID); err != nil {
			t.Fatal(err)
		}
		var expectedCompleted *time.Time
		var expectedAttempts int
		var expectedFirst *time.Time
		if err := pool.QueryRow(ctx, `SELECT refund_completed_at,attempts,refund_first_attempt_at FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&expectedCompleted, &expectedAttempts, &expectedFirst); err != nil {
			t.Fatal(err)
		}
		deferred, err := repo.DeferRefundForAdmission(ctx, claim, guard, time.Now().UTC().Add(15*time.Second))
		if err != nil || !deferred {
			t.Fatalf("late-success neutral defer = owned:%v err=%v; want current owner released", deferred, err)
		}
		var state, providerID, providerStatus, reconciliation string
		var attempts int
		var completed, first *time.Time
		var token *uuid.UUID
		var lease *time.Time
		var operation *string
		if err := pool.QueryRow(ctx, `SELECT refund_state,refund_provider_id,refund_provider_status,reconciliation_state,
			attempts,refund_completed_at,refund_first_attempt_at,claim_token,lease_until,operation_kind FROM payments WHERE id=$1`, claim.PaymentID).
			Scan(&state, &providerID, &providerStatus, &reconciliation, &attempts, &completed, &first, &token, &lease, &operation); err != nil {
			t.Fatal(err)
		}
		if state != "succeeded" || providerID != "re_branch_success" || providerStatus != "succeeded" || reconciliation != "resolved" ||
			attempts != expectedAttempts || completed == nil || expectedCompleted == nil || !completed.Equal(*expectedCompleted) ||
			(first == nil) != (expectedFirst == nil) || first != nil && !first.Equal(*expectedFirst) || token != nil || lease != nil || operation != nil {
			t.Fatalf("late-success facts or release changed: state=%q provider_id=%q provider_status=%q reconciliation=%q attempts=%d completed=%v first=%v owner=%v/%v/%v",
				state, providerID, providerStatus, reconciliation, attempts, completed, first, token, lease, operation)
		}
	})
}

// prepareSyntheticAdmissionWaitForBranchCoverage constructs a positive
// coordination-wait result around an explicitly synthetic pre-send claim. The
// direct state reset is only to exercise neutral-defer preservation branches;
// it is not a supported business transition or a production claim sequence.
func prepareSyntheticAdmissionWaitForBranchCoverage(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *PostgresPaymentRepository, priorAttempts int, priorFirstAttemptAt *time.Time) (domain.CreateOperation, domain.FinancialClaim, RefundPreSendResult) {
	t.Helper()
	op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	refundKey := "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"
	if _, err := repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decisionID,
		Generation: 1, AmountCents: op.Identity.AmountCents, Currency: "usd", Reason: "EXPIRED", Key: refundKey}); err != nil {
		t.Fatal(err)
	}
	token := uuid.New()
	const fence int64 = 1
	if _, err := pool.Exec(ctx, `UPDATE payments SET attempts=$2,refund_first_attempt_at=COALESCE($3,clock_timestamp()),
		claim_token=$4,claim_fence=$5,operation_kind='refund',lease_until=clock_timestamp()+INTERVAL '1 minute',refund_state='claimed'
		WHERE id=$1`, op.PaymentID, priorAttempts+1, priorFirstAttemptAt, token, fence); err != nil {
		t.Fatal(err)
	}
	view, err := repo.GetFinancial(ctx, op.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	var claimedFirst time.Time
	if err := pool.QueryRow(ctx, `SELECT refund_first_attempt_at FROM payments WHERE id=$1`, op.PaymentID).Scan(&claimedFirst); err != nil {
		t.Fatal(err)
	}
	claim := domain.FinancialClaim{PaymentID: op.PaymentID, Token: token, Fence: fence, Kind: "refund", Key: refundKey,
		LeaseUntil: time.Now().UTC().Add(time.Minute), FirstAttemptAt: &claimedFirst, Attempts: priorAttempts + 1,
		PriorRefundState: "pending", PriorAttempts: priorAttempts,
		Operation: domain.CreateOperation{PaymentID: op.PaymentID, Key: refundKey, Identity: op.Identity, FrozenParams: op.FrozenParams},
		View:      view, Refund: &domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decisionID, Generation: 1,
			AmountCents: op.Identity.AmountCents, Currency: "usd", Reason: "EXPIRED", Key: refundKey}}
	if priorFirstAttemptAt != nil {
		var storedPrior *time.Time
		if err := pool.QueryRow(ctx, `SELECT $1::timestamptz`, priorFirstAttemptAt).Scan(&storedPrior); err != nil {
			t.Fatal(err)
		}
		claim.PriorRefundFirstAttemptAt = storedPrior
	}
	// Synthetic only: make the row appear admission-eligible while retaining this
	// fabricated owner so the final guard yields its committed wait proof.
	if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='not_required',refund_amount_cents=NULL,
		reconciliation_state='sale_verified' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	permit, err := repo.GrantAdmission(ctx, paymentAdmissionIdentity(op), paymentAdmissionEvidence())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='claimed',refund_amount_cents=$2,
		reconciliation_state='refund_required' WHERE id=$1`, op.PaymentID, op.Identity.AmountCents); err != nil {
		t.Fatal(err)
	}
	guard, err := repo.CheckRefundPreSend(ctx, claim)
	if err != nil || guard.Decision != RefundPreSendCoordinationWait || !guard.coordinationWaitProof {
		t.Fatalf("synthetic final guard = %+v err=%v; want committed coordination-wait proof (permit=%+v)", guard, err, permit)
	}
	return op, claim, guard
}

func seedPaymentAdmissionEligible(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *PostgresPaymentRepository) domain.CreateOperation {
	t.Helper()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_admission_" + uuid.NewString()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded", IntentID: intentID, PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents,
		Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	return op
}

func paymentAdmissionIdentity(op domain.CreateOperation) admission.Identity {
	actor := uuid.New()
	keyDigest := sha256.Sum256([]byte("payment admission key:" + uuid.NewString()))
	fingerprint := sha256.Sum256([]byte("payment admission request:" + uuid.NewString()))
	return admission.Identity{RequestID: admission.RequestIDFor(actor, keyDigest), PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, TicketID: uuid.New(), EventID: uuid.New(), UserID: op.Identity.UserID,
		ActorID: actor, Generation: 1, KeyDigest: keyDigest, Fingerprint: fingerprint}
}

func paymentAdmissionEvidence() domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState: domain.DisputeEvidenceNoDispute, RefundState: domain.RefundEvidenceNoRefund,
		ObservedAt: time.Now().UTC(), Source: "stripe_test_api"}
}

func paymentAdmissionPreparedAudit(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository, op domain.CreateOperation) domain.RefundAuditRecord {
	t.Helper()
	view, err := repo.GetFinancial(ctx, op.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	key := "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"
	claim := domain.FinancialClaim{PaymentID: op.PaymentID, Token: uuid.New(), Fence: 1, Kind: "refund", Key: key,
		Operation: domain.CreateOperation{PaymentID: op.PaymentID, Key: key, Identity: op.Identity, FrozenParams: op.FrozenParams},
		View:      &domain.FinancialView{Payment: view.Payment},
		Refund: &domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: uuid.New(), Generation: 1,
			AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, Reason: "EXPIRED", Key: key}}
	record, err := domain.NewRefundPreparedAudit(claim, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	return record
}
