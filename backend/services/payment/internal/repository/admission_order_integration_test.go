//go:build recoverycomponents

package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

type admissionOrderGrantResult struct {
	permit admission.Permit
	err    error
}

// TestPaymentAdmissionConcurrentOrderIntegration exercises supported financial
// transitions against real PostgreSQL row locks. The tests use disposable
// schemas supplied by the owner-manifest refund component harness.
func TestPaymentAdmissionConcurrentOrderIntegration(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("admission_first_row_lock_then_refund_resumes", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		identity := paymentAdmissionIdentity(op)
		blocker, blockerPID := holdPaymentAdmissionRowLock(t, ctx, pool, op.PaymentID)
		defer blocker.Rollback(context.Background())

		grantDone := make(chan admissionOrderGrantResult, 1)
		go func() {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			permit, err := repo.GrantAdmission(callCtx, identity, paymentAdmissionEvidence())
			grantDone <- admissionOrderGrantResult{permit: permit, err: err}
		}()
		waitForBlockedPaymentRows(t, ctx, pool, blockerPID, 1)
		if err := blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		grant := receiveAdmissionGrant(t, grantDone)
		if grant.err != nil || grant.permit.State != admission.Pending {
			t.Fatalf("grant after Payment row-lock barrier = %+v err=%v; want pending permit", grant.permit, grant.err)
		}

		if err := requireAdmissionTestRefund(t, ctx, repo, op); err != nil {
			t.Fatal(err)
		}
		claims, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
		if err != nil || len(claims) != 0 {
			t.Fatalf("refund claim while grant is pending = %+v err=%v; want no claim", claims, err)
		}
		assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 0, nil)
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 0)

		settled, err := repo.SettleAdmission(ctx, identity, grant.permit.Token, grant.permit.Fence, admission.Aborted)
		if err != nil || settled.State != admission.Aborted {
			t.Fatalf("terminal abort after durable test outcome = %+v err=%v", settled, err)
		}
		claims, err = repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
		if err != nil || len(claims) != 1 || claims[0].PaymentID != op.PaymentID || claims[0].Kind != "refund" {
			t.Fatalf("refund claim after all pending admission settled = %+v err=%v", claims, err)
		}
		preSend, err := repo.CheckRefundPreSend(ctx, claims[0])
		if err != nil || preSend.Decision != RefundPreSendAllow {
			t.Fatalf("refund PREPARED after admission settled = %+v err=%v", preSend, err)
		}
		assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 1, nonNilAdmissionTime(t, ctx, pool, op.PaymentID))
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 1)
	})

	t.Run("refund_first_prepared_blocks_new_grant", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		if err := requireAdmissionTestRefund(t, ctx, repo, op); err != nil {
			t.Fatal(err)
		}
		claim := claimAdmissionTestRefund(t, ctx, repo, op.PaymentID)
		prepared, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || prepared.Decision != RefundPreSendAllow {
			t.Fatalf("supported refund-first PREPARED = %+v err=%v", prepared, err)
		}
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 1)

		identity := paymentAdmissionIdentity(op)
		if _, err := repo.GrantAdmission(ctx, identity, paymentAdmissionEvidence()); !errors.Is(err, admission.ErrBlocked) {
			t.Fatalf("grant after refund claim/PREPARED error=%v; want blocked", err)
		}
		if _, err := repo.GetAdmission(ctx, identity); !errors.Is(err, admission.ErrNotFound) {
			t.Fatalf("refund-first path persisted a permit: get error=%v", err)
		}
		assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 1, nonNilAdmissionTime(t, ctx, pool, op.PaymentID))
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 1)
	})

	t.Run("simultaneous_grant_vs_refund_linearizes_to_one_safe_outcome", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		identity := paymentAdmissionIdentity(op)
		obligation := admissionTestRefund(op)
		blocker, blockerPID := holdPaymentAdmissionRowLock(t, ctx, pool, op.PaymentID)
		defer blocker.Rollback(context.Background())

		type refundResult struct {
			err error
		}
		start := make(chan struct{})
		ready := make(chan struct{}, 2)
		grantDone := make(chan admissionOrderGrantResult, 1)
		refundDone := make(chan refundResult, 1)
		go func() {
			ready <- struct{}{}
			<-start
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			permit, err := repo.GrantAdmission(callCtx, identity, paymentAdmissionEvidence())
			grantDone <- admissionOrderGrantResult{permit: permit, err: err}
		}()
		go func() {
			ready <- struct{}{}
			<-start
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := repo.RequireRefund(callCtx, obligation)
			refundDone <- refundResult{err: err}
		}()
		<-ready
		<-ready
		close(start)
		waitForBlockedPaymentRows(t, ctx, pool, blockerPID, 2)
		if err := blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		grant := receiveAdmissionGrant(t, grantDone)
		var refund refundResult
		select {
		case refund = <-refundDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent RequireRefund result")
		}
		if refund.err != nil {
			t.Fatalf("concurrent RequireRefund failed: %v", refund.err)
		}

		switch {
		case grant.err == nil:
			if grant.permit.State != admission.Pending {
				t.Fatalf("grant-first race returned nonpending permit: %+v", grant.permit)
			}
			claims, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
			if err != nil || len(claims) != 0 {
				t.Fatalf("grant-first race consumed refund claim: claims=%+v err=%v", claims, err)
			}
			assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 0, nil)
			assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 0)
			stored, err := repo.GetAdmission(ctx, identity)
			if err != nil || stored != grant.permit {
				t.Fatalf("grant-first race lost immutable pending permit: stored=%+v err=%v", stored, err)
			}
		case errors.Is(grant.err, admission.ErrBlocked):
			if _, err := repo.GetAdmission(ctx, identity); !errors.Is(err, admission.ErrNotFound) {
				t.Fatalf("refund-first race persisted a permit: get error=%v", err)
			}
			claim := claimAdmissionTestRefund(t, ctx, repo, op.PaymentID)
			prepared, err := repo.CheckRefundPreSend(ctx, claim)
			if err != nil || prepared.Decision != RefundPreSendAllow {
				t.Fatalf("refund-first race did not finish the supported pre-send guard: %+v err=%v", prepared, err)
			}
			assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 1, nonNilAdmissionTime(t, ctx, pool, op.PaymentID))
			assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 1)
		default:
			t.Fatalf("simultaneous grant returned unexpected error: %v", grant.err)
		}
	})

	t.Run("multiple_tickets_keep_permit_fences_and_other_payments_independent", func(t *testing.T) {
		op := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		firstIdentity := paymentAdmissionIdentity(op)
		first, err := repo.GrantAdmission(ctx, firstIdentity, paymentAdmissionEvidence())
		if err != nil {
			t.Fatal(err)
		}
		secondIdentity := paymentAdmissionIdentity(op)
		if secondIdentity.TicketID == firstIdentity.TicketID || secondIdentity.RequestID == firstIdentity.RequestID {
			t.Fatal("same-Payment admission fixture did not create an independent Ticket identity")
		}
		second, err := repo.GrantAdmission(ctx, secondIdentity, paymentAdmissionEvidence())
		if err != nil {
			t.Fatalf("second Ticket permit on same Payment: %v", err)
		}
		if first.Token == second.Token || first.Fence <= 0 || second.Fence <= first.Fence {
			t.Fatalf("same-Payment permits do not have distinct monotonic fences: first=%+v second=%+v", first, second)
		}

		otherPayment := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		otherIdentity := paymentAdmissionIdentity(otherPayment)
		otherPermit, err := repo.GrantAdmission(ctx, otherIdentity, paymentAdmissionEvidence())
		if err != nil {
			t.Fatalf("independent Payment permit while first Payment has pending permits: %v", err)
		}
		if otherPermit.Fence <= second.Fence {
			t.Fatalf("global sequence did not advance across independent Payment: second=%d other=%d", second.Fence, otherPermit.Fence)
		}

		conflictingIdentity := firstIdentity
		conflictingIdentity.Fingerprint = sha256.Sum256([]byte("changed payload with original request id"))
		if _, err := repo.GrantAdmission(ctx, conflictingIdentity, paymentAdmissionEvidence()); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("same RequestID with conflicting identity error=%v; want conflict", err)
		}
		storedFirst, err := repo.GetAdmission(ctx, firstIdentity)
		if err != nil || storedFirst != first {
			t.Fatalf("later grants or conflicting retry changed first permit: stored=%+v err=%v", storedFirst, err)
		}
		storedSecond, err := repo.GetAdmission(ctx, secondIdentity)
		if err != nil || storedSecond != second {
			t.Fatalf("later grant changed second Ticket permit: stored=%+v err=%v", storedSecond, err)
		}

		// A pending permit on the first Payment does not delay a supported refund
		// on an unrelated Payment.
		independentRefund := seedPaymentAdmissionEligible(t, ctx, pool, repo)
		if err := requireAdmissionTestRefund(t, ctx, repo, independentRefund); err != nil {
			t.Fatal(err)
		}
		claim := claimAdmissionTestRefund(t, ctx, repo, independentRefund.PaymentID)
		preSend, err := repo.CheckRefundPreSend(ctx, claim)
		if err != nil || preSend.Decision != RefundPreSendAllow {
			t.Fatalf("unrelated Payment refund was blocked by pending permits: %+v err=%v", preSend, err)
		}
		assertAdmissionPreparedCount(t, ctx, repo, independentRefund.PaymentID, 1)
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 0)

		if err := requireAdmissionTestRefund(t, ctx, repo, op); err != nil {
			t.Fatal(err)
		}
		claims, err := repo.ClaimFinancialWork(ctx, 16, 30*time.Second)
		if err != nil || len(claims) != 0 {
			t.Fatalf("multiple pending Tickets failed to hold refund claim: claims=%+v err=%v", claims, err)
		}
		assertAdmissionRefundBookkeeping(t, ctx, pool, op.PaymentID, 0, nil)

		settledFirst, err := repo.SettleAdmission(ctx, firstIdentity, first.Token, first.Fence, admission.Aborted)
		if err != nil || settledFirst.State != admission.Aborted {
			t.Fatalf("settle first Ticket permit: %+v err=%v", settledFirst, err)
		}
		claims, err = repo.ClaimFinancialWork(ctx, 16, 30*time.Second)
		if err != nil || len(claims) != 0 {
			t.Fatalf("one remaining pending Ticket did not continue blocking refund: claims=%+v err=%v", claims, err)
		}
		storedSecond, err = repo.GetAdmission(ctx, secondIdentity)
		if err != nil || storedSecond != second {
			t.Fatalf("settling one Ticket changed another Ticket's permit: stored=%+v err=%v", storedSecond, err)
		}
		settledSecond, err := repo.SettleAdmission(ctx, secondIdentity, second.Token, second.Fence, admission.Committed)
		if err != nil || settledSecond.State != admission.Committed || settledSecond.Token != second.Token || settledSecond.Fence != second.Fence {
			t.Fatalf("settle second Ticket permit: %+v err=%v", settledSecond, err)
		}
		claims, err = repo.ClaimFinancialWork(ctx, 16, 30*time.Second)
		if err != nil || len(claims) != 1 || claims[0].PaymentID != op.PaymentID {
			t.Fatalf("refund remained blocked after all Ticket permits settled: claims=%+v err=%v", claims, err)
		}
		preSend, err = repo.CheckRefundPreSend(ctx, claims[0])
		if err != nil || preSend.Decision != RefundPreSendAllow {
			t.Fatalf("refund pre-send after all same-Payment permits settled: %+v err=%v", preSend, err)
		}
		assertAdmissionPreparedCount(t, ctx, repo, op.PaymentID, 1)
	})

	t.Run("inactive_legacy_prepared_transaction_serializes_activation", func(t *testing.T) {
		activationPool, activationCtx, activationRepo := newRefundComponentRepository(t)
		activationCtx, activationCancel := context.WithTimeout(activationCtx, 15*time.Second)
		defer activationCancel()
		op := seedPaymentAdmissionEligible(t, activationCtx, activationPool, activationRepo)
		prepared := paymentAdmissionPreparedAudit(t, activationCtx, activationRepo, op)
		legacyTx, err := activationPool.Begin(activationCtx)
		if err != nil {
			t.Fatal(err)
		}
		defer legacyTx.Rollback(context.Background())
		var blockerPID int32
		if err := legacyTx.QueryRow(activationCtx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
			t.Fatal(err)
		}
		if err := appendRefundAuditRecord(activationCtx, legacyTx, prepared); err != nil {
			t.Fatalf("inactive legacy PREPARED transaction: %v", err)
		}
		activationDone := make(chan error, 1)
		go func() {
			callCtx, cancel := context.WithTimeout(activationCtx, 5*time.Second)
			defer cancel()
			activationDone <- activationRepo.ActivatePaymentAdmission(callCtx)
		}()
		waitForBlockedActivity(t, activationCtx, activationPool, blockerPID, 1, "%UPDATE payment_admission_mode%")
		if err := legacyTx.Commit(activationCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-activationDone:
			if err != nil {
				t.Fatalf("activation after legacy PREPARED commit: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for activation after legacy PREPARED transaction")
		}
		if _, err := activationRepo.GrantAdmission(activationCtx, paymentAdmissionIdentity(op), paymentAdmissionEvidence()); !errors.Is(err, admission.ErrBlocked) {
			t.Fatalf("admission after activation crossed a committed legacy PREPARED write: %v; want blocked by retained history", err)
		}
	})
}

func requireAdmissionTestRefund(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository, op domain.CreateOperation) error {
	t.Helper()
	_, err := repo.RequireRefund(ctx, admissionTestRefund(op))
	return err
}

func admissionTestRefund(op domain.CreateOperation) domain.RefundObligation {
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+op.Identity.BookingID.String()+":g1:expire"))
	return domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: decisionID, Generation: 1,
		AmountCents: op.Identity.AmountCents, Currency: "usd", Reason: "EXPIRED",
		Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}
}

func claimAdmissionTestRefund(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository, paymentID uuid.UUID) domain.FinancialClaim {
	t.Helper()
	claims, err := repo.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 || claims[0].PaymentID != paymentID || claims[0].Kind != "refund" {
		t.Fatalf("claim supported refund for %s = %+v err=%v", paymentID, claims, err)
	}
	return claims[0]
}

func assertAdmissionPreparedCount(t *testing.T, ctx context.Context, repo *PostgresPaymentRepository, paymentID uuid.UUID, want int) {
	t.Helper()
	rows, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{PaymentID: paymentID, Kind: domain.RefundAuditKindPrepared, Limit: 16})
	if err != nil || len(rows) != want {
		t.Fatalf("PREPARED audit count for %s = %d err=%v; want %d", paymentID, len(rows), err, want)
	}
}

func assertAdmissionRefundBookkeeping(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID, wantAttempts int, wantFirst *time.Time) {
	t.Helper()
	var attempts int
	var first *time.Time
	if err := pool.QueryRow(ctx, `SELECT attempts,refund_first_attempt_at FROM payments WHERE id=$1`, paymentID).Scan(&attempts, &first); err != nil {
		t.Fatal(err)
	}
	if attempts != wantAttempts || (first == nil) != (wantFirst == nil) || first != nil && !first.Equal(*wantFirst) {
		t.Fatalf("refund claim bookkeeping = attempts:%d first:%v; want attempts:%d first:%v", attempts, first, wantAttempts, wantFirst)
	}
}

func nonNilAdmissionTime(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) *time.Time {
	t.Helper()
	var first *time.Time
	if err := pool.QueryRow(ctx, `SELECT refund_first_attempt_at FROM payments WHERE id=$1`, paymentID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("refund claim did not persist refund_first_attempt_at")
	}
	return first
}

func holdPaymentAdmissionRowLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) (pgx.Tx, int32) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var pid int32
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE`, paymentID).Scan(&locked); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	return tx, pid
}

func waitForBlockedPaymentRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blockerPID int32, want int) {
	t.Helper()
	waitForBlockedActivity(t, ctx, pool, blockerPID, want, "%FROM payments%FOR UPDATE%")
}

func waitForBlockedActivity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blockerPID int32, want int, queryPattern string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var blocked int
		err := pool.QueryRow(ctx, `WITH RECURSIVE waiters(pid) AS (
			SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))
			UNION
			SELECT a.pid FROM pg_stat_activity a JOIN waiters w ON w.pid=ANY(pg_blocking_pids(a.pid))
		) SELECT count(DISTINCT a.pid) FROM pg_stat_activity a JOIN waiters w ON a.pid=w.pid
			WHERE a.pid<>$1 AND a.state='active' AND a.query ILIKE $2`, blockerPID, queryPattern).Scan(&blocked)
		if err != nil {
			t.Fatalf("inspect PostgreSQL lock waiters: %v", err)
		}
		if blocked >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("observed %d PostgreSQL statements blocked by backend %d, want at least %d for query %q", blocked, blockerPID, want, queryPattern)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func receiveAdmissionGrant(t *testing.T, done <-chan admissionOrderGrantResult) admissionOrderGrantResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for admission grant result")
		return admissionOrderGrantResult{}
	}
}
