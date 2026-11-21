package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/saga/internal/domain"
)

func TestPaidUnissuedRecoveryDryRunAndExecuteAreAtomicAndIdempotentIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	repo := NewSagaRepository(p)
	ctx := context.Background()
	saga, paymentID, paymentIntentID := preparePaidUnissuedAttentionSaga(t, ctx, p, repo)
	state, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil {
		t.Fatal(err)
	}
	command := paidUnissuedRecoveryCommandFixture(state, saga.ID, saga.BookingID, paymentID, paymentIntentID, "dry_run")
	command.Eligible, command.ResultCode = true, "dry_run_eligible"

	dryRun, err := repo.RecordPaidUnissuedRecovery(ctx, command)
	if err != nil || dryRun == nil || !dryRun.Eligible || dryRun.Requeued {
		t.Fatalf("dry-run audit=%+v err=%v, want eligible audit without Saga mutation", dryRun, err)
	}
	dryRunReplay, err := repo.RecordPaidUnissuedRecovery(ctx, command)
	if err != nil || dryRunReplay == nil || !dryRunReplay.Replayed || dryRunReplay.AuditID != dryRun.AuditID || dryRunReplay.Requeued {
		t.Fatalf("dry-run replay=%+v err=%v", dryRunReplay, err)
	}
	afterDryRun, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil || afterDryRun.Saga.RecoveryState != "attention" || afterDryRun.Saga.PaymentID != uuid.Nil ||
		afterDryRun.Saga.PaymentIntentId != "" || afterDryRun.OutboxCount != 0 || afterDryRun.Saga.ClaimFence != state.Saga.ClaimFence {
		t.Fatalf("dry-run changed business state: state=%+v err=%v", afterDryRun, err)
	}

	execute := command
	execute.Mode, execute.ResultCode = "execute", "requeued"
	requeued, err := repo.RecordPaidUnissuedRecovery(ctx, execute)
	if err != nil || requeued == nil || !requeued.Eligible || !requeued.Requeued || requeued.Replayed {
		t.Fatalf("execute audit=%+v err=%v", requeued, err)
	}
	stateAfterExecute, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil || stateAfterExecute.Saga.RecoveryState != "ready" || stateAfterExecute.Saga.PaymentID != paymentID ||
		stateAfterExecute.Saga.PaymentIntentId != paymentIntentID || stateAfterExecute.Saga.CurrentStepIndex != 4 ||
		stateAfterExecute.Saga.Status != domain.SAGA_PROCESSING || stateAfterExecute.Saga.ClaimFence != state.Saga.ClaimFence+1 || stateAfterExecute.OutboxCount != 0 {
		t.Fatalf("execute did not preserve/requeue original final step: state=%+v err=%v", stateAfterExecute, err)
	}

	replay, err := repo.RecordPaidUnissuedRecovery(ctx, execute)
	if err != nil || replay == nil || !replay.Replayed || !replay.Requeued || replay.AuditID != requeued.AuditID {
		t.Fatalf("execute replay=%+v err=%v", replay, err)
	}
	afterReplay, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil || afterReplay.Saga.ClaimFence != stateAfterExecute.Saga.ClaimFence || afterReplay.OutboxCount != 0 {
		t.Fatalf("execute replay changed fence or created duplicate outbox: state=%+v err=%v", afterReplay, err)
	}
	var auditCount int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM saga_paid_unissued_recovery_audit WHERE operation_key=$1`, command.OperationKey).Scan(&auditCount); err != nil || auditCount != 2 {
		t.Fatalf("audit rows=%d err=%v, want one dry-run and one execute", auditCount, err)
	}
}

func TestPaidUnissuedRecoveryRejectsActiveClaimWithoutChangingFenceOrOutboxIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	repo := NewSagaRepository(p)
	ctx := context.Background()
	saga, paymentID, paymentIntentID := preparePaidUnissuedAttentionSaga(t, ctx, p, repo)
	activeToken := uuid.New()
	if _, err := p.Exec(ctx, `UPDATE sagas SET claim_token=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1`, saga.ID, activeToken); err != nil {
		t.Fatal(err)
	}
	state, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil {
		t.Fatal(err)
	}
	command := paidUnissuedRecoveryCommandFixture(state, saga.ID, saga.BookingID, paymentID, paymentIntentID, "execute")
	command.Eligible, command.ResultCode = true, "requeued"
	result, err := repo.RecordPaidUnissuedRecovery(ctx, command)
	if err != nil || result == nil || result.Eligible || result.Requeued || result.ResultCode != "recovery_state_not_eligible" {
		t.Fatalf("live claim was not rejected: result=%+v err=%v", result, err)
	}
	after, err := repo.GetPaidUnissuedSagaState(ctx, saga.ID)
	if err != nil || after.Saga.RecoveryState != "attention" || after.Saga.ClaimToken == nil || *after.Saga.ClaimToken != activeToken ||
		after.Saga.ClaimFence != state.Saga.ClaimFence || after.OutboxCount != 0 {
		t.Fatalf("live claim rejection mutated the owner/fence/outbox: state=%+v err=%v", after, err)
	}
}

func preparePaidUnissuedAttentionSaga(t *testing.T, ctx context.Context, p *pgxpool.Pool, repo *SagaRepository) (*domain.Saga, uuid.UUID, string) {
	t.Helper()
	// This fixture only targets the pinned isolated SAGA_TEST_DATABASE_URL
	// schema created by sagaRecoveryFixture; it never touches a shared database.
	input := recoveryInput()
	saga, _, err := repo.CreateOrGetSaga(ctx, input, recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<4`, saga.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET current_step_index=4,status='PROCESSING',next_attempt_at=clock_timestamp() WHERE id=$1`, saga.ID); err != nil {
		t.Fatal(err)
	}
	claim, err := repo.ClaimSaga(ctx, saga.ID, time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("claim final Saga step: claim=%v err=%v", claim != nil, err)
	}
	stepID := claim.Saga.Steps[4].ID
	if owned, err := repo.PersistStepIntent(ctx, *claim, stepID, "forward"); err != nil || !owned {
		t.Fatalf("persist final intent: owned=%v err=%v", owned, err)
	}
	if owned, err := repo.PersistStepResult(ctx, *claim, domain.StepResult{StepID: stepID, Direction: "forward", State: "completed"}); owned || !errors.Is(err, ErrPaidUnissuedBindingMissing) {
		t.Fatalf("missing payment binding failure: owned=%v err=%v", owned, err)
	}
	if owned, err := repo.PersistClaimFailure(ctx, *claim, "paid_unissued_binding_missing", time.Now().Add(time.Minute), true); err != nil || !owned {
		t.Fatalf("persist attention state: owned=%v err=%v", owned, err)
	}
	return saga, uuid.New(), "pi_paid_unissued_fixture"
}

func paidUnissuedRecoveryCommandFixture(state *PaidUnissuedSagaState, sagaID, bookingID, paymentID uuid.UUID, paymentIntentID, mode string) PaidUnissuedRecoveryCommand {
	return PaidUnissuedRecoveryCommand{
		SagaID: sagaID, BookingID: bookingID, OperationKey: uuid.New(), Operator: "operator-test", Reason: "recover paid unissued fixture",
		ExpectedProviderAccount: "acct_test_fixture", Mode: mode, PaymentID: &paymentID, PaymentIntentID: paymentIntentID,
		Expected: *state, AuthoritySnapshot: []byte(`{"payment":{"livemode":false}}`),
	}
}
