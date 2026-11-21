package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/saga/internal/domain"
)

func prepareFinalSagaStep(t *testing.T, ctx context.Context, p *pgxpool.Pool, repo *SagaRepository) (domain.SagaClaim, domain.SagaInput, uuid.UUID) {
	t.Helper()
	input := recoveryInput()
	saga, _, err := repo.CreateOrGetSaga(ctx, input, recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	paymentID := uuid.New()
	if _, err := p.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<4`, saga.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET current_step_index=4,payment_id=$2,status='PROCESSING',next_attempt_at=clock_timestamp() WHERE id=$1`, saga.ID, paymentID); err != nil {
		t.Fatal(err)
	}
	claims, err := repo.ClaimSaga(ctx, saga.ID, time.Minute)
	if err != nil || claims == nil {
		t.Fatalf("claim final Saga step: claim=%v err=%v", claims != nil, err)
	}
	return *claims, input, paymentID
}

func TestTicketIssuanceOutboxCommitsWithFinalSagaCompletionAndRetriesSafelyIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	repo := NewSagaRepository(p)
	ctx := context.Background()
	claim, input, paymentID := prepareFinalSagaStep(t, ctx, p, repo)
	stepID := claim.Saga.Steps[4].ID
	if owned, err := repo.PersistStepIntent(ctx, claim, stepID, "forward"); err != nil || !owned {
		t.Fatalf("persist final step intent: owned=%v err=%v", owned, err)
	}
	if owned, err := repo.PersistStepResult(ctx, claim, domain.StepResult{StepID: stepID, Direction: "forward", State: "completed"}); err != nil || !owned {
		t.Fatalf("persist final result: owned=%v err=%v", owned, err)
	}
	var status string
	var index int
	if err := p.QueryRow(ctx, `SELECT status,current_step_index FROM sagas WHERE id=$1`, claim.SagaID).Scan(&status, &index); err != nil || status != "COMPLETED" || index != 5 {
		t.Fatalf("final Saga state=%s/%d err=%v", status, index, err)
	}
	first, err := repo.ClaimTicketIssuanceOutbox(ctx, 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("outbox claim=%d err=%v", len(first), err)
	}
	var payload domain.TicketIssuanceRequestedV1
	if err := json.Unmarshal(first[0].Payload, &payload); err != nil || payload.Version != 1 || payload.SagaID != claim.SagaID || payload.BookingID != input.BookingID || payload.PaymentID != paymentID || payload.EventID != input.EventID || payload.Generation != 1 {
		t.Fatalf("issuance event identity mismatch: %+v err=%v", payload, err)
	}
	if released, err := repo.RetryTicketIssuanceOutbox(ctx, first[0].ID, first[0].ClaimToken, time.Now().Add(time.Second)); err != nil || !released {
		t.Fatalf("release for retry=%v err=%v", released, err)
	}
	if claim, err := repo.ClaimTicketIssuanceOutbox(ctx, 10, time.Minute); err != nil || len(claim) != 0 {
		t.Fatalf("outbox retry became due too early: count=%d err=%v", len(claim), err)
	}
}

func TestFinalSagaCompletionRollsBackWhenIssuanceOutboxCannotBeWrittenIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	repo := NewSagaRepository(p)
	ctx := context.Background()
	claim, _, _ := prepareFinalSagaStep(t, ctx, p, repo)
	stepID := claim.Saga.Steps[4].ID
	if owned, err := repo.PersistStepIntent(ctx, claim, stepID, "forward"); err != nil || !owned {
		t.Fatalf("persist final step intent: owned=%v err=%v", owned, err)
	}
	if _, err := p.Exec(ctx, `DROP TABLE saga_ticket_issuance_outbox`); err != nil {
		t.Fatal(err)
	}
	if owned, err := repo.PersistStepResult(ctx, claim, domain.StepResult{StepID: stepID, Direction: "forward", State: "completed"}); err == nil || owned {
		t.Fatalf("final step reported committed without outbox: owned=%v err=%v", owned, err)
	}
	var status string
	var index int
	var stepState string
	if err := p.QueryRow(ctx, `SELECT status,current_step_index FROM sagas WHERE id=$1`, claim.SagaID).Scan(&status, &index); err != nil || status != "PROCESSING" || index != 4 {
		t.Fatalf("Saga completion escaped failed outbox transaction: status=%s index=%d err=%v", status, index, err)
	}
	if err := p.QueryRow(ctx, `SELECT execute_state FROM saga_steps WHERE id=$1`, stepID).Scan(&stepState); err != nil || stepState != "intended" {
		t.Fatalf("final step result escaped failed transaction: state=%s err=%v", stepState, err)
	}
}
