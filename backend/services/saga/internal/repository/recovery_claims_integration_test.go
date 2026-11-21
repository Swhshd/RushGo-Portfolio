package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

func TestSagaDueClaimsAndResultFencingIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	const workers = 4
	var sagaIDs []uuid.UUID
	for i := 0; i < workers; i++ {
		v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
		if err != nil {
			t.Fatal(err)
		}
		sagaIDs = append(sagaIDs, v.ID)
	}
	type claimResult struct {
		claim *domain.SagaClaim
		err   error
	}
	claims := make(chan claimResult, workers)
	seenClaimIDs := make(map[uuid.UUID]bool)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := r.ClaimDueSagas(ctx, 1, 1500*time.Millisecond)
			var claim *domain.SagaClaim
			if len(claimed) == 1 {
				claim = &claimed[0]
			}
			claims <- claimResult{claim: claim, err: err}
		}()
	}
	wg.Wait()
	close(claims)
	var first *domain.SagaClaim
	for result := range claims {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.claim != nil {
			if seenClaimIDs[result.claim.SagaID] {
				t.Fatal("two workers claimed one Saga")
			}
			seenClaimIDs[result.claim.SagaID] = true
			if first == nil {
				first = result.claim
			}
		}
	}
	if first == nil || len(seenClaimIDs) != workers || len(sagaIDs) != workers {
		t.Fatal("workers did not uniquely claim all due Sagas")
	}
	if ok, err := r.PersistStepIntent(ctx, *first, first.Saga.Steps[0].ID, "forward"); err != nil || !ok {
		t.Fatalf("first owner intent: %v", err)
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, first.SagaID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := r.ClaimSaga(ctx, first.SagaID, 1500*time.Millisecond)
	if err != nil || reclaimed == nil || reclaimed.Fence <= first.Fence || reclaimed.Token == first.Token {
		t.Fatalf("expired claim was not fenced and reclaimed: %v", err)
	}
	if ok, err := r.PersistStepResult(ctx, *first, domain.StepResult{StepID: first.Saga.Steps[0].ID, Direction: "forward", State: "completed"}); err != nil || ok {
		t.Fatalf("stale owner persisted result: %v", err)
	}
	if ok, err := r.PersistStepIntent(ctx, *reclaimed, reclaimed.Saga.Steps[0].ID, "forward"); err != nil || !ok {
		t.Fatalf("reclaimed owner could not replay stable step: %v", err)
	}
	if reclaimed.Saga.Steps[0].ID != first.Saga.Steps[0].ID || !reclaimed.Reclaimed {
		t.Fatal("reclaim changed downstream identity")
	}
	if ok, err := r.PersistStepResult(ctx, *reclaimed, domain.StepResult{StepID: first.Saga.Steps[0].ID, Direction: "forward", State: "completed"}); err != nil || !ok {
		t.Fatal("fresh owner could not complete original effect", err)
	}
	got, err := r.GetSagaById(ctx, first.SagaID)
	if err != nil || got.CurrentStepIndex != 1 || got.Steps[0].ExecuteState != "completed" {
		t.Fatal("fresh result was not atomically advanced", err)
	}
}

func TestSagaRenewalSurvivesDirectionTransitionIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimDueSagas(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim missing: %v", err)
	}
	claim := claims[0]
	changed, err := r.SwitchToCompensation(ctx, claim)
	if err != nil || changed == nil || changed.Direction != "compensate" {
		t.Fatalf("direction transition failed: %v", err)
	}
	if renewed, err := r.RenewClaim(ctx, claim, 30*time.Second); err != nil || !renewed {
		t.Fatalf("live owner lost renewal during direction transition: %v", err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.Direction != "compensate" {
		t.Fatalf("direction transition was not retained: %v", err)
	}
}

func TestSagaClaimFailureReleasesDueWorkIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimDueSagas(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim missing: %v", err)
	}
	recorder, ok := any(r).(interface {
		PersistClaimFailure(context.Context, domain.SagaClaim, string, time.Time, bool) (bool, error)
	})
	if !ok {
		t.Fatal("durable claim failure recorder absent")
	}
	if applied, err := recorder.PersistClaimFailure(ctx, claims[0], "infrastructure_unknown", time.Now().Add(-time.Second), false); err != nil || !applied {
		t.Fatalf("claim failure was not persisted: %v", err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FailureAttempts != 1 || got.ErrorCategory != "infrastructure_unknown" || got.NextAttemptAt == nil {
		t.Fatalf("failure accounting not retained: attempts=%d category=%q due=%v", got.FailureAttempts, got.ErrorCategory, got.NextAttemptAt)
	}
	if next, err := r.ClaimDueSagas(ctx, 1, 30*time.Second); err != nil || len(next) != 1 {
		t.Fatalf("released claim was not rediscoverable: %v", err)
	}
	if applied, err := recorder.PersistClaimFailure(ctx, claims[0], "stale_owner", time.Now().Add(-time.Second), false); err != nil || applied {
		t.Fatalf("stale owner released a newer claim: applied=%v err=%v", applied, err)
	}
}
