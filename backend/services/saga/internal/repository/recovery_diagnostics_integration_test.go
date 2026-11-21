package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSagaRecoveryDiagnosticsMatchEligibleClaimsIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	eligible := make(map[uuid.UUID]bool)
	for _, scenario := range []string{"pending", "processing", "compensate", "fail", "payment_wait", "expired_lease", "live_lease", "future", "attention", "completed", "rolled_back"} {
		v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp()-interval '20 seconds' WHERE id=$1`, v.ID); err != nil {
			t.Fatal(err)
		}
		query := ""
		switch scenario {
		case "pending":
			eligible[v.ID] = true
		case "processing":
			query = `UPDATE sagas SET status='PROCESSING',failure_attempts=3 WHERE id=$1`
			eligible[v.ID] = true
		case "compensate":
			query = `UPDATE sagas SET status='ROLLING_BACK',direction='compensate' WHERE id=$1`
			eligible[v.ID] = true
		case "fail":
			query = `UPDATE sagas SET status='FAIL',failure_attempts=3 WHERE id=$1`
			eligible[v.ID] = true
		case "payment_wait":
			if _, err := p.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
				t.Fatal(err)
			}
			query = `UPDATE sagas SET status='WAIT_FOR_PAYMENT',current_step_index=2,attempts=100 WHERE id=$1`
			eligible[v.ID] = true
		case "expired_lease":
			query = `UPDATE sagas SET status='PROCESSING',claim_token=uuid_generate_v4(),lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`
			eligible[v.ID] = true
		case "live_lease":
			query = `UPDATE sagas SET claim_token=uuid_generate_v4(),lease_until=clock_timestamp()+interval '1 minute',next_attempt_at=clock_timestamp()-interval '1 day' WHERE id=$1`
		case "future":
			query = `UPDATE sagas SET next_attempt_at=clock_timestamp()+interval '1 minute' WHERE id=$1`
		case "attention":
			query = `UPDATE sagas SET recovery_state='attention',error_category='invalid_persisted_graph' WHERE id=$1`
		case "completed":
			if _, err := p.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			query = `UPDATE sagas SET status='COMPLETED',current_step_index=5,failure_attempts=100 WHERE id=$1`
		case "rolled_back":
			if _, err := p.Exec(ctx, `UPDATE saga_steps SET compensate_state='completed',status='COMPENSATED',compensated_at=clock_timestamp() WHERE saga_id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			query = `UPDATE sagas SET status='ROLLED_BACK',direction='compensate',current_step_index=-1,failure_attempts=100 WHERE id=$1`
		}
		if query != "" {
			if _, err := p.Exec(ctx, query, v.ID); err != nil {
				t.Fatal(scenario, err)
			}
		}
	}
	if _, err := p.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status,current_step_index,next_attempt_at) VALUES(uuid_generate_v4(),uuid_generate_v4(),'ORDER_SAGA','FAIL',0,clock_timestamp()-interval '1 day')`); err != nil {
		t.Fatal(err)
	}
	due, attention, oldest, err := r.RecoveryStats(ctx, 3)
	if err != nil || due != 6 || attention != 3 || oldest < 20 || oldest > 30 {
		t.Fatalf("diagnostics eligibility/age mismatch: due=%d attention=%d oldest=%f err=%v", due, attention, oldest, err)
	}
	_, attention, _, err = r.RecoveryStats(ctx, 4)
	if err != nil || attention != 1 {
		t.Fatal("configured failure threshold or terminal exclusion ignored", attention, err)
	}
	claimed := 0
	for wave := 0; wave < 2; wave++ {
		claims, err := r.ClaimDueSagas(ctx, 4, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		claimed += len(claims)
		for _, c := range claims {
			if !eligible[c.SagaID] {
				t.Fatal("excluded work claimed", c.SagaID)
			}
			delete(eligible, c.SagaID)
		}
	}
	if claimed != 6 {
		t.Fatal("diagnostics differ from executable claims", claimed)
	}
	due, _, oldest, err = r.RecoveryStats(ctx, 3)
	if err != nil || due != 0 || oldest != 0 {
		t.Fatal("live claims inflated due backlog", due, oldest, err)
	}
}
