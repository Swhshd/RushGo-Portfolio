package repository

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
)

func TestTransitionWaitingSagaIsCompareAndSetIntegration(t *testing.T) {
	ctx := context.Background()
	pool, _ := sagaRecoveryFixture(t)

	bookingID := uuid.New()
	sagaID := uuid.New()
	stepID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO sagas (id, booking_id, name, status, current_step_index) VALUES ($1, $2, 'ORDER_SAGA', 'WAIT_FOR_PAYMENT', 1)`, sagaID, bookingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO saga_steps (id, saga_id, name, status, "order", should_pause_for_payment) VALUES ($1, $2, 'RESERVED_SEAT_STEP', 'COMPLETED', 0, FALSE)`, stepID, sagaID); err != nil {
		t.Fatal(err)
	}

	repo := NewSagaRepository(pool)
	const contenders = 12
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := repo.TransitionWaitingSaga(ctx, bookingID, domain.SAGA_ROLLED_BACK)
			if err != nil {
				t.Errorf("transition saga: %v", err)
			}
			results <- changed
		}()
	}
	wg.Wait()
	close(results)

	changedCount := 0
	for changed := range results {
		if changed {
			changedCount++
		}
	}
	if changedCount != 1 {
		t.Fatalf("CAS transition winners=%d, want 1", changedCount)
	}

	var status string
	var currentStep int
	if err := pool.QueryRow(ctx, `SELECT status, current_step_index FROM sagas WHERE id = $1`, sagaID).Scan(&status, &currentStep); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.SAGA_ROLLED_BACK) || currentStep != 0 {
		t.Fatalf("saga status=%s current_step=%d, want ROLLED_BACK/0", status, currentStep)
	}
	var stepStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM saga_steps WHERE id = $1`, stepID).Scan(&stepStatus); err != nil {
		t.Fatal(err)
	}
	if stepStatus != string(domain.SAGA_STEP_COMPENSATED) {
		t.Fatalf("reservation step status=%s, want COMPENSATED", stepStatus)
	}

	changed, err := repo.TransitionWaitingSaga(ctx, bookingID, domain.SAGA_ROLLED_BACK)
	if err != nil {
		t.Fatalf("duplicate expiry transition: %v", err)
	}
	if changed {
		t.Fatal("duplicate expiry transition must not win")
	}
}
