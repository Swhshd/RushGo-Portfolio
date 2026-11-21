package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Faults wrap real migrated PostgreSQL transactions. They model transport errors
// without replacing the durable graph or ownership checks with an in-memory fake.
type recoveryFaultRepository struct {
	*repository.SagaRepository
	initialRenewError error
	intentError       error
	resultAckLost     bool
	resultReleased    chan struct{}
	renewAfterRelease chan struct{}
	renewCalls        int
}

func (r *recoveryFaultRepository) RenewClaim(ctx context.Context, c domain.SagaClaim, ttl time.Duration) (bool, error) {
	r.renewCalls++
	if r.renewCalls == 1 && r.initialRenewError != nil {
		return false, r.initialRenewError
	}
	owned, err := r.SagaRepository.RenewClaim(ctx, c, ttl)
	if r.renewCalls > 1 && r.renewAfterRelease != nil {
		close(r.renewAfterRelease)
	}
	return owned, err
}

func (r *recoveryFaultRepository) PersistStepIntent(ctx context.Context, c domain.SagaClaim, id uuid.UUID, direction string) (bool, error) {
	if r.intentError != nil {
		return false, r.intentError
	}
	return r.SagaRepository.PersistStepIntent(ctx, c, id, direction)
}

func (r *recoveryFaultRepository) PersistStepResult(ctx context.Context, c domain.SagaClaim, result domain.StepResult) (bool, error) {
	owned, err := r.SagaRepository.PersistStepResult(ctx, c, result)
	if err == nil && owned && r.resultReleased != nil {
		close(r.resultReleased)
		select {
		case <-r.renewAfterRelease:
		case <-ctx.Done():
			return owned, ctx.Err()
		}
	}
	if err == nil && owned && r.resultAckLost {
		return false, errors.New("result commit ACK lost")
	}
	return owned, err
}

func recoveryMetricReader(t *testing.T, s *SagaService) *sdkmetric.ManualReader {
	t.Helper()
	r := sdkmetric.NewManualReader()
	p := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	t.Cleanup(func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var err error
	s.metrics, err = observability.NewMetrics(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assertRecoveryMetric(t *testing.T, reader *sdkmetric.ManualReader, direction, result string) {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "rushgo.saga.recovery.attempts" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				d, _ := point.Attributes.Value("direction")
				r, _ := point.Attributes.Value("result")
				if d.AsString() != direction || r.AsString() != result {
					t.Fatalf("attempt mislabeled: direction=%q result=%q; want %q/%q", d.AsString(), r.AsString(), direction, result)
				}
				count += point.Value
			}
		}
	}
	if count != 1 {
		t.Fatalf("attempt count=%d, want 1", count)
	}
}

func TestSagaInitialRenewalFailureRetainsRetryIntegration(t *testing.T) {
	s, r, db, v, _, _, e := storedReplayFixture(t)
	reader := recoveryMetricReader(t, s)
	fault := errors.New("initial renewal transport failure")
	rp := &recoveryFaultRepository{SagaRepository: r, initialRenewError: fault}
	s.repo = rp
	w, err := NewRecoveryWorker(rp, s, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.ClaimSaga(context.Background(), v.ID, 30*time.Second)
	if err != nil || c == nil {
		t.Fatal("claim missing", err)
	}
	before := time.Now()
	if err := w.execute(context.Background(), *c); !errors.Is(err, fault) {
		t.Fatal("renewal error lost", err)
	}
	got, err := r.GetSagaById(context.Background(), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FailureAttempts != 1 || got.ErrorCategory != "infrastructure_unknown" || got.NextAttemptAt == nil || !got.NextAttemptAt.After(before) || e.reserve != 0 {
		t.Fatalf("initial renewal failure bypassed durable retry: failures=%d category=%q due=%v reserve=%d", got.FailureAttempts, got.ErrorCategory, got.NextAttemptAt, e.reserve)
	}
	var released bool
	if err := db.QueryRow(context.Background(), `SELECT claim_token IS NULL AND lease_until IS NULL FROM sagas WHERE id=$1`, v.ID).Scan(&released); err != nil || !released {
		t.Fatal("failed claim not released", err)
	}
	assertRecoveryMetric(t, reader, "forward", "retry")
}

func TestSagaRecoveryLabelsCompensationIntegration(t *testing.T) {
	s, r, _, v, b, _, _ := storedReplayFixture(t)
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "FAILED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "fail")
	reader := recoveryMetricReader(t, s)
	w, err := NewRecoveryWorker(r, s, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.RunOnce(context.Background(), 1); err != nil || n != 1 {
		t.Fatal("compensation run", n, err)
	}
	assertRecoveryMetric(t, reader, "compensate", "financial_wait")
}

func TestSagaRecoveryReleaseRenewalRaceIntegration(t *testing.T) {
	s, r, db, v, b, p, _ := storedReplayFixture(t)
	b.view.Phase = "OPEN"
	p.entry.ProviderOutcome = "unknown"
	ctx := context.Background()
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=1 WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	reader := recoveryMetricReader(t, s)
	rp := &recoveryFaultRepository{SagaRepository: r, resultReleased: make(chan struct{}), renewAfterRelease: make(chan struct{})}
	s.repo = rp
	w, err := NewRecoveryWorker(rp, s, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time, 1)
	w.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
	if err != nil || c == nil {
		t.Fatal("claim missing", err)
	}
	done := make(chan error, 1)
	go func() { done <- w.execute(ctx, *c) }()
	awaitRecoverySignal(t, rp.resultReleased)
	ticks <- time.Now()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("release/renewal race stuck")
	}
	assertRecoveryMetric(t, reader, "forward", "financial_wait")
}

func TestSagaTransientIntentFailuresRemainExecutableIntegration(t *testing.T) {
	s, r, db, v, _, _, e := storedReplayFixture(t)
	rp := &recoveryFaultRepository{SagaRepository: r, intentError: errors.New("intent transaction unavailable")}
	s.repo = rp
	ctx := context.Background()
	for attempt := 1; attempt <= 3; attempt++ {
		c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
		if err != nil || c == nil {
			t.Fatal("retry disappeared", err)
		}
		before := time.Now()
		if err := s.executeClaimWithJitter(ctx, *c, 5*time.Second, 2, func(time.Duration) time.Duration { return 1500 * time.Millisecond }); !errors.Is(err, rp.intentError) {
			t.Fatal(err)
		}
		got, err := r.GetSagaById(ctx, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		category, minDelay := "infrastructure_unknown", 1500*time.Millisecond
		if attempt >= 2 {
			category, minDelay = "retry_age_attention", time.Minute
		}
		if got.FailureAttempts != attempt || got.RecoveryState != "ready" || got.ErrorCategory != category || got.NextAttemptAt == nil || got.NextAttemptAt.Before(before.Add(minDelay)) || got.NextAttemptAt.After(time.Now().Add(minDelay)) || e.reserve != 0 {
			t.Fatalf("transient intent responsibility lost at attempt %d: failures=%d state=%q category=%q due=%v", attempt, got.FailureAttempts, got.RecoveryState, got.ErrorCategory, got.NextAttemptAt)
		}
		if _, err := db.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSagaResultCommitAckLossRetainsProgressIntegration(t *testing.T) {
	s, r, db, v, b, _, e := storedReplayFixture(t)
	b.view.Phase = "OPEN"
	rp := &recoveryFaultRepository{SagaRepository: r, resultAckLost: true}
	s.repo = rp
	c, err := r.ClaimSaga(context.Background(), v.ID, 30*time.Second)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if err := s.ExecuteClaim(context.Background(), *c); err == nil {
		t.Fatal("lost ACK hidden")
	}
	got, err := r.GetSagaById(context.Background(), v.ID)
	if err != nil || got.CurrentStepIndex != 1 || got.Steps[0].ExecuteState != "completed" || e.reserve != 1 {
		t.Fatal("committed effect progress was erased", err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	s.repo = r
	if err := claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatal(err)
	}
	if e.reserve != 1 || e.operations[0] != v.Steps[0].ID.String() {
		t.Fatal("ACK recovery replayed a completed effect or changed identity")
	}
}

func TestSagaRecoveryOutcomeMetricsIntegration(t *testing.T) {
	for _, scenario := range []string{"retry", "invalid_graph", "completed", "ownership_lost"} {
		t.Run(scenario, func(t *testing.T) {
			s, r, db, v, b, _, e := storedReplayFixture(t)
			ctx := context.Background()
			if scenario == "completed" {
				b.view.Phase = "CONFIRMING"
				if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=2 WHERE id=$1`, v.ID); err != nil {
					t.Fatal(err)
				}
			}
			reader := recoveryMetricReader(t, s)
			rp := &recoveryFaultRepository{SagaRepository: r}
			if scenario == "retry" {
				rp.intentError = errors.New("intent unavailable")
			}
			s.repo = rp
			w, err := NewRecoveryWorker(rp, s, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
			if err != nil || c == nil {
				t.Fatal(err)
			}
			want := scenario
			switch scenario {
			case "invalid_graph":
				want = "retry"
				if _, err := db.Exec(ctx, `UPDATE saga_steps SET name='corrupt' WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
					t.Fatal(err)
				}
			case "ownership_lost":
				if _, err := db.Exec(ctx, `UPDATE sagas SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, v.ID); err != nil {
					t.Fatal(err)
				}
				if fresh, err := r.ClaimSaga(ctx, v.ID, 30*time.Second); err != nil || fresh == nil {
					t.Fatal(err)
				}
			}
			err = w.execute(ctx, *c)
			if scenario == "completed" && err != nil {
				t.Fatal(err)
			}
			if scenario != "completed" && err == nil {
				t.Fatal("failed/invalid/stale attempt silently succeeded")
			}
			assertRecoveryMetric(t, reader, "forward", want)
			if scenario != "completed" && (e.reserve != 0 || e.book != 0 || e.account != 0) {
				t.Fatal("unowned/invalid claim performed remote effects")
			}
		})
	}
}

func TestSagaCollectedS1ReportsProgressIntegration(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprintf("renewal_race_%v", race), func(t *testing.T) {
			s, r, db, v, b, _, _ := storedReplayFixture(t)
			b.view.Phase = "OPEN"
			ctx := context.Background()
			if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=1 WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			reader := recoveryMetricReader(t, s)
			rp := &recoveryFaultRepository{SagaRepository: r}
			if race {
				rp.resultReleased, rp.renewAfterRelease = make(chan struct{}), make(chan struct{})
			}
			s.repo = rp
			w, err := NewRecoveryWorker(rp, s, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			ticks := make(chan time.Time, 1)
			w.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
			c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
			if err != nil || c == nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- w.execute(ctx, *c) }()
			if race {
				awaitRecoverySignal(t, rp.resultReleased)
				ticks <- time.Now()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("S1 progress stuck")
			}
			got, err := r.GetSagaById(ctx, v.ID)
			if err != nil || got.Status != domain.SAGA_PROCESSING || got.CurrentStepIndex != 2 {
				t.Fatal("collected S1 did not retain forward work", err)
			}
			assertRecoveryMetric(t, reader, "forward", "progress")
		})
	}
}
