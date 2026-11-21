package service

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
)

type emptyRecoveryRepository struct {
	claims atomic.Int64
}

func (r *emptyRecoveryRepository) CreateOrGetSaga(context.Context, domain.SagaInput, []domain.SagaStep) (*domain.Saga, bool, error) {
	return nil, false, repository.ErrRecoveryAttention
}
func (r *emptyRecoveryRepository) ClaimDueSagas(context.Context, int, time.Duration) ([]domain.SagaClaim, error) {
	r.claims.Add(1)
	return nil, nil
}
func (*emptyRecoveryRepository) RenewClaim(context.Context, domain.SagaClaim, time.Duration) (bool, error) {
	return false, nil
}
func (*emptyRecoveryRepository) PersistStepIntent(context.Context, domain.SagaClaim, uuid.UUID, string) (bool, error) {
	return false, nil
}
func (*emptyRecoveryRepository) PersistStepResult(context.Context, domain.SagaClaim, domain.StepResult) (bool, error) {
	return false, nil
}
func (*emptyRecoveryRepository) WakeSaga(context.Context, uuid.UUID, int64, *uuid.UUID, string) error {
	return nil
}

func TestRecoveryWorkerRunsStartupAndPeriodicScans(t *testing.T) {
	r := &emptyRecoveryRepository{}
	w, err := NewRecoveryWorker(r, &SagaService{}, zap.NewNop(), RecoveryConfig{ScanInterval: time.Second, LeaseTTL: 30 * time.Second, RemoteTimeout: 5 * time.Second, BatchSize: 1, Concurrency: 1, MaxAttempts: 10})
	if err != nil {
		t.Fatal(err)
	}
	scanTicks := make(chan time.Time, 1)
	w.ticker = func(interval time.Duration) (<-chan time.Time, func()) {
		if interval != w.config.ScanInterval {
			t.Fatalf("unexpected ticker interval: %s", interval)
		}
		return scanTicks, func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	deadline := time.Now().Add(time.Second)
	for r.claims.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.claims.Load() != 1 {
		t.Fatal("startup scan did not run")
	}
	scanTicks <- time.Now()
	deadline = time.Now().Add(time.Second)
	for r.claims.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("periodic scan worker did not stop")
	}
	if r.claims.Load() != 2 {
		t.Fatalf("periodic scan count=%d, want 2", r.claims.Load())
	}
}

func TestRecoveryConfigFromEnvRejectsUnsafeOverrides(t *testing.T) {
	keys := []string{"RELIABILITY_SCAN_INTERVAL", "RELIABILITY_BATCH_SIZE", "RELIABILITY_CONCURRENCY", "RELIABILITY_LEASE_TTL", "RELIABILITY_REMOTE_TIMEOUT", "RELIABILITY_MAX_ATTEMPTS"}
	original := make(map[string]string)
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			original[key] = value
		}
		_ = os.Unsetenv(key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if value, ok := original[key]; ok {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})
	if got, err := RecoveryConfigFromEnv(); err != nil || got.ScanInterval != time.Second || got.BatchSize != 16 || got.Concurrency != 4 {
		t.Fatalf("default recovery config: %+v, %v", got, err)
	}
	if err := os.Setenv("RELIABILITY_REMOTE_TIMEOUT", "28s"); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoveryConfigFromEnv(); err == nil {
		t.Fatal("remote timeout that cannot renew before lease expiry was accepted")
	}
}

func TestRecoveryRetryJitterIsInjectableAndCapped(t *testing.T) {
	called := false
	got := retryDelay(1, func(delay time.Duration) time.Duration {
		called = true
		return delay + time.Second
	})
	if got != 1500*time.Millisecond || !called {
		t.Fatalf("injected recovery jitter was not persisted into the retry delay: %s", got)
	}
	if got = retryDelay(10, func(delay time.Duration) time.Duration { return 2 * delay }); got != time.Minute {
		t.Fatalf("jitter exceeded the locked retry cap: %s", got)
	}
}
