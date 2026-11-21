package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type RecoveryConfig struct {
	ScanInterval, LeaseTTL, RemoteTimeout time.Duration
	BatchSize, Concurrency, MaxAttempts   int
}

func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{ScanInterval: time.Second, LeaseTTL: 30 * time.Second, RemoteTimeout: 5 * time.Second, BatchSize: 16, Concurrency: 4, MaxAttempts: 10}
}

// RecoveryConfigFromEnv keeps process-owned recovery tuning explicit while
// rejecting values that could create an unbounded or unrenewable claim queue.
func RecoveryConfigFromEnv() (RecoveryConfig, error) {
	c := DefaultRecoveryConfig()
	var err error
	if c.ScanInterval, err = envDuration("RELIABILITY_SCAN_INTERVAL", c.ScanInterval); err != nil {
		return c, err
	}
	if c.LeaseTTL, err = envDuration("RELIABILITY_LEASE_TTL", c.LeaseTTL); err != nil {
		return c, err
	}
	if c.RemoteTimeout, err = envDuration("RELIABILITY_REMOTE_TIMEOUT", c.RemoteTimeout); err != nil {
		return c, err
	}
	if c.BatchSize, err = envInt("RELIABILITY_BATCH_SIZE", c.BatchSize); err != nil {
		return c, err
	}
	if c.Concurrency, err = envInt("RELIABILITY_CONCURRENCY", c.Concurrency); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = envInt("RELIABILITY_MAX_ATTEMPTS", c.MaxAttempts); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

func envInt(name string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

func (c RecoveryConfig) Validate() error {
	if c.ScanInterval <= 0 || c.LeaseTTL < time.Second || c.LeaseTTL > time.Minute || c.RemoteTimeout <= 0 || c.RemoteTimeout+3*time.Second >= c.LeaseTTL || c.BatchSize < 1 || c.BatchSize > 16 || c.Concurrency < 1 || c.Concurrency > 4 || c.MaxAttempts < 1 {
		return fmt.Errorf("invalid Saga recovery configuration")
	}
	return nil
}

type RecoveryWorker struct {
	repo    repository.RecoveryRepository
	service *SagaService
	logger  *zap.Logger
	config  RecoveryConfig
	metrics *observability.Metrics
	runMu   sync.Mutex
	ticker  func(time.Duration) (<-chan time.Time, func())
	jitter  func(time.Duration) time.Duration
}

// runClaimWithHeartbeat keeps a Saga claim owned while its executor is blocked
// on an external effect or an isolated reliability hook. The heartbeat is
// shared by the synchronous StartOrderSaga fast path and the recovery worker.
func runClaimWithHeartbeat(
	ctx context.Context,
	c domain.SagaClaim,
	leaseTTL, remoteTimeout time.Duration,
	ticker func(time.Duration) (<-chan time.Time, func()),
	renew func(context.Context, domain.SagaClaim, time.Duration) (bool, error),
	execute func(context.Context) error,
) (executionErr, renewalErr error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewalDone := make(chan error, 1)
	go func() {
		ticks, stop := ticker(leaseTTL / 3)
		defer stop()
		for {
			select {
			case <-workCtx.Done():
				renewalDone <- nil
				return
			case <-ticks:
				renewCtx, renewCancel := context.WithTimeout(workCtx, remoteTimeout)
				owned, err := renew(renewCtx, c, leaseTTL)
				renewCancel()
				if err != nil || !owned {
					if workCtx.Err() != nil {
						renewalDone <- nil
						return
					}
					cancel()
					if err == nil {
						err = repository.ErrRecoveryConflict
					}
					renewalDone <- err
					return
				}
			}
		}
	}()
	executionErr = execute(workCtx)
	cancel()
	renewalErr = <-renewalDone
	return executionErr, renewalErr
}

func NewRecoveryWorker(r repository.RecoveryRepository, s *SagaService, logger *zap.Logger, configs ...RecoveryConfig) (*RecoveryWorker, error) {
	c := DefaultRecoveryConfig()
	if len(configs) > 1 {
		return nil, fmt.Errorf("invalid recovery configuration count")
	}
	if len(configs) == 1 {
		c = configs[0]
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if r == nil || s == nil || logger == nil {
		return nil, fmt.Errorf("missing Saga recovery dependency")
	}
	return &RecoveryWorker{repo: r, service: s, logger: logger, config: c, metrics: s.metrics, jitter: defaultRetryJitter, ticker: func(interval time.Duration) (<-chan time.Time, func()) {
		t := time.NewTicker(interval)
		return t.C, t.Stop
	}}, nil
}
func (s *SagaService) RunRecoveryOnce(ctx context.Context, limit int) (int, error) {
	r, err := s.replayRepository()
	if err != nil {
		return 0, err
	}
	w, err := NewRecoveryWorker(r, s, s.logger)
	if err != nil {
		return 0, err
	}
	return w.RunOnce(ctx, limit)
}

// RunOnce claims only immediately executable slots. Independent financial waits
// cannot abandon other claims; every wave completes before claiming more rows.
func (w *RecoveryWorker) RunOnce(ctx context.Context, limit int) (int, error) {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	if limit < 1 || limit > 16 {
		return 0, repository.ErrInvalidRecoveryInput
	}
	handled := 0
	var first error
	defer w.recordStats(ctx)
	for handled < limit && ctx.Err() == nil {
		free := min(w.config.Concurrency, limit-handled)
		claims, err := w.repo.ClaimDueSagas(ctx, free, w.config.LeaseTTL)
		if err != nil {
			return handled, errors.Join(first, err)
		}
		if len(claims) == 0 {
			break
		}
		outcomes := make(chan error, len(claims))
		for _, claim := range claims {
			go func(c domain.SagaClaim) { outcomes <- w.execute(ctx, c) }(claim)
		}
		for range claims {
			err := <-outcomes
			if err != nil && !errors.Is(err, errFinancialWait) && !errors.Is(err, repository.ErrRecoveryConflict) && first == nil {
				first = err
			}
			handled++
		}
	}
	return handled, first
}
func (w *RecoveryWorker) execute(ctx context.Context, c domain.SagaClaim) (executionErr error) {
	var renewErr error
	defer func() {
		if w.metrics != nil {
			direction, result := w.durableOutcome(ctx, c, executionErr, renewErr)
			w.metrics.SagaRecoveryAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("direction", direction), attribute.String("result", result)))
		}
	}()
	claimCtx, claimCancel := context.WithTimeout(ctx, w.config.RemoteTimeout)
	owned, err := w.repo.RenewClaim(claimCtx, c, w.config.LeaseTTL)
	claimCancel()
	if err != nil {
		return w.service.persistRecoveryFailure(c, err, "infrastructure_unknown", false, w.config.MaxAttempts, w.jitter)
	}
	if !owned {
		return repository.ErrRecoveryConflict
	}
	if c.Reclaimed && w.metrics != nil {
		w.metrics.SagaRecoveryReclaims.Add(ctx, 1)
	}
	executionErr, renewErr = runClaimWithHeartbeat(
		ctx, c, w.config.LeaseTTL, w.config.RemoteTimeout, w.ticker, w.repo.RenewClaim,
		func(workCtx context.Context) error {
			return w.service.executeClaimWithJitter(workCtx, c, w.config.RemoteTimeout, w.config.MaxAttempts, w.jitter)
		})
	return errors.Join(executionErr, renewErr)
}

func (w *RecoveryWorker) durableOutcome(ctx context.Context, c domain.SagaClaim, executionErr, renewErr error) (string, string) {
	v, err := w.service.repo.GetSagaById(ctx, c.SagaID)
	if err != nil {
		if renewErr != nil || errors.Is(executionErr, repository.ErrRecoveryConflict) {
			return c.Direction, "ownership_lost"
		}
		if errors.Is(executionErr, errFinancialWait) {
			return c.Direction, "financial_wait"
		}
		return c.Direction, "retry"
	}
	if v.ClaimFence != c.Fence || (v.ClaimToken != nil && *v.ClaimToken != c.Token) {
		return c.Direction, "ownership_lost"
	}
	if v.Status == domain.SAGA_COMPLETED || v.Status == domain.SAGA_ROLLED_BACK {
		return v.Direction, "completed"
	}
	if v.Status == domain.SAGA_WAIT_FOR_PAYMENT || v.ErrorCategory == "financial_wait" {
		return v.Direction, "financial_wait"
	}
	// S1 may release the same fence with an early collection wake, leaving
	// PROCESSING at S2. That is committed progress, not terminal completion or
	// ownership loss when renewal observes the intentionally cleared token.
	if v.ClaimToken == nil && v.RecoveryState == "ready" && v.ErrorCategory == "" && c.Saga != nil && v.Direction == c.Direction && v.CurrentStepIndex > c.Saga.CurrentStepIndex {
		return v.Direction, "progress"
	}
	if renewErr != nil || errors.Is(executionErr, repository.ErrRecoveryConflict) {
		return v.Direction, "ownership_lost"
	}
	if executionErr == nil {
		return v.Direction, "progress"
	}
	return v.Direction, "retry"
}

func defaultRetryJitter(delay time.Duration) time.Duration {
	return time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
}
func (w *RecoveryWorker) Start(ctx context.Context) error {
	ticks, stop := w.ticker(w.config.ScanInterval)
	defer stop()
	for {
		if _, err := w.RunOnce(ctx, w.config.BatchSize); err != nil && ctx.Err() == nil {
			w.logger.Warn("Saga recovery retained durable retry responsibility")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
		}
	}
}
func (w *RecoveryWorker) recordStats(ctx context.Context) {
	if w.metrics == nil {
		return
	}
	if stats, ok := w.repo.(interface {
		RecoveryStats(context.Context, int) (int64, int64, float64, error)
	}); ok {
		due, attention, oldest, err := stats.RecoveryStats(ctx, w.config.MaxAttempts)
		if err == nil {
			w.metrics.SagaRecoveryDue.Record(ctx, due)
			w.metrics.SagaRecoveryAttention.Record(ctx, attention)
			w.metrics.SagaRecoveryOldestAge.Record(ctx, oldest)
		}
	}
}
