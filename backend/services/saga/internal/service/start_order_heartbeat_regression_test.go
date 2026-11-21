package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
)

type startOrderHeartbeatRenewal struct {
	claim      domain.SagaClaim
	leaseTTL   time.Duration
	ctxBounded bool
}

type startOrderHeartbeatRepository struct {
	repository.SagaRepositoryInterface
	repository.ReplayRepository
	saga     *domain.Saga
	claim    domain.SagaClaim
	renewals chan startOrderHeartbeatRenewal
}

func (r *startOrderHeartbeatRepository) CreateOrGetSaga(context.Context, domain.SagaInput, []domain.SagaStep) (*domain.Saga, bool, error) {
	return r.saga, false, nil
}

func (r *startOrderHeartbeatRepository) GetSagaById(context.Context, uuid.UUID) (*domain.Saga, error) {
	return r.saga, nil
}

func (r *startOrderHeartbeatRepository) ClaimSaga(context.Context, uuid.UUID, time.Duration) (*domain.SagaClaim, error) {
	claim := r.claim
	return &claim, nil
}

func (r *startOrderHeartbeatRepository) RenewClaim(ctx context.Context, claim domain.SagaClaim, leaseTTL time.Duration) (bool, error) {
	_, bounded := ctx.Deadline()
	r.renewals <- startOrderHeartbeatRenewal{claim: claim, leaseTTL: leaseTTL, ctxBounded: bounded}
	return false, nil
}

func (*startOrderHeartbeatRepository) PersistStepIntent(context.Context, domain.SagaClaim, uuid.UUID, string) (bool, error) {
	return true, nil
}

func (*startOrderHeartbeatRepository) PersistStepResult(context.Context, domain.SagaClaim, domain.StepResult) (bool, error) {
	return true, nil
}

func TestStartOrderSagaRenewsClaimAndCancelsWorkOnOwnershipLoss(t *testing.T) {
	s, saga, booking, _, _ := replayLiteralFixture()
	saga.CurrentStepIndex = 0
	booking.view.Phase = "OPEN"

	claim := domain.SagaClaim{
		SagaID: saga.ID, Token: uuid.New(), Fence: 9, Direction: "forward",
		Saga: saga, Input: *saga.Input,
	}
	repo := &startOrderHeartbeatRepository{
		saga: saga, claim: claim, renewals: make(chan startOrderHeartbeatRenewal, 1),
	}
	s.repo = repo

	const token = "private-start-order-heartbeat-test-token"
	dir := t.TempDir()
	hook, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureReliabilityHook(hook); err != nil {
		t.Fatal(err)
	}
	const windowID = "saga.forward.S0.before_effect"
	command, err := json.Marshal(reliabilityhook.Command{
		WindowID: windowID, OperationID: saga.BookingID.String(), Checkpoint: windowID,
		Action: reliabilityhook.ActionPause, Token: token, Nonce: "nonce-start-order-heartbeat",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "command.json"), command, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	type startResult struct {
		response *StartOrderSagaResponse
		err      error
	}
	done := make(chan startResult, 1)
	finished := false
	defer func() {
		cancel()
		if !finished {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("StartOrderSaga did not stop after its context was canceled")
			}
		}
	}()
	go func() {
		response, err := s.StartOrderSaga(ctx, &StartOrderSagaRequest{
			BookingId: saga.BookingID.String(), UserId: saga.Input.UserID.String(),
			EventID: saga.Input.EventID.String(), SeatIds: []string{saga.Input.SeatIDs[0].String()},
			TotalCents: int(saga.Input.AmountCents),
		})
		done <- startResult{response: response, err: err}
	}()

	if !waitForSagaHeartbeatBarrier(t, dir, saga.BookingID.String(), saga.ID.String(), saga.Steps[0].ID.String()) {
		t.Fatal("StartOrderSaga did not reach the controlled pre-effect boundary")
	}

	var renewal startOrderHeartbeatRenewal
	select {
	case renewal = <-repo.renewals:
	case <-time.After(15 * time.Second):
		t.Fatal("the initial synchronous StartOrderSaga claim was not renewed")
	}
	if renewal.claim.SagaID != claim.SagaID || renewal.claim.Token != claim.Token || renewal.claim.Fence != claim.Fence ||
		renewal.leaseTTL != 30*time.Second || !renewal.ctxBounded {
		t.Fatalf("StartOrderSaga renewed a different or unbounded claim: %+v", renewal)
	}

	select {
	case result := <-done:
		finished = true
		if result.err != nil || result.response == nil || result.response.SagaId != saga.ID.String() {
			t.Fatalf("StartOrderSaga failed to return the durable claim state after ownership loss: response=%+v err=%v", result.response, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("work did not stop after renewal reported lost ownership")
	}
}

func waitForSagaHeartbeatBarrier(t *testing.T, dir, bookingID, sagaID, stepID string) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		paths, _ := filepath.Glob(filepath.Join(dir, "barrier-*.json"))
		if len(paths) == 1 {
			raw, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			var barrier reliabilityhook.Barrier
			if err := json.Unmarshal(raw, &barrier); err != nil {
				t.Fatal(err)
			}
			if barrier.WindowID != "saga.forward.S0.before_effect" || barrier.OperationID != bookingID ||
				barrier.Facts["saga_id"] != sagaID || barrier.Facts["step_id"] != stepID || barrier.Facts["claim_fence"] != "9" {
				t.Fatalf("pre-effect barrier identifies a different claim: %+v", barrier)
			}
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestClaimHeartbeatRenewsWorkWhileSagaHookIsPaused(t *testing.T) {
	ticks := make(chan time.Time, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	var renewals atomic.Int64
	var interval atomic.Int64
	done := make(chan error, 1)
	claim := domain.SagaClaim{SagaID: uuid.New(), Token: uuid.New(), Fence: 7}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if !released {
			close(release)
		}
		cancel()
	}()
	go func() {
		executionErr, renewalErr := runClaimWithHeartbeat(ctx, claim, 30*time.Second, 5*time.Second,
			func(d time.Duration) (<-chan time.Time, func()) {
				interval.Store(int64(d))
				return ticks, func() {}
			},
			func(context.Context, domain.SagaClaim, time.Duration) (bool, error) {
				renewals.Add(1)
				return true, nil
			},
			func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		done <- errors.Join(executionErr, renewalErr)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("claim execution did not start")
	}
	ticks <- time.Now()
	deadline := time.Now().Add(time.Second)
	for renewals.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := renewals.Load(); got != 1 {
		t.Fatalf("claim renewals=%d, want one renewal while work is paused", got)
	}
	if got := time.Duration(interval.Load()); got != 10*time.Second {
		t.Fatalf("renewal interval=%s, want one third of the lease", got)
	}
	close(release)
	released = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("claim execution returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("claim execution did not finish")
	}
}

func TestClaimHeartbeatCancelsExecutionWhenOwnershipIsLost(t *testing.T) {
	ticks := make(chan time.Time, 1)
	started := make(chan struct{})
	canceled := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		executionErr, renewalErr := runClaimWithHeartbeat(ctx, domain.SagaClaim{}, 30*time.Second, 5*time.Second,
			func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} },
			func(context.Context, domain.SagaClaim, time.Duration) (bool, error) { return false, nil },
			func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(canceled)
				return ctx.Err()
			})
		done <- errors.Join(executionErr, renewalErr)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("claim execution did not start")
	}
	ticks <- time.Now()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("claim execution continued after its lease was lost")
	}
	select {
	case err := <-done:
		if !errors.Is(err, repository.ErrRecoveryConflict) {
			t.Fatalf("claim heartbeat error=%v, want ownership conflict", err)
		}
	case <-time.After(time.Second):
		t.Fatal("claim heartbeat did not finish")
	}
}
