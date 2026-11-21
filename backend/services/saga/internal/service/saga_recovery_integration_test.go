package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"google.golang.org/grpc"
)

func runRecovery(t *testing.T, s *SagaService, ctx context.Context, limit int) (int, error) {
	t.Helper()
	return s.RunRecoveryOnce(ctx, limit)
}

type afterBookBarrier struct {
	eventv1.EventServiceClient
	entered, release, canceled chan struct{}
	first                      bool
}

func (e *afterBookBarrier) ApplySeatLifecycle(ctx context.Context, req *eventv1.SeatLifecycleRequest, opts ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	result, err := e.EventServiceClient.ApplySeatLifecycle(ctx, req, opts...)
	if req.Action == "book" && e.first {
		e.first = false
		close(e.entered)
		select {
		case <-e.release:
		case <-ctx.Done():
			if e.canceled != nil {
				select {
				case e.canceled <- struct{}{}:
				default:
				}
			}
			return nil, ctx.Err()
		}
	}
	return result, err
}
func awaitRecoverySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery barrier not reached")
	}
}

func TestExpiredLeaseAfterEffectBeforeResultIntegration(t *testing.T) {
	s, r, db, v, b, _, e := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "CONFIRMING"
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=2,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	original, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
	if err != nil || original == nil {
		t.Fatal(err)
	}
	barrier := &afterBookBarrier{EventServiceClient: e, entered: make(chan struct{}), release: make(chan struct{}), first: true}
	s.eventClient = barrier
	oldDone := make(chan error, 1)
	go func() { oldDone <- s.ExecuteClaim(ctx, *original) }()
	awaitRecoverySignal(t, barrier.entered)
	if _, err := db.Exec(ctx, `UPDATE sagas SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	restarted := NewSagaService(s.logger, r, s.sagaStepRegistry, s.bookingClient, s.paymentClient, e)
	_, err = runRecovery(t, restarted, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	close(barrier.release)
	oldErr := <-oldDone
	if got.Status != domain.SAGA_COMPLETED {
		t.Fatal("PROCESSING after effect not recovered by due scanner")
	}
	if !errors.Is(oldErr, repository.ErrRecoveryConflict) {
		t.Fatal("expired worker retained result ownership", oldErr)
	}
	final, err := r.GetSagaById(ctx, v.ID)
	if err != nil || final.Status != domain.SAGA_COMPLETED || final.CurrentStepIndex != 5 {
		t.Fatal("stale result regressed recovered graph", err)
	}
	if e.book != 2 || e.account != 1 {
		t.Fatal("original effect was not replayed before one accounting")
	}
	for i, st := range final.Steps {
		if st.ID != v.Steps[i].ID {
			t.Fatal("restart allocated new step")
		}
	}
}

func TestSagaRecoveryKeepsFinancialWaitDiscoverableIntegration(t *testing.T) {
	s, r, db, v, b, p, e := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "FAILED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "fail")
	if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=1,direction='compensate',status='ROLLING_BACK' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := runRecovery(t, s, ctx, 1); err != nil || n != 1 {
		t.Fatal("financial wait run", n, err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.resolutions != 1 || got.Status != domain.SAGA_ROLLING_BACK || got.NextAttemptAt == nil || got.ErrorCategory != "financial_wait" {
		t.Fatal("refund wait disappeared from restart recovery")
	}
	p.entry.RefundState, p.entry.RefundAmountCents = "succeeded", 725
	if _, err := db.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	restarted := NewSagaService(s.logger, r, s.sagaStepRegistry, b, p, e)
	if n, err := runRecovery(t, restarted, ctx, 1); err != nil || n != 1 {
		t.Fatal("restarted financial wait", n, err)
	}
	got, err = r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_ROLLED_BACK || e.close != 1 {
		t.Fatal("verified refund was not rediscovered on restart", err)
	}
}

type recoveryTickRepo struct {
	repository.RecoveryRepository
	renews atomic.Int64
	signal chan struct{}
	fail   bool
}

func (r *recoveryTickRepo) RenewClaim(ctx context.Context, c domain.SagaClaim, ttl time.Duration) (bool, error) {
	if r.renews.Add(1) > 1 {
		if r.signal != nil {
			select {
			case r.signal <- struct{}{}:
			default:
			}
		}
		if r.fail {
			return false, nil
		}
	}
	return r.RecoveryRepository.RenewClaim(ctx, c, ttl)
}

func TestSagaRecoveryStartupPeriodicRenewalAndShutdownIntegration(t *testing.T) {
	for _, scenario := range []string{"renew", "renewal_lost", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			s, r, db, v, b, _, e := storedReplayFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.view.Phase = "CONFIRMING"
			if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=2,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			barrier := &afterBookBarrier{EventServiceClient: e, entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{}, 1), first: true}
			s.eventClient = barrier
			rp := &recoveryTickRepo{RecoveryRepository: r, signal: make(chan struct{}, 1), fail: scenario == "renewal_lost"}
			worker, err := NewRecoveryWorker(rp, s, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			// A manual tick tests ordering without sleeping for a lease interval.
			renewTicks := make(chan time.Time, 1)
			scanTicks := make(chan time.Time, 1)
			worker.ticker = func(interval time.Duration) (<-chan time.Time, func()) {
				if interval == worker.config.ScanInterval {
					return scanTicks, func() {}
				}
				return renewTicks, func() {}
			}
			done := make(chan error, 1)
			go func() { done <- worker.Start(ctx) }()
			awaitRecoverySignal(t, barrier.entered)
			if scenario == "shutdown" {
				cancel()
			} else {
				renewTicks <- time.Now()
				awaitRecoverySignal(t, rp.signal)
				if scenario == "renew" {
					close(barrier.release)
				}
			}
			if scenario != "renew" {
				started := time.Now()
				if scenario == "renewal_lost" || scenario == "shutdown" {
					select {
					case <-barrier.canceled:
					case <-time.After(time.Second):
						t.Fatal("cancellation did not reach the blocked downstream RPC")
					}
				}
				if scenario == "renewal_lost" {
					cancel()
				}
				select {
				case <-done:
					if time.Since(started) > time.Second {
						t.Fatal("worker did not stop promptly after cancellation")
					}
				case <-time.After(time.Second):
					t.Fatal("shutdown failed to stop RPC")
				}
			}
			if scenario == "renew" {
				// Startup executes immediately, then a distinct tick must run a due scan.
				for attempt := 0; attempt < 100; attempt++ {
					got, err := r.GetSagaById(ctx, v.ID)
					if err != nil {
						t.Fatal(err)
					}
					if got.Status == domain.SAGA_COMPLETED {
						break
					}
					if attempt == 99 {
						t.Fatal("renewed claim did not complete")
					}
				}
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("worker shutdown stuck")
				}
			} else {
				got, err := r.GetSagaById(context.Background(), v.ID)
				if err != nil || got.Status == domain.SAGA_COMPLETED || got.CurrentStepIndex != 2 {
					t.Fatal("cancelled worker advanced uncertain effect", err)
				}
			}
		})
	}
}
