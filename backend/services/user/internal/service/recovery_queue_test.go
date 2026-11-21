package service

import (
	"context"
	"errors"
	"github.com/ticketbox/user/internal/domain"
	"sync"
	"testing"
	"time"
)

func TestRecoveryBoundedAdmissionAndCancellation(t *testing.T) {
	started := make(chan struct{}, 2)
	q := NewRecoveryQueue(func(ctx context.Context, _ RecoveryJob) error { started <- struct{}{}; <-ctx.Done(); return ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)
	for i := 0; i < 2; i++ {
		if q.Enqueue(RecoveryJob{CanonicalEmail: "synthetic@example.com"}) != nil {
			t.Fatal("worker admission failed")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two workers not started")
		}
	}
	for i := 0; i < 128; i++ {
		if q.Enqueue(RecoveryJob{CanonicalEmail: "synthetic@example.com"}) != nil {
			t.Fatal("bounded capacity too small")
		}
	}
	if !errors.Is(q.Enqueue(RecoveryJob{}), domain.ErrTemporaryUnavailable) {
		t.Fatal("full queue admitted")
	}
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if q.Shutdown(shutdown) != nil {
		t.Fatal("shutdown did not cancel workers")
	}
	if q.Enqueue(RecoveryJob{}) == nil {
		t.Fatal("shutdown admitted job")
	}
}
func TestRecoveryConcurrentShutdownAdmission(t *testing.T) {
	q := NewRecoveryQueue(func(context.Context, RecoveryJob) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = q.Enqueue(RecoveryJob{})
			}
		}()
	}
	cancel()
	if q.Shutdown(context.Background()) != nil {
		t.Fatal("shutdown failed")
	}
	wg.Wait()
	if q.Enqueue(RecoveryJob{}) == nil {
		t.Fatal("closed queue accepted")
	}
}
