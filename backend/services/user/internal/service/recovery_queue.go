package service

import (
	"context"
	"github.com/ticketbox/user/internal/domain"
	"sync"
	"time"
)

type RecoveryJob struct {
	CanonicalEmail string
	EnqueuedAt     time.Time
}
type RecoveryQueue struct {
	mu              sync.Mutex
	jobs            chan RecoveryJob
	handler         func(context.Context, RecoveryJob) error
	ctx             context.Context
	cancel          context.CancelFunc
	started, closed bool
	done            chan struct{}
	wg              sync.WaitGroup
}

func NewRecoveryQueue(handler func(context.Context, RecoveryJob) error) *RecoveryQueue {
	return &RecoveryQueue{jobs: make(chan RecoveryJob, 128), handler: handler, done: make(chan struct{})}
}
func (q *RecoveryQueue) Start(parent context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.closed {
		return
	}
	q.started = true
	q.ctx, q.cancel = context.WithCancel(parent)
	q.wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer q.wg.Done()
			for {
				select {
				case <-q.ctx.Done():
					return
				case job := <-q.jobs:
					if q.ctx.Err() != nil {
						return
					}
					ctx, cancel := context.WithTimeout(q.ctx, 15*time.Second)
					_ = q.handler(ctx, job)
					cancel()
				}
			}
		}()
	}
	go func() {
		q.wg.Wait()
		q.mu.Lock()
		q.closed = true
		for len(q.jobs) > 0 {
			<-q.jobs
		}
		q.mu.Unlock()
		close(q.done)
	}()
}
func (q *RecoveryQueue) Enqueue(job RecoveryJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.started || q.closed || q.ctx.Err() != nil {
		return domain.ErrTemporaryUnavailable
	}
	select {
	case q.jobs <- job:
		return nil
	default:
		return domain.ErrTemporaryUnavailable
	}
}
func (q *RecoveryQueue) Shutdown(parent context.Context) error {
	q.mu.Lock()
	q.closed = true
	if !q.started {
		q.mu.Unlock()
		return nil
	}
	q.cancel()
	q.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return domain.ErrTemporaryUnavailable
	}
}
