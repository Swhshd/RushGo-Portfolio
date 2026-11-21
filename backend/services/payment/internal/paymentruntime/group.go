package paymentruntime

import (
	"context"
	"errors"
	"sync"
)

// ErrTasksStillRunning indicates that a bounded stop deadline expired before
// all background tasks returned.
var ErrTasksStillRunning = errors.New("payment runtime tasks are still running")

// Group runs a set of context-aware background tasks and waits for each task
// to finish before its caller releases resources those tasks may still use.
type Group struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	errs []error
}

// Start runs each task with a shared child context. A task error cancels its
// siblings; expected context errors during cancellation are not returned.
func Start(parent context.Context, tasks ...func(context.Context) error) *Group {
	taskCtx, cancel := context.WithCancel(parent)
	group := &Group{cancel: cancel, done: make(chan struct{})}

	var workers sync.WaitGroup
	for _, task := range tasks {
		if task == nil {
			group.addError(errors.New("payment runtime task is nil"))
			cancel()
			continue
		}
		workers.Add(1)
		go func(run func(context.Context) error) {
			defer workers.Done()
			if err := run(taskCtx); err != nil {
				if taskCtx.Err() != nil && errors.Is(err, taskCtx.Err()) {
					return
				}
				group.addError(err)
				cancel()
			}
		}(task)
	}

	go func() {
		workers.Wait()
		close(group.done)
	}()
	return group
}

// Done closes when every task has returned.
func (g *Group) Done() <-chan struct{} { return g.done }

// Err returns joined task errors after Done has closed. It reports
// ErrTasksStillRunning if any task is still active.
func (g *Group) Err() error {
	select {
	case <-g.done:
	default:
		return ErrTasksStillRunning
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return errors.Join(g.errs...)
}

// StopAndWait cancels every task and waits for its in-flight work and
// finalization to finish, unless waitCtx expires first. A timeout does not
// signal task completion; callers must keep task dependencies alive or exit
// the process without running dependency cleanup.
func (g *Group) StopAndWait(waitCtx context.Context) error {
	g.cancel()
	select {
	case <-g.done:
		return g.Err()
	case <-waitCtx.Done():
		select {
		case <-g.done:
			return g.Err()
		default:
		}
		return errors.Join(ErrTasksStillRunning, waitCtx.Err())
	}
}

func (g *Group) addError(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.errs = append(g.errs, err)
}
