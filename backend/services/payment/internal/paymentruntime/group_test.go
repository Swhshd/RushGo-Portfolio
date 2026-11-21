package paymentruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStopAndWaitJoinsTaskFinalization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	finalizing := make(chan struct{})
	release := make(chan struct{})
	group := Start(ctx, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(finalizing)
		<-release
		return ctx.Err()
	})
	<-started

	stopDone := make(chan error, 1)
	go func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		stopDone <- group.StopAndWait(stopCtx)
	}()
	<-finalizing

	select {
	case err := <-stopDone:
		t.Fatalf("StopAndWait returned before task finalization completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopAndWait returned unexpected cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopAndWait did not finish after task finalization")
	}
}

func TestTaskFailureCancelsSiblingsAndIsReturned(t *testing.T) {
	wantErr := errors.New("fixture task failed")
	siblingCanceled := make(chan struct{})
	group := Start(context.Background(),
		func(context.Context) error { return wantErr },
		func(ctx context.Context) error {
			<-ctx.Done()
			close(siblingCanceled)
			return ctx.Err()
		},
	)

	select {
	case <-siblingCanceled:
	case <-time.After(time.Second):
		t.Fatal("task failure did not cancel sibling")
	}
	<-group.Done()
	if err := group.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait error = %v; want %v", err, wantErr)
	}
}

func TestStopAndWaitTimeoutDoesNotForgeCompletion(t *testing.T) {
	started := make(chan struct{})
	finalizing := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
	group := Start(context.Background(), func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(finalizing)
		<-release
		return ctx.Err()
	})
	t.Cleanup(func() {
		releaseTask()
		<-group.Done()
	})
	<-started

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := group.StopAndWait(waitCtx)
	if !errors.Is(err, ErrTasksStillRunning) {
		t.Fatalf("StopAndWait timeout error=%v; want ErrTasksStillRunning", err)
	}
	<-finalizing
	select {
	case <-group.Done():
		t.Fatal("timeout claimed task completion before finalization returned")
	default:
	}

	releaseTask()
	select {
	case <-group.Done():
	case <-time.After(time.Second):
		t.Fatal("task did not finish after release")
	}
	if err := group.Err(); err != nil {
		t.Fatalf("completed task group returned unexpected error: %v", err)
	}
}
