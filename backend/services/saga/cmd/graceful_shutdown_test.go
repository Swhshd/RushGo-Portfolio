package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type blockingGracefulStopper struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingGracefulStopper) GracefulStop() {
	close(s.entered)
	<-s.release
}

func TestGracefulStopAsyncDoesNotBlockShutdownCoordinator(t *testing.T) {
	server := &blockingGracefulStopper{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := gracefulStopAsync(server)

	select {
	case <-server.entered:
	case <-time.After(time.Second):
		t.Fatal("graceful stop was not started")
	}
	select {
	case <-done:
		t.Fatal("graceful stop completed while the fake server remained blocked")
	default:
	}

	close(server.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("graceful-stop completion was not reported")
	}
}

func TestShouldLogGRPCServeFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "normal server stop", err: grpc.ErrServerStopped, want: false},
		{name: "wrapped normal server stop", err: fmt.Errorf("serve returned: %w", grpc.ErrServerStopped), want: false},
		{name: "unexpected failure", err: errors.New("listener failed"), want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldLogGRPCServeFailure(test.err); got != test.want {
				t.Fatalf("shouldLogGRPCServeFailure(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}
