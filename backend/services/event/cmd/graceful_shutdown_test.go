package main

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestShouldLogGRPCServeFailureTreatsStoppedBeforeServeAsExpected(t *testing.T) {
	server := grpc.NewServer()
	server.GracefulStop()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serveErr := server.Serve(listener)
	if !errors.Is(serveErr, grpc.ErrServerStopped) {
		t.Fatalf("Serve error = %v, want grpc.ErrServerStopped", serveErr)
	}
	if shouldLogGRPCServeFailure(serveErr) {
		t.Fatal("a server stopped before Serve was treated as an unexpected failure")
	}
	if !shouldLogGRPCServeFailure(errors.New("listener failed")) {
		t.Fatal("an unexpected Serve error was not treated as a failure")
	}
}

type eventShutdownFixture struct {
	started      chan struct{}
	release      chan struct{}
	gracefulDone chan struct{}
	once         sync.Once
	stops        atomic.Int32
}

func newEventShutdownFixture() *eventShutdownFixture {
	return &eventShutdownFixture{
		started:      make(chan struct{}),
		release:      make(chan struct{}),
		gracefulDone: make(chan struct{}),
	}
}

func (s *eventShutdownFixture) GracefulStop() {
	close(s.started)
	<-s.release
	close(s.gracefulDone)
}

func (s *eventShutdownFixture) Stop() {
	s.stops.Add(1)
	s.once.Do(func() { close(s.release) })
}

func TestStopGRPCServersRunsGracefullyInParallelAndForcesAtDeadline(t *testing.T) {
	first, second := newEventShutdownFixture(), newEventShutdownFixture()
	done := make(chan struct{})
	go func() {
		stopGRPCServers(25*time.Millisecond, first, second)
		close(done)
	}()
	for name, server := range map[string]*eventShutdownFixture{"first": first, "second": second} {
		select {
		case <-server.started:
		case <-time.After(time.Second):
			t.Fatalf("%s graceful stop did not start", name)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bounded shutdown did not return after forcing blocked servers")
	}
	if first.stops.Load() != 1 || second.stops.Load() != 1 {
		t.Fatalf("forced stop calls = (%d,%d), want (1,1)", first.stops.Load(), second.stops.Load())
	}
}

func TestStopGRPCServersWaitsForAuthorityServeLoop(t *testing.T) {
	server := newEventShutdownFixture()
	serveDone := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		stopGRPCServersAndWaitForServe(time.Second, serveDone, server)
		close(shutdownDone)
	}()
	select {
	case <-server.started:
	case <-time.After(time.Second):
		t.Fatal("graceful stop did not start")
	}
	server.once.Do(func() { close(server.release) })
	select {
	case <-server.gracefulDone:
	case <-time.After(time.Second):
		t.Fatal("graceful stop did not finish")
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the authority Serve loop exited")
	case <-time.After(25 * time.Millisecond):
	}

	close(serveDone)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after the authority Serve loop exited")
	}
}
