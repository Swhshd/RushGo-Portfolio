package main

import (
	"errors"
	"time"

	"google.golang.org/grpc"
)

func shouldLogGRPCServeFailure(err error) bool {
	return err != nil && !errors.Is(err, grpc.ErrServerStopped)
}

type eventGRPCStopper interface {
	GracefulStop()
	Stop()
}

func stopGRPCServers(timeout time.Duration, servers ...eventGRPCStopper) {
	if len(servers) == 0 {
		return
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	done := make(chan int, len(servers))
	for i, server := range servers {
		go func(index int, value eventGRPCStopper) {
			value.GracefulStop()
			done <- index
		}(i, server)
	}

	completed := make([]bool, len(servers))
	remaining := len(servers)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for remaining > 0 {
		select {
		case index := <-done:
			if !completed[index] {
				completed[index] = true
				remaining--
			}
		case <-timer.C:
			for i, server := range servers {
				if !completed[i] {
					server.Stop()
				}
			}
			return
		}
	}
}

func stopGRPCServersAndWaitForServe(timeout time.Duration, serveDone <-chan struct{}, servers ...eventGRPCStopper) {
	stopGRPCServers(timeout, servers...)
	<-serveDone
}
