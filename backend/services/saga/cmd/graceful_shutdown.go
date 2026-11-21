package main

import (
	"errors"

	"google.golang.org/grpc"
)

type gracefulStopper interface {
	GracefulStop()
}

func shouldLogGRPCServeFailure(err error) bool {
	return err != nil && !errors.Is(err, grpc.ErrServerStopped)
}

func gracefulStopAsync(server gracefulStopper) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	return done
}
