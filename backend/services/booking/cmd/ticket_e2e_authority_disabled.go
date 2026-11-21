//go:build !ticket_e2e

package main

import (
	"context"

	"github.com/ticketbox/pkg/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// The normal Booking binary has no simulated Payment authority implementation.
func validateTicketE2EStartup(*config.Config) error { return nil }

func registerTicketE2ESimulatedAuthority(context.Context, *grpc.Server, *config.Config, *zap.Logger) (func(), error) {
	return func() {}, nil
}
