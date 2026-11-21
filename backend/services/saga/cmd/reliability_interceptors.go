package main

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ticketbox/pkg/grpcsecurity"
	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/reliabilityhook"
)

func sagaLegacyUnaryServerInterceptors(hook *reliabilityhook.Controller, logger *zap.Logger) []grpc.UnaryServerInterceptor {
	interceptors := []grpc.UnaryServerInterceptor{
		middleware.UnaryLoggingInterceptor(logger),
		grpcsecurity.RejectTicketAuthorityMethodsOnLegacyListener(),
	}
	if hook != nil && hook.Enabled() {
		interceptors = append(interceptors, hook.SagaStartRequestObserver())
	}
	return interceptors
}
