package main

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ticketbox/pkg/middleware"
	"github.com/ticketbox/pkg/reliabilityhook"
)

func bookingSagaClientDialOptions(hook *reliabilityhook.Controller, options ...grpc.DialOption) []grpc.DialOption {
	result := append([]grpc.DialOption(nil), options...)
	if hook != nil && hook.Enabled() {
		result = append(result, grpc.WithChainUnaryInterceptor(hook.UnaryClientInterceptor()))
	}
	return result
}

func bookingUnaryServerInterceptors(hook *reliabilityhook.Controller, logger *zap.Logger) []grpc.UnaryServerInterceptor {
	interceptors := []grpc.UnaryServerInterceptor{middleware.UnaryLoggingInterceptor(logger)}
	if hook != nil && hook.Enabled() {
		interceptors = append(interceptors, hook.UnaryServerInterceptor())
	}
	return interceptors
}
