package middleware

import (
	"context"
	"github.com/ticketbox/pkg/observability"
	"go.opentelemetry.io/otel/baggage"
	"google.golang.org/grpc/metadata"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func UnaryLoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		auth := observability.IsUserAuthMethod(info.FullMethod)
		if auth {
			ctx = baggage.ContextWithBaggage(ctx, baggage.Baggage{})
			ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
		}
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		if auth {
			logger.Info("Auth RPC", zap.String("method", info.FullMethod), zap.String("status", st.Code().String()))
			return resp, err
		}
		logger.Info("gRPC call",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.String("status", st.Code().String()),
			zap.Any("error", err),
		)

		return resp, err
	}
}
