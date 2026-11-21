package observability

import (
	"context"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

type authStatsKey struct{}
type authStats struct {
	normal, auth stats.Handler
	client       bool
}

func (h *authStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	selected := h.normal
	if IsUserAuthMethod(info.FullMethodName) {
		selected = h.auth
		ctx = baggage.ContextWithBaggage(ctx, baggage.Baggage{})
		if h.client {
			ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
		}
	}
	ctx = selected.TagRPC(ctx, info)
	return context.WithValue(ctx, authStatsKey{}, selected)
}
func (h *authStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if selected, ok := ctx.Value(authStatsKey{}).(stats.Handler); ok {
		selected.HandleRPC(ctx, s)
	}
}
func (h *authStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return h.normal.TagConn(ctx, info)
}
func (h *authStats) HandleConn(ctx context.Context, s stats.ConnStats) { h.normal.HandleConn(ctx, s) }
func (r *Runtime) userAuthHandler(client bool) stats.Handler {
	opts := []otelgrpc.Option{otelgrpc.WithTracerProvider(r.TracerProvider), otelgrpc.WithMeterProvider(r.MeterProvider), otelgrpc.WithPropagators(r.Propagator)}
	authOpts := []otelgrpc.Option{otelgrpc.WithTracerProvider(r.TracerProvider), otelgrpc.WithMeterProvider(r.MeterProvider), otelgrpc.WithPropagators(propagation.TraceContext{}), otelgrpc.WithFilter(func(*stats.RPCTagInfo) bool { return false })}
	if client {
		return &authStats{otelgrpc.NewClientHandler(opts...), otelgrpc.NewClientHandler(authOpts...), true}
	}
	return &authStats{otelgrpc.NewServerHandler(opts...), otelgrpc.NewServerHandler(authOpts...), false}
}
func (r *Runtime) UserAuthGRPCServerOptions() []grpc.ServerOption {
	if r == nil {
		return nil
	}
	return []grpc.ServerOption{grpc.StatsHandler(r.userAuthHandler(false))}
}
func (r *Runtime) UserAuthGRPCDialOptions() []grpc.DialOption {
	if r == nil {
		return nil
	}
	return []grpc.DialOption{grpc.WithStatsHandler(r.userAuthHandler(true))}
}
