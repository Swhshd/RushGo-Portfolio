package observability

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// GRPCServerOptions returns options that instrument unary and streaming RPCs.
func (r *Runtime) GRPCServerOptions() []grpc.ServerOption {
	if r == nil {
		return nil
	}
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithTracerProvider(r.TracerProvider),
			otelgrpc.WithMeterProvider(r.MeterProvider),
			otelgrpc.WithPropagators(r.Propagator),
		)),
	}
}

// GRPCDialOptions returns options that instrument unary and streaming RPCs.
func (r *Runtime) GRPCDialOptions() []grpc.DialOption {
	if r == nil {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(r.TracerProvider),
			otelgrpc.WithMeterProvider(r.MeterProvider),
			otelgrpc.WithPropagators(r.Propagator),
		)),
	}
}
