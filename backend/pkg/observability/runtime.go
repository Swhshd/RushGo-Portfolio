package observability

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Runtime owns the providers and propagator used by one service process.
// Provider interfaces are exposed for instrumentation packages; concrete SDK
// providers are retained privately so Shutdown can flush both pipelines.
type Runtime struct {
	Enabled        bool
	TracerProvider trace.TracerProvider
	MeterProvider  otelmetric.MeterProvider
	Propagator     propagation.TextMapPropagator
	Metrics        *Metrics

	tracerProvider  *sdktrace.TracerProvider
	meterProvider   *metric.MeterProvider
	shutdownTimeout time.Duration
	shutdownOnce    sync.Once
	shutdownErr     error
	shutdownMu      sync.Mutex
}

func Initialize(ctx context.Context, cfg Config, logger *zap.Logger) (*Runtime, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.Environment == "" {
		cfg.Environment = "local"
	}
	if cfg.SamplingRatio == 0 && !cfg.Enabled {
		cfg.SamplingRatio = 1
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	if !cfg.Enabled {
		meterProvider := metric.NewMeterProvider()
		return &Runtime{
			Enabled:         false,
			TracerProvider:  noop.NewTracerProvider(),
			MeterProvider:   meterProvider,
			Propagator:      propagator,
			Metrics:         newMetrics(meterProvider),
			shutdownTimeout: cfg.shutdownTimeout(),
		}, nil
	}

	res := resource.NewWithAttributes(
		"",
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("deployment.environment", cfg.Environment),
	)
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))

	endpoint, insecure, err := exporterEndpoint(cfg.OTLPEndpoint)
	if err != nil {
		return nil, err
	}
	traceOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	if insecure {
		traceOptions = append(traceOptions, otlptracegrpc.WithInsecure())
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	metricOptions := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
	if insecure {
		metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
	if err != nil {
		_ = traceExporter.Shutdown(context.Background())
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	periodicReader := metric.NewPeriodicReader(metricExporter)
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(periodicReader),
	)

	// Explicit providers are passed to instrumentation adapters by Runtime.
	// Setting globals also keeps manually created package-level instrumentation
	// compatible with the repository's existing constructor style.
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagator)

	logger.Debug("OpenTelemetry runtime initialized", zap.String("service_name", cfg.ServiceName))
	metrics := newMetrics(meterProvider)
	return &Runtime{
		Enabled:         true,
		TracerProvider:  tracerProvider,
		MeterProvider:   meterProvider,
		Propagator:      propagator,
		Metrics:         metrics,
		tracerProvider:  tracerProvider,
		meterProvider:   meterProvider,
		shutdownTimeout: cfg.shutdownTimeout(),
	}, nil
}

func newMetrics(provider otelmetric.MeterProvider) *Metrics {
	metrics, err := NewMetrics(provider)
	if err != nil {
		return nil
	}
	return metrics
}

func exporterEndpoint(endpoint string) (string, bool, error) {
	parsedEndpoint := endpoint
	if !strings.Contains(parsedEndpoint, "://") {
		parsedEndpoint = "grpc://" + parsedEndpoint
	}
	parsed, err := url.Parse(parsedEndpoint)
	if err != nil || parsed.Host == "" {
		return "", false, fmt.Errorf("invalid OTLP endpoint")
	}
	return parsed.Host, parsed.Scheme == "grpc" || parsed.Scheme == "http", nil
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, r.shutdownTimeout)
		defer cancel()
		if r.meterProvider != nil {
			r.shutdownErr = r.meterProvider.Shutdown(shutdownCtx)
		}
		if r.tracerProvider != nil {
			traceErr := r.tracerProvider.Shutdown(shutdownCtx)
			if r.shutdownErr == nil {
				r.shutdownErr = traceErr
			}
		}
	})
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	return r.shutdownErr
}

func (r *Runtime) Tracer(name string) trace.Tracer {
	if r == nil || r.TracerProvider == nil {
		return otel.GetTracerProvider().Tracer(name)
	}
	return r.TracerProvider.Tracer(name)
}

func (r *Runtime) Meter(name string) otelmetric.Meter {
	if r == nil || r.MeterProvider == nil {
		return metric.NewMeterProvider().Meter(name)
	}
	return r.MeterProvider.Meter(name)
}
