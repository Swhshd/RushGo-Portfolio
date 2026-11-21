package observability

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func TestInitializeDisabledUsesSafeNoopRuntime(t *testing.T) {
	runtime, err := Initialize(context.Background(), Config{
		Enabled:     false,
		ServiceName: "test-service",
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if runtime.Enabled {
		t.Fatal("disabled runtime reported Enabled=true")
	}
	_, span := runtime.Tracer("test").Start(context.Background(), "disabled")
	if span.SpanContext().IsValid() {
		t.Fatal("disabled tracer created a valid span context")
	}
	span.End()
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestInitializeRejectsInvalidSamplingRatio(t *testing.T) {
	_, err := Initialize(context.Background(), Config{
		Enabled:         true,
		ServiceName:     "test-service",
		OTLPEndpoint:    "localhost:4317",
		SamplingRatio:   1.1,
		ShutdownTimeout: time.Second,
	}, zap.NewNop())
	if err == nil {
		t.Fatal("Initialize() unexpectedly accepted sampling ratio > 1")
	}
}

func TestInitializeEnabledProvidesResourceAndConfigurableSampling(t *testing.T) {
	runtime, err := Initialize(context.Background(), Config{
		Enabled:         true,
		ServiceName:     "test-service",
		Environment:     "test",
		OTLPEndpoint:    "localhost:4317",
		SamplingRatio:   0,
		ShutdownTimeout: time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer runtime.Shutdown(context.Background())
	if !runtime.Enabled {
		t.Fatal("enabled runtime reported Enabled=false")
	}
	spanCtx, span := runtime.Tracer("test").Start(context.Background(), "resource-check")
	span.SetAttributes(attribute.String("rushgo.result", "success"))
	span.End()
	if trace.SpanContextFromContext(spanCtx).IsSampled() {
		t.Fatal("ratio=0 unexpectedly sampled span")
	}
}
