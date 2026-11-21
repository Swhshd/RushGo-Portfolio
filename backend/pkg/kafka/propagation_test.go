package kafka

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectTraceContextPreservesExistingKafkaHeaders(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("kafka-test")
	ctx, span := tracer.Start(context.Background(), "publish")
	defer span.End()

	headers := []kafka.Header{{Key: "x-existing", Value: []byte("keep")}}
	result := injectTraceContext(ctx, headers, propagation.TraceContext{})

	if len(result) != 2 {
		t.Fatalf("header count = %d, want 2", len(result))
	}
	if string(result[0].Value) != "keep" {
		t.Fatalf("existing header changed to %q", result[0].Value)
	}
	carrier := kafkaHeaders(result)
	extracted := propagation.TraceContext{}.Extract(context.Background(), carrier)
	if got := trace.SpanContextFromContext(extracted).TraceID(); got != span.SpanContext().TraceID() {
		t.Fatalf("extracted trace ID = %s, want %s", got, span.SpanContext().TraceID())
	}
}

func TestExtractTraceContextWithoutHeadersStartsWithOriginalContext(t *testing.T) {
	base := context.WithValue(context.Background(), struct{}{}, "preserved")
	got := extractTraceContext(base, nil, otel.GetTextMapPropagator())
	if got.Value(struct{}{}) != "preserved" {
		t.Fatal("context without headers was replaced")
	}
	if trace.SpanContextFromContext(got).IsValid() {
		t.Fatal("missing headers unexpectedly produced a valid span context")
	}
}
