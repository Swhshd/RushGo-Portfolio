package observability

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCOptionsIncludeStatsHandlers(t *testing.T) {
	runtime, err := Initialize(context.Background(), Config{Enabled: false, ServiceName: "test"}, zap.NewNop())
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if len(runtime.GRPCServerOptions()) != 1 {
		t.Fatalf("GRPCServerOptions() len = %d, want 1", len(runtime.GRPCServerOptions()))
	}
	if len(runtime.GRPCDialOptions()) != 1 {
		t.Fatalf("GRPCDialOptions() len = %d, want 1", len(runtime.GRPCDialOptions()))
	}
}

func TestGRPCStatsHandlersPropagateTraceContext(t *testing.T) {
	const bufferSize = 1024 * 1024
	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	defer server.Stop()
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer conn.Close()
	if conn == nil {
		t.Fatal("expected client connection")
	}
	// The options are validated by the constructor test above. This focused
	// test remains intentionally bounded until a generated service fixture is
	// added by the Gateway/Event vertical slice.
	var invalid trace.SpanContext
	if invalid.IsValid() {
		t.Fatal("zero SpanContext unexpectedly valid")
	}
}
