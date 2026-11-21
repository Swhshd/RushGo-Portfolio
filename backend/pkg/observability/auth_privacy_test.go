package observability

import (
	"context"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"net/http/httptest"
	"testing"
	"time"
)

func privacyRuntime() (*Runtime, *tracetest.InMemoryExporter, *sdkmetric.ManualReader) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, _ := NewMetrics(mp)
	return &Runtime{TracerProvider: tp, MeterProvider: mp, Propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}), Metrics: metrics}, exporter, reader
}
func TestPrivacyGinAuthExcluded(t *testing.T) {
	r, exporter, _ := privacyRuntime()
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(r.GinMiddleware())
	engine.POST("/api/auth/reset-password", func(c *gin.Context) { c.Status(400) })
	for _, path := range []string{"/api/auth/reset-password?token=synthetic-canary", "/api/auth/unmatched?email=synthetic-canary"} {
		req := httptest.NewRequest("POST", path, nil)
		req.Header.Set("Baggage", "secret=synthetic-canary")
		engine.ServeHTTP(httptest.NewRecorder(), req)
	}
	if len(exporter.GetSpans()) != 0 {
		t.Fatal("automatic auth HTTP span captured identity/query")
	}
}
func TestPrivacyGRPCAuthExtraction(t *testing.T) {
	r, exporter, _ := privacyRuntime()
	member, _ := baggage.NewMember("secret", "synthetic-canary")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("baggage", "secret=synthetic-canary"))
	handler := r.userAuthHandler(false)
	tagged := handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/user.v1.UserService/ResetPassword"})
	handler.HandleRPC(tagged, &stats.End{BeginTime: time.Now(), EndTime: time.Now()})
	if baggage.FromContext(tagged).Len() != 0 || len(exporter.GetSpans()) != 0 {
		t.Fatal("automatic auth RPC captured caller baggage/span")
	}
}
func TestAuthMetricsBoundedResults(t *testing.T) {
	r, _, reader := privacyRuntime()
	RecordAuth(r.Metrics, "send", "synthetic-canary")
	RecordAuth(r.Metrics, "verify", "success")
	RecordAuth(r.Metrics, "request", "invalid")
	RecordAuth(r.Metrics, "complete", "expired")
	var data metricdata.ResourceMetrics
	if reader.Collect(context.Background(), &data) != nil {
		t.Fatal("metric capture failed")
	}
	count := 0
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if sum, ok := metric.Data.(metricdata.Sum[int64]); ok {
				count++
				for _, point := range sum.DataPoints {
					for _, attr := range point.Attributes.ToSlice() {
						if string(attr.Key) != "result" || attr.Value.AsString() == "synthetic-canary" {
							t.Fatal("unbounded auth metric")
						}
					}
				}
			}
		}
	}
	if count != 4 {
		t.Fatal("approved counters absent")
	}
}
