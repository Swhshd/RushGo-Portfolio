package observability

import (
	"go.opentelemetry.io/otel/baggage"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// GinMiddleware instruments Gin routes using the Runtime providers.
func (r *Runtime) GinMiddleware() gin.HandlerFunc {
	if r == nil {
		return func(c *gin.Context) { c.Next() }
	}
	instrument := otelgin.Middleware(
		"rushgo-gateway",
		otelgin.WithTracerProvider(r.TracerProvider),
		otelgin.WithMeterProvider(r.MeterProvider),
		otelgin.WithPropagators(r.Propagator),
		otelgin.WithFilter(func(req *http.Request) bool { return !strings.HasPrefix(req.URL.Path, "/api/auth/") }),
	)
	return func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/auth/") {
			c.Request = c.Request.WithContext(baggage.ContextWithBaggage(c.Request.Context(), baggage.Baggage{}))
		}
		instrument(c)
	}
}

// HTTPHandler wraps a net/http handler, preserving request context for
// webhook and other non-Gin HTTP boundaries.
func (r *Runtime) HTTPHandler(operation string, next http.Handler) http.Handler {
	if r == nil {
		return next
	}
	return otelhttp.NewHandler(next, operation,
		otelhttp.WithTracerProvider(r.TracerProvider),
		otelhttp.WithMeterProvider(r.MeterProvider),
		otelhttp.WithPropagators(r.Propagator),
	)
}
