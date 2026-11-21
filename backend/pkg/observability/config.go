package observability

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Config controls the local, best-effort telemetry runtime.
type Config struct {
	Enabled         bool
	ServiceName     string
	Environment     string
	OTLPEndpoint    string
	SamplingRatio   float64
	ShutdownTimeout time.Duration
}

func (c Config) validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("service name is required when observability is enabled")
	}
	if c.SamplingRatio < 0 || c.SamplingRatio > 1 {
		return fmt.Errorf("sampling ratio must be between 0 and 1")
	}
	if strings.TrimSpace(c.OTLPEndpoint) == "" {
		return fmt.Errorf("OTLP endpoint is required when observability is enabled")
	}
	endpoint := c.OTLPEndpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "grpc://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("invalid OTLP endpoint")
	}
	return nil
}

func (c Config) shutdownTimeout() time.Duration {
	if c.ShutdownTimeout <= 0 {
		return 5 * time.Second
	}
	return c.ShutdownTimeout
}
