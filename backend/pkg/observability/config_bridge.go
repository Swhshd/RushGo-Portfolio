package observability

import (
	"context"

	"github.com/ticketbox/pkg/config"
	"go.uber.org/zap"
)

// InitializeFromConfig keeps service startup wiring consistent while allowing
// each process to supply its own service name.
func InitializeFromConfig(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Runtime, error) {
	if cfg.Release.External() && logger != nil {
		logger.Info("Release identity", zap.String("sourceCommit", cfg.Release.SourceCommit), zap.String("buildIdentity", cfg.Release.BuildIdentity), zap.String("configSchemaVersion", cfg.Release.ConfigSchemaVersion), zap.String("migrationVersion", cfg.Release.MigrationVersion), zap.String("environment", cfg.Release.Environment), zap.String("deploymentIdentity", cfg.Release.DeploymentIdentity), zap.String("providerMode", cfg.Release.ProviderMode))
	}
	serviceName := cfg.OTEL.ServiceName
	if serviceName == "" {
		serviceName = cfg.ServiceName
	}
	return Initialize(ctx, Config{
		Enabled:         cfg.OTEL.Enabled,
		ServiceName:     serviceName,
		Environment:     cfg.OTEL.Environment,
		OTLPEndpoint:    cfg.OTEL.ExporterEndpoint,
		SamplingRatio:   cfg.OTEL.SamplingRatio,
		ShutdownTimeout: cfg.OTEL.ShutdownTimeout,
	}, logger)
}
