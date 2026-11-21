package database

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func databaseTarget(databaseURL string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	if parsed.Host == "" || parsed.Path == "" {
		return "", fmt.Errorf("database URL missing host or database")
	}
	return parsed.Host + parsed.EscapedPath(), nil
}

func NewPostgresPool(ctx context.Context, databaseURL string, logger *zap.Logger) (*pgxpool.Pool, error) {
	target, err := databaseTarget(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("format database target: %w", err)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("Connected to PostgreSQL", zap.String("target", target))
	registerHealthMetric(ctx, pool)
	return pool, nil
}
