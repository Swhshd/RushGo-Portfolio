package config

import (
	"errors"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Release     ReleaseIdentity
	ServiceName string `mapstructure:"SERVICE_NAME"`
	GRPCPort    string `mapstructure:"GRPC_PORT"`
	HTTPPort    string `mapstructure:"HTTP_PORT"`

	DatabaseURL string `mapstructure:"DATABASE_URL"`
	RedisURL    string `mapstructure:"REDIS_URL"`

	KafkaBrokers []string `mapstructure:"KAFKA_BROKERS"`

	JWTSecret          string `mapstructure:"JWT_SECRET"`
	JWTAccessTokenTTL  int    `mapstructure:"JWT_ACCESS_TOKEN_TTL"`
	JWTRefreshTokenTTL int    `mapstructure:"JWT_REFRESH_TOKEN_TTL"`
	SMTPHost           string
	SMTPPort           int
	SMTPTLSMode        string
	SMTPUsername       string
	SMTPPassword       string
	SMTPFrom           string
	AuthFrontendOrigin string
	TrustedProxies     string

	BookingMode       string `mapstructure:"BOOKING_MODE"`
	SeatAdmissionMode string `mapstructure:"SEAT_ADMISSION_MODE"`

	UserServiceAddr          string `mapstructure:"USER_SERVICE_ADDR"`
	EventServiceAddr         string `mapstructure:"EVENT_SERVICE_ADDR"`
	EventTicketAuthorityAddr string `mapstructure:"EVENT_TICKET_AUTHORITY_ADDR"`
	BookingServiceAddr       string `mapstructure:"BOOKING_SERVICE_ADDR"`
	PaymentServiceAddr       string `mapstructure:"PAYMENT_SERVICE_ADDR"`
	SagaServiceAddr          string `mapstructure:"SAGA_SERVICE_ADDR"`
	SagaTicketAuthorityAddr  string `mapstructure:"SAGA_TICKET_AUTHORITY_ADDR"`
	SchedulerServiceAddr     string `mapstructure:"SCHEDULER_SERVICE_ADDR"`

	StripePublishableKey string `mapstructure:"STRIPE_PUBLISHABLE_KEY"`
	StripeSecretKey      string `mapstructure:"STRIPE_SECRET_KEY"`
	StripeSecretWebhook  string `mapstructure:"STRIPE_SECRET_WEBHOOK"`

	OTEL struct {
		Enabled          bool
		ExporterEndpoint string
		ServiceName      string
		Environment      string
		SamplingRatio    float64
		ShutdownTimeout  time.Duration
	}
}

func Load() (*Config, error) {
	// Only local development may inherit a working-directory dotenv file.
	mode := os.Getenv("RUSHGO_ENV")
	if mode == "" || mode == "local" {
		_ = godotenv.Load()
		if loaded := os.Getenv("RUSHGO_ENV"); loaded != "" && loaded != "local" {
			return nil, errors.New("external environment must be explicit before startup, not inherited from dotenv")
		}
	}
	viper := viper.New()

	viper.AutomaticEnv()
	identity, err := loadRelease(viper)
	if err != nil {
		return nil, err
	}

	viper.SetDefault("GRPC_PORT", "50051")
	viper.SetDefault("HTTP_PORT", "8000")
	viper.SetDefault("REDIS_URL", "redis://localhost:6379")
	viper.SetDefault("KAFKA_BROKERS", []string{"localhost:9092"})
	viper.SetDefault("JWT_ACCESS_TOKEN_TTL", 15)
	viper.SetDefault("JWT_REFRESH_TOKEN_TTL", 7)
	viper.SetDefault("BOOKING_MODE", "pessimistic")
	viper.SetDefault("SEAT_ADMISSION_MODE", "POSTGRES_ONLY")
	viper.SetDefault("OTEL_ENABLED", false)
	viper.SetDefault("OTEL_ENVIRONMENT", "local")
	viper.SetDefault("OTEL_TRACES_SAMPLER_RATIO", 1.0)
	viper.SetDefault("OTEL_SHUTDOWN_TIMEOUT", "5s")

	cfg := &Config{}
	cfg.Release = identity
	cfg.ServiceName = viper.GetString("SERVICE_NAME")
	cfg.GRPCPort = viper.GetString("GRPC_PORT")
	cfg.HTTPPort = viper.GetString("HTTP_PORT")
	cfg.DatabaseURL = viper.GetString("DATABASE_URL")
	cfg.RedisURL = viper.GetString("REDIS_URL")
	cfg.KafkaBrokers = viper.GetStringSlice("KAFKA_BROKERS")
	cfg.JWTSecret = viper.GetString("JWT_SECRET")
	cfg.JWTAccessTokenTTL = viper.GetInt("JWT_ACCESS_TOKEN_TTL")
	cfg.JWTRefreshTokenTTL = viper.GetInt("JWT_REFRESH_TOKEN_TTL")
	cfg.SMTPHost = viper.GetString("SMTP_HOST")
	cfg.SMTPPort = viper.GetInt("SMTP_PORT")
	cfg.SMTPTLSMode = viper.GetString("SMTP_TLS_MODE")
	cfg.SMTPUsername = viper.GetString("SMTP_USERNAME")
	cfg.SMTPPassword = viper.GetString("SMTP_PASSWORD")
	cfg.SMTPFrom = viper.GetString("SMTP_FROM")
	cfg.AuthFrontendOrigin = viper.GetString("AUTH_FRONTEND_ORIGIN")
	cfg.TrustedProxies = viper.GetString("TRUSTED_PROXIES")
	cfg.BookingMode = viper.GetString("BOOKING_MODE")
	cfg.SeatAdmissionMode = viper.GetString("SEAT_ADMISSION_MODE")
	cfg.UserServiceAddr = viper.GetString("USER_SERVICE_ADDR")
	cfg.EventServiceAddr = viper.GetString("EVENT_SERVICE_ADDR")
	cfg.EventTicketAuthorityAddr = viper.GetString("EVENT_TICKET_AUTHORITY_ADDR")
	cfg.BookingServiceAddr = viper.GetString("BOOKING_SERVICE_ADDR")
	cfg.PaymentServiceAddr = viper.GetString("PAYMENT_SERVICE_ADDR")
	cfg.StripePublishableKey = viper.GetString("STRIPE_PUBLISHABLE_KEY")
	cfg.StripeSecretKey = viper.GetString("STRIPE_SECRET_KEY")
	cfg.StripeSecretWebhook = viper.GetString("STRIPE_SECRET_WEBHOOK")
	cfg.SagaServiceAddr = viper.GetString("SAGA_SERVICE_ADDR")
	cfg.SagaTicketAuthorityAddr = viper.GetString("SAGA_TICKET_AUTHORITY_ADDR")
	cfg.SchedulerServiceAddr = viper.GetString("SCHEDULER_SERVICE_ADDR")
	cfg.OTEL.Enabled = viper.GetBool("OTEL_ENABLED")
	cfg.OTEL.ExporterEndpoint = viper.GetString("OTEL_EXPORTER_OTLP_ENDPOINT")
	cfg.OTEL.ServiceName = viper.GetString("OTEL_SERVICE_NAME")
	cfg.OTEL.Environment = viper.GetString("OTEL_ENVIRONMENT")
	cfg.OTEL.SamplingRatio = viper.GetFloat64("OTEL_TRACES_SAMPLER_RATIO")
	if timeout := viper.GetString("OTEL_SHUTDOWN_TIMEOUT"); timeout != "" {
		if parsed, parseErr := time.ParseDuration(timeout); parseErr == nil {
			cfg.OTEL.ShutdownTimeout = parsed
		}
	}
	if err := validateExternal(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
