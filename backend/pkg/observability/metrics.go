package observability

import (
	"go.opentelemetry.io/otel/metric"
)

// Metrics is the bounded RushGo domain-metric surface. Instrument names are
// OTel-native; exporters decide the Prometheus spelling.
type Metrics struct {
	AuthVerificationSend          metric.Int64Counter
	AuthVerificationVerify        metric.Int64Counter
	AuthResetRequest              metric.Int64Counter
	AuthResetComplete             metric.Int64Counter
	SeatAdmissionAttempts         metric.Int64Counter
	SeatAdmissionFallbacks        metric.Int64Counter
	SeatAdmissionDuration         metric.Float64Histogram
	AdmissionRepair               metric.Int64Counter
	AdmissionRepairDuration       metric.Float64Histogram
	AdmissionRepairBacklog        metric.Int64Gauge
	AdmissionRepairCycleAge       metric.Float64Gauge
	SeatPostgresAttempts          metric.Int64Counter
	OutboxRelayAttempts           metric.Int64Counter
	OutboxPublish                 metric.Int64Counter
	OutboxRetries                 metric.Int64Counter
	OutboxPending                 metric.Int64Gauge
	OutboxDeliveryDelay           metric.Float64Histogram
	NotificationProcessed         metric.Int64Counter
	NotificationFailures          metric.Int64Counter
	NotificationDeliveryPending   metric.Int64Gauge
	NotificationDeliveryOldestAge metric.Float64Gauge
	SSEActiveConnections          metric.Int64UpDownCounter
	SSEConnections                metric.Int64Counter
	RealtimePublish               metric.Int64Counter
	RealtimeReceived              metric.Int64Counter
	SSEFanoutDeliveries           metric.Int64Counter
	SSEFanoutDuration             metric.Float64Histogram
	SubscriptionGenerationEvent   metric.Int64Counter
	SchedulerRuns                 metric.Int64Counter
	SchedulerExpiredBookings      metric.Int64Counter
	SchedulerDuration             metric.Float64Histogram
	SagaTransitions               metric.Int64Counter
	SagaRecoveryAttempts          metric.Int64Counter
	SagaRecoveryDue               metric.Int64Gauge
	SagaRecoveryAttention         metric.Int64Gauge
	SagaRecoveryOldestAge         metric.Float64Gauge
	SagaRecoveryReclaims          metric.Int64Counter
	PaymentWebhookEvents          metric.Int64Counter
	PaymentFinancialAttempts      metric.Int64Counter
	PaymentFinancialDue           metric.Int64Gauge
	PaymentFinancialAttention     metric.Int64Gauge
	PaymentFinancialOldestAge     metric.Float64Gauge
	KafkaPublishes                metric.Int64Counter
	KafkaProcesses                metric.Int64Counter
}

func NewMetrics(provider metric.MeterProvider) (*Metrics, error) {
	if provider == nil {
		return nil, nil
	}
	meter := provider.Meter("github.com/ticketbox/rushgo/observability")
	var err error
	metrics := &Metrics{}
	if metrics.AuthVerificationSend, err = meter.Int64Counter("rushgo.auth.email_verification.send"); err != nil {
		return nil, err
	}
	if metrics.AuthVerificationVerify, err = meter.Int64Counter("rushgo.auth.email_verification.verify"); err != nil {
		return nil, err
	}
	if metrics.AuthResetRequest, err = meter.Int64Counter("rushgo.auth.password_reset.request"); err != nil {
		return nil, err
	}
	if metrics.AuthResetComplete, err = meter.Int64Counter("rushgo.auth.password_reset.complete"); err != nil {
		return nil, err
	}
	if metrics.SeatAdmissionAttempts, err = meter.Int64Counter("rushgo.seat_admission.attempts", metric.WithUnit("{attempt}")); err != nil {
		return nil, err
	}
	if metrics.SeatAdmissionFallbacks, err = meter.Int64Counter("rushgo.seat_admission.fallbacks", metric.WithUnit("{fallback}")); err != nil {
		return nil, err
	}
	if metrics.SeatAdmissionDuration, err = meter.Float64Histogram("rushgo.seat_admission.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.AdmissionRepair, err = meter.Int64Counter("rushgo.admission_repair.work"); err != nil {
		return nil, err
	}
	if metrics.AdmissionRepairDuration, err = meter.Float64Histogram("rushgo.admission_repair.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.AdmissionRepairBacklog, err = meter.Int64Gauge("rushgo.admission_repair.backlog"); err != nil {
		return nil, err
	}
	if metrics.AdmissionRepairCycleAge, err = meter.Float64Gauge("rushgo.admission_repair.cycle_age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.SeatPostgresAttempts, err = meter.Int64Counter("rushgo.seat_postgres_reservation.attempts", metric.WithUnit("{attempt}")); err != nil {
		return nil, err
	}
	if metrics.OutboxRelayAttempts, err = meter.Int64Counter("rushgo.outbox.relay.attempts", metric.WithUnit("{attempt}")); err != nil {
		return nil, err
	}
	if metrics.OutboxPublish, err = meter.Int64Counter("rushgo.outbox.publish", metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if metrics.OutboxRetries, err = meter.Int64Counter("rushgo.outbox.retries", metric.WithUnit("{retry}")); err != nil {
		return nil, err
	}
	if metrics.OutboxPending, err = meter.Int64Gauge("rushgo.outbox.pending", metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if metrics.OutboxDeliveryDelay, err = meter.Float64Histogram("rushgo.outbox.delivery_delay", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.NotificationProcessed, err = meter.Int64Counter("rushgo.notification.processed", metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if metrics.NotificationFailures, err = meter.Int64Counter("rushgo.notification.processing_failures", metric.WithUnit("{failure}")); err != nil {
		return nil, err
	}
	if metrics.NotificationDeliveryPending, err = meter.Int64Gauge("rushgo.notification.delivery.pending", metric.WithUnit("{notification}")); err != nil {
		return nil, err
	}
	if metrics.NotificationDeliveryOldestAge, err = meter.Float64Gauge("rushgo.notification.delivery.oldest_age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.SSEActiveConnections, err = meter.Int64UpDownCounter("rushgo.sse.active_connections", metric.WithUnit("{connection}")); err != nil {
		return nil, err
	}
	if metrics.SSEConnections, err = meter.Int64Counter("rushgo.sse.connections", metric.WithUnit("{connection}")); err != nil {
		return nil, err
	}
	if metrics.RealtimePublish, err = meter.Int64Counter("rushgo.realtime.publish", metric.WithUnit("{message}")); err != nil {
		return nil, err
	}
	if metrics.RealtimeReceived, err = meter.Int64Counter("rushgo.realtime.received", metric.WithUnit("{message}")); err != nil {
		return nil, err
	}
	if metrics.SSEFanoutDeliveries, err = meter.Int64Counter("rushgo.sse.fanout.deliveries", metric.WithUnit("{delivery}")); err != nil {
		return nil, err
	}
	if metrics.SSEFanoutDuration, err = meter.Float64Histogram("rushgo.sse.fanout.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.SubscriptionGenerationEvent, err = meter.Int64Counter("rushgo.realtime.subscription_generation.events", metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if metrics.SchedulerRuns, err = meter.Int64Counter("rushgo.scheduler.runs", metric.WithUnit("{run}")); err != nil {
		return nil, err
	}
	if metrics.SchedulerExpiredBookings, err = meter.Int64Counter("rushgo.scheduler.expired_bookings", metric.WithUnit("{booking}")); err != nil {
		return nil, err
	}
	if metrics.SchedulerDuration, err = meter.Float64Histogram("rushgo.scheduler.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.SagaTransitions, err = meter.Int64Counter("rushgo.saga.transitions", metric.WithUnit("{transition}")); err != nil {
		return nil, err
	}
	if metrics.SagaRecoveryAttempts, err = meter.Int64Counter("rushgo.saga.recovery.attempts"); err != nil {
		return nil, err
	}
	if metrics.SagaRecoveryDue, err = meter.Int64Gauge("rushgo.saga.recovery.due"); err != nil {
		return nil, err
	}
	if metrics.SagaRecoveryAttention, err = meter.Int64Gauge("rushgo.saga.recovery.attention"); err != nil {
		return nil, err
	}
	if metrics.SagaRecoveryOldestAge, err = meter.Float64Gauge("rushgo.saga.recovery.oldest_age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.SagaRecoveryReclaims, err = meter.Int64Counter("rushgo.saga.recovery.reclaims"); err != nil {
		return nil, err
	}
	if metrics.PaymentWebhookEvents, err = meter.Int64Counter("rushgo.payment.webhook.events", metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if metrics.KafkaPublishes, err = meter.Int64Counter("rushgo.kafka.publish", metric.WithUnit("{message}")); err != nil {
		return nil, err
	}
	if metrics.KafkaProcesses, err = meter.Int64Counter("rushgo.kafka.process", metric.WithUnit("{message}")); err != nil {
		return nil, err
	}
	if metrics.PaymentFinancialAttempts, err = meter.Int64Counter("rushgo.payment.financial.attempts"); err != nil {
		return nil, err
	}
	if metrics.PaymentFinancialDue, err = meter.Int64Gauge("rushgo.payment.financial.due"); err != nil {
		return nil, err
	}
	if metrics.PaymentFinancialAttention, err = meter.Int64Gauge("rushgo.payment.financial.attention"); err != nil {
		return nil, err
	}
	if metrics.PaymentFinancialOldestAge, err = meter.Float64Gauge("rushgo.payment.financial.oldest_age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	return metrics, nil
}
