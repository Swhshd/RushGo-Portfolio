package domain

import (
	"errors"
	"time"
)

var ErrMetricsBounds = errors.New("metrics window exceeds bounded scope")

type MoneyMetric struct {
	Currency, Mode                              string
	Payments, UnknownPayments, ConfirmedRefunds int64
	CollectedKnownMinor, RefundedKnownMinor     int64
}
type MoneyMetrics struct {
	Groups     []MoneyMetric
	ObservedAt time.Time
}
