package admissioninspect

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SourceBoth    = "both"
	SourcePayment = "payment"
	SourceBooking = "booking"

	paymentAdmissionMigrationVersion int64 = 202611020001
	bookingAdmissionMigrationVersion int64 = 7
)

type Options struct {
	Source           string
	PaymentDSN       string
	BookingDSN       string
	SourceVersion    string
	TotalTimeout     time.Duration
	StatementTimeout time.Duration
}

type Report struct {
	Status                      string          `json:"status"`
	SourceVersion               string          `json:"source_version"`
	TotalTimeoutMillis          int64           `json:"total_timeout_ms"`
	StatementTimeoutMillis      int64           `json:"statement_timeout_ms"`
	CrossDatabaseAtomicSnapshot bool            `json:"cross_database_atomic_snapshot"`
	SnapshotNote                string          `json:"snapshot_note"`
	Payment                     *DatabaseReport `json:"payment,omitempty"`
	Booking                     *DatabaseReport `json:"booking,omitempty"`
}

type DatabaseReport struct {
	Status                                    string            `json:"status"`
	Identity                                  *DatabaseIdentity `json:"database_identity,omitempty"`
	ObservedAt                                *time.Time        `json:"observed_at,omitempty"`
	TransactionMode                           string            `json:"transaction_mode,omitempty"`
	DataScope                                 []string          `json:"data_scope,omitempty"`
	Migration                                 MigrationReport   `json:"migration"`
	Mode                                      ModeReport        `json:"admission_mode"`
	Admission                                 *AdmissionReport  `json:"admission,omitempty"`
	RefundQueueCandidatesWithPendingAdmission *CountMetric      `json:"refund_queue_candidates_with_pending_admission,omitempty"`
	FailurePhase                              string            `json:"failure_phase,omitempty"`
	FailureCategory                           string            `json:"failure_category,omitempty"`
}

type DatabaseIdentity struct {
	DatabaseName  string  `json:"database_name"`
	SchemaName    string  `json:"schema_name"`
	ServerAddress *string `json:"server_address,omitempty"`
	ServerPort    *int32  `json:"server_port,omitempty"`
}

type MigrationReport struct {
	Status          string `json:"status"`
	Version         *int64 `json:"version,omitempty"`
	Dirty           *bool  `json:"dirty,omitempty"`
	RequiredVersion int64  `json:"required_version"`
}

type ModeReport struct {
	Status string `json:"status"`
	Active *bool  `json:"active,omitempty"`
}

type AdmissionReport struct {
	Status                     string           `json:"status"`
	PendingCount               CountMetric      `json:"pending_count"`
	TerminalCount              CountMetric      `json:"terminal_count"`
	UnknownStateCount          CountMetric      `json:"unknown_state_count"`
	RecoveryDueCount           CountMetric      `json:"recovery_due_count"`
	OldestPendingCreatedAt     *time.Time       `json:"oldest_pending_created_at,omitempty"`
	OldestPendingAgeSeconds    *float64         `json:"oldest_pending_age_seconds,omitempty"`
	PendingErrorCategoryCounts map[string]int64 `json:"pending_error_category_counts,omitempty"`
}

type CountMetric struct {
	Status          string `json:"status"`
	Value           *int64 `json:"value,omitempty"`
	FailureCategory string `json:"failure_category,omitempty"`
}

func availableCount(value int64) CountMetric {
	return CountMetric{Status: "available", Value: &value}
}

func unavailableCount(category string) CountMetric {
	return CountMetric{Status: "unavailable", FailureCategory: category}
}

func missingCount(status string) CountMetric {
	return CountMetric{Status: status}
}

func migrationStatus(installed bool, version *int64, dirty *bool, target int64) string {
	if !installed {
		return "not_installed"
	}
	if version == nil || dirty == nil {
		return "no_version"
	}
	if *dirty {
		return "dirty"
	}
	switch {
	case *version < target:
		return "behind"
	case *version > target:
		return "ahead"
	default:
		return "current"
	}
}

func failureCategory(ctx context.Context, err error) string {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch state := pgErr.SQLState(); {
		case state == "57014":
			return "statement_timeout"
		case state == "42501":
			return "permission_denied"
		case len(state) >= 2 && state[:2] == "28":
			return "permission_denied"
		case state == "42P01" || state == "42703" || state == "3F000":
			return "schema_unavailable"
		case len(state) >= 2 && state[:2] == "08", state == "57P01" || state == "57P02" || state == "57P03" || state == "3D000":
			return "connection_failed"
		default:
			return "query_failed"
		}
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return "connection_failed"
	}
	return "query_failed"
}

func (r Report) HasFailure() bool {
	return r.Status == "partial" || r.Status == "unavailable"
}
