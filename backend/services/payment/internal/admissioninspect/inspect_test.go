package admissioninspect

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrationStatusKeepsMissingVersionUnknown(t *testing.T) {
	tests := []struct {
		name      string
		version   *int64
		dirty     *bool
		installed bool
		target    int64
		want      string
	}{
		{name: "table not installed", installed: false, target: 7, want: "not_installed"},
		{name: "no version row", installed: true, target: 7, want: "no_version"},
		{name: "behind", version: int64Pointer(6), dirty: boolPointer(false), installed: true, target: 7, want: "behind"},
		{name: "current", version: int64Pointer(7), dirty: boolPointer(false), installed: true, target: 7, want: "current"},
		{name: "ahead", version: int64Pointer(8), dirty: boolPointer(false), installed: true, target: 7, want: "ahead"},
		{name: "dirty takes priority", version: int64Pointer(7), dirty: boolPointer(true), installed: true, target: 7, want: "dirty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := migrationStatus(tt.installed, tt.version, tt.dirty, tt.target); got != tt.want {
				t.Fatalf("migrationStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFailureCategoryDoesNotExposeDatabaseError(t *testing.T) {
	secret := "postgres://operator:super-secret@db.example/private?token=hidden"
	err := errors.New(secret)
	category := failureCategory(nil, err)
	if category != "query_failed" {
		t.Fatalf("failure category = %q, want query_failed", category)
	}
	if strings.Contains(category, secret) || strings.Contains(category, "super-secret") {
		t.Fatal("failure category exposed the original database error")
	}
	if got := failureCategory(nil, &pgconn.PgError{Code: "42501", Message: secret}); got != "permission_denied" {
		t.Fatalf("permission SQLSTATE category = %q, want permission_denied", got)
	}
}

func TestInvalidDSNDoesNotInventAdmissionCountsOrExposeCredentials(t *testing.T) {
	dsn := "postgres://operator:super-secret@host.example/%zz"
	report := Inspect(context.Background(), Options{
		Source:           SourcePayment,
		PaymentDSN:       dsn,
		TotalTimeout:     time.Second,
		StatementTimeout: time.Second,
	})
	if report.Status != "unavailable" || report.Payment == nil || report.Payment.Status != "unavailable" || report.Payment.FailureCategory != "configuration_invalid" {
		t.Fatalf("invalid DSN report status = %+v", report)
	}
	if report.Payment.Admission != nil || report.Payment.RefundQueueCandidatesWithPendingAdmission != nil {
		t.Fatal("invalid DSN produced admission or refund counts")
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal("invalid DSN report could not be encoded")
	}
	if strings.Contains(string(encoded), dsn) || strings.Contains(string(encoded), "super-secret") {
		t.Fatal("invalid DSN report exposed connection credentials")
	}
}

func TestCountMetricDoesNotInventZeroWhenUnavailable(t *testing.T) {
	got := unavailableCount("query_failed")
	if got.Status != "unavailable" || got.Value != nil || got.FailureCategory != "query_failed" {
		t.Fatalf("unavailable metric = %+v, want unavailable with no value", got)
	}
	zero := availableCount(0)
	if zero.Status != "available" || zero.Value == nil || *zero.Value != 0 {
		t.Fatalf("successful zero metric = %+v, want available value 0", zero)
	}
}

func int64Pointer(value int64) *int64 { return &value }

func boolPointer(value bool) *bool { return &value }
