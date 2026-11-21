package cronjob

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/scheduler/internal/domain"
	"go.uber.org/zap"
)

type recoveryJob struct{}

func (recoveryJob) Name() string              { return "recovery-fixture" }
func (recoveryJob) Run(context.Context) error { return nil }

func TestSchedulerReplacementFailurePreservesAppliedVersion(t *testing.T) {
	m := NewCronjobManager(zap.NewNop(), nil, WithSecond)
	cfg := domain.SchedulerConfig{Id: uuid.New(), Name: "recovery-fixture", Version: 1, IsEnabled: true, Timeout: time.Second, IntervalExpression: "*/5 * * * * *", Valid: true}
	m.jobConfig[cfg.Name] = cfg
	if err := m.RegisterJob(context.Background(), recoveryJob{}); err != nil {
		t.Fatal(err)
	}
	original := m.jobEntryID[cfg.Name]
	cfg.Version = 2
	cfg.IntervalExpression = "invalid cron"
	if err := m.ReRegisterJob(context.Background(), cfg); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	if m.jobConfig[cfg.Name].Version != 1 || m.jobEntryID[cfg.Name] != original || len(m.cron.Entries()) != 1 {
		t.Fatal("replacement failure removed old cron or advanced applied version")
	}
	cfg.IntervalExpression = "*/10 * * * * *"
	if err := m.ReRegisterJob(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if m.jobConfig[cfg.Name].Version != 2 || len(m.cron.Entries()) != 1 {
		t.Fatal("valid version was not applied exactly once")
	}
	// Six-field validation succeeds, but the runtime five-field cron parser
	// rejects registration. This exercises AddFunc failure after validation.
	other := NewCronjobManager(zap.NewNop(), nil)
	cfg.Version = 1
	cfg.IsEnabled = false
	other.jobConfig[cfg.Name] = cfg
	if err := other.RegisterJob(context.Background(), recoveryJob{}); err != nil {
		t.Fatal(err)
	}
	cfg.Version = 2
	cfg.IsEnabled = true
	if err := other.ReRegisterJob(context.Background(), cfg); err == nil {
		t.Fatal("runtime registration failure not exercised")
	}
	if other.jobConfig[cfg.Name].Version != 1 || len(other.cron.Entries()) != 0 {
		t.Fatal("runtime AddFunc failure advanced applied version")
	}
}
