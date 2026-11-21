package cronjob

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/scheduler/internal/domain"
	"go.uber.org/zap"
)

type reservationCleanerFixture struct{}

func (reservationCleanerFixture) Name() string              { return "reservation-cleaner-job" }
func (reservationCleanerFixture) Run(context.Context) error { return nil }

func TestOneRegisteredHandlerSchedulesDistinctConfigsByName(t *testing.T) {
	m := NewCronjobManager(zap.NewNop(), nil, WithSecond)
	first := domain.SchedulerConfig{
		Id: uuid.New(), Name: "cleaner-fast", JobType: domain.JobTypeReservationCleaner,
		Version: 1, Valid: true, IsEnabled: true, Timeout: 30 * time.Second,
		IntervalExpression: "*/5 * * * * *",
	}
	second := domain.SchedulerConfig{
		Id: uuid.New(), Name: "cleaner-slow", JobType: domain.JobTypeReservationCleaner,
		Version: 1, Valid: true, IsEnabled: false, Timeout: 60 * time.Second,
		IntervalExpression: "0 */5 * * * *",
	}
	m.jobConfig[first.Name] = first
	m.jobConfig[second.Name] = second

	if err := m.RegisterJob(context.Background(), reservationCleanerFixture{}); err != nil {
		t.Fatal(err)
	}
	if len(m.cron.Entries()) != 1 || m.jobEntryID[first.Name] == 0 {
		t.Fatalf("initial entries=%d, scheduled IDs=%v; expected only enabled first config", len(m.cron.Entries()), m.jobEntryID)
	}

	second.IsEnabled = true
	second.Version = 2
	if err := m.ReRegisterJob(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if len(m.cron.Entries()) != 2 || m.jobEntryID[second.Name] == 0 || m.jobConfig[second.Name].Id != second.Id {
		t.Fatalf("replacement did not schedule the distinct second config: entries=%d, IDs=%v, config=%+v", len(m.cron.Entries()), m.jobEntryID, m.jobConfig[second.Name])
	}
}
