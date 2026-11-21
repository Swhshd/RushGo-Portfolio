package cronjob

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/scheduler/internal/domain"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

type authoritativeSchedulerRepo struct {
	repository.SchedulerRepository
	configs []domain.SchedulerConfig
	get     *domain.SchedulerConfig
	getErr  error
	calls   int
}

func (r *authoritativeSchedulerRepo) ListSchedulersConfig(context.Context) ([]domain.SchedulerConfig, error) {
	return append([]domain.SchedulerConfig(nil), r.configs...), nil
}

func (r *authoritativeSchedulerRepo) GetById(_ context.Context, id uuid.UUID) (*domain.SchedulerConfig, error) {
	r.calls++
	if r.getErr != nil {
		return nil, r.getErr
	}
	if r.get == nil {
		return nil, repository.ErrConfigNotFound
	}
	config := *r.get
	if id != config.Id {
		return nil, repository.ErrConfigNotFound
	}
	return &config, nil
}

func TestReloadInstallsAuthoritativeCurrentVersionWhenManagerMissedEarlierEvents(t *testing.T) {
	current := authoritativeCleanerConfig(2)
	current.IsEnabled = true
	repo := &authoritativeSchedulerRepo{configs: []domain.SchedulerConfig{current}, get: &current}
	manager := NewCronjobManager(zap.NewNop(), repo, WithSecond)
	manager.jobs[string(domain.JobTypeReservationCleaner)] = reservationCleanerFixture{}

	if err := manager.Reload(context.Background()); err != nil {
		t.Fatalf("Reload() error = %v, want the authoritative version 2 config to load", err)
	}
	loaded, ok := manager.jobConfig[current.Name]
	if !ok || loaded.Id != current.Id || loaded.Version != current.Version || !loaded.IsEnabled {
		t.Fatalf("loaded config = (%+v, %v), want authoritative enabled version %d", loaded, ok, current.Version)
	}
	if len(manager.cron.Entries()) != 1 || manager.jobEntryID[current.Name] == 0 || repo.calls != 1 {
		t.Fatalf("reload did not schedule authoritative row: entries=%d, entryID=%d, repo reads=%d", len(manager.cron.Entries()), manager.jobEntryID[current.Name], repo.calls)
	}

	stale := current
	stale.Version = 1
	stale.IsEnabled = false
	if err := manager.ReRegisterJob(context.Background(), stale); err != nil {
		t.Fatalf("stale version replay error = %v, want no-op", err)
	}
	if manager.jobConfig[current.Name].Version != current.Version || len(manager.cron.Entries()) != 1 {
		t.Fatal("a later version-1 replay rolled back the current version-2 schedule")
	}
}

func TestFirstConfigEventUsesAuthoritativeCurrentRowInsteadOfPayload(t *testing.T) {
	current := authoritativeCleanerConfig(2)
	current.IsEnabled = true
	current.IntervalExpression = "0 */10 * * * *"
	current.Timeout = 45 * time.Second
	repo := &authoritativeSchedulerRepo{get: &current}
	manager := NewCronjobManager(zap.NewNop(), repo, WithSecond)
	manager.jobs[string(domain.JobTypeReservationCleaner)] = reservationCleanerFixture{}

	// The event identity and version match, but its mutable schedule is not
	// authoritative. The manager must apply the versioned row from the repo.
	event := current
	event.IntervalExpression = "*/1 * * * * *"
	event.Timeout = time.Second
	event.IsEnabled = false
	if err := manager.ReRegisterJob(context.Background(), event); err != nil {
		t.Fatalf("ReRegisterJob() error = %v", err)
	}
	loaded := manager.jobConfig[current.Name]
	if loaded.Version != current.Version || loaded.IntervalExpression != current.IntervalExpression || loaded.Timeout != current.Timeout || !loaded.IsEnabled {
		t.Fatalf("loaded config = %+v, want repository row %+v", loaded, current)
	}
	if len(manager.cron.Entries()) != 1 || repo.calls != 1 {
		t.Fatalf("authoritative config was not scheduled: entries=%d, repo reads=%d", len(manager.cron.Entries()), repo.calls)
	}
}

func TestUnknownConfigRejectsEventVersionAheadOfRepository(t *testing.T) {
	current := authoritativeCleanerConfig(1)
	event := current
	event.Version = 2
	event.IsEnabled = true
	repo := &authoritativeSchedulerRepo{get: &current}
	manager := NewCronjobManager(zap.NewNop(), repo, WithSecond)
	manager.jobs[string(domain.JobTypeReservationCleaner)] = reservationCleanerFixture{}

	if err := manager.ReRegisterJob(context.Background(), event); err == nil {
		t.Fatal("event version ahead of the authoritative row was accepted")
	}
	if len(manager.jobConfig) != 0 || len(manager.cron.Entries()) != 0 || repo.calls != 1 {
		t.Fatalf("future event changed scheduler state: configs=%v entries=%d repo reads=%d", manager.jobConfig, len(manager.cron.Entries()), repo.calls)
	}
}

func TestUnknownConfigFailsClosedOnRepositoryErrorOrIdentityMismatch(t *testing.T) {
	base := authoritativeCleanerConfig(2)
	for _, tc := range []struct {
		name   string
		row    domain.SchedulerConfig
		getErr error
	}{
		{name: "repository error", row: base, getErr: errors.New("database unavailable")},
		{name: "id mismatch", row: func() domain.SchedulerConfig { c := base; c.Id = uuid.New(); return c }()},
		{name: "name mismatch", row: func() domain.SchedulerConfig { c := base; c.Name = "different-cleaner"; return c }()},
		{name: "job type mismatch", row: func() domain.SchedulerConfig { c := base; c.JobType = "legacy-unsupported"; c.Valid = true; return c }()},
		{name: "invalid authoritative row", row: func() domain.SchedulerConfig { c := base; c.Valid = false; return c }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &authoritativeSchedulerRepo{get: &tc.row, getErr: tc.getErr}
			manager := NewCronjobManager(zap.NewNop(), repo, WithSecond)
			manager.jobs[string(domain.JobTypeReservationCleaner)] = reservationCleanerFixture{}

			if err := manager.ReRegisterJob(context.Background(), base); err == nil {
				t.Fatal("unknown config was accepted without a matching valid authoritative row")
			}
			if len(manager.jobConfig) != 0 || len(manager.cron.Entries()) != 0 || repo.calls != 1 {
				t.Fatalf("failed authorization changed scheduler state: configs=%v entries=%d repo reads=%d", manager.jobConfig, len(manager.cron.Entries()), repo.calls)
			}
		})
	}
}

func authoritativeCleanerConfig(version int) domain.SchedulerConfig {
	return domain.SchedulerConfig{
		Id: uuid.New(), Name: "cleaner-authoritative", JobType: domain.JobTypeReservationCleaner,
		Version: version, Valid: true, Timeout: 30 * time.Second,
		IntervalExpression: "*/5 * * * * *",
	}
}
