package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/scheduler/internal/domain"
)

func TestCreateIfAbsentIsStableAndDatabaseRestrictsJobTypeIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	repo := NewSchedulerConfigRepo(p)
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("scheduler-create-integration:nightly-cleaner"))
	cfg := domain.SchedulerConfig{
		Id: id, Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner,
		Version: 1, IsEnabled: false, Timeout: 30 * time.Second,
		IntervalExpression: "0 */5 * * * *",
	}

	created, inserted, err := repo.CreateIfAbsent(ctx, cfg)
	if err != nil || !inserted || !created.Valid || created.Id != id || created.JobType != cfg.JobType || created.IsEnabled {
		t.Fatalf("first CreateIfAbsent = (%+v, %t, %v)", created, inserted, err)
	}
	retry, inserted, err := repo.CreateIfAbsent(ctx, cfg)
	if err != nil || inserted || retry.Id != id || retry.Version != 1 {
		t.Fatalf("idempotent CreateIfAbsent = (%+v, %t, %v)", retry, inserted, err)
	}

	changed := cfg
	changed.IntervalExpression = "0 */10 * * * *"
	if _, _, err := repo.CreateIfAbsent(ctx, changed); err != ErrConfigConflict {
		t.Fatalf("same name with changed config error = %v, want ErrConfigConflict", err)
	}

	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable)
		VALUES($1,'unsafe-job-type','shell',30,1,'0 */5 * * * *',false)`, uuid.New()); err == nil {
		t.Fatal("database accepted an unregistered scheduler job type")
	}
	configs, err := repo.ListSchedulersConfig(ctx)
	if err != nil || len(configs) != 1 || configs[0].Id != id {
		t.Fatalf("ListSchedulersConfig = (%+v, %v), want only the stable inserted row", configs, err)
	}
}
