package service_scheduler

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
	cronjob "github.com/ticketbox/scheduler/internal/jobs"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

func TestCreateSchedulerConfigPersistsDisabledConfigAndOutboxAtomicallyIntegration(t *testing.T) {
	u, err := url.Parse(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25439" || u.Path != "/ticketbox_scheduler_test" {
		t.Fatal("BLOCKED: isolated Scheduler PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal("could not connect to isolated Scheduler PG")
	}
	defer admin.Close()
	schema := "scheduler_create_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal("could not create unique test schema")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 4*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("could not remove unique test schema: %v", err)
		}
	}()
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if _, err := exec.CommandContext(ctx, "migrate", "-path", "../../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatal("could not apply Scheduler migrations in unique test schema")
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal("could not connect to unique Scheduler schema")
	}
	defer p.Close()

	configRepo := repository.NewSchedulerConfigRepo(p)
	outboxRepo := repository.NewOutboundEventRepository(p)
	svc := NewSchedulerService(zap.NewNop(), configRepo, outboxRepo, repository.NewUnitOfWork(p), cronjob.NewCronjobManager(zap.NewNop(), configRepo))
	target := domain.SchedulerConfig{
		Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner,
		IsEnabled: true, Version: 99, Timeout: 30 * time.Second,
		IntervalExpression: "0 */5 * * * *",
	}

	created, err := svc.CreateSchedulerConfig(ctx, target)
	if err != nil || !created.Valid || created.Id == uuid.Nil || created.JobType != target.JobType || created.Version != 1 || created.IsEnabled {
		t.Fatalf("create = (%+v, %v), want stable disabled version 1 config", created, err)
	}
	eventID := SchedulerOutboundID(created.Id, 1)
	event, err := outboxRepo.GetByID(ctx, eventID)
	if err != nil || event.Id != eventID || event.Topic != topics.TopicSchedulerConfigEvent || event.EventType != topics.TypeSchedulerConfigChanged || event.Status == "" || event.PublishAt != nil {
		t.Fatalf("outbox event = (%+v, %v), want unpublished version 1 config event", event, err)
	}
	var frozen domain.SchedulerConfig
	if err := json.Unmarshal(event.Payload, &frozen); err != nil || frozen.Id != created.Id || frozen.Name != created.Name || frozen.JobType != created.JobType || frozen.Version != 1 || frozen.IsEnabled {
		t.Fatalf("frozen event payload = (%+v, %v), does not match created config", frozen, err)
	}

	retry, err := svc.CreateSchedulerConfig(ctx, target)
	if err != nil || retry.Id != created.Id || retry.Version != 1 {
		t.Fatalf("stable retry = (%+v, %v), want original config", retry, err)
	}
	if _, err := svc.CreateSchedulerConfig(ctx, domain.SchedulerConfig{
		Name: target.Name, JobType: target.JobType, Timeout: target.Timeout,
		IntervalExpression: "0 */10 * * * *",
	}); err != repository.ErrConfigConflict {
		t.Fatalf("same name with changed config error = %v, want conflict", err)
	}
	if _, err := svc.CreateSchedulerConfig(ctx, domain.SchedulerConfig{
		Name: " padded ", JobType: target.JobType, Timeout: target.Timeout,
		IntervalExpression: target.IntervalExpression,
	}); err == nil {
		t.Fatal("noncanonical whitespace in a name was silently normalized")
	}
	var configs, events int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM scheduler_configs`).Scan(&configs); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbound_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if configs != 1 || events != 1 {
		t.Fatalf("retry created extra rows: scheduler_configs=%d outbound_events=%d", configs, events)
	}
}
