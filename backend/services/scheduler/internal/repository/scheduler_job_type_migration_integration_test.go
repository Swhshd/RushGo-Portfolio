package repository_test

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/scheduler/internal/repository"
)

func TestJobTypeMigrationQuarantinesLegacyUnknownConfigsIntegration(t *testing.T) {
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
	schema := "scheduler_job_type_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", u.String(), "up", "2").CombinedOutput(); err != nil {
		_ = out // Do not print a command error that might contain connection parameters.
		t.Fatal("could not apply the legacy Scheduler schema")
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal("could not connect to the legacy Scheduler schema")
	}
	defer p.Close()
	knownID, legacyID := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,timeout,version,interval_expression,is_enable)
		VALUES($1,'reservation-cleaner-job',30,1,'*/30 * * * * *',true),
		($2,'legacy-custom-job',30,1,'*/30 * * * * *',true)`, knownID, legacyID); err != nil {
		t.Fatal("could not create pre-migration Scheduler rows")
	}
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		_ = out
		t.Fatal("could not apply the job type migration")
	}

	repo := repository.NewSchedulerConfigRepo(p)
	known, err := repo.GetById(ctx, knownID)
	if err != nil || !known.Valid || string(known.JobType) != "reservation-cleaner-job" {
		t.Fatalf("known legacy config = (%+v, %v), want registered cleaner type", known, err)
	}
	legacy, err := repo.GetById(ctx, legacyID)
	if err != nil || legacy.Valid || legacy.ValidationError == "" || string(legacy.JobType) != "legacy-unsupported" {
		t.Fatalf("unknown legacy config = (%+v, %v), want quarantined invalid row", legacy, err)
	}

}
