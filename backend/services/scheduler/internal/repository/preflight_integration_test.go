//go:build windows

package repository

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestSchedulerPreflightRetainsNullableHistoryIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx := context.Background()
	id := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,timeout,version,interval_expression,is_enable) VALUES($1,'legacy',NULL,NULL,'invalid',NULL)`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO outbound_events(topic,event_type,status,payload) VALUES('legacy','legacy',NULL,NULL)`); err != nil {
		t.Fatal(err)
	}
	missingID := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable) VALUES($1,'missing-event','reservation-cleaner-job',10,2,'*/5 * * * * *',true)`, missingID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO outbound_events(topic,event_type,status,payload) VALUES('scheduler.config.events','SchedulerConfigChanged','pending',jsonb_build_object('Id',$1::text,'Version',2,'Name','missing-event','IntervalExpression','*/5 * * * * *','IsEnabled','true','Timeout','10000000000'))`, missingID.String()); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := p.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]any{"version": 1, "compose_project": os.Getenv("RELIABILITY_TEST_PROJECT"), "postgres_containers": map[string]any{"scheduler": map[string]any{"container": os.Getenv("RELIABILITY_TEST_PG_CONTAINER"), "database": "ticketbox_scheduler_test", "user": "reliability_fixture", "schema": schema}}}
	raw, _ := json.Marshal(manifest)
	code := `import json,sys,importlib.util;spec=importlib.util.spec_from_file_location('preflight',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);print(json.dumps(m.run_read_only(json.loads(sys.argv[2]))))`
	out, err := exec.Command("python", "-c", code, filepath.FromSlash("../../../../scripts/reliability/preflight.py"), string(raw)).CombinedOutput()
	if err != nil {
		t.Fatalf("actual preflight: %s", out)
	}
	var report struct {
		Status   string
		Services map[string]struct {
			Counts    map[string]int
			Mutation  bool
			Automatic bool `json:"automatic_adoption"`
		}
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatal(err)
	}
	s := report.Services["scheduler"]
	if report.Status != "BLOCKED" || s.Counts["invalid_configs"] < 1 || s.Counts["nullable_outbound"] != 1 || s.Counts["missing_config_events"] != 1 || s.Counts["invalid_outbound"] != 1 || s.Mutation || s.Automatic {
		t.Fatal("unproven history accepted", string(out))
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM scheduler_configs WHERE timeout IS NULL`).Scan(&count); err != nil || count != 1 {
		t.Fatal("preflight altered history", count, err)
	}
}
