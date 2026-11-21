package repository

import (
	"context"
	"os"
	"testing"
)

func seedSchedulerSQL(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("../../../../scripts/seed-scheduler.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestSeedSchedulerConfigBootstrapIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx := context.Background()
	sql := seedSchedulerSQL(t)

	for i := 0; i < 2; i++ {
		if _, err := p.Exec(ctx, sql); err != nil {
			t.Fatalf("seed run %d: %v", i+1, err)
		}
	}

	var timeout, version int
	var interval string
	var enabled bool
	if err := p.QueryRow(ctx, `SELECT timeout,version,interval_expression,is_enable
		FROM scheduler_configs WHERE name='reservation-cleaner-job'`).
		Scan(&timeout, &version, &interval, &enabled); err != nil {
		t.Fatal(err)
	}
	if timeout != 30 || version != 1 || interval != "*/30 * * * * *" || !enabled {
		t.Fatalf("unexpected seeded config: timeout=%d version=%d interval=%q enabled=%v",
			timeout, version, interval, enabled)
	}
	var rows, outbound int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM scheduler_configs`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbound_events`).Scan(&outbound); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || outbound != 0 {
		t.Fatalf("seed was not idempotent/silent: configs=%d outbound=%d", rows, outbound)
	}
}

func TestSeedSchedulerConfigPreservesValidOperatorConfigIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(name,job_type,timeout,version,interval_expression,is_enable)
		VALUES('reservation-cleaner-job','reservation-cleaner-job',45,3,'*/15 * * * * *',false)`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, seedSchedulerSQL(t)); err != nil {
		t.Fatal(err)
	}
	var timeout, version int
	var interval string
	var enabled bool
	if err := p.QueryRow(ctx, `SELECT timeout,version,interval_expression,is_enable
		FROM scheduler_configs WHERE name='reservation-cleaner-job'`).
		Scan(&timeout, &version, &interval, &enabled); err != nil {
		t.Fatal(err)
	}
	if timeout != 45 || version != 3 || interval != "*/15 * * * * *" || enabled {
		t.Fatalf("operator config was overwritten: timeout=%d version=%d interval=%q enabled=%v",
			timeout, version, interval, enabled)
	}
}

func TestSeedSchedulerConfigRepairsZeroTimeoutIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(name,job_type,timeout,version,interval_expression,is_enable)
		VALUES('reservation-cleaner-job','reservation-cleaner-job',0,7,'*/30 * * * * *',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, seedSchedulerSQL(t)); err != nil {
		t.Fatal(err)
	}
	var timeout, version int
	var enabled bool
	if err := p.QueryRow(ctx, `SELECT timeout,version,is_enable FROM scheduler_configs
		WHERE name='reservation-cleaner-job'`).Scan(&timeout, &version, &enabled); err != nil {
		t.Fatal(err)
	}
	if timeout != 30 || version != 7 || !enabled {
		t.Fatalf("zero timeout was not repaired safely: timeout=%d version=%d enabled=%v",
			timeout, version, enabled)
	}
}

func TestSeedSchedulerConfigRepairsNullTimeoutIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(name,job_type,timeout,version,interval_expression,is_enable)
		VALUES('reservation-cleaner-job','reservation-cleaner-job',NULL,1,'*/30 * * * * *',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, seedSchedulerSQL(t)); err != nil {
		t.Fatal(err)
	}
	var timeout, version int
	if err := p.QueryRow(ctx, `SELECT timeout,version FROM scheduler_configs
		WHERE name='reservation-cleaner-job'`).Scan(&timeout, &version); err != nil {
		t.Fatal(err)
	}
	if timeout != 30 || version != 1 {
		t.Fatalf("NULL timeout was not repaired safely: timeout=%d version=%d", timeout, version)
	}
}
