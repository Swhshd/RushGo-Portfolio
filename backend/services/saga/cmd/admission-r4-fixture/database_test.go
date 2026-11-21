//go:build admission_r4

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run only against a new disposable database, never a Demo's existing state.
func TestFixturePristineBookingDatabaseGuard(t *testing.T) {
	dsn := os.Getenv("R4_FIXTURE_GUARD_TEST_DSN")
	if dsn == "" {
		t.Skip("requires a fresh isolated r4_fixture_guard_test database")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var database, user string
	var tables int
	if err := pool.QueryRow(ctx, `SELECT current_database(), current_user, (SELECT count(*) FROM pg_tables WHERE schemaname='public')`).Scan(&database, &user, &tables); err != nil {
		t.Fatal(err)
	}
	if database != "r4_fixture_guard_test" || user != database || tables != 0 {
		t.Fatal("test requires its exact empty disposable database/user")
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "booking", "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatal("missing booking migrations", err)
	}
	for _, path := range files {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(path), err)
		}
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE schema_migrations(version bigint, dirty boolean); INSERT INTO schema_migrations VALUES(13,false)`); err != nil {
		t.Fatal(err)
	}
	target := &databaseTarget{Service: "booking", Database: database, User: user, Migration: 13, Pool: pool}
	if err := validateTarget(ctx, target); err != nil {
		t.Fatalf("pristine migrations rejected: %v", err)
	}
	for _, tc := range []struct{ name, change, restore string }{
		{"enabled flag", `UPDATE presentation_feature_flags SET enabled=true`, `UPDATE presentation_feature_flags SET enabled=false`},
		{"changed flag version", `UPDATE presentation_feature_flags SET version=1`, `UPDATE presentation_feature_flags SET version=0`},
		{"flag timestamp", `UPDATE presentation_feature_flags SET updated_at=now()`, `UPDATE presentation_feature_flags SET updated_at=NULL`},
		{"flag actor", `UPDATE presentation_feature_flags SET actor_id='00000000-0000-4000-8000-000000000001'`, `UPDATE presentation_feature_flags SET actor_id=NULL`},
		{"missing flag", `DELETE FROM presentation_feature_flags`, `INSERT INTO presentation_feature_flags(key) VALUES('discovery_advanced_filters')`},
		{"existing business data", `CREATE TABLE fixture_guard_probe(id int); INSERT INTO fixture_guard_probe VALUES(1)`, `DROP TABLE fixture_guard_probe`},
		{"dirty migration", `UPDATE schema_migrations SET dirty=true`, `UPDATE schema_migrations SET dirty=false`},
		{"wrong migration", `UPDATE schema_migrations SET version=12`, `UPDATE schema_migrations SET version=13`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tc.change); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := pool.Exec(ctx, tc.restore); err != nil {
					t.Fatal(err)
				}
			}()
			if err := validateTarget(ctx, target); err == nil {
				t.Fatal("non-pristine database accepted")
			}
		})
	}
	if err := validateTarget(ctx, target); err != nil {
		t.Fatal("restored baseline rejected", err)
	}
}
