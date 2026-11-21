package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func migrationFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("AUTH_PREFLIGHT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated migration database not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database config")
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "rushgo_mvp_") {
		t.Fatal("test database must have rushgo_mvp_ prefix")
	}
	admin, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	name := "rushgo_mvp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(context.Background(), `CREATE DATABASE `+name); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), `DROP DATABASE `+name); err != nil {
			t.Error("owned test database cleanup failed")
		}
		admin.Close()
	})
	target, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("test database URL must be a URI")
	}
	target.Path = "/" + name
	return pool, target.String()
}

func migrateTo(t *testing.T, dsn string, args ...string) {
	t.Helper()
	path, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("migrate", append([]string{"-path", ".", "-database", dsn}, args...)...)
	command.Dir = path
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated migration failed: %s", out)
	}
}

func TestMigrationFreshAndUpgrade(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(fmt.Sprint("old=", old), func(t *testing.T) {
			pool, dsn := migrationFixture(t)
			ctx := context.Background()
			state, err := migrationState(ctx, pool)
			if err != nil || state != "FRESH" {
				t.Fatalf("empty state %s %v", state, err)
			}
			migrateTo(t, dsn, "goto", "1")
			if old {
				if _, err := pool.Exec(ctx, `INSERT INTO users(email,password_hash,name) VALUES('Mixed@Example.test','test-hash','retained')`); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err = audit(ctx, pool, false); err != nil {
				t.Fatal(err)
			}
			migrateTo(t, dsn, "goto", "2")
			if _, _, err = audit(ctx, pool, true); err != nil {
				t.Fatal(err)
			}
			migrateTo(t, dsn, "up")
			if _, _, err = audit(ctx, pool, true); err != nil {
				t.Fatal(err)
			}
			if old {
				var email, name string
				if err := pool.QueryRow(ctx, `SELECT email_canonical,name FROM users`).Scan(&email, &name); err != nil || email != "mixed@example.test" || name != "retained" {
					t.Fatal("existing account changed or missing")
				}
			}
		})
	}
}

func TestMigrationCollisionAndDirtyFailClosed(t *testing.T) {
	pool, dsn := migrationFixture(t)
	ctx := context.Background()
	migrateTo(t, dsn, "goto", "1")
	_, err := pool.Exec(ctx, `INSERT INTO users(email,password_hash,name) VALUES('Mixed@Example.test','hash','one'),('mixed@example.test','hash','two')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, n, err := audit(ctx, pool, false); !errors.Is(err, errCollision) || n != 1 {
		t.Fatal("collision not blocked")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&count); err != nil || count != 2 {
		t.Fatal("collision audit changed rows")
	}
	_, err = pool.Exec(ctx, `UPDATE schema_migrations SET dirty=true`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = migrationState(ctx, pool); err == nil {
		t.Fatal("dirty migration accepted")
	}
}
