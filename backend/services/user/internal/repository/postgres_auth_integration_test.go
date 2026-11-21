package repository

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgAuthFixture struct {
	pool         *pgxpool.Pool
	dsn, backend string
	t            *testing.T
}

func newPGAuthFixture(t *testing.T) *pgAuthFixture {
	return newPGAuthFixtureAt(t, "")
}

func newPGAuthFixtureAt(t *testing.T, version string) *pgAuthFixture {
	t.Helper()
	raw := os.Getenv("USER_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated USER_TEST_DATABASE_URL required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host != "127.0.0.1:15463" || u.Path != "/rushgo_m6_auth" {
		t.Fatal("refusing non-isolated database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("fixture pool unavailable")
	}
	name := "m6_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal("fixture database unavailable")
	}
	u.Path = "/" + name
	dsn := u.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("fixture connect failed")
	}
	backend, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal("fixture path failed")
	}
	f := &pgAuthFixture{pool: pool, dsn: dsn, backend: backend, t: t}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, `DROP DATABASE "`+name+`" WITH (FORCE)`); admin.Close() })
	if version == "" {
		f.migrate("up")
	} else {
		f.migrate("goto", version)
	}
	return f
}
func (f *pgAuthFixture) migrate(args ...string) {
	f.t.Helper()
	cmd := exec.Command("migrate", append([]string{"-path", "services/user/migrations", "-database", f.dsn}, args...)...)
	cmd.Dir = f.backend
	if out, err := cmd.CombinedOutput(); err != nil {
		f.t.Fatal("isolated migration failed: " + strings.ReplaceAll(string(out), f.dsn, "[isolated DSN]"))
	}
}
func (f *pgAuthFixture) seed(email string) uuid.UUID {
	f.t.Helper()
	id := uuid.New()
	_, err := f.pool.Exec(context.Background(), `INSERT INTO users(id,email,password_hash,name) VALUES($1,$2,'synthetic hash','M6 fixture')`, id, email)
	if err != nil {
		f.t.Fatal("seed failed")
	}
	return id
}
func (f *pgAuthFixture) preflight(apply bool) error {
	args := []string{"run", "./services/user/cmd/auth_preflight"}
	if apply {
		args = append(args, "--apply-canonical")
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = f.backend
	cmd.Env = append(os.Environ(), "AUTH_PREFLIGHT_DATABASE_URL="+f.dsn)
	return cmd.Run()
}
func TestM6MigrationSchema(t *testing.T) {
	f := newPGAuthFixture(t)
	var exists bool
	if err := f.pool.QueryRow(context.Background(), `SELECT to_regclass('email_verification_guards') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal("schema probe failed")
	}
	if !exists {
		t.Fatal("durable registration generation guard absent")
	}
}
func TestM6MigrationLegacyPreserved(t *testing.T) {
	// This checks the reversible M6 migration, before credential epochs were
	// introduced. Migration 4 deliberately forbids rolling back revocation.
	f := newPGAuthFixtureAt(t, "3")
	f.migrate("goto", "1")
	id := f.seed("Legacy@Example.com")
	f.migrate("goto", "2")
	if err := f.preflight(true); err != nil {
		t.Fatal("canonical preflight failed")
	}
	f.migrate("goto", "3")
	var email, hash, canonical string
	var proof *string
	err := f.pool.QueryRow(context.Background(), `SELECT email,password_hash,email_canonical,email_verified_at::text FROM users WHERE id=$1`, id).Scan(&email, &hash, &canonical, &proof)
	if err != nil || email != "Legacy@Example.com" || hash != "synthetic hash" || canonical != "legacy@example.com" || proof != nil {
		t.Fatal("legacy identity/password/proof altered")
	}
	f.migrate("goto", "1")
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM users WHERE id=$1`, id).Scan(&count); err != nil || count != 1 {
		t.Fatal("down migration damaged legacy row")
	}
}
func TestCanonicalPreflightCollision(t *testing.T) {
	f := newPGAuthFixtureAt(t, "3")
	f.migrate("goto", "1")
	f.seed("User@Example.com")
	f.seed("\u2003user@example.com\u2003")
	f.migrate("goto", "2")
	if err := f.preflight(true); err == nil {
		t.Fatal("collision accepted")
	}
	var count, proofs, filled int
	err := f.pool.QueryRow(context.Background(), `SELECT count(*),count(email_verified_at),count(email_canonical) FROM users`).Scan(&count, &proofs, &filled)
	if err != nil || count != 2 || proofs != 0 || filled != 0 {
		t.Fatal("collision audit wrote legacy state")
	}
}
