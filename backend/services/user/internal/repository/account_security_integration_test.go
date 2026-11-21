package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/user/internal/domain"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func accountFixture(t *testing.T) (*PostgresAuthRepository, *pgxpool.Pool, uuid.UUID) {
	t.Helper()
	raw := os.Getenv("MVP_USER_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated MVP_USER_DATABASE_URL required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/rushgo_mvp_") {
		t.Fatal("refusing non-isolated database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	name := "rushgo_mvp_account_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, `DROP DATABASE "`+name+`"`); admin.Close() })
	dir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("migrate", "-path", ".", "-database", dsn, "up")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migration: %v %s", err, output)
	}
	id := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,email_canonical,password_hash,name)VALUES($1,'owner@rushgo.test','owner@rushgo.test','old hash','Owner')`, id)
	if err != nil {
		t.Fatal(err)
	}
	return NewPostgresAuthRepository(pool, time.Now), pool, id
}
func oldPassword(hash string) bool { return hash == "old hash" }
func TestAccountPasswordChangeRevokesSessionsAndReplay(t *testing.T) {
	repo, pool, id := accountFixture(t)
	ctx := context.Background()
	_, session, err := repo.LoginAndIssueRefresh(ctx, "owner@rushgo.test", oldPassword, issueSynthetic)
	if err != nil {
		t.Fatal(err)
	}
	change, _, err := repo.PrepareEmailChange(ctx, id, "next@rushgo.test", "code hash", oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.ChangePassword(ctx, id, "new hash", oldPassword); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.RotateRefreshToken(ctx, session.StoredRefresh.TokenHash, issueSynthetic); err == nil {
		t.Fatal("old refresh survived password change")
	}
	if err = repo.ChangePassword(ctx, id, "again", oldPassword); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("old password replay %v", err)
	}
	var epoch int64
	var invalid bool
	if err = pool.QueryRow(ctx, `SELECT credential_version FROM users WHERE id=$1`, id).Scan(&epoch); err != nil || epoch != 1 {
		t.Fatalf("epoch %d %v", epoch, err)
	}
	if err = pool.QueryRow(ctx, `SELECT invalidated_at IS NOT NULL FROM account_email_changes WHERE id=$1`, change).Scan(&invalid); err != nil || !invalid {
		t.Fatal("pending email proof survived")
	}
	var audits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM account_security_audit WHERE user_id=$1 AND action='password_changed'`, id).Scan(&audits); err != nil || audits != 1 {
		t.Fatal("audit mismatch")
	}
}
func TestAccountEmailVerificationOwnershipReplayExpiry(t *testing.T) {
	repo, pool, id := accountFixture(t)
	ctx := context.Background()
	code := func(hash string) bool { return hash == "code hash" }
	change, _, err := repo.PrepareEmailChange(ctx, id, "next@rushgo.test", "code hash", oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.CompleteEmailChange(ctx, id, change, oldPassword, code); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("unaccepted proof used")
	}
	if err = repo.FinalizeEmailChangeDelivery(ctx, change, domain.Accepted); err != nil {
		t.Fatal(err)
	}
	other := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO users(id,email,email_canonical,password_hash,name)VALUES($1,'other@rushgo.test','other@rushgo.test','old hash','Other')`, other); err != nil {
		t.Fatal(err)
	}
	if err = repo.CompleteEmailChange(ctx, other, change, oldPassword, code); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("cross-account proof used")
	}
	if err = repo.CompleteEmailChange(ctx, id, change, oldPassword, func(string) bool { return false }); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("bad code accepted")
	}
	var attempts int
	_ = pool.QueryRow(ctx, `SELECT attempts FROM account_email_changes WHERE id=$1`, change).Scan(&attempts)
	if attempts != 1 {
		t.Fatal("bad code attempt not durable")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var passed atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if repo.CompleteEmailChange(ctx, id, change, oldPassword, code) == nil {
				passed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if passed.Load() != 1 {
		t.Fatalf("proof consumed %d times", passed.Load())
	}
	var email string
	var epoch int64
	if err = pool.QueryRow(ctx, `SELECT email_canonical,credential_version FROM users WHERE id=$1`, id).Scan(&email, &epoch); err != nil || email != "next@rushgo.test" || epoch != 1 {
		t.Fatal("email/epoch mismatch")
	}
	expired, _, err := repo.PrepareEmailChange(ctx, other, "expired@rushgo.test", "code hash", oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	_ = repo.FinalizeEmailChangeDelivery(ctx, expired, domain.Accepted)
	_, err = pool.Exec(ctx, `UPDATE account_email_changes SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, expired)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.CompleteEmailChange(ctx, other, expired, oldPassword, code); !errors.Is(err, domain.ErrCodeExpired) {
		t.Fatalf("expired proof: %v", err)
	}
}
