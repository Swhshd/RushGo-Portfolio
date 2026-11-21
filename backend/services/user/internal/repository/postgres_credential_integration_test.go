package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/user/internal/domain"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type credentialFixture struct {
	*pgAuthFixture
	repo *PostgresAuthRepository
	now  time.Time
}

func newCredentialFixture(t *testing.T) *credentialFixture {
	f := &credentialFixture{pgAuthFixture: newPGAuthFixture(t), now: time.Now().UTC()}
	f.repo = NewPostgresAuthRepository(f.pool, func() time.Time { return f.now })
	return f
}

var testCtx = context.Background()

func (f *credentialFixture) prepare(email string) *domain.RegistrationVerification {
	f.t.Helper()
	v, err := f.repo.PrepareRegistrationVerification(testCtx, RegistrationPreparation{uuid.New(), email, "synthetic-code-hash"})
	if err != nil {
		f.t.Fatal("prepare failed")
	}
	return v
}
func (f *credentialFixture) accepted(email string) *domain.RegistrationVerification {
	f.t.Helper()
	v := f.prepare(email)
	if err := f.repo.FinalizeRegistrationDelivery(testCtx, v.ID, domain.Accepted); err != nil {
		f.t.Fatal("finalize failed")
	}
	return v
}
func (f *credentialFixture) complete(v *domain.RegistrationVerification, match bool) (*domain.User, error) {
	return f.repo.RegisterVerifiedUser(testCtx, domain.RegisterCompletion{UserID: uuid.New(), VerificationID: v.ID, CanonicalEmail: v.CanonicalEmail, Name: "M6", PasswordHash: "synthetic-password-hash"}, func(string) bool { return match })
}
func TestRegistrationDoubleConsume(t *testing.T) {
	f := newCredentialFixture(t)
	v := f.accepted("new@example.com")
	start := make(chan struct{})
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := f.complete(v, true); err == nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	var users, consumed int
	_ = f.pool.QueryRow(testCtx, `SELECT count(*) FROM users`).Scan(&users)
	_ = f.pool.QueryRow(testCtx, `SELECT count(consumed_at) FROM email_verification_requests`).Scan(&consumed)
	if successes.Load() != 1 || users != 1 || consumed != 1 {
		t.Fatal("single-use creation violated")
	}
}
func TestVerificationAttemptsExpiryAndGeneration(t *testing.T) {
	f := newCredentialFixture(t)
	v := f.accepted("attempt@example.com")
	for i := 1; i <= 5; i++ {
		_, err := f.complete(v, false)
		want := domain.ErrInvalidCode
		if i == 5 {
			want = domain.ErrCodeAttemptsExhausted
		}
		if !errors.Is(err, want) {
			t.Fatal("wrong comparison outcome")
		}
	}
	called := false
	_, err := f.repo.RegisterVerifiedUser(testCtx, domain.RegisterCompletion{UserID: uuid.New(), VerificationID: v.ID, CanonicalEmail: v.CanonicalEmail, Name: "M6", PasswordHash: "hash"}, func(string) bool { called = true; return true })
	if called || !errors.Is(err, domain.ErrCodeAttemptsExhausted) {
		t.Fatal("exhaustion bypassed")
	}
	var attempts int
	var expiry time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT attempt_count,expires_at FROM email_verification_requests WHERE id=$1`, v.ID).Scan(&attempts, &expiry)
	if attempts != 5 || !expiry.Equal(v.ExpiresAt) {
		t.Fatal("attempt state or expiry changed")
	}
	if _, err := f.repo.PrepareRegistrationVerification(testCtx, RegistrationPreparation{uuid.New(), v.CanonicalEmail, "hash"}); !errors.Is(err, domain.ErrRateLimited) {
		t.Fatal("cooldown bypass")
	}
	f.now = f.now.Add(time.Minute)
	newV := f.prepare(v.CanonicalEmail)
	if newV.Generation != v.Generation+1 {
		t.Fatal("generation did not advance")
	}
	if err := f.repo.FinalizeRegistrationDelivery(testCtx, v.ID, domain.Accepted); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("superseded generation reactivated")
	}
	if _, err := f.complete(newV, true); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("PREPARED proof used")
	}
	_ = f.repo.FinalizeRegistrationDelivery(testCtx, newV.ID, domain.Accepted)
	f.now = newV.ExpiresAt
	if _, err := f.complete(newV, true); !errors.Is(err, domain.ErrCodeExpired) {
		t.Fatal("expiry equality accepted")
	}
}
func TestRegistrationFirstGenerationConcurrent(t *testing.T) {
	f := newCredentialFixture(t)
	start := make(chan struct{})
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := f.repo.PrepareRegistrationVerification(testCtx, RegistrationPreparation{uuid.New(), "same@example.com", "hash"}); err == nil {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatal("initial absent-row cooldown race")
	}
}
func (f *credentialFixture) legacy(email string) uuid.UUID {
	f.t.Helper()
	id := uuid.New()
	_, err := f.pool.Exec(testCtx, `INSERT INTO users(id,email,email_canonical,password_hash,name)VALUES($1,$2,$2,'old hash','legacy')`, id, email)
	if err != nil {
		f.t.Fatal("legacy seed failed")
	}
	return id
}
func issueSynthetic(u *domain.User, now time.Time) (*domain.SessionPair, error) {
	id := uuid.New()
	return &domain.SessionPair{AccessToken: "synthetic", RefreshToken: id.String(), StoredRefresh: domain.RefreshToken{ID: id, UserID: u.ID, TokenHash: id.String(), ExpiresAt: now.Add(time.Hour), CreatedAt: now}}, nil
}
func (f *credentialFixture) login(email string) *domain.SessionPair {
	f.t.Helper()
	_, p, err := f.repo.LoginAndIssueRefresh(testCtx, email, func(hash string) bool { return hash == "old hash" }, issueSynthetic)
	if err != nil {
		f.t.Fatal("legacy login failed")
	}
	return p
}
func (f *credentialFixture) reset(id uuid.UUID, email string) *domain.PasswordResetRequest {
	f.t.Helper()
	v, err := f.repo.PreparePasswordReset(testCtx, PasswordResetPreparation{uuid.New(), id, email, strings.Repeat("a", 64)})
	if err != nil {
		f.t.Fatal("reset prepare failed")
	}
	if err = f.repo.FinalizePasswordResetDelivery(testCtx, v.ID, domain.Accepted); err != nil {
		f.t.Fatal("reset finalize failed")
	}
	return v
}
func TestRegistrationProvenDuplicatePreservesLegacy(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("exists@example.com")
	v := f.accepted("exists@example.com")
	if _, err := f.complete(v, true); !errors.Is(err, domain.ErrEmailExists) {
		t.Fatal("proven duplicate mismatch")
	}
	var hash string
	var proof *time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT password_hash,email_verified_at FROM users WHERE id=$1`, id).Scan(&hash, &proof)
	if hash != "old hash" || proof != nil {
		t.Fatal("duplicate altered legacy credentials")
	}
	if _, err := f.complete(v, true); !errors.Is(err, domain.ErrInvalidCode) {
		t.Fatal("duplicate proof replay accepted")
	}
}
func TestPasswordResetAtomicRevocationAndReplay(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("reset@example.com")
	f.login("reset@example.com")
	f.login("reset@example.com")
	v := f.reset(id, "reset@example.com")
	if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); err != nil {
		t.Fatal("reset failed")
	}
	var live int
	var hash string
	var proof *time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`).Scan(&live)
	_ = f.pool.QueryRow(testCtx, `SELECT password_hash,email_verified_at FROM users WHERE id=$1`, id).Scan(&hash, &proof)
	if live != 0 || hash != "new hash" || proof == nil {
		t.Fatal("reset not atomic")
	}
	if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "other"}); !errors.Is(err, domain.ErrResetTokenUsed) {
		t.Fatal("reset replay")
	}
}
func TestRefreshConcurrentDoubleUse(t *testing.T) {
	f := newCredentialFixture(t)
	f.legacy("refresh@example.com")
	p := f.login("refresh@example.com")
	start := make(chan struct{})
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, _, err := f.repo.RotateRefreshToken(testCtx, p.StoredRefresh.TokenHash, issueSynthetic); err == nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal("refresh reused")
	}
}
func TestRefreshInsertFailureRollsBack(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("rollback@example.com")
	p := f.login("rollback@example.com")
	_, _, err := f.repo.RotateRefreshToken(testCtx, p.StoredRefresh.TokenHash, func(*domain.User, time.Time) (*domain.SessionPair, error) {
		return &domain.SessionPair{StoredRefresh: domain.RefreshToken{ID: p.StoredRefresh.ID, UserID: id, TokenHash: "duplicate", ExpiresAt: f.now.Add(time.Hour), CreatedAt: f.now}}, nil
	})
	if err == nil {
		t.Fatal("duplicate insert accepted")
	}
	var revoked *time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT revoked_at FROM refresh_tokens WHERE id=$1`, p.StoredRefresh.ID).Scan(&revoked)
	if revoked != nil {
		t.Fatal("failed insert revoked old session")
	}
}
func TestLogoutCurrentOwnershipAndIdempotency(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("logout@example.com")
	other := f.legacy("other@example.com")
	p := f.login("logout@example.com")
	keep := f.login("logout@example.com")
	if err := f.repo.RevokeCurrentRefresh(testCtx, other, p.StoredRefresh.TokenHash); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal("other-owner logout accepted")
	}
	for i := 0; i < 2; i++ {
		if err := f.repo.RevokeCurrentRefresh(testCtx, id, p.StoredRefresh.TokenHash); err != nil {
			t.Fatal("own logout failed")
		}
	}
	var revoked *time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT revoked_at FROM refresh_tokens WHERE id=$1`, keep.StoredRefresh.ID).Scan(&revoked)
	if revoked != nil {
		t.Fatal("other-device logout revoked")
	}
}
func TestPostgresCommitAcknowledgmentReconciled(t *testing.T) {
	f := newCredentialFixture(t)
	f.repo.commit = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errors.New("synthetic lost acknowledgment")
	}
	v := f.accepted("ack@example.com")
	if _, err := f.complete(v, true); err != nil {
		t.Fatal("committed creation not reconciled")
	}
}
func TestPasswordResetKnownRollback(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("rollback-reset@example.com")
	p := f.login("rollback-reset@example.com")
	v := f.reset(id, "rollback-reset@example.com")
	f.repo.commit = func(ctx context.Context, tx pgx.Tx) error {
		_ = tx.Rollback(ctx)
		return errors.New("synthetic known rollback")
	}
	if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); err == nil {
		t.Fatal("rolled back reset reported success")
	}
	var hash string
	var revoked, consumed, proof *time.Time
	_ = f.pool.QueryRow(testCtx, `SELECT password_hash,email_verified_at FROM users WHERE id=$1`, id).Scan(&hash, &proof)
	_ = f.pool.QueryRow(testCtx, `SELECT revoked_at FROM refresh_tokens WHERE id=$1`, p.StoredRefresh.ID).Scan(&revoked)
	_ = f.pool.QueryRow(testCtx, `SELECT consumed_at FROM password_reset_requests WHERE id=$1`, v.ID).Scan(&consumed)
	if hash != "old hash" || proof != nil || revoked != nil || consumed != nil {
		t.Fatal("known rollback partial writes")
	}
}

// A rolled-back submission must not acknowledge the competing consumer's write.
func TestPasswordResetCommitReconciliationAttributesOwnPassword(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("competing-reset@example.com")
	f.login("competing-reset@example.com")
	v := f.reset(id, "competing-reset@example.com")
	rolledBack, competitorDone := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(testCtx, 15*time.Second)
	defer cancel()
	f.repo.commit = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		close(rolledBack)
		select {
		case <-competitorDone:
		case <-ctx.Done():
		}
		return errors.New("synthetic lost commit acknowledgment after rollback")
	}
	first := make(chan error, 1)
	go func() {
		first <- f.repo.ResetPasswordAndRevokeSessions(ctx, ResetCompletion{v.TokenHash, "first new hash"})
	}()
	select {
	case <-rolledBack:
	case <-ctx.Done():
		t.Fatal("rollback barrier failed")
	}
	competitor := NewPostgresAuthRepository(f.pool, func() time.Time { return f.now })
	secondErr := competitor.ResetPasswordAndRevokeSessions(ctx, ResetCompletion{v.TokenHash, "second new hash"})
	close(competitorDone)
	firstErr := <-first
	if secondErr != nil {
		t.Fatal("competing reset failed")
	}
	if !errors.Is(firstErr, domain.ErrTemporaryUnavailable) {
		t.Fatal("rolled-back reset acknowledged another submission's password")
	}
	var consumed, active int
	if err := f.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM password_reset_requests WHERE consumed_at IS NOT NULL),(SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL)`).Scan(&consumed, &active); err != nil || consumed != 1 || active != 0 {
		t.Fatal("competing consumption/revocation not atomic")
	}
	if _, _, err := competitor.LoginAndIssueRefresh(ctx, "competing-reset@example.com", func(hash string) bool { return hash == "first new hash" }, issueSynthetic); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatal("rolled-back password authorized")
	}
	if _, _, err := competitor.LoginAndIssueRefresh(ctx, "competing-reset@example.com", func(hash string) bool { return hash == "second new hash" }, issueSynthetic); err != nil {
		t.Fatal("committed competing password not authorized")
	}
}

func TestProfileRepositoryNameOnly(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("profile@example.com")
	repo := NewPostgresUserRepository(f.pool)
	u, err := repo.GetByID(testCtx, id)
	if err != nil {
		t.Fatal("profile read failed")
	}
	u.Email = "changed@example.com"
	u.Name = "Renamed"
	if err = repo.Update(testCtx, u); err != nil {
		t.Fatal("name update failed")
	}
	var email, name string
	_ = f.pool.QueryRow(testCtx, `SELECT email,name FROM users WHERE id=$1`, id).Scan(&email, &name)
	if email != "profile@example.com" || name != "Renamed" {
		t.Fatal("name update mutated auth identity")
	}
}
func TestPostgresCanonicalLegacyLookup(t *testing.T) {
	f := newCredentialFixture(t)
	f.legacy("canonical@example.com")
	repo := NewPostgresUserRepository(f.pool)
	u, err := repo.GetByEmail(testCtx, "\u2003Canonical@Example.COM\u2003")
	if err != nil || u.EmailCanonical != "canonical@example.com" || u.EmailVerifiedAt != nil {
		t.Fatal("legacy canonical identity/proof lookup failed")
	}
}

func TestSessionResetRefreshBothOrders(t *testing.T) {
	for _, refreshFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "refresh_first", false: "reset_first"}[refreshFirst], func(t *testing.T) {
			f := newCredentialFixture(t)
			id := f.legacy("order@example.com")
			p := f.login("order@example.com")
			v := f.reset(id, "order@example.com")
			if refreshFirst {
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, _, err := f.repo.RotateRefreshToken(testCtx, p.StoredRefresh.TokenHash, func(u *domain.User, now time.Time) (*domain.SessionPair, error) {
						close(entered)
						<-release
						return issueSynthetic(u, now)
					})
					done <- err
				}()
				<-entered
				resetStarted := make(chan struct{})
				resetDone := make(chan error, 1)
				go func() {
					close(resetStarted)
					resetDone <- f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"})
				}()
				<-resetStarted
				close(release)
				if <-done != nil || <-resetDone != nil {
					t.Fatal("ordered reset/refresh failed")
				}
			} else {
				held := make(chan struct{})
				release := make(chan struct{})
				resetDone := make(chan error, 1)
				f.repo.commit = func(ctx context.Context, tx pgx.Tx) error { close(held); <-release; return tx.Commit(ctx) }
				go func() {
					resetDone <- f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"})
				}()
				<-held
				refreshDone := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					_, _, err := f.repo.RotateRefreshToken(testCtx, p.StoredRefresh.TokenHash, issueSynthetic)
					refreshDone <- err
				}()
				<-started
				close(release)
				if <-resetDone != nil || !errors.Is(<-refreshDone, domain.ErrTokenExpired) {
					t.Fatal("reset-first minted replacement")
				}
			}
			var live int
			_ = f.pool.QueryRow(testCtx, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`).Scan(&live)
			if live != 0 {
				t.Fatal("reset left refresh authority")
			}
		})
	}
}
func TestSessionResetLoginBothOrders(t *testing.T) {
	for _, loginFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "login_first", false: "reset_first"}[loginFirst], func(t *testing.T) {
			f := newCredentialFixture(t)
			id := f.legacy("login-order@example.com")
			v := f.reset(id, "login-order@example.com")
			if loginFirst {
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, _, err := f.repo.LoginAndIssueRefresh(testCtx, "login-order@example.com", func(hash string) bool { close(entered); <-release; return hash == "old hash" }, issueSynthetic)
					done <- err
				}()
				<-entered
				resetDone := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					resetDone <- f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"})
				}()
				<-started
				close(release)
				if <-done != nil || <-resetDone != nil {
					t.Fatal("login-first race failed")
				}
			} else {
				if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); err != nil {
					t.Fatal("reset failed")
				}
				_, _, err := f.repo.LoginAndIssueRefresh(testCtx, "login-order@example.com", func(hash string) bool { return hash == "old hash" }, issueSynthetic)
				if !errors.Is(err, domain.ErrInvalidCredentials) {
					t.Fatal("old password authorized after reset")
				}
			}
			var live int
			_ = f.pool.QueryRow(testCtx, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`).Scan(&live)
			if live != 0 {
				t.Fatal("old-password session escaped reset")
			}
		})
	}
}
func TestPasswordResetStates(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("states@example.com")
	v := f.reset(id, "states@example.com")
	f.now = v.ExpiresAt
	if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); !errors.Is(err, domain.ErrResetTokenExpired) {
		t.Fatal("expiry equality accepted")
	}
	f.now = f.now.Add(time.Minute)
	next, err := f.repo.PreparePasswordReset(testCtx, PasswordResetPreparation{uuid.New(), id, "states@example.com", strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal("new reset failed")
	}
	if err = f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); !errors.Is(err, domain.ErrInvalidResetToken) {
		t.Fatal("superseded reset accepted")
	}
	if err = f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{next.TokenHash, "new hash"}); !errors.Is(err, domain.ErrInvalidResetToken) {
		t.Fatal("prepared reset accepted")
	}
}

func TestPostgresCleanupBoundAndGuards(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("cleanup@example.com")
	_, err := f.pool.Exec(testCtx, `INSERT INTO email_verification_guards VALUES('cleanup@example.com','registration',202,clock_timestamp()+interval '1 minute',clock_timestamp(),clock_timestamp()); INSERT INTO email_verification_requests(id,email_canonical,purpose,generation,credential_hash,expires_at,next_resend_at,delivery_state,invalidated_at,created_at,updated_at) SELECT uuid_generate_v4(),'cleanup@example.com','registration',n,'synthetic hash',clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days','FAILED',clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days' FROM generate_series(1,201)n`)
	if err != nil {
		t.Fatal("cleanup fixture failed")
	}
	_, err = f.pool.Exec(testCtx, `INSERT INTO password_reset_guards VALUES($1,202,clock_timestamp()+interval '1 minute',clock_timestamp(),clock_timestamp())`, id)
	if err != nil {
		t.Fatal("guard seed failed")
	}
	_, err = f.pool.Exec(testCtx, `INSERT INTO password_reset_requests(id,user_id,email_canonical,token_hash,generation,expires_at,next_resend_at,delivery_state,invalidated_at,created_at,updated_at) SELECT uuid_generate_v4(),$1,'cleanup@example.com',lpad(n::text,64,'0'),n,clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days','FAILED',clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days',clock_timestamp()-interval '40 days' FROM generate_series(1,201)n`, id)
	if err != nil {
		t.Fatal("cleanup reset fixture failed")
	}
	f.repo.cleanup()
	var remaining, guards int
	_ = f.pool.QueryRow(testCtx, `SELECT (SELECT count(*) FROM email_verification_requests)+(SELECT count(*) FROM password_reset_requests)`).Scan(&remaining)
	_ = f.pool.QueryRow(testCtx, `SELECT (SELECT count(*) FROM email_verification_guards)+(SELECT count(*) FROM password_reset_guards)`).Scan(&guards)
	if remaining < 302 || guards != 2 {
		t.Fatal("cleanup exceeded 100 total rows or removed guards")
	}
}
func TestPostgresCommitAckAllOperations(t *testing.T) {
	f := newCredentialFixture(t)
	id := f.legacy("ack-all@example.com")
	f.repo.commit = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errors.New("synthetic lost ack")
	}
	p := f.login("ack-all@example.com")
	_, next, err := f.repo.RotateRefreshToken(testCtx, p.StoredRefresh.TokenHash, issueSynthetic)
	if err != nil {
		t.Fatal("rotation reconciliation failed")
	}
	if err = f.repo.RevokeCurrentRefresh(testCtx, id, next.StoredRefresh.TokenHash); err != nil {
		t.Fatal("revoke reconciliation failed")
	}
	v := f.reset(id, "ack-all@example.com")
	if err = f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); err != nil {
		t.Fatal("reset reconciliation failed")
	}
}
func TestPasswordResetWriteFailuresRollback(t *testing.T) {
	for _, stage := range []string{"password", "consume", "invalidate", "revoke"} {
		t.Run(stage, func(t *testing.T) {
			f := newCredentialFixture(t)
			id := f.legacy("write-fault@example.com")
			p := f.login("write-fault@example.com")
			v := f.reset(id, "write-fault@example.com")
			constraint := map[string]string{
				"password":   `ALTER TABLE users ADD CONSTRAINT fault CHECK (password_hash='old hash')`,
				"consume":    `ALTER TABLE password_reset_requests ADD CONSTRAINT fault CHECK (consumed_at IS NULL)`,
				"invalidate": `ALTER TABLE password_reset_requests ADD CONSTRAINT fault CHECK (invalidated_at IS NULL)`,
				"revoke":     `ALTER TABLE refresh_tokens ADD CONSTRAINT fault CHECK (revoked_at IS NULL)`,
			}[stage]
			if stage == "invalidate" {
				_, err := f.pool.Exec(testCtx, `INSERT INTO password_reset_requests(id,user_id,email_canonical,token_hash,generation,expires_at,next_resend_at,delivery_state,created_at,updated_at)VALUES($1,$2,'write-fault@example.com',$3,2,$4,$4,'PREPARED',$4,$4)`, uuid.New(), id, strings.Repeat("b", 64), f.now.Add(time.Hour))
				if err != nil {
					t.Fatal("pending seed failed")
				}
			}
			if _, err := f.pool.Exec(testCtx, constraint); err != nil {
				t.Fatal("fault injection failed")
			}
			if err := f.repo.ResetPasswordAndRevokeSessions(testCtx, ResetCompletion{v.TokenHash, "new hash"}); err == nil {
				t.Fatal("faulted reset reported success")
			}
			var hash string
			var proof, revoked, consumed *time.Time
			_ = f.pool.QueryRow(testCtx, `SELECT password_hash,email_verified_at FROM users WHERE id=$1`, id).Scan(&hash, &proof)
			_ = f.pool.QueryRow(testCtx, `SELECT revoked_at FROM refresh_tokens WHERE id=$1`, p.StoredRefresh.ID).Scan(&revoked)
			_ = f.pool.QueryRow(testCtx, `SELECT consumed_at FROM password_reset_requests WHERE id=$1`, v.ID).Scan(&consumed)
			if hash != "old hash" || proof != nil || revoked != nil || consumed != nil {
				t.Fatal("write failure partially committed")
			}
		})
	}
}
