package repository

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/user/internal/domain"
	"time"
)

type PostgresAuthRepository struct {
	pool   *pgxpool.Pool
	clock  func() time.Time
	commit func(context.Context, pgx.Tx) error
}

func NewPostgresAuthRepository(pool *pgxpool.Pool, clocks ...func() time.Time) *PostgresAuthRepository {
	clock := time.Now
	if len(clocks) > 0 {
		clock = clocks[0]
	}
	return &PostgresAuthRepository{pool: pool, clock: func() time.Time { return clock().UTC().Truncate(time.Microsecond) }, commit: func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }}
}

const userColumns = `id,email,email_canonical,email_verified_at,password_hash,name,role,created_at,updated_at,locale,credential_version`
const verificationColumns = `id,email_canonical,purpose,credential_hash,generation,attempt_count,max_attempts,delivery_state,expires_at,next_resend_at,created_at,updated_at,consumed_at,invalidated_at`
const resetColumns = `id,user_id,email_canonical,token_hash,generation,delivery_state,expires_at,next_resend_at,created_at,updated_at,consumed_at,invalidated_at`

func scanUser(row pgx.Row) (*domain.User, error) {
	u := &domain.User{}
	err := row.Scan(&u.ID, &u.Email, &u.EmailCanonical, &u.EmailVerifiedAt, &u.PasswordHash, &u.Name, &u.Role, &u.CreatedAt, &u.UpdatedAt, &u.Locale, &u.CredentialVersion)
	return u, err
}
func scanVerification(row pgx.Row) (*domain.RegistrationVerification, error) {
	v := &domain.RegistrationVerification{}
	err := row.Scan(&v.ID, &v.CanonicalEmail, &v.Purpose, &v.CredentialHash, &v.Generation, &v.AttemptCount, &v.MaxAttempts, &v.DeliveryState, &v.ExpiresAt, &v.NextResendAt, &v.CreatedAt, &v.UpdatedAt, &v.ConsumedAt, &v.InvalidatedAt)
	return v, err
}
func scanReset(row pgx.Row) (*domain.PasswordResetRequest, error) {
	v := &domain.PasswordResetRequest{}
	err := row.Scan(&v.ID, &v.UserID, &v.CanonicalEmail, &v.TokenHash, &v.Generation, &v.DeliveryState, &v.ExpiresAt, &v.NextResendAt, &v.CreatedAt, &v.UpdatedAt, &v.ConsumedAt, &v.InvalidatedAt)
	return v, err
}
func registrationLock(ctx context.Context, tx pgx.Tx, email string) error {
	sum := sha256.Sum256([]byte("rushgo:user:registration\x00" + email))
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(binary.BigEndian.Uint64(sum[:8])))
	return err
}
func (r *PostgresAuthRepository) finish(ctx context.Context, tx pgx.Tx, establish func(context.Context) bool) error {
	if err := r.commit(ctx, tx); err == nil {
		return nil
	}
	// Never replay writes. Even a failed request context gets one bounded authoritative read.
	check, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if establish(check) {
		return nil
	}
	return domain.ErrTemporaryUnavailable
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (r *PostgresAuthRepository) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	return tx, nil
}

func (r *PostgresAuthRepository) PrepareRegistrationVerification(ctx context.Context, p RegistrationPreparation) (*domain.RegistrationVerification, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if registrationLock(ctx, tx, p.CanonicalEmail) != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	var generation int64
	var resend time.Time
	err = tx.QueryRow(ctx, `SELECT last_generation,next_resend_at FROM email_verification_guards WHERE email_canonical=$1 AND purpose='registration' FOR UPDATE`, p.CanonicalEmail).Scan(&generation, &resend)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if now.Before(resend) {
		return nil, domain.ErrRateLimited
	}
	generation++
	v := &domain.RegistrationVerification{ID: p.ID, CanonicalEmail: p.CanonicalEmail, Purpose: "registration", CredentialHash: p.CredentialHash, Generation: generation, MaxAttempts: 5, DeliveryState: domain.Prepared, ExpiresAt: now.Add(10 * time.Minute), NextResendAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	if _, err = tx.Exec(ctx, `INSERT INTO email_verification_guards(email_canonical,purpose,last_generation,next_resend_at,created_at,updated_at) VALUES($1,'registration',$2,$3,$4,$4) ON CONFLICT(email_canonical,purpose) DO UPDATE SET last_generation=$2,next_resend_at=$3,updated_at=$4`, p.CanonicalEmail, generation, v.NextResendAt, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE email_verification_requests SET invalidated_at=$2,updated_at=$2 WHERE email_canonical=$1 AND purpose='registration' AND consumed_at IS NULL AND invalidated_at IS NULL`, p.CanonicalEmail, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO email_verification_requests(id,email_canonical,purpose,generation,credential_hash,expires_at,next_resend_at,delivery_state,created_at,updated_at)VALUES($1,$2,'registration',$3,$4,$5,$6,'PREPARED',$7,$7)`, p.ID, p.CanonicalEmail, generation, p.CredentialHash, v.ExpiresAt, v.NextResendAt, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	err = r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM email_verification_requests WHERE id=$1 AND email_canonical=$2 AND credential_hash=$3)`, p.ID, p.CanonicalEmail, p.CredentialHash).Scan(&found) == nil && found
	})
	if err != nil {
		return nil, err
	}
	r.cleanup()
	return v, nil
}
func deliveryState(outcome domain.DeliveryOutcome) domain.DeliveryState {
	switch outcome {
	case domain.Accepted:
		return domain.AcceptedForDelivery
	case domain.Unknown:
		return domain.UnknownDelivery
	default:
		return domain.Failed
	}
}
func (r *PostgresAuthRepository) FinalizeRegistrationDelivery(ctx context.Context, id uuid.UUID, outcome domain.DeliveryOutcome) error {
	var email string
	if err := r.pool.QueryRow(ctx, `SELECT email_canonical FROM email_verification_requests WHERE id=$1`, id).Scan(&email); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrInvalidCode
		}
		return domain.ErrTemporaryUnavailable
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if registrationLock(ctx, tx, email) != nil {
		return domain.ErrTemporaryUnavailable
	}
	v, err := scanVerification(tx.QueryRow(ctx, `SELECT `+verificationColumns+` FROM email_verification_requests WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT last_generation FROM email_verification_guards WHERE email_canonical=$1 AND purpose='registration'`, email).Scan(&current); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if v.Generation != current || v.InvalidatedAt != nil || v.ConsumedAt != nil || !now.Before(v.ExpiresAt) || v.DeliveryState != domain.Prepared {
		return domain.ErrInvalidCode
	}
	state := deliveryState(outcome)
	var invalidated *time.Time
	if state != domain.AcceptedForDelivery {
		invalidated = &now
	}
	if _, err = tx.Exec(ctx, `UPDATE email_verification_requests SET delivery_state=$2,invalidated_at=$3,updated_at=$4 WHERE id=$1`, id, state, invalidated, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM email_verification_requests WHERE id=$1 AND delivery_state=$2)`, id, state).Scan(&found) == nil && found
	})
}
func (r *PostgresAuthRepository) RegisterVerifiedUser(ctx context.Context, p domain.RegisterCompletion, compare func(string) bool) (*domain.User, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if registrationLock(ctx, tx, p.CanonicalEmail) != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	v, err := scanVerification(tx.QueryRow(ctx, `SELECT `+verificationColumns+` FROM email_verification_requests WHERE id=$1 FOR UPDATE`, p.VerificationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrInvalidCode
	}
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if v.CanonicalEmail != p.CanonicalEmail || v.Purpose != "registration" || v.DeliveryState != domain.AcceptedForDelivery || v.InvalidatedAt != nil || v.ConsumedAt != nil {
		return nil, domain.ErrInvalidCode
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT last_generation FROM email_verification_guards WHERE email_canonical=$1 AND purpose='registration'`, p.CanonicalEmail).Scan(&current); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if current != v.Generation {
		return nil, domain.ErrInvalidCode
	}
	now := r.clock().UTC()
	if !now.Before(v.ExpiresAt) {
		return nil, domain.ErrCodeExpired
	}
	if v.AttemptCount >= v.MaxAttempts {
		return nil, domain.ErrCodeAttemptsExhausted
	}
	if !compare(v.CredentialHash) {
		count := v.AttemptCount + 1
		if _, err = tx.Exec(ctx, `UPDATE email_verification_requests SET attempt_count=$2,updated_at=$3 WHERE id=$1`, v.ID, count, now); err != nil {
			return nil, domain.ErrTemporaryUnavailable
		}
		if err = r.finish(ctx, tx, func(c context.Context) bool {
			var actual int
			return r.pool.QueryRow(c, `SELECT attempt_count FROM email_verification_requests WHERE id=$1`, v.ID).Scan(&actual) == nil && actual >= count
		}); err != nil {
			return nil, err
		}
		if count >= 5 {
			return nil, domain.ErrCodeAttemptsExhausted
		}
		return nil, domain.ErrInvalidCode
	}
	u := &domain.User{ID: p.UserID, Email: p.CanonicalEmail, EmailCanonical: p.CanonicalEmail, EmailVerifiedAt: &now, PasswordHash: p.PasswordHash, Name: p.Name, Role: "user", CreatedAt: now, UpdatedAt: now}
	tag, err := tx.Exec(ctx, `INSERT INTO users(id,email,email_canonical,email_verified_at,password_hash,name,role,created_at,updated_at)VALUES($1,$2,$2,$3,$4,$5,'user',$3,$3) ON CONFLICT DO NOTHING`, p.UserID, p.CanonicalEmail, now, p.PasswordHash, p.Name)
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	created := tag.RowsAffected() == 1
	if _, err = tx.Exec(ctx, `UPDATE email_verification_requests SET consumed_at=$2,updated_at=$2 WHERE id=$1`, v.ID, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	err = r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		if created {
			return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM users u JOIN email_verification_requests v ON v.id=$2 WHERE u.id=$1 AND v.consumed_at IS NOT NULL)`, p.UserID, v.ID).Scan(&found) == nil && found
		}
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM users u JOIN email_verification_requests v ON v.id=$2 WHERE u.email_canonical=$1 AND v.consumed_at IS NOT NULL)`, p.CanonicalEmail, v.ID).Scan(&found) == nil && found
	})
	if err != nil {
		return nil, err
	}
	r.cleanup()
	if !created {
		return nil, domain.ErrEmailExists
	}
	return u, nil
}
func (r *PostgresAuthRepository) PreparePasswordReset(ctx context.Context, p PasswordResetPreparation) (*domain.PasswordResetRequest, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, p.UserID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if u.EmailCanonical != p.CanonicalEmail {
		return nil, domain.ErrInvalidResetToken
	}
	var generation int64
	var resend time.Time
	err = tx.QueryRow(ctx, `SELECT last_generation,next_resend_at FROM password_reset_guards WHERE user_id=$1 FOR UPDATE`, p.UserID).Scan(&generation, &resend)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if now.Before(resend) {
		return nil, domain.ErrRateLimited
	}
	generation++
	v := &domain.PasswordResetRequest{ID: p.ID, UserID: p.UserID, CanonicalEmail: p.CanonicalEmail, TokenHash: p.TokenHash, Generation: generation, DeliveryState: domain.Prepared, ExpiresAt: now.Add(20 * time.Minute), NextResendAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	if _, err = tx.Exec(ctx, `INSERT INTO password_reset_guards(user_id,last_generation,next_resend_at,created_at,updated_at)VALUES($1,$2,$3,$4,$4) ON CONFLICT(user_id) DO UPDATE SET last_generation=$2,next_resend_at=$3,updated_at=$4`, p.UserID, generation, v.NextResendAt, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE password_reset_requests SET invalidated_at=$2,updated_at=$2 WHERE user_id=$1 AND consumed_at IS NULL AND invalidated_at IS NULL`, p.UserID, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO password_reset_requests(id,user_id,email_canonical,token_hash,generation,expires_at,next_resend_at,delivery_state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,'PREPARED',$8,$8)`, v.ID, v.UserID, v.CanonicalEmail, v.TokenHash, v.Generation, v.ExpiresAt, v.NextResendAt, now); err != nil {
		return nil, domain.ErrTemporaryUnavailable
	}
	err = r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM password_reset_requests WHERE id=$1 AND user_id=$2 AND token_hash=$3)`, v.ID, v.UserID, v.TokenHash).Scan(&found) == nil && found
	})
	if err != nil {
		return nil, err
	}
	r.cleanup()
	return v, nil
}
func (r *PostgresAuthRepository) FinalizePasswordResetDelivery(ctx context.Context, id uuid.UUID, outcome domain.DeliveryOutcome) error {
	var owner uuid.UUID
	if err := r.pool.QueryRow(ctx, `SELECT user_id FROM password_reset_requests WHERE id=$1`, id).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrInvalidResetToken
		}
		return domain.ErrTemporaryUnavailable
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, owner))
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	v, err := scanReset(tx.QueryRow(ctx, `SELECT `+resetColumns+` FROM password_reset_requests WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT last_generation FROM password_reset_guards WHERE user_id=$1`, owner).Scan(&current); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if v.CanonicalEmail != u.EmailCanonical || v.Generation != current || v.InvalidatedAt != nil || v.ConsumedAt != nil || !now.Before(v.ExpiresAt) || v.DeliveryState != domain.Prepared {
		return domain.ErrInvalidResetToken
	}
	state := deliveryState(outcome)
	var invalidated *time.Time
	if state != domain.AcceptedForDelivery {
		invalidated = &now
	}
	if _, err = tx.Exec(ctx, `UPDATE password_reset_requests SET delivery_state=$2,invalidated_at=$3,updated_at=$4 WHERE id=$1`, id, state, invalidated, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM password_reset_requests WHERE id=$1 AND delivery_state=$2)`, id, state).Scan(&found) == nil && found
	})
}
func (r *PostgresAuthRepository) ResetPasswordAndRevokeSessions(ctx context.Context, p ResetCompletion) error {
	var owner uuid.UUID
	if err := r.pool.QueryRow(ctx, `SELECT user_id FROM password_reset_requests WHERE token_hash=$1`, p.TokenHash).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrInvalidResetToken
		}
		return domain.ErrTemporaryUnavailable
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, owner))
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	v, err := scanReset(tx.QueryRow(ctx, `SELECT `+resetColumns+` FROM password_reset_requests WHERE token_hash=$1 FOR UPDATE`, p.TokenHash))
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if v.ConsumedAt != nil {
		return domain.ErrResetTokenUsed
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT last_generation FROM password_reset_guards WHERE user_id=$1`, owner).Scan(&current); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if v.UserID != u.ID || v.CanonicalEmail != u.EmailCanonical || v.Generation != current || v.InvalidatedAt != nil || v.DeliveryState != domain.AcceptedForDelivery {
		return domain.ErrInvalidResetToken
	}
	now := r.clock().UTC()
	if !now.Before(v.ExpiresAt) {
		return domain.ErrResetTokenExpired
	}
	if _, err = tx.Exec(ctx, `UPDATE users SET password_hash=$2,email_verified_at=COALESCE(email_verified_at,$3),updated_at=$3,credential_version=credential_version+1 WHERE id=$1`, owner, p.PasswordHash, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE password_reset_requests SET consumed_at=$2,updated_at=$2 WHERE id=$1`, v.ID, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE password_reset_requests SET invalidated_at=$2,updated_at=$2 WHERE user_id=$1 AND id<>$3 AND consumed_at IS NULL AND invalidated_at IS NULL`, owner, now, v.ID); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at=$2 WHERE user_id=$1 AND revoked_at IS NULL`, owner, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE account_email_changes SET invalidated_at=$2 WHERE user_id=$1 AND consumed_at IS NULL AND invalidated_at IS NULL`, owner, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	err = r.finish(ctx, tx, func(c context.Context) bool {
		var committed bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM password_reset_requests p JOIN users u ON u.id=p.user_id WHERE p.id=$1 AND p.consumed_at IS NOT NULL AND u.password_hash=$2)`, v.ID, p.PasswordHash).Scan(&committed) == nil && committed
	})
	if err == nil {
		r.cleanup()
	}
	return err
}
func insertSession(ctx context.Context, tx pgx.Tx, p *domain.SessionPair, owner uuid.UUID) error {
	if p == nil || p.StoredRefresh.UserID != owner || p.StoredRefresh.ID == uuid.Nil || p.StoredRefresh.TokenHash == "" {
		return domain.ErrTemporaryUnavailable
	}
	t := p.StoredRefresh
	_, err := tx.Exec(ctx, `INSERT INTO refresh_tokens(id,user_id,token_hash,expires_at,created_at)VALUES($1,$2,$3,$4,$5)`, t.ID, t.UserID, t.TokenHash, t.ExpiresAt, t.CreatedAt)
	return err
}
func (r *PostgresAuthRepository) sessionCommitted(c context.Context, p *domain.SessionPair) bool {
	var found bool
	return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM refresh_tokens WHERE id=$1 AND user_id=$2 AND token_hash=$3)`, p.StoredRefresh.ID, p.StoredRefresh.UserID, p.StoredRefresh.TokenHash).Scan(&found) == nil && found
}
func (r *PostgresAuthRepository) LoginAndIssueRefresh(ctx context.Context, email string, verify func(string) bool, issue IssueSession) (*domain.User, *domain.SessionPair, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rollback(tx)
	u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE email_canonical=$1 FOR UPDATE`, email))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, domain.ErrInvalidCredentials
	}
	if err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if !verify(u.PasswordHash) {
		return nil, nil, domain.ErrInvalidCredentials
	}
	p, err := issue(u, now)
	if err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if insertSession(ctx, tx, p, u.ID) != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if err = r.finish(ctx, tx, func(c context.Context) bool { return r.sessionCommitted(c, p) }); err != nil {
		return nil, nil, err
	}
	return u, p, nil
}
func (r *PostgresAuthRepository) RotateRefreshToken(ctx context.Context, hash string, issue IssueSession) (*domain.User, *domain.SessionPair, error) {
	var owner uuid.UUID
	if err := r.pool.QueryRow(ctx, `SELECT user_id FROM refresh_tokens WHERE token_hash=$1`, hash).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, domain.ErrTokenExpired
		}
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rollback(tx)
	u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, owner))
	if err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	var id, userID uuid.UUID
	var expiry time.Time
	var revoked *time.Time
	err = tx.QueryRow(ctx, `SELECT id,user_id,expires_at,revoked_at FROM refresh_tokens WHERE token_hash=$1 FOR UPDATE`, hash).Scan(&id, &userID, &expiry, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, domain.ErrTokenExpired
	}
	if err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	now := r.clock().UTC()
	if userID != owner || revoked != nil || !now.Before(expiry) {
		return nil, nil, domain.ErrTokenExpired
	}
	p, err := issue(u, now)
	if err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at=$2 WHERE id=$1`, id, now); err != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if insertSession(ctx, tx, p, owner) != nil {
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if err = r.finish(ctx, tx, func(c context.Context) bool { return r.sessionCommitted(c, p) }); err != nil {
		return nil, nil, err
	}
	return u, p, nil
}
func (r *PostgresAuthRepository) RevokeCurrentRefresh(ctx context.Context, owner uuid.UUID, hash string) error {
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, owner)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrInvalidSession
		}
		return domain.ErrTemporaryUnavailable
	}
	var id, userID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id,user_id FROM refresh_tokens WHERE token_hash=$1 FOR UPDATE`, hash).Scan(&id, &userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrInvalidSession
	}
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if userID != owner {
		return domain.ErrInvalidSession
	}
	now := r.clock().UTC()
	if _, err = tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at=COALESCE(revoked_at,$2) WHERE id=$1`, id, now); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return r.finish(ctx, tx, func(c context.Context) bool {
		var found bool
		return r.pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM refresh_tokens WHERE id=$1 AND user_id=$2 AND revoked_at IS NOT NULL)`, id, owner).Scan(&found) == nil && found
	})
}
func (r *PostgresAuthRepository) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Current generation/guard rows survive forever; cleanup is never correctness authority.
	_, _ = r.pool.Exec(ctx, `WITH candidates AS MATERIALIZED (
 SELECT * FROM (
  SELECT 'verification' AS kind,v.id,v.expires_at FROM email_verification_requests v JOIN email_verification_guards g USING(email_canonical,purpose) WHERE v.generation<g.last_generation AND v.expires_at<clock_timestamp()-interval '30 days' AND (v.consumed_at IS NOT NULL OR v.invalidated_at IS NOT NULL OR v.delivery_state IN ('FAILED','UNKNOWN'))
  UNION ALL
  SELECT 'reset',v.id,v.expires_at FROM password_reset_requests v JOIN password_reset_guards g USING(user_id) WHERE v.generation<g.last_generation AND v.expires_at<clock_timestamp()-interval '30 days' AND (v.consumed_at IS NOT NULL OR v.invalidated_at IS NOT NULL OR v.delivery_state IN ('FAILED','UNKNOWN'))
 ) terminal ORDER BY expires_at,id LIMIT 100
 ), removed AS (DELETE FROM email_verification_requests WHERE id IN (SELECT id FROM candidates WHERE kind='verification') RETURNING id)
 DELETE FROM password_reset_requests WHERE id IN (SELECT id FROM candidates WHERE kind='reset')`)
}
