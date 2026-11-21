package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/user/internal/domain"
	"time"
)

type AccountSecurityRepository interface {
	ChangePassword(context.Context, uuid.UUID, string, func(string) bool) error
	PrepareEmailChange(context.Context, uuid.UUID, string, string, func(string) bool) (uuid.UUID, time.Time, error)
	FinalizeEmailChangeDelivery(context.Context, uuid.UUID, domain.DeliveryOutcome) error
	CompleteEmailChange(context.Context, uuid.UUID, uuid.UUID, func(string) bool, func(string) bool) error
}

// Serialize credentials, refresh issuance and reauthentication on the same User
// row. Failed password/code attempts are committed without changing credentials.
func (r *PostgresAuthRepository) beginAccountChange(ctx context.Context, id uuid.UUID, verify func(string) bool) (pgx.Tx, *domain.User, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	user, err := scanUser(tx.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		rollback(tx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, domain.ErrInvalidCredentials
		}
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	var attempts int
	err = tx.QueryRow(ctx, `INSERT INTO account_change_attempts(user_id,window_started_at,attempts) VALUES($1,clock_timestamp(),1)
 ON CONFLICT(user_id) DO UPDATE SET attempts=CASE WHEN account_change_attempts.window_started_at<clock_timestamp()-interval '5 minutes' THEN 1 ELSE account_change_attempts.attempts+1 END,
 window_started_at=CASE WHEN account_change_attempts.window_started_at<clock_timestamp()-interval '5 minutes' THEN clock_timestamp() ELSE account_change_attempts.window_started_at END RETURNING attempts`, id).Scan(&attempts)
	if err != nil {
		rollback(tx)
		return nil, nil, domain.ErrTemporaryUnavailable
	}
	if attempts > 10 {
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, domain.ErrTemporaryUnavailable
		}
		return nil, nil, domain.ErrRateLimited
	}
	if verify == nil || !verify(user.PasswordHash) {
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, domain.ErrTemporaryUnavailable
		}
		return nil, nil, domain.ErrInvalidCredentials
	}
	return tx, user, nil
}

func revokeAccountSessions(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at=clock_timestamp() WHERE user_id=$1 AND revoked_at IS NULL`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE password_reset_requests SET invalidated_at=clock_timestamp() WHERE user_id=$1 AND consumed_at IS NULL AND invalidated_at IS NULL`, id)
	return err
}

func (r *PostgresAuthRepository) ChangePassword(ctx context.Context, id uuid.UUID, newHash string, verify func(string) bool) error {
	tx, _, err := r.beginAccountChange(ctx, id, verify)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `UPDATE users SET password_hash=$2,credential_version=credential_version+1,updated_at=clock_timestamp() WHERE id=$1`, id, newHash); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if err = revokeAccountSessions(ctx, tx, id); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE account_email_changes SET invalidated_at=clock_timestamp() WHERE user_id=$1 AND consumed_at IS NULL AND invalidated_at IS NULL`, id); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO account_security_audit(id,user_id,action) VALUES($1,$2,'password_changed')`, uuid.New(), id); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return nil
}

func (r *PostgresAuthRepository) PrepareEmailChange(ctx context.Context, id uuid.UUID, email, hash string, verify func(string) bool) (uuid.UUID, time.Time, error) {
	tx, user, err := r.beginAccountChange(ctx, id, verify)
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	defer rollback(tx)
	if user.EmailCanonical == email {
		return uuid.Nil, time.Time{}, domain.ErrValidation
	}
	if err = registrationLock(ctx, tx, email); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE email_canonical=$1)`, email).Scan(&exists); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	if exists {
		return uuid.Nil, time.Time{}, domain.ErrEmailExists
	}
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM account_email_changes WHERE user_id=$1 AND created_at>clock_timestamp()-interval '1 minute')`, id).Scan(&exists); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	if exists {
		return uuid.Nil, time.Time{}, domain.ErrRateLimited
	}
	if _, err = tx.Exec(ctx, `UPDATE account_email_changes SET invalidated_at=clock_timestamp() WHERE user_id=$1 AND consumed_at IS NULL AND invalidated_at IS NULL`, id); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	change := uuid.New()
	var expiry time.Time
	err = tx.QueryRow(ctx, `INSERT INTO account_email_changes(id,user_id,email_canonical,credential_hash,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '10 minutes') RETURNING expires_at`, change, id, email, hash).Scan(&expiry)
	if err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO account_security_audit(id,user_id,action) VALUES($1,$2,'email_change_requested')`, change, id); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return uuid.Nil, time.Time{}, domain.ErrTemporaryUnavailable
	}
	return change, expiry, nil
}

func (r *PostgresAuthRepository) FinalizeEmailChangeDelivery(ctx context.Context, id uuid.UUID, outcome domain.DeliveryOutcome) error {
	state := "UNKNOWN"
	if outcome == domain.Accepted {
		state = "ACCEPTED"
	}
	if outcome == domain.ExplicitFailure || outcome == domain.ConfigurationFailure {
		state = "FAILED"
	}
	_, err := r.pool.Exec(ctx, `UPDATE account_email_changes SET delivery_state=$2 WHERE id=$1 AND delivery_state='PREPARED'`, id, state)
	return err
}

func (r *PostgresAuthRepository) CompleteEmailChange(ctx context.Context, id, change uuid.UUID, verifyPassword, verifyCode func(string) bool) error {
	tx, _, err := r.beginAccountChange(ctx, id, verifyPassword)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var email, hash, state string
	var attempts int
	var expiry time.Time
	var used, invalid *time.Time
	err = tx.QueryRow(ctx, `SELECT email_canonical,credential_hash,delivery_state,attempts,expires_at,consumed_at,invalidated_at FROM account_email_changes WHERE id=$1 AND user_id=$2 FOR UPDATE`, change, id).Scan(&email, &hash, &state, &attempts, &expiry, &used, &invalid)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrInvalidCode
	}
	if err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if used != nil || invalid != nil || state != "ACCEPTED" {
		return domain.ErrInvalidCode
	}
	if !r.clock().Before(expiry) {
		return domain.ErrCodeExpired
	}
	if attempts >= 5 {
		return domain.ErrCodeAttemptsExhausted
	}
	if verifyCode == nil || !verifyCode(hash) {
		if _, err = tx.Exec(ctx, `UPDATE account_email_changes SET attempts=attempts+1 WHERE id=$1`, change); err != nil {
			return domain.ErrTemporaryUnavailable
		}
		if err = tx.Commit(ctx); err != nil {
			return domain.ErrTemporaryUnavailable
		}
		return domain.ErrInvalidCode
	}
	if err = registrationLock(ctx, tx, email); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE email_canonical=$1 AND id<>$2)`, email, id).Scan(&exists); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if exists {
		return domain.ErrEmailExists
	}
	if _, err = tx.Exec(ctx, `UPDATE users SET email=$2,email_canonical=$2,email_verified_at=clock_timestamp(),credential_version=credential_version+1,updated_at=clock_timestamp() WHERE id=$1`, id, email); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `UPDATE account_email_changes SET consumed_at=clock_timestamp() WHERE id=$1`, change); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if err = revokeAccountSessions(ctx, tx, id); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO account_security_audit(id,user_id,action) VALUES($1,$2,'email_changed')`, uuid.New(), id); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.ErrTemporaryUnavailable
	}
	return nil
}
