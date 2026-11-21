package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/refundtime"
)

type PostgresRefundTimeRepository struct {
	pool *pgxpool.Pool
}

var _ refundtime.Authority = (*PostgresRefundTimeRepository)(nil)

func NewPostgresRefundTimeRepository(pool *pgxpool.Pool) *PostgresRefundTimeRepository {
	return &PostgresRefundTimeRepository{pool: pool}
}

func (r *PostgresRefundTimeRepository) Acquire(ctx context.Context, binding refundtime.Binding) (refundtime.Permit, error) {
	if !binding.Valid() {
		return refundtime.Permit{}, ErrInvalidRefundTimeBinding
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return refundtime.Permit{}, fmt.Errorf("begin refund time acquisition: %w", err)
	}
	defer tx.Rollback(ctx)

	startsAt, dateVersion, eventExists, err := lockRefundTimeEvent(ctx, tx, binding.EventID)
	if err != nil {
		return refundtime.Permit{}, err
	}
	if !eventExists {
		stored, found, err := refundTimePermitByAttempt(ctx, tx, binding.AttemptID, true)
		if err != nil {
			return refundtime.Permit{}, err
		}
		if found {
			if stored.permit.Binding != binding {
				return refundtime.Permit{}, ErrBindingMismatch
			}
			return commitRefundTimePermit(ctx, tx, stored.permit)
		}
		stored, found, err = refundTimePermitByRequest(ctx, tx, binding.RequestID, true)
		if err != nil {
			return refundtime.Permit{}, err
		}
		if found {
			return refundtime.Permit{}, ErrBindingMismatch
		}
		return refundtime.Permit{}, ErrNotFound
	}

	stored, found, err := refundTimePermitByAttempt(ctx, tx, binding.AttemptID, true)
	if err != nil {
		return refundtime.Permit{}, err
	}
	if found {
		if stored.permit.Binding != binding {
			return refundtime.Permit{}, ErrBindingMismatch
		}
		return commitRefundTimePermit(ctx, tx, stored.permit)
	}
	stored, found, err = refundTimePermitByRequest(ctx, tx, binding.RequestID, true)
	if err != nil {
		return refundtime.Permit{}, err
	}
	if found {
		return refundtime.Permit{}, ErrBindingMismatch
	}

	var qualifiedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&qualifiedAt); err != nil {
		return refundtime.Permit{}, fmt.Errorf("read Event database qualification time: %w", err)
	}
	state := "QUALIFIED"
	if !qualifiedAt.Before(startsAt) {
		state = "REJECTED"
	}
	insertTag, err := tx.Exec(ctx, `INSERT INTO event_refund_time_permits(
		attempt_id, request_id, booking_id, payment_id, event_id, approver_id,
		generation, fingerprint, state, starts_at, qualified_at, event_date_version
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT DO NOTHING`,
		binding.AttemptID, binding.RequestID, binding.BookingID, binding.PaymentID, binding.EventID, binding.ApproverID,
		binding.Generation, binding.Fingerprint, state, startsAt, qualifiedAt, dateVersion)
	if err != nil {
		return refundtime.Permit{}, fmt.Errorf("persist refund time qualification: %w", err)
	}
	permit := refundtime.Permit{Binding: binding, State: state, StartsAt: startsAt, QualifiedAt: qualifiedAt, Version: dateVersion}
	if insertTag.RowsAffected() == 0 {
		stored, found, err = refundTimePermitByAttempt(ctx, tx, binding.AttemptID, true)
		if err != nil {
			return refundtime.Permit{}, err
		}
		if found {
			if stored.permit.Binding != binding {
				return refundtime.Permit{}, ErrBindingMismatch
			}
			return commitRefundTimePermit(ctx, tx, stored.permit)
		}
		if _, found, err = refundTimePermitByRequest(ctx, tx, binding.RequestID, true); err != nil {
			return refundtime.Permit{}, err
		} else if found {
			return refundtime.Permit{}, ErrBindingMismatch
		}
		return refundtime.Permit{}, fmt.Errorf("refund time insert conflict without matching row")
	}
	return commitRefundTimePermit(ctx, tx, permit)
}

func (r *PostgresRefundTimeRepository) Release(ctx context.Context, binding refundtime.Binding, outcome string) (refundtime.Permit, error) {
	if !binding.Valid() {
		return refundtime.Permit{}, ErrInvalidRefundTimeBinding
	}
	if !validRefundTimeTerminalOutcome(outcome) {
		return refundtime.Permit{}, ErrInvalidRefundTimeOutcome
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return refundtime.Permit{}, fmt.Errorf("begin refund time release: %w", err)
	}
	defer tx.Rollback(ctx)

	_, _, _, err = lockRefundTimeEvent(ctx, tx, binding.EventID)
	if err != nil {
		return refundtime.Permit{}, err
	}
	stored, found, err := refundTimePermitByAttempt(ctx, tx, binding.AttemptID, true)
	if err != nil {
		return refundtime.Permit{}, err
	}
	if !found {
		stored, found, err = refundTimePermitByRequest(ctx, tx, binding.RequestID, true)
		if err != nil {
			return refundtime.Permit{}, err
		}
		if found {
			return refundtime.Permit{}, ErrBindingMismatch
		}
		insertTag, err := tx.Exec(ctx, `INSERT INTO event_refund_time_permits(
			attempt_id, request_id, booking_id, payment_id, event_id, approver_id,
			generation, fingerprint, state, outcome
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'RELEASED',$9) ON CONFLICT DO NOTHING`,
			binding.AttemptID, binding.RequestID, binding.BookingID, binding.PaymentID, binding.EventID, binding.ApproverID,
			binding.Generation, binding.Fingerprint, outcome)
		if err != nil {
			return refundtime.Permit{}, fmt.Errorf("persist refund time release tombstone: %w", err)
		}
		if insertTag.RowsAffected() == 0 {
			stored, found, err = refundTimePermitByAttempt(ctx, tx, binding.AttemptID, true)
			if err != nil {
				return refundtime.Permit{}, err
			}
			if !found {
				if _, found, err = refundTimePermitByRequest(ctx, tx, binding.RequestID, true); err != nil {
					return refundtime.Permit{}, err
				} else if found {
					return refundtime.Permit{}, ErrBindingMismatch
				}
				return refundtime.Permit{}, fmt.Errorf("refund time tombstone conflict without matching row")
			}
			if stored.permit.Binding != binding {
				return refundtime.Permit{}, ErrBindingMismatch
			}
			if stored.permit.State != "RELEASED" || stored.outcome != outcome {
				return refundtime.Permit{}, ErrConflict
			}
			return commitRefundTimePermit(ctx, tx, stored.permit)
		}
		return commitRefundTimePermit(ctx, tx, refundtime.Permit{Binding: binding, State: "RELEASED"})
	}
	if stored.permit.Binding != binding {
		return refundtime.Permit{}, ErrBindingMismatch
	}
	if stored.permit.State == "RELEASED" {
		if stored.outcome != outcome {
			return refundtime.Permit{}, ErrConflict
		}
		return commitRefundTimePermit(ctx, tx, stored.permit)
	}
	if stored.permit.State == "REJECTED" && outcome == "APPROVED" {
		return refundtime.Permit{}, ErrConflict
	}
	updated, err := scanStoredRefundTimePermit(tx.QueryRow(ctx, `UPDATE event_refund_time_permits
		SET state='RELEASED', outcome=$2, updated_at=clock_timestamp()
		WHERE attempt_id=$1
		RETURNING attempt_id,request_id,booking_id,payment_id,event_id,approver_id,generation,fingerprint,state,
			starts_at,qualified_at,event_date_version,outcome`, binding.AttemptID, outcome))
	if err != nil {
		return refundtime.Permit{}, fmt.Errorf("persist refund time terminal outcome: %w", err)
	}
	return commitRefundTimePermit(ctx, tx, updated.permit)
}

type storedRefundTimePermit struct {
	permit  refundtime.Permit
	outcome string
}

const refundTimePermitColumns = `attempt_id,request_id,booking_id,payment_id,event_id,approver_id,generation,fingerprint,state,
	starts_at,qualified_at,event_date_version,outcome`

func refundTimePermitByAttempt(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID, lock bool) (storedRefundTimePermit, bool, error) {
	query := `SELECT ` + refundTimePermitColumns + ` FROM event_refund_time_permits WHERE attempt_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	stored, err := scanStoredRefundTimePermit(tx.QueryRow(ctx, query, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return storedRefundTimePermit{}, false, nil
	}
	if err != nil {
		return storedRefundTimePermit{}, false, fmt.Errorf("read refund time attempt: %w", err)
	}
	return stored, true, nil
}

func refundTimePermitByRequest(ctx context.Context, tx pgx.Tx, requestID uuid.UUID, lock bool) (storedRefundTimePermit, bool, error) {
	query := `SELECT ` + refundTimePermitColumns + ` FROM event_refund_time_permits WHERE request_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	stored, err := scanStoredRefundTimePermit(tx.QueryRow(ctx, query, requestID))
	if errors.Is(err, pgx.ErrNoRows) {
		return storedRefundTimePermit{}, false, nil
	}
	if err != nil {
		return storedRefundTimePermit{}, false, fmt.Errorf("read refund time request: %w", err)
	}
	return stored, true, nil
}

func scanStoredRefundTimePermit(row pgx.Row) (storedRefundTimePermit, error) {
	var binding refundtime.Binding
	var permit refundtime.Permit
	var outcome pgtype.Text
	var startsAt, qualifiedAt pgtype.Timestamptz
	var dateVersion pgtype.Int8
	err := row.Scan(
		&binding.AttemptID, &binding.RequestID, &binding.BookingID, &binding.PaymentID, &binding.EventID, &binding.ApproverID,
		&binding.Generation, &binding.Fingerprint, &permit.State, &startsAt, &qualifiedAt, &dateVersion, &outcome,
	)
	if err != nil {
		return storedRefundTimePermit{}, err
	}
	permit.Binding = binding
	if startsAt.Valid {
		permit.StartsAt = startsAt.Time
	}
	if qualifiedAt.Valid {
		permit.QualifiedAt = qualifiedAt.Time
	}
	if dateVersion.Valid {
		permit.Version = dateVersion.Int64
	}
	stored := storedRefundTimePermit{permit: permit}
	if outcome.Valid {
		stored.outcome = outcome.String
	}
	return stored, nil
}

func lockRefundTimeEvent(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (time.Time, int64, bool, error) {
	var startsAt time.Time
	var dateVersion int64
	err := tx.QueryRow(ctx, `SELECT date,date_version FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(&startsAt, &dateVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, 0, false, nil
	}
	if err != nil {
		return time.Time{}, 0, false, fmt.Errorf("lock Event for refund time coordination: %w", err)
	}
	return startsAt, dateVersion, true, nil
}

func commitRefundTimePermit(ctx context.Context, tx pgx.Tx, permit refundtime.Permit) (refundtime.Permit, error) {
	if err := tx.Commit(ctx); err != nil {
		return refundtime.Permit{}, fmt.Errorf("commit refund time coordination: %w", err)
	}
	return permit, nil
}

func validRefundTimeTerminalOutcome(outcome string) bool {
	return outcome == "APPROVED" || outcome == "REJECTED" || outcome == "ABORTED"
}
