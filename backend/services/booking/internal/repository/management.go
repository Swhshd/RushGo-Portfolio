package repository

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"time"
)

type ManagementRepository interface {
	ListFeatureFlags(context.Context) ([]domain.FeatureFlag, []domain.FeatureChange, error)
	ChangeFeatureFlag(context.Context, string, bool, int64, uuid.UUID) (*domain.FeatureFlag, error)
	BookingMetrics(context.Context, time.Time, time.Time) (*domain.BookingMetrics, error)
}

func (r *PostgresBookingRepository) ListFeatureFlags(ctx context.Context) ([]domain.FeatureFlag, []domain.FeatureChange, error) {
	rows, err := r.pool.Query(ctx, `SELECT key,enabled,version,updated_at,actor_id FROM presentation_feature_flags ORDER BY key`)
	if err != nil {
		return nil, nil, err
	}
	flags := []domain.FeatureFlag{}
	for rows.Next() {
		var v domain.FeatureFlag
		if err = rows.Scan(&v.Key, &v.Enabled, &v.Version, &v.UpdatedAt, &v.ActorID); err != nil {
			rows.Close()
			return nil, nil, err
		}
		var known bool
		v.Description, known = domain.FeatureDescription(v.Key)
		if !known {
			rows.Close()
			return nil, nil, domain.ErrUnknownFeature
		}
		flags = append(flags, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	rows, err = r.pool.Query(ctx, `SELECT key,before_enabled,after_enabled,version,actor_id,changed_at FROM presentation_feature_audit ORDER BY changed_at DESC,id DESC LIMIT 50`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	audit := []domain.FeatureChange{}
	for rows.Next() {
		var v domain.FeatureChange
		if err = rows.Scan(&v.Key, &v.Before, &v.After, &v.Version, &v.ActorID, &v.ChangedAt); err != nil {
			return nil, nil, err
		}
		audit = append(audit, v)
	}
	return flags, audit, rows.Err()
}

func (r *PostgresBookingRepository) ChangeFeatureFlag(ctx context.Context, key string, enabled bool, version int64, actor uuid.UUID) (*domain.FeatureFlag, error) {
	description, known := domain.FeatureDescription(key)
	if !known {
		return nil, domain.ErrUnknownFeature
	}
	if actor == uuid.Nil || version < 0 {
		return nil, domain.ErrManagementBounds
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	v := &domain.FeatureFlag{Key: key, Description: description}
	err = tx.QueryRow(ctx, `SELECT enabled,version,updated_at,actor_id FROM presentation_feature_flags WHERE key=$1 FOR UPDATE`, key).Scan(&v.Enabled, &v.Version, &v.UpdatedAt, &v.ActorID)
	if err != nil {
		return nil, err
	}
	if v.Version != version {
		return nil, domain.ErrFeatureConflict
	}
	if v.Enabled == enabled {
		return v, tx.Commit(ctx)
	}
	before := v.Enabled
	err = tx.QueryRow(ctx, `UPDATE presentation_feature_flags SET enabled=$2,version=version+1,updated_at=clock_timestamp(),actor_id=$3 WHERE key=$1 RETURNING enabled,version,updated_at,actor_id`, key, enabled, actor).Scan(&v.Enabled, &v.Version, &v.UpdatedAt, &v.ActorID)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO presentation_feature_audit(id,key,before_enabled,after_enabled,version,actor_id) VALUES($1,$2,$3,$4,$5,$6)`, uuid.New(), key, before, enabled, v.Version, actor)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *PostgresBookingRepository) BookingMetrics(ctx context.Context, from, to time.Time) (*domain.BookingMetrics, error) {
	if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		return nil, domain.ErrManagementBounds
	}
	// Counts describe current records of bookings created in this window. The
	// cap is checked in the same statement; truncated results are never totals.
	v := &domain.BookingMetrics{}
	err := r.pool.QueryRow(ctx, `WITH scope AS MATERIALIZED(SELECT id FROM bookings WHERE created_at>=$1 AND created_at<$2 ORDER BY created_at,id LIMIT 10001)
 SELECT (SELECT count(*) FROM scope),
 (SELECT count(*) FROM tickets t JOIN scope s ON s.id=t.booking_id),
 (SELECT count(*) FROM tickets t JOIN scope s ON s.id=t.booking_id WHERE t.status='CHECKED_IN'),
 (SELECT count(*) FROM booking_refund_requests r JOIN scope s ON s.id=r.booking_id),
 (SELECT count(*) FROM booking_refund_requests r JOIN scope s ON s.id=r.booking_id WHERE r.status='REFUNDED'),clock_timestamp()`, from, to).Scan(&v.Orders, &v.IssuedTickets, &v.CheckedInTickets, &v.RefundRequests, &v.ConfirmedRefunds, &v.ObservedAt)
	if err != nil {
		return nil, err
	}
	if v.Orders > 10000 {
		return nil, domain.ErrManagementBounds
	}
	return v, nil
}
