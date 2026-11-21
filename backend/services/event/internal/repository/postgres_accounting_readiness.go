package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// accountingQuerier is satisfied by both pgx.Tx and pgxpool.Pool. Readiness
// must be evaluated against the same evidence the final accounting step uses.
type accountingQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// accountingTierReady applies the exact predicate enforced by
// ApplySaleAccounting. It returns false for a tier whose evidence is not yet
// trustworthy, without rewriting any counts or ledger state.
func accountingTierReady(tier lockedAccountingTier, count int64, unknownBooked bool, applied int64) bool {
	if tier.baseline == nil || *tier.baseline != "verified" || tier.total < 0 || tier.available < 0 || tier.available > tier.total {
		return false
	}
	return count == tier.total && !unknownBooked && tier.available == tier.total-applied
}

// readTierAccountingReadiness evaluates the tier and both aggregates in one
// statement. The reserve guard runs inside the lifecycle transaction at READ
// COMMITTED, so separate statements could observe tier availability and ledger
// sums from different snapshots and spuriously reject a healthy reserve.
func readTierAccountingReadiness(ctx context.Context, q accountingQuerier, tierID uuid.UUID) (bool, error) {
	var tier lockedAccountingTier
	var count, applied int64
	var unknownBooked bool
	err := q.QueryRow(ctx, `SELECT t.id,t.total_quantity,t.available_quantity,t.lifecycle_accounting_baseline,
		COALESCE(s.seat_count,0),COALESCE(s.unknown_booked,false),
		COALESCE((SELECT SUM((l.debits->>t.id::text)::bigint) FROM booking_seat_lifecycles l WHERE l.accounting_state='applied'),0)
		FROM ticket_tiers t
		LEFT JOIN LATERAL (
			SELECT COUNT(*) AS seat_count,
				COALESCE(bool_or(s.status='booked' AND (s.lifecycle_generation IS NULL OR l.state IS DISTINCT FROM 'booked')),false) AS unknown_booked
			FROM seats s
			LEFT JOIN booking_seat_lifecycles l ON l.booking_id=s.booking_id AND l.generation=s.lifecycle_generation
			WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL
		) s ON true
		WHERE t.id=$1`, tierID).
		Scan(&tier.id, &tier.total, &tier.available, &tier.baseline, &count, &unknownBooked, &applied)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	return accountingTierReady(tier, count, unknownBooked, applied), nil
}

// AccountingReadinessForTiers evaluates the same predicate for many tiers in a
// single statement, so a catalog response costs one round trip and observes one
// snapshot. Missing tiers are absent from the result; callers must treat an
// absent entry as not ready.
func (r *PostgresTicketTierRepository) AccountingReadinessForTiers(ctx context.Context, tierIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	ready := make(map[uuid.UUID]bool, len(tierIDs))
	if len(tierIDs) == 0 {
		return ready, nil
	}
	tierKeys := make([]string, 0, len(tierIDs))
	for _, id := range tierIDs {
		tierKeys = append(tierKeys, id.String())
	}
	// Restrict ledger rows to requested tier keys before expanding their JSON
	// debit objects, so a catalog read does not aggregate unrelated sales.
	rows, err := r.pool.Query(ctx, `WITH applied AS (
			SELECT e.key AS tier_id, SUM(e.value::bigint) AS applied
			FROM booking_seat_lifecycles b, jsonb_each_text(b.debits) e
			WHERE b.accounting_state='applied'
				AND b.debits ?| $2::text[]
				AND e.key = ANY($2::text[])
			GROUP BY e.key
		)
		SELECT t.id,t.total_quantity,t.available_quantity,t.lifecycle_accounting_baseline,
			COALESCE(s.seat_count,0),COALESCE(s.unknown_booked,false),COALESCE(a.applied,0)
		FROM ticket_tiers t
		LEFT JOIN LATERAL (
			SELECT COUNT(*) AS seat_count,
				COALESCE(bool_or(s.status='booked' AND (s.lifecycle_generation IS NULL OR l.state IS DISTINCT FROM 'booked')),false) AS unknown_booked
			FROM seats s
			LEFT JOIN booking_seat_lifecycles l ON l.booking_id=s.booking_id AND l.generation=s.lifecycle_generation
			WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL
		) s ON true
		LEFT JOIN applied a ON a.tier_id = t.id::text
		WHERE t.id = ANY($1)`, tierIDs, tierKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tier lockedAccountingTier
		var count, applied int64
		var unknownBooked bool
		if err := rows.Scan(&tier.id, &tier.total, &tier.available, &tier.baseline, &count, &unknownBooked, &applied); err != nil {
			return nil, err
		}
		ready[tier.id] = accountingTierReady(tier, count, unknownBooked, applied)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ready, nil
}

// AccountingReadiness is a read-only, snapshot-consistent projection used by
// Booking and Payment before they create a sale or a PaymentIntent.
func (r *PostgresTicketTierRepository) AccountingReadiness(ctx context.Context, tierID uuid.UUID) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		return false, err
	}
	ready, err := readTierAccountingReadiness(ctx, tx, tierID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return ready, nil
}
