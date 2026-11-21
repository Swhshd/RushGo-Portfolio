package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/event/internal/domain"
)

func (r *PostgresSeatRepository) ApplySaleAccounting(ctx context.Context, req domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	if req.Action != "account" {
		return nil, ErrLifecycleConflict
	}
	return r.ApplyLifecycle(ctx, req)
}

type lockedAccountingTier struct {
	id               uuid.UUID
	total, available int64
	baseline         *string
}

func applyLifecycleAccounting(ctx context.Context, tx pgx.Tx, req domain.SeatLifecycleRequest, seats []lockedSeatState, receipt seatLifecycleReceipt, result *domain.SeatLifecycleResult) (bool, error) {
	derived := map[uuid.UUID]int32{}
	for _, s := range seats {
		derived[s.tierID]++
	}
	if req.Action == "account" || len(req.TierQuantities) > 0 {
		if !maps.Equal(req.TierQuantities, derived) {
			return false, ErrLifecycleConflict
		}
	}
	// Reserve is the first sale-side mutation. Do not create a hold on a tier
	// whose accounting evidence is not yet trustworthy. Book and close retain
	// their existing behavior so already-collected payments can be reconciled
	// and compensated without creating a new failure mode.
	if req.Action == "reserve" {
		for tierID := range derived {
			ready, err := readTierAccountingReadiness(ctx, tx, tierID)
			if err != nil {
				return false, err
			}
			if !ready {
				return false, ErrAccountingBaseline
			}
		}
	}
	if req.Action != "account" && req.Action != "close" {
		return false, nil
	}
	if receipt.accounting != "not_applied" {
		var saved map[uuid.UUID]int32
		if err := json.Unmarshal(receipt.debits, &saved); err != nil || !maps.Equal(saved, derived) {
			return false, ErrAccountingBaseline
		}
	}
	if req.Action == "close" && receipt.accounting != "applied" {
		return false, nil
	}
	if req.Action == "account" && receipt.accounting == "applied" {
		return false, nil
	}
	if req.Action == "account" && receipt.accounting != "not_applied" {
		return false, ErrLifecycleConflict
	}
	ids := slices.Collect(maps.Keys(derived))
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	rows, err := tx.Query(ctx, `SELECT id,total_quantity,available_quantity,lifecycle_accounting_baseline FROM ticket_tiers WHERE id=ANY($1) AND event_id=$2 ORDER BY id FOR UPDATE`, ids, req.EventID)
	if err != nil {
		return false, err
	}
	var tiers []lockedAccountingTier
	for rows.Next() {
		var tier lockedAccountingTier
		if err := rows.Scan(&tier.id, &tier.total, &tier.available, &tier.baseline); err != nil {
			rows.Close()
			return false, err
		}
		tiers = append(tiers, tier)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	if len(tiers) != len(ids) {
		return false, ErrNotFound
	}
	for _, tier := range tiers {
		if tier.baseline == nil || *tier.baseline != "verified" || tier.total < 0 || tier.available < 0 || tier.available > tier.total {
			return false, ErrAccountingBaseline
		}
		// The evidence flag is necessary, but not sufficient: current full seat
		// coverage and the durable debit projection must still agree. No count
		// is rewritten when history or initialization is uncertain.
		var count, applied int64
		var unknownBooked bool
		if err := tx.QueryRow(ctx, `SELECT COUNT(*),COALESCE(bool_or(s.status='booked' AND (s.lifecycle_generation IS NULL OR l.state IS DISTINCT FROM 'booked')),false)
		 FROM seats s LEFT JOIN booking_seat_lifecycles l ON l.booking_id=s.booking_id AND l.generation=s.lifecycle_generation
		 WHERE s.ticket_tier_id=$1 AND s.deleted_at IS NULL`, tier.id).Scan(&count, &unknownBooked); err != nil {
			return false, err
		}
		if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM((debits->>$1)::bigint),0) FROM booking_seat_lifecycles WHERE accounting_state='applied'`, tier.id.String()).Scan(&applied); err != nil {
			return false, fmt.Errorf("accounting evidence invalid: %w", err)
		}
		if count != tier.total || unknownBooked || tier.available != tier.total-applied {
			return false, ErrAccountingBaseline
		}
		delta := -int64(derived[tier.id])
		if req.Action == "close" {
			delta = -delta
		}
		if tier.available+delta < 0 || tier.available+delta > tier.total {
			return false, ErrInsufficientTickets
		}
	}
	for _, tier := range tiers {
		delta := -int64(derived[tier.id])
		if req.Action == "close" {
			delta = -delta
		}
		tag, err := tx.Exec(ctx, `UPDATE ticket_tiers SET available_quantity=available_quantity+$2,version=version+1 WHERE id=$1 AND available_quantity+$2 BETWEEN 0 AND total_quantity`, tier.id, delta)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() != 1 {
			return false, ErrInsufficientTickets
		}
	}
	if req.Action == "account" {
		debits, err := json.Marshal(derived)
		if err != nil {
			return false, err
		}
		_, err = tx.Exec(ctx, `UPDATE booking_seat_lifecycles SET accounting_state='applied',debits=$3,accounted_at=clock_timestamp() WHERE booking_id=$1 AND generation=$2`, req.BookingID, req.Generation, debits)
		if err != nil {
			return false, err
		}
		result.AccountingState = "applied"
	} else {
		// This same transaction releases only the original owner, changes all
		// counters and records closed/reversed. An ACK loss replays that fact.
		_, err = tx.Exec(ctx, `UPDATE booking_seat_lifecycles SET state='closed',accounting_state='reversed',reversed_at=clock_timestamp() WHERE booking_id=$1 AND generation=$2`, req.BookingID, req.Generation)
		if err != nil {
			return false, err
		}
		result.AccountingState = "reversed"
	}
	return true, nil
}

func rejectManagedTierMutation(ctx context.Context, tx pgx.Tx, tierID uuid.UUID) error {
	var managed bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM seats WHERE ticket_tier_id=$1 AND lifecycle_generation IS NOT NULL) OR EXISTS(SELECT 1 FROM ticket_tiers WHERE id=$1 AND lifecycle_accounting_baseline='verified')`, tierID).Scan(&managed); err != nil {
		return err
	}
	if managed {
		return ErrManagedSeat
	}
	return nil
}

func newSeatBatchBaselines(ctx context.Context, tx pgx.Tx, seats []*domain.Seat) ([]uuid.UUID, error) {
	counts := map[uuid.UUID]int64{}
	available := map[uuid.UUID]bool{}
	for _, s := range seats {
		counts[s.TicketTierID]++
		if _, seen := available[s.TicketTierID]; !seen {
			available[s.TicketTierID] = true
		}
		available[s.TicketTierID] = available[s.TicketTierID] && s.Status == domain.SeatStatusAvailable && s.BookingID == nil && s.ReservedByBookingID == nil
	}
	ids := slices.Collect(maps.Keys(counts))
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	rows, err := tx.Query(ctx, `SELECT id,total_quantity,available_quantity,lifecycle_accounting_baseline FROM ticket_tiers WHERE id=ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	var tiers []lockedAccountingTier
	for rows.Next() {
		var tier lockedAccountingTier
		if err := rows.Scan(&tier.id, &tier.total, &tier.available, &tier.baseline); err != nil {
			rows.Close()
			return nil, err
		}
		tiers = append(tiers, tier)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(tiers) != len(ids) {
		return nil, ErrNotFound
	}
	var proven []uuid.UUID
	for _, tier := range tiers {
		var existing int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM seats WHERE ticket_tier_id=$1`, tier.id).Scan(&existing); err != nil {
			return nil, err
		}
		if existing == 0 && tier.total > 0 && tier.total == tier.available && counts[tier.id] == tier.total && available[tier.id] && tier.baseline == nil {
			proven = append(proven, tier.id)
		}
	}
	return proven, nil
}
