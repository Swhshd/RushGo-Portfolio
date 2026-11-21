package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/event/internal/domain"
)

func lockEventSaleGate(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) error {
	eligible, reason, err := lockEventSaleState(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !eligible {
		return fmt.Errorf("%s: %w", reason, ErrSaleNotAllowed)
	}
	return nil
}

func lockEventSaleState(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (bool, string, error) {
	if eventID == uuid.Nil {
		return false, "event_id_missing", fmt.Errorf("event ID required for sale gate: %w", ErrSaleNotAllowed)
	}
	var event domain.Event
	var salesStartAt, salesEndAt *time.Time
	err := tx.QueryRow(ctx, `SELECT date,status,publication_status,sales_status,sales_start_at,sales_end_at
		FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(
		&event.Date, &event.Status, &event.PublicationStatus, &event.SalesStatus, &salesStartAt, &salesEndAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "event_not_found", ErrNotFound
	}
	if err != nil {
		return false, "sale_state_unknown", fmt.Errorf("lock event sale lifecycle: %w", err)
	}
	event.SalesStartAt = salesStartAt
	event.SalesEndAt = salesEndAt
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, "sale_state_unknown", fmt.Errorf("read sale eligibility time: %w", err)
	}
	eligible, reason := domain.EventSaleEligibility(&event, now)
	return eligible, reason, nil
}

func lockEventSaleForSeats(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, seatIDs []uuid.UUID) (uuid.UUID, bool, string, error) {
	if eventID == uuid.Nil {
		rows, err := tx.Query(ctx, `SELECT DISTINCT event_id FROM seats WHERE id=ANY($1) AND deleted_at IS NULL ORDER BY event_id`, seatIDs)
		if err != nil {
			return uuid.Nil, false, "sale_state_unknown", fmt.Errorf("resolve event for seat sale gate: %w", err)
		}
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return uuid.Nil, false, "sale_state_unknown", err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return uuid.Nil, false, "sale_state_unknown", err
		}
		rows.Close()
		if len(ids) == 0 {
			return uuid.Nil, false, "event_not_found", ErrNotFound
		}
		if len(ids) != 1 {
			return uuid.Nil, false, "event_mismatch", ErrLifecycleConflict
		}
		eventID = ids[0]
	}
	eligible, reason, err := lockEventSaleState(ctx, tx, eventID)
	if err != nil {
		return uuid.Nil, false, reason, err
	}
	return eventID, eligible, reason, nil
}

func legacyReservationReplay(seats []lockedSeatState, owner uuid.UUID, now time.Time) bool {
	if len(seats) == 0 {
		return false
	}
	for _, seat := range seats {
		if seat.status != domain.SeatStatusReserved || seat.generation != nil || !seatOwnedBy(seat, &owner) || !reservationActive(seat, now) {
			return false
		}
	}
	return true
}

func lifecycleReservationReplay(seats []lockedSeatState, generation int64, deadline time.Time, owner uuid.UUID, now time.Time) bool {
	if len(seats) == 0 || !deadline.After(now) {
		return false
	}
	for _, seat := range seats {
		if seat.status != domain.SeatStatusReserved || !seatOwnedBy(seat, &owner) || seat.generation == nil || *seat.generation != generation || seat.expiresAt == nil || !seat.expiresAt.Equal(deadline) {
			return false
		}
	}
	return true
}
