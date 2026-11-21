package repository

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/event/internal/domain"
)

type seatLifecycleReceipt struct {
	eventID           uuid.UUID
	seats             []uuid.UUID
	hash              []byte
	deadline          time.Time
	state, accounting string
	debits            []byte
}

func validateLifecycleRequest(req domain.SeatLifecycleRequest) error {
	hash, err := domain.SeatSetFingerprint(req.EventID, req.SeatIDs)
	if err != nil || hash != req.Fingerprint || req.BookingID == uuid.Nil || req.OperationID == uuid.Nil || req.Generation != 1 || req.ReservationDeadline.IsZero() {
		return fmt.Errorf("invalid immutable seat lifecycle: %w", ErrLifecycleConflict)
	}
	if req.Action != "reserve" && req.Action != "book" && req.Action != "close" && req.Action != "account" {
		return fmt.Errorf("invalid lifecycle action: %w", ErrLifecycleConflict)
	}
	return nil
}

func (r *PostgresSeatRepository) ApplyLifecycle(ctx context.Context, req domain.SeatLifecycleRequest) (*domain.SeatLifecycleResult, error) {
	if err := validateLifecycleRequest(req); err != nil {
		return nil, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	saleEligible := true
	saleReason := ""
	if req.Action == "reserve" || req.Action == "close" {
		saleEligible, saleReason, err = lockEventSaleState(ctx, tx, req.EventID)
		if err != nil {
			return nil, err
		}
	}
	if err := lockSeatLifecycleIdentity(ctx, tx, req.BookingID); err != nil {
		return nil, err
	}
	ids := slices.Clone(req.SeatIDs)
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	initial := "held"
	if req.Action == "close" {
		initial = "closed"
	}
	// A book must have an existing hold. Reserve and close can establish the
	// immutable identity, with uniqueness waiting before the separate reload.
	inserted := false
	if req.Action == "reserve" || req.Action == "close" {
		tag, err := tx.Exec(ctx, `INSERT INTO booking_seat_lifecycles(booking_id,generation,event_id,seat_ids,seat_set_hash,reservation_deadline,state,last_operation_id)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(booking_id,generation) DO NOTHING`, req.BookingID, req.Generation, req.EventID, ids, req.Fingerprint[:], req.ReservationDeadline, initial, req.OperationID)
		if err != nil {
			return nil, fmt.Errorf("insert lifecycle identity: %w", err)
		}
		inserted = tag.RowsAffected() == 1
	}
	var receipt seatLifecycleReceipt
	err = tx.QueryRow(ctx, `SELECT event_id,seat_ids,seat_set_hash,reservation_deadline,state,accounting_state,debits FROM booking_seat_lifecycles WHERE booking_id=$1 AND generation=$2 FOR UPDATE`, req.BookingID, req.Generation).
		Scan(&receipt.eventID, &receipt.seats, &receipt.hash, &receipt.deadline, &receipt.state, &receipt.accounting, &receipt.debits)
	if err == pgx.ErrNoRows {
		return nil, ErrSeatNotAvailable
	}
	if err != nil {
		return nil, fmt.Errorf("lock lifecycle: %w", err)
	}
	if receipt.eventID != req.EventID || !slices.Equal(receipt.seats, ids) || !bytes.Equal(receipt.hash, req.Fingerprint[:]) || !receipt.deadline.Equal(req.ReservationDeadline.UTC().Truncate(time.Microsecond)) {
		return nil, ErrLifecycleConflict
	}
	if receipt.state == "closed" && req.Action != "close" {
		return nil, ErrLifecycleClosed
	}
	locked, err := lockSeatStates(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	for _, seat := range locked {
		if seat.eventID != req.EventID {
			return nil, ErrLifecycleConflict
		}
	}
	var inventoryWasReliable bool
	var inventoryBefore int64
	if req.Action == "close" {
		if err := lockEventTicketTiers(ctx, tx, req.EventID); err != nil {
			return nil, err
		}
		inventoryWasReliable, inventoryBefore, err = eventInventoryTrackingState(ctx, tx, req.EventID)
		if err != nil {
			return nil, fmt.Errorf("read pre-release inventory evidence: %w", err)
		}
	}
	result := &domain.SeatLifecycleResult{State: receipt.state, AccountingState: receipt.accounting, ReservationDeadline: &receipt.deadline}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if req.Action == "reserve" && !saleEligible && (inserted || receipt.state != "held" || !lifecycleReservationReplay(locked, req.Generation, receipt.deadline, req.BookingID, now)) {
		return nil, fmt.Errorf("%s: %w", saleReason, ErrSaleNotAllowed)
	}
	var mutationIDs []uuid.UUID
	var target domain.SeatStatus
	switch req.Action {
	case "reserve":
		if !receipt.deadline.After(now) {
			return nil, ErrSeatNotAvailable
		}
		if receipt.state != "held" {
			return nil, ErrLifecycleConflict
		}
		target = domain.SeatStatusReserved
		for _, seat := range locked {
			if seat.status == domain.SeatStatusAvailable && seat.generation == nil && inserted {
				mutationIDs = append(mutationIDs, seat.id)
				continue
			}
			if seat.status != domain.SeatStatusReserved || !seatOwnedBy(seat, &req.BookingID) || seat.generation == nil || *seat.generation != req.Generation || seat.expiresAt == nil || !seat.expiresAt.Equal(receipt.deadline) {
				return nil, ErrSeatNotAvailable
			}
		}
	case "book":
		target = domain.SeatStatusBooked
		for _, seat := range locked {
			if seat.generation == nil || *seat.generation != req.Generation {
				return nil, ErrSeatNotAvailable
			}
			if receipt.state == "booked" && seat.status == domain.SeatStatusBooked && seat.bookingID != nil && *seat.bookingID == req.BookingID {
				continue
			}
			if receipt.state != "held" || !receipt.deadline.After(now) || seat.status != domain.SeatStatusReserved || !seatOwnedBy(seat, &req.BookingID) || !reservationActive(seat, now) || !seat.expiresAt.Equal(receipt.deadline) {
				return nil, ErrSeatNotAvailable
			}
			mutationIDs = append(mutationIDs, seat.id)
		}
		result.State = "booked"
	case "close":
		target = domain.SeatStatusAvailable
		for _, seat := range locked {
			owned := seat.status == domain.SeatStatusReserved && seatOwnedBy(seat, &req.BookingID) || seat.status == domain.SeatStatusBooked && seat.bookingID != nil && *seat.bookingID == req.BookingID
			if owned {
				if seat.generation == nil || *seat.generation != req.Generation {
					return nil, ErrManagedSeat
				}
				mutationIDs = append(mutationIDs, seat.id)
			}
		}
		result.State = "closed"
	case "account":
		if receipt.state != "booked" {
			return nil, ErrLifecycleConflict
		}
		for _, seat := range locked {
			if seat.status != domain.SeatStatusBooked || seat.bookingID == nil || *seat.bookingID != req.BookingID || seat.generation == nil || *seat.generation != req.Generation {
				return nil, ErrSeatNotAvailable
			}
		}
	}
	accountingChanged, err := applyLifecycleAccounting(ctx, tx, req, locked, receipt, result)
	if err != nil {
		return nil, err
	}
	if len(mutationIDs) > 0 {
		result.ChangedSeats, err = updateLifecycleSeats(ctx, tx, mutationIDs, target, req.BookingID, req.Generation, receipt.deadline)
		if err != nil {
			return nil, err
		}
	}
	result.Applied = inserted || receipt.state != result.State || len(mutationIDs) > 0 || accountingChanged
	if result.Applied {
		_, err = tx.Exec(ctx, `UPDATE booking_seat_lifecycles SET state=$3,last_operation_id=$4,updated_at=clock_timestamp() WHERE booking_id=$1 AND generation=$2`, req.BookingID, req.Generation, result.State, req.OperationID)
		if err != nil {
			return nil, err
		}
	}
	if req.Action == "close" && saleEligible && inventoryWasReliable {
		inventoryAfterReliable, inventoryAfter, err := eventInventoryTrackingState(ctx, tx, req.EventID)
		if err != nil {
			return nil, fmt.Errorf("read post-release inventory evidence: %w", err)
		}
		if inventoryAfterReliable && inventoryBefore == 0 && inventoryAfter > 0 {
			if err := enqueueRestockAlerts(ctx, tx, req.EventID, req.OperationID); err != nil {
				return nil, fmt.Errorf("enqueue restock reminder intents: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit lifecycle outcome unknown; replay original identity: %w", err)
	}
	return result, nil
}

// lockEventTicketTiers uses the same stable UUID order as inventory accounting
// so each close observes one authoritative counter/seat transition.
func lockEventTicketTiers(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) error {
	rows, err := tx.Query(ctx, `SELECT id FROM ticket_tiers WHERE event_id=$1 ORDER BY id FOR UPDATE`, eventID)
	if err != nil {
		return fmt.Errorf("lock event ticket tiers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ignored uuid.UUID
		if err := rows.Scan(&ignored); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func updateLifecycleSeats(ctx context.Context, tx pgx.Tx, ids []uuid.UUID, target domain.SeatStatus, owner uuid.UUID, generation int64, deadline time.Time) ([]*domain.Seat, error) {
	query := `UPDATE seats SET status='reserved',booking_id=NULL,reserved_by_booking_id=$2,reservation_expired_at=$4,lifecycle_generation=$3,seat_revision=seat_revision+1,updated_at=clock_timestamp()
	 WHERE id=ANY($1) AND status='available' AND lifecycle_generation IS NULL AND deleted_at IS NULL`
	args := []any{ids, owner, generation, deadline}
	if target == domain.SeatStatusBooked {
		query = `UPDATE seats SET status='booked',booking_id=$2,reserved_by_booking_id=NULL,reservation_expired_at=NULL,seat_revision=seat_revision+1,updated_at=clock_timestamp()
		 WHERE id=ANY($1) AND status='reserved' AND reserved_by_booking_id=$2 AND lifecycle_generation=$3 AND reservation_expired_at=$4 AND reservation_expired_at>clock_timestamp() AND deleted_at IS NULL`
	} else if target == domain.SeatStatusAvailable {
		query = `UPDATE seats SET status='available',booking_id=NULL,reserved_by_booking_id=NULL,reservation_expired_at=NULL,lifecycle_generation=NULL,seat_revision=seat_revision+1,updated_at=clock_timestamp()
		 WHERE id=ANY($1) AND lifecycle_generation=$3 AND ((status='reserved' AND reserved_by_booking_id=$2) OR (status='booked' AND booking_id=$2)) AND deleted_at IS NULL`
		args = args[:3]
	}
	query += ` RETURNING id,event_id,ticket_tier_id,status,booking_id,order_id,position,created_at,updated_at,seat_revision`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var changed []*domain.Seat
	for rows.Next() {
		seat, err := scanSeat(rows)
		if err != nil {
			return nil, err
		}
		changed = append(changed, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(changed) != len(ids) {
		return nil, ErrSeatNotAvailable
	}
	return changed, nil
}

// Legacy calls lack generation and immutable input. Validate after sorted seat
// locking so a concurrent receipt close/creation either follows this effect or
// is visible here; never lock a receipt after seats.
func rejectManagedSeatMutation(ctx context.Context, tx pgx.Tx, locked []lockedSeatState, owner *uuid.UUID) error {
	for _, seat := range locked {
		if seat.generation != nil {
			return ErrManagedSeat
		}
	}
	if owner != nil {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM booking_seat_lifecycles WHERE booking_id=$1)`, owner).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrManagedSeat
		}
	}
	return nil
}

func lockSeatLifecycleIdentity(ctx context.Context, tx pgx.Tx, owner uuid.UUID) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ticketbox:seat:"+owner.String())
	return err
}

func (r *PostgresSeatRepository) GetSeatAuthorities(ctx context.Context, ids []uuid.UUID) ([]domain.SeatAuthority, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if len(ids) > 512 {
		return nil, fmt.Errorf("seat authority batch exceeds 512")
	}
	rows, err := r.pool.Query(ctx, `SELECT s.id,s.event_id,s.status,COALESCE(s.booking_id,s.reserved_by_booking_id),s.reservation_expired_at,s.lifecycle_generation,l.state,
	 COALESCE(CASE
	 WHEN s.status='available' THEN s.booking_id IS NULL AND s.reserved_by_booking_id IS NULL AND s.reservation_expired_at IS NULL AND s.lifecycle_generation IS NULL
	 WHEN s.status='reserved' THEN s.booking_id IS NULL AND s.reserved_by_booking_id IS NOT NULL AND s.reserved_by_booking_id<>'00000000-0000-0000-0000-000000000000'::uuid AND s.reservation_expired_at IS NOT NULL
	 AND ((s.lifecycle_generation IS NULL AND NOT EXISTS(SELECT 1 FROM booking_seat_lifecycles x WHERE x.booking_id=s.reserved_by_booking_id)) OR (l.state='held' AND l.event_id=s.event_id AND s.id=ANY(l.seat_ids) AND l.reservation_deadline=s.reservation_expired_at))
	 WHEN s.status='booked' THEN s.reserved_by_booking_id IS NULL AND s.booking_id IS NOT NULL AND s.booking_id<>'00000000-0000-0000-0000-000000000000'::uuid AND s.reservation_expired_at IS NULL
	 AND ((s.lifecycle_generation IS NULL AND NOT EXISTS(SELECT 1 FROM booking_seat_lifecycles x WHERE x.booking_id=s.booking_id)) OR (l.state='booked' AND l.event_id=s.event_id AND s.id=ANY(l.seat_ids)))
	 ELSE false END,false)
	 FROM seats s LEFT JOIN booking_seat_lifecycles l ON l.booking_id=COALESCE(s.booking_id,s.reserved_by_booking_id) AND l.generation=s.lifecycle_generation
	 WHERE s.id=ANY($1) AND s.deleted_at IS NULL ORDER BY s.id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.SeatAuthority, 0, len(ids))
	for rows.Next() {
		var a domain.SeatAuthority
		if err := rows.Scan(&a.SeatID, &a.EventID, &a.Status, &a.BookingID, &a.ReservationDeadline, &a.Generation, &a.LifecycleState, &a.Coherent); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}
