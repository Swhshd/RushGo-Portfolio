package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const paidRecoveryTicketSnapshotTimeout = 4 * time.Second

// PaidRecoveryTicketSnapshot contains only Booking-owned recovery facts. It
// deliberately has no ticket credential or QR data.
type PaidRecoveryTicketSnapshot struct {
	BookingID            uuid.UUID
	SagaID               uuid.UUID
	Generation           int64
	TicketCount          int64
	IssuanceReceiptCount int64
	ObservedAt           time.Time
}

// PaidRecoveryTicketSnapshotReader is an optional repository capability so
// existing BookingRepository test doubles need not implement this internal
// Saga-only read.
type PaidRecoveryTicketSnapshotReader interface {
	ReadPaidRecoveryTicketSnapshot(context.Context, uuid.UUID, int64) (PaidRecoveryTicketSnapshot, error)
}

func (r *PostgresBookingRepository) ReadPaidRecoveryTicketSnapshot(ctx context.Context, bookingID uuid.UUID, generation int64) (PaidRecoveryTicketSnapshot, error) {
	if r == nil || r.pool == nil {
		return PaidRecoveryTicketSnapshot{}, errors.New("paid recovery ticket snapshot reader unavailable")
	}
	if bookingID == uuid.Nil || generation != 1 {
		return PaidRecoveryTicketSnapshot{}, ErrInvalidLifecycleDecision
	}

	bounded, cancel := context.WithTimeout(ctx, paidRecoveryTicketSnapshotTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("begin paid recovery ticket snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(bounded, `SET LOCAL statement_timeout = '3s'`); err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("bound paid recovery ticket snapshot: %w", err)
	}
	if _, err := tx.Exec(bounded, `SET LOCAL lock_timeout = '1s'`); err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("bound paid recovery ticket snapshot locks: %w", err)
	}

	var status string
	var managed bool
	var currentGeneration *int64
	var sagaID *uuid.UUID
	err = tx.QueryRow(bounded, `SELECT status,managed,lifecycle_generation,saga_id FROM bookings WHERE id=$1`, bookingID).
		Scan(&status, &managed, &currentGeneration, &sagaID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PaidRecoveryTicketSnapshot{}, ErrNotFound
	}
	if err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("read paid recovery Booking identity: %w", err)
	}
	if !managed || currentGeneration == nil || *currentGeneration != generation || status != "CONFIRMED" || sagaID == nil || *sagaID == uuid.Nil {
		return PaidRecoveryTicketSnapshot{}, ErrLifecycleConflict
	}

	snapshot := PaidRecoveryTicketSnapshot{BookingID: bookingID, SagaID: *sagaID, Generation: *currentGeneration}
	// Count every persisted row for this Booking. Do not filter by the current
	// generation or Saga binding: recovery must see conflicting/older facts.
	if err := tx.QueryRow(bounded, `
		SELECT
			(SELECT count(*)::bigint FROM tickets WHERE booking_id=$1),
			(SELECT count(*)::bigint FROM ticket_issuance_receipts WHERE booking_id=$1)`, bookingID).
		Scan(&snapshot.TicketCount, &snapshot.IssuanceReceiptCount); err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("count paid recovery ticket facts: %w", err)
	}
	if err := tx.QueryRow(bounded, `SELECT clock_timestamp()`).Scan(&snapshot.ObservedAt); err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("observe paid recovery ticket snapshot: %w", err)
	}
	if err := tx.Commit(bounded); err != nil {
		return PaidRecoveryTicketSnapshot{}, fmt.Errorf("commit paid recovery ticket snapshot: %w", err)
	}
	return snapshot, nil
}
