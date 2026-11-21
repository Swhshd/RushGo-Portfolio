package repository

import (
	"context"
	"encoding/json"
	"maps"
	"time"

	"github.com/google/uuid"
)

type bookingSeatLifecycleEvidence struct {
	BookingID, EventID     uuid.UUID
	Generation             int64
	State, AccountingState string
	SeatIDs                []uuid.UUID
	Debits                 []byte
}

type bookingSeatEvidenceRow struct {
	ID, EventID, TicketTierID      uuid.UUID
	BookingID, ReservedByBookingID *uuid.UUID
	LifecycleGeneration            *int64
	Status                         string
	Deleted                        bool
}

type bookingSeatEligibilitySnapshot struct {
	Lifecycle bookingSeatLifecycleEvidence
	Seats     []bookingSeatEvidenceRow
}

func validateBookingTicketEligibility(bookingID, eventID uuid.UUID, generation int64, requested []uuid.UUID, snapshot bookingSeatEligibilitySnapshot) (bool, string) {
	if bookingID == uuid.Nil || eventID == uuid.Nil || generation != 1 || len(requested) == 0 || len(requested) > 512 {
		return false, "request_identity_invalid"
	}
	if snapshot.Lifecycle.BookingID != bookingID || snapshot.Lifecycle.EventID != eventID || snapshot.Lifecycle.Generation != generation {
		return false, "event_lifecycle_identity_mismatch"
	}
	if snapshot.Lifecycle.State != "booked" || snapshot.Lifecycle.AccountingState != "applied" {
		return false, "event_lifecycle_not_accounted"
	}
	want := make(map[uuid.UUID]struct{}, len(requested))
	for _, id := range requested {
		if id == uuid.Nil {
			return false, "request_seat_set_invalid"
		}
		if _, duplicate := want[id]; duplicate {
			return false, "request_seat_set_invalid"
		}
		want[id] = struct{}{}
	}
	lifecycleSeats := make(map[uuid.UUID]struct{}, len(snapshot.Lifecycle.SeatIDs))
	for _, id := range snapshot.Lifecycle.SeatIDs {
		if id == uuid.Nil {
			return false, "event_lifecycle_seat_set_invalid"
		}
		if _, duplicate := lifecycleSeats[id]; duplicate {
			return false, "event_lifecycle_seat_set_invalid"
		}
		lifecycleSeats[id] = struct{}{}
	}
	if !sameUUIDSet(want, lifecycleSeats) || len(snapshot.Seats) != len(want) {
		return false, "event_booking_seat_set_mismatch"
	}
	derivedDebits := make(map[uuid.UUID]int32)
	seenSeats := make(map[uuid.UUID]struct{}, len(snapshot.Seats))
	for _, seat := range snapshot.Seats {
		if _, exists := want[seat.ID]; !exists || seat.ID == uuid.Nil {
			return false, "event_booking_seat_set_mismatch"
		}
		if _, duplicate := seenSeats[seat.ID]; duplicate {
			return false, "event_booking_seat_set_mismatch"
		}
		seenSeats[seat.ID] = struct{}{}
		if seat.EventID != eventID || seat.TicketTierID == uuid.Nil || seat.Status != "booked" || seat.BookingID == nil || *seat.BookingID != bookingID ||
			seat.ReservedByBookingID != nil || seat.LifecycleGeneration == nil || *seat.LifecycleGeneration != generation || seat.Deleted {
			return false, "event_booking_seat_ownership_mismatch"
		}
		derivedDebits[seat.TicketTierID]++
	}
	var recordedDebits map[uuid.UUID]int32
	if len(snapshot.Lifecycle.Debits) == 0 || json.Unmarshal(snapshot.Lifecycle.Debits, &recordedDebits) != nil || len(recordedDebits) == 0 || !maps.Equal(recordedDebits, derivedDebits) {
		return false, "event_lifecycle_debit_mismatch"
	}
	return true, "event_booking_seats_accounted"
}

func sameUUIDSet(left, right map[uuid.UUID]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for id := range left {
		if _, exists := right[id]; !exists {
			return false
		}
	}
	return true
}

// VerifyBookingTicketEligibility reads the exact lifecycle and seat ownership
// evidence in one PostgreSQL statement snapshot. It never changes seat or
// accounting state.
func (r *PostgresSeatRepository) VerifyBookingTicketEligibility(ctx context.Context, bookingID, eventID uuid.UUID, generation int64, seatIDs []uuid.UUID) (bool, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	if bookingID == uuid.Nil || eventID == uuid.Nil || generation != 1 || len(seatIDs) == 0 || len(seatIDs) > 512 {
		return false, "request_identity_invalid", nil
	}
	rows, err := r.pool.Query(ctx, `SELECT l.booking_id,l.event_id,l.generation,l.seat_ids,l.state,l.accounting_state,l.debits,
	 expected.seat_id,s.id,s.event_id,s.ticket_tier_id,s.status,s.booking_id,s.reserved_by_booking_id,s.lifecycle_generation,s.deleted_at
	 FROM booking_seat_lifecycles l
	 CROSS JOIN LATERAL unnest(l.seat_ids) AS expected(seat_id)
	 LEFT JOIN seats s ON s.id=expected.seat_id
	 WHERE l.booking_id=$1 AND l.generation=$2
	 ORDER BY expected.seat_id`, bookingID, generation)
	if err != nil {
		return false, "event_authority_unavailable", err
	}
	defer rows.Close()
	snapshot := bookingSeatEligibilitySnapshot{}
	loaded := false
	for rows.Next() {
		loaded = true
		var lifecycle bookingSeatLifecycleEvidence
		var expectedID uuid.UUID
		var seatID, seatEventID, tierID *uuid.UUID
		var status *string
		var bookingOwner, reservedOwner *uuid.UUID
		var seatGeneration *int64
		var deletedAt *time.Time
		if err := rows.Scan(&lifecycle.BookingID, &lifecycle.EventID, &lifecycle.Generation, &lifecycle.SeatIDs, &lifecycle.State, &lifecycle.AccountingState, &lifecycle.Debits,
			&expectedID, &seatID, &seatEventID, &tierID, &status, &bookingOwner, &reservedOwner, &seatGeneration, &deletedAt); err != nil {
			return false, "event_authority_unavailable", err
		}
		snapshot.Lifecycle = lifecycle
		seat := bookingSeatEvidenceRow{BookingID: bookingOwner, ReservedByBookingID: reservedOwner, LifecycleGeneration: seatGeneration, Deleted: deletedAt != nil}
		if seatID != nil {
			seat.ID = *seatID
		}
		if seatEventID != nil {
			seat.EventID = *seatEventID
		}
		if tierID != nil {
			seat.TicketTierID = *tierID
		}
		if status != nil {
			seat.Status = *status
		}
		if seat.ID != expectedID {
			seat.ID = uuid.Nil
		}
		snapshot.Seats = append(snapshot.Seats, seat)
	}
	if err := rows.Err(); err != nil {
		return false, "event_authority_unavailable", err
	}
	if !loaded {
		return false, "event_lifecycle_missing", nil
	}
	eligible, reason := validateBookingTicketEligibility(bookingID, eventID, generation, seatIDs, snapshot)
	return eligible, reason, nil
}
