package repository

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/booking/internal/domain"
)

func requestSnapshot(ctx context.Context, q bookingQuerier, identity domain.BookingRequestIdentity) (*domain.LifecycleSnapshot, error) {
	var id uuid.UUID
	var hash []byte
	if err := q.QueryRow(ctx, `SELECT id,request_hash FROM bookings WHERE user_id=$1 AND request_id=$2`, identity.UserID, identity.RequestID).Scan(&id, &hash); err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if string(hash) != string(identity.Hash[:]) {
		return nil, ErrRequestConflict
	}
	v, _, _, err := scanLifecycle(q.QueryRow(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE id=$1`, id))
	if err != nil {
		return nil, err
	}
	if err := loadLifecycleItems(ctx, q, v.Booking); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *PostgresBookingRepository) GetByRequest(ctx context.Context, identity domain.BookingRequestIdentity) (*domain.LifecycleSnapshot, error) {
	if identity.UserID == uuid.Nil || !domain.ValidBookingRequestID(identity.RequestID) {
		return nil, ErrInvalidBookingRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return requestSnapshot(ctx, r.pool, identity)
}

func (r *PostgresBookingRepository) CreateOrGetRequest(ctx context.Context, command domain.CreateBookingCommand) (*domain.LifecycleSnapshot, bool, error) {
	b := command.Booking
	if b == nil || b.ID == uuid.Nil || b.UserID != command.Identity.UserID || b.UserID == uuid.Nil || command.Currency != "usd" || command.ReservationDeadline.IsZero() || b.TotalAmountCents < 1 || b.TotalAmountCents > math.MaxInt32 {
		return nil, false, ErrInvalidBookingRequest
	}
	hash, mode, err := domain.CanonicalBookingRequest(b.EventID, b.Items, command.BookingMode)
	if err != nil || mode != command.BookingMode || hash != command.Identity.Hash || (command.Identity.RequestID != "" && !domain.ValidBookingRequestID(command.Identity.RequestID)) {
		return nil, false, ErrInvalidBookingRequest
	}
	var total int64
	for _, item := range b.Items {
		if item.ID == uuid.Nil || item.UnitPriceCents < 1 || item.UnitPriceCents > math.MaxInt32 || int64(item.Quantity) > (math.MaxInt32-total)/item.UnitPriceCents {
			return nil, false, ErrInvalidBookingRequest
		}
		total += int64(item.Quantity) * item.UnitPriceCents
	}
	if total != b.TotalAmountCents {
		return nil, false, ErrInvalidBookingRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer rollbackBooking(tx)
	var key, requestHash any
	if command.Identity.RequestID != "" {
		key, requestHash = command.Identity.RequestID, command.Identity.Hash[:]
	}
	tag, err := tx.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,created_at,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline,request_id,request_hash,dispatch_kind,dispatch_state,dispatch_due_at,dispatch_version)
	VALUES($1,$2,$3,'PENDING',$4,1,$5,true,1,'OPEN',$6,$7,$8,$9,$10,'start','pending',clock_timestamp(),1)
	ON CONFLICT(user_id,request_id) WHERE request_id IS NOT NULL DO NOTHING`, b.ID, b.UserID, b.EventID, b.TotalAmountCents, b.CreatedAt, command.Currency, command.BookingMode, command.ReservationDeadline, key, requestHash)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 0 {
		v, err := requestSnapshot(ctx, tx, command.Identity)
		if err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("Booking request replay commit outcome unknown: %w", err)
		}
		return v, false, nil
	}
	for _, item := range b.Items {
		if _, err := tx.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,$4,$5,$6)`, item.ID, b.ID, item.TicketTierID, item.Quantity, item.UnitPriceCents, item.SeatIDs); err != nil {
			return nil, false, err
		}
	}
	v, _, _, err := scanLifecycle(tx.QueryRow(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE id=$1`, b.ID))
	if err != nil {
		return nil, false, err
	}
	if err := loadLifecycleItems(ctx, tx, v.Booking); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("Booking create commit outcome unknown: %w", err)
	}
	return v, true, nil
}

func (r *PostgresBookingRepository) ClaimDispatch(ctx context.Context, limit int, lease time.Duration) ([]domain.DispatchClaim, error) {
	return r.claimDispatch(ctx, limit, lease, nil)
}
func (r *PostgresBookingRepository) ClaimBookingDispatch(ctx context.Context, id uuid.UUID, lease time.Duration) (*domain.DispatchClaim, error) {
	claims, err := r.claimDispatch(ctx, 1, lease, &id)
	if err != nil || len(claims) == 0 {
		return nil, err
	}
	return &claims[0], nil
}
func (r *PostgresBookingRepository) claimDispatch(ctx context.Context, limit int, lease time.Duration, target *uuid.UUID) ([]domain.DispatchClaim, error) {
	if limit < 1 || limit > 4 || lease < time.Second || lease > time.Minute || (target != nil && *target == uuid.Nil) {
		return nil, ErrInvalidBookingRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackBooking(tx)
	rows, err := tx.Query(ctx, `SELECT id FROM bookings WHERE managed AND dispatch_state IN ('pending','claimed') AND dispatch_due_at<=clock_timestamp() AND (dispatch_lease_until IS NULL OR dispatch_lease_until<=clock_timestamp()) AND ($2::uuid IS NULL OR id=$2) ORDER BY dispatch_due_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit, target)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	claims := make([]domain.DispatchClaim, 0, len(ids))
	for _, id := range ids {
		v, _, _, err := scanLifecycle(tx.QueryRow(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE id=$1`, id))
		if err != nil {
			return nil, err
		}
		if err := loadLifecycleItems(ctx, tx, v.Booking); err != nil {
			return nil, err
		}
		c := domain.DispatchClaim{Ref: domain.LifecycleRef{BookingID: id, Generation: v.Generation}, Token: uuid.New(), Version: v.Booking.Version, Snapshot: *v}
		if err := tx.QueryRow(ctx, `UPDATE bookings SET dispatch_state='claimed',dispatch_token=$2,dispatch_fence=dispatch_fence+1,dispatch_lease_until=clock_timestamp()+$3*interval '1 second',dispatch_attempts=dispatch_attempts+1 WHERE id=$1 RETURNING dispatch_kind,dispatch_fence,dispatch_lease_until`, id, c.Token, lease.Seconds()).Scan(&c.Kind, &c.Fence, &c.LeaseUntil); err != nil {
			return nil, err
		}
		claims = append(claims, c)
		if err := tx.QueryRow(ctx, `SELECT dispatch_attempts FROM bookings WHERE id=$1`, id).Scan(&claims[len(claims)-1].Snapshot.DispatchAttempts); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("Booking dispatch claim commit outcome unknown: %w", err)
	}
	return claims, nil
}
func (r *PostgresBookingRepository) RenewDispatch(ctx context.Context, c domain.DispatchClaim, lease time.Duration) (bool, error) {
	if lease < time.Second || lease > time.Minute {
		return false, ErrInvalidBookingRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackBooking(tx)
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, c.Ref.BookingID).Scan(&id); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE bookings SET dispatch_lease_until=clock_timestamp()+$7*interval '1 second' WHERE id=$1 AND dispatch_token=$2 AND dispatch_fence=$3 AND version=$4 AND dispatch_kind=$5 AND lifecycle_phase=$6 AND dispatch_state='claimed' AND dispatch_lease_until>clock_timestamp()`, id, c.Token, c.Fence, c.Version, c.Kind, c.Snapshot.Phase, lease.Seconds())
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
func (r *PostgresBookingRepository) FinishDispatch(ctx context.Context, c domain.DispatchClaim, sagaID uuid.UUID, accepted bool, category string, next time.Time) (bool, error) {
	if (accepted && sagaID == uuid.Nil) || (!accepted && (category == "" || next.IsZero())) {
		return false, ErrInvalidBookingRequest
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackBooking(tx)
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, c.Ref.BookingID).Scan(&id); err != nil {
		return false, err
	}
	state := "pending"
	if accepted {
		state = "accepted"
		next = time.Now()
	}
	var saga any
	if sagaID != uuid.Nil {
		saga = sagaID
	}
	tag, err := tx.Exec(ctx, `UPDATE bookings SET dispatch_state=$7,dispatch_due_at=$8,dispatch_error=NULLIF($9,''),saga_id=COALESCE(saga_id,$10),dispatch_token=NULL,dispatch_lease_until=NULL,terminal_dispatch_needed=CASE WHEN $11 AND dispatch_kind='settle' THEN false ELSE terminal_dispatch_needed END
	WHERE id=$1 AND dispatch_token=$2 AND dispatch_fence=$3 AND version=$4 AND dispatch_version=$4 AND dispatch_kind=$5 AND lifecycle_phase=$6 AND dispatch_state='claimed' AND dispatch_lease_until>clock_timestamp() AND ($10::uuid IS NULL OR saga_id IS NULL OR saga_id=$10)`, id, c.Token, c.Fence, c.Version, c.Kind, c.Snapshot.Phase, state, next, category, saga, accepted)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("Booking dispatch result commit outcome unknown: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
