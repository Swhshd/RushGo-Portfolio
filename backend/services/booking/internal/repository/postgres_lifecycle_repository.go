package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/topics"
)

type bookingQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func rollbackBooking(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// request_id is an existing nullable Booking identity column. This SELECT
// projects it for dispatch-boundary correlation; it adds no persisted state.
const lifecycleColumns = `id,user_id,event_id,status,total_amount_cents,version,created_at,managed,
	lifecycle_generation,lifecycle_phase,decision_id,confirmation_decision_id,decision_reason,currency,booking_mode,reservation_deadline,decision_kind,terminal_dispatch_needed,dispatch_state,request_id`

func scanLifecycle(row pgx.Row) (*domain.LifecycleSnapshot, string, bool, error) {
	b := &domain.Booking{}
	v := &domain.LifecycleSnapshot{Booking: b}
	var generation *int64
	var phase, reason, currency, mode, kind *string
	var requestID *string
	var status string
	var dispatch bool
	var dispatchState *string
	err := row.Scan(&b.ID, &b.UserID, &b.EventID, &status, &b.TotalAmountCents, &b.Version, &b.CreatedAt, &b.Managed,
		&generation, &phase, &v.DecisionID, &v.ConfirmationDecisionID, &reason, &currency, &mode, &v.ReservationDeadline, &kind, &dispatch, &dispatchState, &requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", false, ErrNotFound
	}
	if err != nil {
		return nil, "", false, fmt.Errorf("scan Booking lifecycle: %w", err)
	}
	b.Status = domain.BookingStatus(status)
	if requestID != nil {
		b.RequestID = *requestID
	}
	if generation != nil {
		v.Generation = *generation
	}
	if phase != nil {
		v.Phase = *phase
	} else if b.Status != domain.StatusPending {
		v.Phase = "TERMINAL"
	} else {
		v.Phase = "UNKNOWN"
	}
	if reason != nil {
		v.Reason = *reason
	}
	if currency != nil {
		v.Currency = *currency
	}
	if mode != nil {
		v.BookingMode = *mode
	}
	if dispatch {
		v.DispatchState = "pending"
	}
	if dispatchState != nil {
		v.DispatchState = *dispatchState
	}
	b.LifecycleGeneration, b.LifecyclePhase, b.Currency, b.BookingMode, b.ReservationDeadline = v.Generation, v.Phase, v.Currency, v.BookingMode, v.ReservationDeadline
	decisionKind := ""
	if kind != nil {
		decisionKind = *kind
	}
	return v, decisionKind, dispatch, nil
}

func loadLifecycleItems(ctx context.Context, q bookingQuerier, b *domain.Booking) error {
	rows, err := q.Query(ctx, `SELECT id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids FROM booking_items WHERE booking_id=$1 ORDER BY id`, b.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item domain.BookingItem
		if err := rows.Scan(&item.ID, &item.BookingID, &item.TicketTierID, &item.Quantity, &item.UnitPriceCents, &item.SeatIDs); err != nil {
			return err
		}
		b.Items = append(b.Items, item)
	}
	return rows.Err()
}

func (r *PostgresBookingRepository) GetLifecycle(ctx context.Context, ref domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if ref.BookingID == uuid.Nil || ref.Generation != 1 {
		return nil, ErrInvalidLifecycleDecision
	}
	v, _, _, err := scanLifecycle(r.pool.QueryRow(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE id=$1`, ref.BookingID))
	if err != nil {
		return nil, err
	}
	if v.Booking.Managed && v.Generation != ref.Generation {
		return nil, ErrLifecycleConflict
	}
	if err = loadLifecycleItems(ctx, r.pool, v.Booking); err != nil {
		return nil, err
	}
	return v, nil
}

func validateLifecycleDecision(d domain.LifecycleDecision, event *OutboxEvent) error {
	if d.Ref.BookingID == uuid.Nil || d.Ref.Generation != 1 || d.ExpectedVersion < 1 || d.DecisionID != domain.BookingDecisionID(d.Ref.BookingID, d.Kind) {
		return ErrInvalidLifecycleDecision
	}
	want := ""
	switch d.Kind {
	case "claim_confirmation":
		if event != nil {
			return ErrInvalidLifecycleDecision
		}
		return nil
	case "confirm":
		want = topics.TypeBookingConfirmed
	case "cancel":
		want = topics.TypeBookingCancelled
	case "expire", "fail", "abort_confirmation":
		want = topics.TypeBookingFailed
	default:
		return ErrInvalidLifecycleDecision
	}
	if event == nil || event.ID != domain.BookingOutboxID(d.Ref.BookingID, d.Kind) || event.EventType != want || event.EventKey != d.Ref.BookingID.String() || event.CreatedAt.IsZero() {
		return ErrInvalidLifecycleDecision
	}
	var payload struct {
		BookingID string `json:"booking_id"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil || payload.BookingID != d.Ref.BookingID.String() {
		return ErrInvalidLifecycleDecision
	}
	if (d.Kind == "confirm" || d.Kind == "abort_confirmation") && (d.ConfirmationDecisionID == nil || *d.ConfirmationDecisionID != domain.BookingDecisionID(d.Ref.BookingID, "claim_confirmation")) {
		return ErrInvalidLifecycleDecision
	}
	return nil
}

func (r *PostgresBookingRepository) DecideLifecycle(ctx context.Context, d domain.LifecycleDecision, event *OutboxEvent) (*domain.LifecycleSnapshot, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := validateLifecycleDecision(d, event); err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer rollbackBooking(tx)
	v, kind, _, err := scanLifecycle(tx.QueryRow(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE id=$1 FOR UPDATE`, d.Ref.BookingID))
	if err != nil {
		return nil, false, err
	}
	if !v.Booking.Managed {
		return v, false, ErrLegacyLifecycleAttention
	}
	if err = loadLifecycleItems(ctx, tx, v.Booking); err != nil {
		return nil, false, err
	}
	if v.Generation != d.Ref.Generation {
		return v, false, ErrLifecycleConflict
	}
	if v.DecisionID != nil && *v.DecisionID == d.DecisionID && kind == d.Kind {
		return v, false, tx.Commit(ctx)
	}
	allowed := v.Booking.Version == d.ExpectedVersion
	phase, status := v.Phase, v.Booking.Status
	switch d.Kind {
	case "claim_confirmation":
		allowed = allowed && v.Phase == "OPEN" && v.ReservationDeadline != nil && v.ReservationDeadline.After(time.Now())
		phase = "CONFIRMING"
	case "confirm", "abort_confirmation":
		allowed = allowed && v.Phase == "CONFIRMING" && v.ConfirmationDecisionID != nil && d.ConfirmationDecisionID != nil && *v.ConfirmationDecisionID == *d.ConfirmationDecisionID
		phase = "TERMINAL"
		status = domain.StatusConfirmed
		if d.Kind == "abort_confirmation" {
			status = domain.StatusFailed
		}
	case "cancel", "expire", "fail":
		allowed = allowed && v.Phase == "OPEN"
		if d.Kind == "expire" {
			allowed = allowed && v.ReservationDeadline != nil && !v.ReservationDeadline.After(time.Now())
		}
		phase = "TERMINAL"
		status = domain.StatusCancelled
		if d.Kind == "expire" {
			status = domain.StatusExpired
		}
		if d.Kind == "fail" {
			status = domain.StatusFailed
		}
	}
	if !allowed {
		return v, false, tx.Commit(ctx)
	}
	confirmation := v.ConfirmationDecisionID
	if d.Kind == "claim_confirmation" {
		confirmation = &d.DecisionID
	}
	_, err = tx.Exec(ctx, `UPDATE bookings SET lifecycle_phase=$2,status=$3,decision_kind=$4,decision_reason=$5,
		decision_id=$6,confirmation_decision_id=$7,version=version+1,terminal_dispatch_needed=$8 WHERE id=$1`,
		d.Ref.BookingID, phase, string(status), d.Kind, d.Reason, d.DecisionID, confirmation, phase == "TERMINAL" && status != domain.StatusConfirmed)
	if err != nil {
		return nil, false, err
	}
	if event != nil {
		_, err = tx.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload,published,created_at) VALUES($1,$2,$3,$4,false,$5)`, event.ID, event.EventType, event.EventKey, event.Payload, event.CreatedAt)
		if err != nil {
			return nil, false, fmt.Errorf("atomic Booking decision outbox: %w", err)
		}
	}
	// Every version transition fences the old RPC. A terminal loser replaces
	// start with settle; intermediate confirmation retains original start work.
	if _, err := tx.Exec(ctx, `UPDATE bookings SET dispatch_kind=CASE WHEN $2 THEN 'settle' ELSE COALESCE(dispatch_kind,'start') END,
	 dispatch_state=CASE WHEN $3 THEN 'accepted' ELSE 'pending' END,dispatch_version=version,dispatch_due_at=clock_timestamp(),dispatch_token=NULL,dispatch_lease_until=NULL,dispatch_fence=dispatch_fence+1,dispatch_error=NULL
	 WHERE id=$1`, d.Ref.BookingID, phase == "TERMINAL" && status != domain.StatusConfirmed, status == domain.StatusConfirmed); err != nil {
		return nil, false, err
	}
	v.Booking.Version++
	v.Booking.Status = status
	v.Booking.LifecyclePhase = phase
	v.Phase = phase
	v.DecisionID = &d.DecisionID
	v.ConfirmationDecisionID = confirmation
	v.Reason = d.Reason
	if phase == "TERMINAL" && status != domain.StatusConfirmed {
		v.DispatchState = "pending"
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("Booking decision commit outcome unknown: %w", err)
	}
	return v, true, nil
}

func (r *PostgresBookingRepository) ListExpiryCandidates(ctx context.Context, limit int, after uuid.UUID) ([]domain.LifecycleSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidLifecycleDecision
	}
	rows, err := r.pool.Query(ctx, `SELECT `+lifecycleColumns+` FROM bookings WHERE managed AND lifecycle_phase='OPEN' AND reservation_deadline<=NOW() AND id>$1 ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.LifecycleSnapshot{}
	for rows.Next() {
		v, _, _, err := scanLifecycle(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i := range result {
		if err = loadLifecycleItems(ctx, r.pool, result[i].Booking); err != nil {
			return nil, err
		}
	}
	return result, nil
}
