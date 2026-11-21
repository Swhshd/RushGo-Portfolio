package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/booking/internal/domain"
)

func (r *PostgresBookingRepository) LookupTicketIssuanceReceipt(ctx context.Context, event domain.TicketIssuanceEvent) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation
		FROM ticket_issuance_receipts WHERE event_id=$1 OR (saga_id=$2 AND booking_id=$3 AND lifecycle_generation=$4)`,
		event.ID, event.SagaID, event.BookingID, event.Generation)
	if err != nil {
		return false, fmt.Errorf("lookup Ticket issuance receipt: %w", err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var eventID, sagaID, bookingID, paymentID, eventRefID uuid.UUID
		var generation int64
		if err := rows.Scan(&eventID, &sagaID, &bookingID, &paymentID, &eventRefID, &generation); err != nil {
			return false, fmt.Errorf("scan Ticket issuance receipt identity: %w", err)
		}
		if eventID != event.ID || sagaID != event.SagaID || bookingID != event.BookingID || paymentID != event.PaymentID || eventRefID != event.EventID || generation != event.Generation {
			return false, ErrTicketIdentityConflict
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate Ticket issuance receipts: %w", err)
	}
	return found, nil
}

func (r *PostgresBookingRepository) LoadTicketIssuanceCandidate(ctx context.Context, event domain.TicketIssuanceEvent) (domain.TicketIssuanceCandidate, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var candidate domain.TicketIssuanceCandidate
	var status string
	var generation *int64
	var sagaID *uuid.UUID
	var currency *string
	err := r.pool.QueryRow(ctx, `SELECT id,user_id,event_id,status,total_amount_cents,managed,lifecycle_generation,saga_id,currency
		FROM bookings WHERE id=$1`, event.BookingID).Scan(&candidate.BookingID, &candidate.UserID, &candidate.EventID, &status, &candidate.TotalAmountCents, &candidate.Managed, &generation, &sagaID, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TicketIssuanceCandidate{}, ErrTicketBookingMissing
	}
	if err != nil {
		return domain.TicketIssuanceCandidate{}, fmt.Errorf("load ticket Booking candidate: %w", err)
	}
	candidate.Status = domain.BookingStatus(status)
	if generation != nil {
		candidate.Generation = *generation
	}
	if sagaID != nil {
		candidate.SagaID = *sagaID
	}
	if currency != nil {
		candidate.Currency = *currency
	}
	candidate.PaymentID = event.PaymentID
	booking := &domain.Booking{ID: candidate.BookingID}
	if err := loadLifecycleItems(ctx, r.pool, booking); err != nil {
		return domain.TicketIssuanceCandidate{}, fmt.Errorf("load ticket Booking items: %w", err)
	}
	candidate.Items = booking.Items
	return candidate, nil
}

func (r *PostgresBookingRepository) ApplyTicketIssuance(ctx context.Context, event domain.TicketIssuanceEvent, candidate domain.TicketIssuanceCandidate, decision domain.TicketIssuanceDecision, evidence domain.TicketAuthorityEvidence) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if candidate.BookingID == uuid.Nil || candidate.BookingID != event.BookingID {
		return ErrTicketIdentityConflict
	}
	verified := domain.ValidateTicketIssuance(event, candidate, evidence)
	if verified.Eligible != decision.Eligible {
		return ErrTicketIdentityConflict
	}
	if decision.ReasonCode == "" {
		decision.ReasonCode = verified.ReasonCode
	}
	if decision.Eligible && (evidence.ObservedAt.IsZero() || !domain.TicketAuthoritySourceAllowed(evidence.Source)) {
		return ErrTicketIdentityConflict
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Ticket issuance: %w", err)
	}
	defer rollbackBooking(tx)

	var current domain.TicketIssuanceCandidate
	var status string
	var generation *int64
	var sagaID *uuid.UUID
	var currency *string
	err = tx.QueryRow(ctx, `SELECT id,user_id,event_id,status,total_amount_cents,managed,lifecycle_generation,saga_id,currency
		FROM bookings WHERE id=$1 FOR UPDATE`, event.BookingID).Scan(&current.BookingID, &current.UserID, &current.EventID, &status, &current.TotalAmountCents, &current.Managed, &generation, &sagaID, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTicketBookingMissing
	}
	if err != nil {
		return fmt.Errorf("lock ticket Booking: %w", err)
	}
	current.Status = domain.BookingStatus(status)
	if generation != nil {
		current.Generation = *generation
	}
	if sagaID != nil {
		current.SagaID = *sagaID
	}
	if currency != nil {
		current.Currency = *currency
	}
	current.PaymentID = event.PaymentID
	booking := &domain.Booking{ID: current.BookingID}
	if err = loadLifecycleItems(ctx, tx, booking); err != nil {
		return fmt.Errorf("reload ticket Booking items: %w", err)
	}
	current.Items = booking.Items
	if current.BookingID != candidate.BookingID || current.UserID != candidate.UserID || current.EventID != candidate.EventID || current.Status != candidate.Status ||
		current.Managed != candidate.Managed || current.Generation != candidate.Generation || current.SagaID != candidate.SagaID || current.TotalAmountCents != candidate.TotalAmountCents ||
		current.Currency != candidate.Currency || !reflect.DeepEqual(current.Items, candidate.Items) {
		return ErrTicketIdentityConflict
	}

	var existingEvent, existingSaga, existingBooking, existingPayment, existingEventRef uuid.UUID
	var existingGeneration int64
	err = tx.QueryRow(ctx, `SELECT event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation
		FROM ticket_issuance_receipts WHERE saga_id=$1 AND booking_id=$2 AND lifecycle_generation=$3 FOR UPDATE`, event.SagaID, event.BookingID, event.Generation).
		Scan(&existingEvent, &existingSaga, &existingBooking, &existingPayment, &existingEventRef, &existingGeneration)
	if err == nil {
		if existingEvent != event.ID || existingSaga != event.SagaID || existingBooking != event.BookingID || existingPayment != event.PaymentID || existingEventRef != event.EventID || existingGeneration != event.Generation {
			return ErrTicketIdentityConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read Ticket issuance receipt: %w", err)
	}

	count := 0
	if decision.Eligible {
		for _, item := range current.Items {
			for _, seatID := range item.SeatIDs {
				ticketID := uuid.New()
				if _, err = tx.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation)
					VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, ticketID, event.BookingID, item.ID, event.SagaID, event.PaymentID, event.EventID, item.TicketTierID, seatID, event.Generation); err != nil {
					return fmt.Errorf("insert Ticket entitlement: %w", err)
				}
				count++
			}
		}
	}
	outcome := "denied"
	if decision.Eligible {
		outcome = "issued"
	}
	observedAt := any(nil)
	if !evidence.ObservedAt.IsZero() {
		observedAt = evidence.ObservedAt.UTC()
	}
	disputeState, refundState := string(evidence.DisputeState), string(evidence.RefundState)
	if disputeState == "" {
		disputeState = string(domain.EvidenceUnknown)
	}
	if refundState == "" {
		refundState = string(domain.RefundUnknown)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, event.ID, event.SagaID, event.BookingID, event.PaymentID, event.EventID, event.Generation, outcome, decision.ReasonCode, disputeState, refundState, evidence.Source, observedAt, count); err != nil {
		return fmt.Errorf("insert Ticket issuance receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("Ticket issuance transaction outcome unknown: %w", err)
	}
	return nil
}
