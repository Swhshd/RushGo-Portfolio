package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const AdminEventTicketOperationsCap = 5000

const (
	adminEventTicketOperationsRowLimit    = AdminEventTicketOperationsCap + 1
	adminEventTicketOperationsReadTimeout = 8 * time.Second
)

var ErrAdminEventTicketOperationsCapacityExceeded = errors.New("admin event ticket operations exceeds capacity")

type AdminEventTicketOperationsTicket struct {
	TicketID    uuid.UUID
	EventID     uuid.UUID
	Status      string
	CheckedInAt *time.Time
	RevokedAt   *time.Time
}

type AdminEventTicketOperationsAudit struct {
	TicketID    uuid.UUID
	EventID     uuid.UUID
	ResultCode  string
	CheckedInAt time.Time
}

// AdminEventAfterSalesException contains only Booking-owned delivery facts.
// A confirmed Booking with a Ticket count mismatch does not prove Payment
// collection; callers must not present it as paid or eligible for recovery.
type AdminEventAfterSalesException struct {
	BookingID              uuid.UUID
	State                  string
	BookingStatus          string
	ExpectedTicketCount    int64
	RecordedTicketCount    int64
	IssuanceReceiptOutcome string
	IssuanceReasonCode     string
	BookingCreatedAt       time.Time
}

type AdminEventTicketOperationsSnapshot struct {
	EventID            uuid.UUID
	ObservedAt         time.Time
	Tickets            []AdminEventTicketOperationsTicket
	Audits             []AdminEventTicketOperationsAudit
	DeliveryExceptions []AdminEventAfterSalesException
}

type AdminEventTicketOperationsReader interface {
	ReadAdminEventTicketOperations(context.Context, uuid.UUID) (AdminEventTicketOperationsSnapshot, error)
}

func (r *PostgresBookingRepository) ReadAdminEventTicketOperations(ctx context.Context, eventID uuid.UUID) (AdminEventTicketOperationsSnapshot, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil {
		return AdminEventTicketOperationsSnapshot{}, errors.New("Admin event ticket operations reader unavailable")
	}
	bounded, cancel := context.WithTimeout(ctx, adminEventTicketOperationsReadTimeout)
	defer cancel()

	tx, err := r.pool.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(bounded, `SET LOCAL statement_timeout = '5s'`); err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	if _, err := tx.Exec(bounded, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}

	snapshot := AdminEventTicketOperationsSnapshot{EventID: eventID}
	// This order matches tickets(event_id,status) and keeps the bounded scan
	// independent of booking status or other service-owned tables.
	tickets, err := tx.Query(bounded, `
		SELECT id, event_id, status, checked_in_at, revoked_at
		FROM tickets
		WHERE event_id = $1
		ORDER BY status
		LIMIT $2`, eventID, adminEventTicketOperationsRowLimit)
	if err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	snapshot.Tickets = make([]AdminEventTicketOperationsTicket, 0, adminEventTicketOperationsRowLimit)
	for tickets.Next() {
		var ticket AdminEventTicketOperationsTicket
		if err := tickets.Scan(&ticket.TicketID, &ticket.EventID, &ticket.Status, &ticket.CheckedInAt, &ticket.RevokedAt); err != nil {
			tickets.Close()
			return AdminEventTicketOperationsSnapshot{}, err
		}
		snapshot.Tickets = append(snapshot.Tickets, ticket)
	}
	ticketsErr := tickets.Err()
	tickets.Close()
	if ticketsErr != nil {
		return AdminEventTicketOperationsSnapshot{}, ticketsErr
	}
	if len(snapshot.Tickets) > AdminEventTicketOperationsCap {
		return AdminEventTicketOperationsSnapshot{}, ErrAdminEventTicketOperationsCapacityExceeded
	}

	audits, err := tx.Query(bounded, `
		SELECT ticket_id, event_id, result_code, checked_in_at
		FROM ticket_checkins
		WHERE event_id = $1
		ORDER BY checked_in_at DESC
		LIMIT $2`, eventID, adminEventTicketOperationsRowLimit)
	if err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	snapshot.Audits = make([]AdminEventTicketOperationsAudit, 0, adminEventTicketOperationsRowLimit)
	for audits.Next() {
		var audit AdminEventTicketOperationsAudit
		if err := audits.Scan(&audit.TicketID, &audit.EventID, &audit.ResultCode, &audit.CheckedInAt); err != nil {
			audits.Close()
			return AdminEventTicketOperationsSnapshot{}, err
		}
		snapshot.Audits = append(snapshot.Audits, audit)
	}
	auditsErr := audits.Err()
	audits.Close()
	if auditsErr != nil {
		return AdminEventTicketOperationsSnapshot{}, auditsErr
	}
	if len(snapshot.Audits) > AdminEventTicketOperationsCap {
		return AdminEventTicketOperationsSnapshot{}, ErrAdminEventTicketOperationsCapacityExceeded
	}

	if err := readAdminEventAfterSalesExceptions(bounded, tx, eventID, &snapshot); err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	// A Repeatable Read snapshot can include commits made after BEGIN but before
	// the first source read, so observe time only after all facts are loaded.
	if err := tx.QueryRow(bounded, `SELECT clock_timestamp()`).Scan(&snapshot.ObservedAt); err != nil {
		return AdminEventTicketOperationsSnapshot{}, err
	}
	if err := tx.Commit(bounded); err != nil {
		return AdminEventTicketOperationsSnapshot{}, fmt.Errorf("Admin event ticket operations snapshot commit failed: %w", err)
	}
	return snapshot, nil
}

func readAdminEventAfterSalesExceptions(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, snapshot *AdminEventTicketOperationsSnapshot) error {
	if snapshot == nil {
		return errors.New("Admin event aftersales snapshot unavailable")
	}
	rowLimit := AdminEventTicketOperationsCap + 1
	for _, query := range []struct {
		name string
		sql  string
	}{
		{
			name: "Booking",
			sql:  `SELECT count(*) FROM (SELECT id FROM bookings WHERE event_id=$1 LIMIT $2) bounded_event_bookings`,
		},
		{
			name: "BookingItem",
			sql: `SELECT count(*) FROM (
				SELECT bi.id FROM booking_items bi JOIN bookings b ON b.id=bi.booking_id
				WHERE b.event_id=$1 LIMIT $2
			) bounded_event_booking_items`,
		},
		{
			name: "Ticket",
			sql: `SELECT count(*) FROM (
				SELECT t.id FROM tickets t JOIN bookings b ON b.id=t.booking_id
				WHERE b.event_id=$1 LIMIT $2
			) bounded_event_booking_tickets`,
		},
		{
			name: "TicketIssuanceReceipt",
			sql: `SELECT count(*) FROM (
				SELECT r.event_id FROM ticket_issuance_receipts r JOIN bookings b ON b.id=r.booking_id
				WHERE b.event_id=$1 LIMIT $2
			) bounded_event_booking_issuance_receipts`,
		},
	} {
		var count int64
		if err := tx.QueryRow(ctx, query.sql, eventID, rowLimit).Scan(&count); err != nil {
			return fmt.Errorf("count bounded Admin event %s records: %w", query.name, err)
		}
		if count > AdminEventTicketOperationsCap {
			return ErrAdminEventTicketOperationsCapacityExceeded
		}
	}

	rows, err := tx.Query(ctx, `
		WITH event_bookings AS MATERIALIZED (
			SELECT b.id,b.event_id,b.status,b.created_at,b.saga_id,b.lifecycle_generation
			FROM bookings b
			WHERE b.event_id=$1 AND b.status='CONFIRMED'
			ORDER BY b.created_at,b.id
			LIMIT $2
		), expected AS (
			SELECT bi.booking_id,COALESCE(SUM(bi.quantity)::bigint,0) AS ticket_count
			FROM booking_items bi JOIN event_bookings b ON b.id=bi.booking_id
			GROUP BY bi.booking_id
		), recorded AS (
			SELECT t.booking_id,COUNT(*)::bigint AS ticket_count,
				COUNT(*) FILTER (WHERE t.event_id<>b.event_id OR t.lifecycle_generation<>b.lifecycle_generation)::bigint AS identity_conflict_count
			FROM tickets t JOIN event_bookings b ON b.id=t.booking_id
			GROUP BY t.booking_id
		), receipts AS (
			SELECT r.booking_id,COUNT(*)::bigint AS receipt_count,
				COUNT(*) FILTER (WHERE r.saga_id=b.saga_id AND r.event_ref_id=b.event_id AND r.lifecycle_generation=b.lifecycle_generation)::bigint AS matching_receipt_count,
				MAX(r.outcome) FILTER (WHERE r.saga_id=b.saga_id AND r.event_ref_id=b.event_id AND r.lifecycle_generation=b.lifecycle_generation) AS outcome,
				MAX(r.reason_code) FILTER (WHERE r.saga_id=b.saga_id AND r.event_ref_id=b.event_id AND r.lifecycle_generation=b.lifecycle_generation) AS reason_code
			FROM ticket_issuance_receipts r JOIN event_bookings b ON b.id=r.booking_id
			GROUP BY r.booking_id
		)
		SELECT b.id,b.status,COALESCE(e.ticket_count,0),COALESCE(t.ticket_count,0),
			COALESCE(t.identity_conflict_count,0),COALESCE(r.receipt_count,0),COALESCE(r.matching_receipt_count,0),
			COALESCE(r.outcome,''),COALESCE(r.reason_code,''),b.created_at
		FROM event_bookings b
		LEFT JOIN expected e ON e.booking_id=b.id
		LEFT JOIN recorded t ON t.booking_id=b.id
		LEFT JOIN receipts r ON r.booking_id=b.id
		WHERE COALESCE(e.ticket_count,0)<>COALESCE(t.ticket_count,0)
			OR COALESCE(t.identity_conflict_count,0)>0
			OR COALESCE(r.receipt_count,0)>1
			OR (COALESCE(r.receipt_count,0)=1 AND COALESCE(r.matching_receipt_count,0)<>1)
		ORDER BY b.created_at,b.id
		LIMIT $2`, eventID, rowLimit)
	if err != nil {
		return fmt.Errorf("read bounded Admin event delivery exceptions: %w", err)
	}
	defer rows.Close()
	snapshot.DeliveryExceptions = make([]AdminEventAfterSalesException, 0)
	for rows.Next() {
		var item AdminEventAfterSalesException
		var ticketIdentityConflicts, receiptCount, matchingReceiptCount int64
		if err := rows.Scan(&item.BookingID, &item.BookingStatus, &item.ExpectedTicketCount, &item.RecordedTicketCount,
			&ticketIdentityConflicts, &receiptCount, &matchingReceiptCount, &item.IssuanceReceiptOutcome, &item.IssuanceReasonCode, &item.BookingCreatedAt); err != nil {
			return fmt.Errorf("scan Admin event delivery exception: %w", err)
		}
		item.State = "CONFIRMED_TICKET_DELIVERY_PENDING"
		switch {
		case ticketIdentityConflicts > 0:
			item.IssuanceReceiptOutcome = "CONFLICT"
			item.IssuanceReasonCode = "ticket_identity_conflict"
		case receiptCount > 1:
			item.IssuanceReceiptOutcome = "CONFLICT"
			item.IssuanceReasonCode = "multiple_ticket_issuance_receipts"
		case receiptCount == 1 && matchingReceiptCount != 1:
			item.IssuanceReceiptOutcome = "CONFLICT"
			item.IssuanceReasonCode = "ticket_issuance_receipt_identity_conflict"
		case receiptCount == 0:
			item.IssuanceReceiptOutcome = "NOT_RECORDED"
		case item.IssuanceReceiptOutcome == "issued" || item.IssuanceReceiptOutcome == "denied":
			// Preserve the durable receipt outcome and reason.
		default:
			item.IssuanceReceiptOutcome = "CONFLICT"
			item.IssuanceReasonCode = "ticket_issuance_receipt_outcome_unknown"
		}
		snapshot.DeliveryExceptions = append(snapshot.DeliveryExceptions, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read Admin event delivery exceptions: %w", err)
	}
	if len(snapshot.DeliveryExceptions) > AdminEventTicketOperationsCap {
		return ErrAdminEventTicketOperationsCapacityExceeded
	}
	return nil
}
