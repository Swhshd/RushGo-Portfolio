package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/adminticketlist"
)

const adminEventTicketListReadTimeout = 8 * time.Second

var ErrAdminEventTicketListUnavailable = errors.New("Admin event ticket list source unavailable")

type AdminEventTicketListRecord struct {
	TicketID    uuid.UUID
	EventID     uuid.UUID
	Status      string
	IssuedAt    time.Time
	CheckedInAt *time.Time
	RevokedAt   *time.Time
}

type AdminEventTicketListQuery struct {
	StatusFilter string
	PageSize     int32
	After        *adminticketlist.Cursor
}

type AdminEventTicketListSnapshot struct {
	EventID    uuid.UUID
	ObservedAt time.Time
	Tickets    []AdminEventTicketListRecord
}

type AdminEventTicketListReader interface {
	ReadAdminEventTickets(context.Context, uuid.UUID, AdminEventTicketListQuery) (AdminEventTicketListSnapshot, error)
}

func (r *PostgresBookingRepository) ReadAdminEventTickets(ctx context.Context, eventID uuid.UUID, query AdminEventTicketListQuery) (AdminEventTicketListSnapshot, error) {
	if r == nil || r.pool == nil || eventID == uuid.Nil {
		return AdminEventTicketListSnapshot{}, ErrAdminEventTicketListUnavailable
	}
	filter, err := adminticketlist.NormalizeStatusFilter(query.StatusFilter)
	if err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	pageSize, err := adminticketlist.NormalizePageSize(query.PageSize)
	if err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	if query.After != nil && (query.After.EventID != eventID || query.After.StatusFilter != filter) {
		return AdminEventTicketListSnapshot{}, adminticketlist.ErrInvalidCursor
	}

	bounded, cancel := context.WithTimeout(ctx, adminEventTicketListReadTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(bounded, `SET LOCAL statement_timeout = '5s'`); err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	if _, err := tx.Exec(bounded, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return AdminEventTicketListSnapshot{}, err
	}

	var ticketCount int64
	if err := tx.QueryRow(bounded, `
		SELECT count(*)
		FROM (
			SELECT 1
			FROM tickets
			WHERE event_id = $1
			ORDER BY status
			LIMIT $2
		) bounded_event_tickets`, eventID, AdminEventTicketOperationsCap+1).Scan(&ticketCount); err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	if ticketCount > AdminEventTicketOperationsCap {
		return AdminEventTicketListSnapshot{}, ErrAdminEventTicketOperationsCapacityExceeded
	}

	var auditCount int64
	if err := tx.QueryRow(bounded, `
		SELECT count(*)
		FROM (
			SELECT 1
			FROM ticket_checkins
			WHERE event_id = $1
			ORDER BY checked_in_at DESC
			LIMIT $2
		) bounded_event_ticket_audits`, eventID, AdminEventTicketOperationsCap+1).Scan(&auditCount); err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	if auditCount > AdminEventTicketOperationsCap {
		return AdminEventTicketListSnapshot{}, ErrAdminEventTicketOperationsCapacityExceeded
	}

	const boundedTicketCTE = `WITH bounded_tickets AS MATERIALIZED (
		SELECT id, event_id, status, issued_at, checked_in_at, revoked_at
		FROM tickets
		WHERE event_id = $1
		ORDER BY status
		LIMIT $2
	)`
	var rows pgx.Rows
	if query.After == nil {
		rows, err = tx.Query(bounded, boundedTicketCTE+`
			SELECT id, event_id, status, issued_at, checked_in_at, revoked_at
			FROM bounded_tickets
			WHERE $3 = 'ALL' OR status = $3
			ORDER BY issued_at DESC, id DESC
			LIMIT $4`, eventID, AdminEventTicketOperationsCap+1, filter, pageSize+1)
	} else {
		rows, err = tx.Query(bounded, boundedTicketCTE+`
			SELECT id, event_id, status, issued_at, checked_in_at, revoked_at
			FROM bounded_tickets
			WHERE ($3 = 'ALL' OR status = $3)
			  AND (issued_at, id) < ($4, $5)
			ORDER BY issued_at DESC, id DESC
			LIMIT $6`, eventID, AdminEventTicketOperationsCap+1, filter, query.After.IssuedAt, query.After.TicketID, pageSize+1)
	}
	if err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	snapshot := AdminEventTicketListSnapshot{EventID: eventID, Tickets: make([]AdminEventTicketListRecord, 0, pageSize+1)}
	for rows.Next() {
		var ticket AdminEventTicketListRecord
		if err := rows.Scan(&ticket.TicketID, &ticket.EventID, &ticket.Status, &ticket.IssuedAt, &ticket.CheckedInAt, &ticket.RevokedAt); err != nil {
			rows.Close()
			return AdminEventTicketListSnapshot{}, err
		}
		snapshot.Tickets = append(snapshot.Tickets, ticket)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return AdminEventTicketListSnapshot{}, rowsErr
	}
	if len(snapshot.Tickets) > int(pageSize+1) {
		return AdminEventTicketListSnapshot{}, fmt.Errorf("Admin event ticket list returned too many rows: %d", len(snapshot.Tickets))
	}
	if err := tx.QueryRow(bounded, `SELECT clock_timestamp()`).Scan(&snapshot.ObservedAt); err != nil {
		return AdminEventTicketListSnapshot{}, err
	}
	if err := tx.Commit(bounded); err != nil {
		return AdminEventTicketListSnapshot{}, fmt.Errorf("Admin event ticket list snapshot commit failed: %w", err)
	}
	return snapshot, nil
}
