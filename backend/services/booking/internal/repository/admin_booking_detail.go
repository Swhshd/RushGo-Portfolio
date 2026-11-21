package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/booking/internal/domain"
)

// AdminBookingDetailRepository is an additive, read-only exact-booking
// projection. It is intentionally separate from the buyer-facing repository
// methods and never joins another service's database.
type AdminBookingDetailRepository interface {
	GetAdminBookingDetail(context.Context, uuid.UUID) (*domain.AdminBookingDetail, error)
}

func (r *PostgresBookingRepository) GetAdminBookingDetail(ctx context.Context, bookingID uuid.UUID) (*domain.AdminBookingDetail, error) {
	if r == nil || r.pool == nil || bookingID == uuid.Nil {
		return nil, ErrNotFound
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin admin booking detail read: %w", err)
	}
	defer tx.Rollback(ctx)

	detail := &domain.AdminBookingDetail{}
	booking := &domain.Booking{}
	var status string
	if err := tx.QueryRow(ctx, `SELECT id,user_id,event_id,status,total_amount_cents,version,created_at,
		COALESCE(managed,false),COALESCE(lifecycle_generation,0),COALESCE(lifecycle_phase,''),
		COALESCE(currency,''),COALESCE(booking_mode,''),clock_timestamp()
		FROM bookings WHERE id=$1`, bookingID).Scan(
		&booking.ID, &booking.UserID, &booking.EventID, &status, &booking.TotalAmountCents, &booking.Version, &booking.CreatedAt,
		&booking.Managed, &booking.LifecycleGeneration, &booking.LifecyclePhase,
		&booking.Currency, &booking.BookingMode, &detail.ObservedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read admin booking detail: %w", err)
	}
	booking.Status = domain.BookingStatus(status)
	detail.Booking = booking

	items, err := tx.Query(ctx, `SELECT id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids
		FROM booking_items WHERE booking_id=$1 ORDER BY id LIMIT 1001`, bookingID)
	if err != nil {
		return nil, fmt.Errorf("read admin booking items: %w", err)
	}
	for items.Next() {
		var item domain.BookingItem
		if err := items.Scan(&item.ID, &item.BookingID, &item.TicketTierID, &item.Quantity, &item.UnitPriceCents, &item.SeatIDs); err != nil {
			items.Close()
			return nil, fmt.Errorf("scan admin booking item: %w", err)
		}
		booking.Items = append(booking.Items, item)
	}
	if err := items.Err(); err != nil {
		items.Close()
		return nil, fmt.Errorf("iterate admin booking items: %w", err)
	}
	items.Close()

	tickets, err := tx.Query(ctx, `SELECT id,status,seat_id,ticket_tier_id,issued_at,checked_in_at,revoked_at
		FROM tickets WHERE booking_id=$1 ORDER BY issued_at,id LIMIT 5001`, bookingID)
	if err != nil {
		return nil, fmt.Errorf("read admin booking tickets: %w", err)
	}
	for tickets.Next() {
		var ticket domain.AdminBookingTicket
		if err := tickets.Scan(&ticket.TicketID, &ticket.Status, &ticket.SeatID, &ticket.TicketTierID,
			&ticket.IssuedAt, &ticket.CheckedInAt, &ticket.RevokedAt); err != nil {
			tickets.Close()
			return nil, fmt.Errorf("scan admin booking ticket: %w", err)
		}
		detail.Tickets = append(detail.Tickets, ticket)
	}
	if err := tickets.Err(); err != nil {
		tickets.Close()
		return nil, fmt.Errorf("iterate admin booking tickets: %w", err)
	}
	tickets.Close()

	receipts, err := tx.Query(ctx, `SELECT outcome,reason_code,ticket_count,observed_at,created_at
		FROM ticket_issuance_receipts WHERE booking_id=$1 ORDER BY created_at DESC,event_id DESC LIMIT 11`, bookingID)
	if err != nil {
		return nil, fmt.Errorf("read admin booking receipt: %w", err)
	}
	for receipts.Next() {
		var receipt domain.AdminBookingReceipt
		if err := receipts.Scan(&receipt.Outcome, &receipt.ReasonCode, &receipt.TicketCount, &receipt.ObservedAt, &receipt.CreatedAt); err != nil {
			receipts.Close()
			return nil, fmt.Errorf("scan admin booking receipt: %w", err)
		}
		receipt.ReceiptCount = 1
		if detail.Receipt == nil {
			detail.Receipt = &receipt
		}
	}
	if err := receipts.Err(); err != nil {
		receipts.Close()
		return nil, fmt.Errorf("iterate admin booking receipts: %w", err)
	}
	receipts.Close()
	if detail.Receipt != nil {
		var count int32
		if err := tx.QueryRow(ctx, `SELECT count(*)::int FROM ticket_issuance_receipts WHERE booking_id=$1`, bookingID).Scan(&count); err != nil {
			return nil, fmt.Errorf("count admin booking receipts: %w", err)
		}
		detail.Receipt.ReceiptCount = count
	}

	refund, err := scanBookingRefundRequest(tx.QueryRow(ctx, bookingRefundRequestSelect+`
		WHERE r.booking_id=$1 ORDER BY r.requested_at DESC,r.request_id DESC LIMIT 1`, bookingID))
	if err == nil {
		detail.Refund = &refund
	} else if !errors.Is(err, ErrRefundRequestNotFound) {
		return nil, fmt.Errorf("read admin booking refund: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit admin booking detail read: %w", err)
	}
	detail.ObservedAt = detail.ObservedAt.UTC()
	return detail, nil
}

var _ AdminBookingDetailRepository = (*PostgresBookingRepository)(nil)
