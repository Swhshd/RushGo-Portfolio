package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
)

type PostgresBookingRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresBookingRepository(pool *pgxpool.Pool) *PostgresBookingRepository {
	return &PostgresBookingRepository{pool: pool}
}

func (r *PostgresBookingRepository) Create(ctx context.Context, booking *domain.Booking) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO bookings (id, user_id, event_id, status, total_amount_cents, version, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err = tx.Exec(ctx, query, booking.ID, booking.UserID, booking.EventID,
		string(booking.Status), booking.TotalAmountCents, booking.Version, booking.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert booking: %w", err)
	}

	numFieldStepUpdate := 6
	insertValSQL := ""
	bookingItemFieldValUpdate := []interface{}{}
	for stepIdx, item := range booking.Items {
		delta := stepIdx * numFieldStepUpdate
		for index := range numFieldStepUpdate {
			switch index {
			case numFieldStepUpdate - 1:
				insertValSQL += fmt.Sprintf(`$%d)`, delta+index+1)
			case 0:
				insertValSQL += fmt.Sprintf(`($%d, `, delta+index+1)
			default:
				insertValSQL += fmt.Sprintf(`$%d, `, delta+index+1)
			}
		}
		if stepIdx < len(booking.Items)-1 {
			insertValSQL += ",\n"
		}
		bookingItemFieldValUpdate = append(bookingItemFieldValUpdate, item.ID, booking.ID, item.TicketTierID, item.Quantity, item.UnitPriceCents, item.SeatIDs)
	}

	itemQuery := `INSERT INTO booking_items (id, booking_id, ticket_tier_id, quantity, unit_price_cents, seat_ids)
									VALUES
									` + insertValSQL
	_, err = tx.Exec(ctx, itemQuery, bookingItemFieldValUpdate...)
	if err != nil {
		return fmt.Errorf("insert booking item: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *PostgresBookingRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Booking, error) {
	query := `SELECT id, user_id, event_id, status, total_amount_cents, version, created_at, COALESCE(currency, '') FROM bookings WHERE id = $1`
	b := &domain.Booking{}
	var status string
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&b.ID, &b.UserID, &b.EventID, &status, &b.TotalAmountCents, &b.Version, &b.CreatedAt, &b.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get booking: %w", err)
	}
	b.Status = domain.BookingStatus(status)

	itemQuery := `SELECT id, booking_id, ticket_tier_id, quantity, unit_price_cents, seat_ids FROM booking_items WHERE booking_id = $1`
	rows, err := r.pool.Query(ctx, itemQuery, id)
	if err != nil {
		return nil, fmt.Errorf("get items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item domain.BookingItem
		if err := rows.Scan(&item.ID, &item.BookingID, &item.TicketTierID, &item.Quantity, &item.UnitPriceCents, &item.SeatIDs); err != nil {
			return nil, fmt.Errorf("scan item: %w", err)
		}
		b.Items = append(b.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate booking items: %w", err)
	}
	return b, nil
}

func (r *PostgresBookingRepository) ListByUserID(ctx context.Context, userID uuid.UUID, page, pageSize int) ([]*domain.Booking, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	var total int
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM bookings WHERE user_id = $1", userID).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	query := `SELECT id, user_id, event_id, status, total_amount_cents, version, created_at, COALESCE(currency, '')
            FROM bookings WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`
	rows, err := r.pool.Query(ctx, query, userID, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var bookings []*domain.Booking
	for rows.Next() {
		b := &domain.Booking{}
		var status string
		if err := rows.Scan(&b.ID, &b.UserID, &b.EventID, &status, &b.TotalAmountCents, &b.Version, &b.CreatedAt, &b.Currency); err != nil {
			return nil, 0, err
		}
		b.Status = domain.BookingStatus(status)

		itemQuery := `SELECT id, booking_id, ticket_tier_id, quantity, unit_price_cents, seat_ids FROM booking_items WHERE booking_id = $1`
		itemRows, err := r.pool.Query(ctx, itemQuery, b.ID)
		if err != nil {
			return nil, 0, err
		}
		for itemRows.Next() {
			var item domain.BookingItem
			if err := itemRows.Scan(&item.ID, &item.BookingID, &item.TicketTierID, &item.Quantity, &item.UnitPriceCents, &item.SeatIDs); err != nil {
				itemRows.Close()
				return nil, 0, err
			}
			b.Items = append(b.Items, item)
		}
		if err := itemRows.Err(); err != nil {
			itemRows.Close()
			return nil, 0, err
		}
		itemRows.Close()

		bookings = append(bookings, b)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return bookings, total, nil
}

func (r *PostgresBookingRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.BookingStatus) error {
	changed, err := r.CompareAndSetStatus(ctx, id, domain.StatusPending, status)
	if err != nil {
		return err
	}
	if changed {
		return nil
	}
	b, err := r.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if b.Status == status {
		return nil
	}
	return ErrLifecycleConflict
}

func (r *PostgresBookingRepository) CompareAndSetStatus(ctx context.Context, id uuid.UUID, expected, status domain.BookingStatus) (bool, error) {
	result, err := r.pool.Exec(ctx, `UPDATE bookings SET status = $3 WHERE id = $1 AND status = $2 AND NOT managed AND status='PENDING' AND $3<>'PENDING'`, id, string(expected), string(status))
	if err != nil {
		return false, fmt.Errorf("compare and set booking status: %w", err)
	}
	if result.RowsAffected() == 0 {
		var managed bool
		if err := r.pool.QueryRow(ctx, `SELECT managed FROM bookings WHERE id=$1`, id).Scan(&managed); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, ErrNotFound
			}
			return false, err
		}
		if managed {
			return false, ErrLifecycleConflict
		}
	}
	return result.RowsAffected() == 1, nil
}

func (r *PostgresBookingRepository) ConfirmBookingWithOutbox(ctx context.Context, id uuid.UUID, event OutboxEvent) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin confirmation transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx,
		`UPDATE bookings SET status = $3 WHERE id = $1 AND status = $2 AND NOT managed`,
		id, string(domain.StatusPending), string(domain.StatusConfirmed),
	)
	if err != nil {
		return false, fmt.Errorf("confirm booking: %w", err)
	}
	if result.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit unchanged booking confirmation: %w", err)
		}
		return false, nil
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (id, event_type, event_key, payload, published, created_at)
		VALUES ($1, $2, $3, $4, FALSE, $5)
	`, event.ID, event.EventType, event.EventKey, event.Payload, event.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("insert booking confirmation outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit booking confirmation and outbox: %w", err)
	}
	return true, nil
}

func (r *PostgresBookingRepository) ProcessPendingOutbox(ctx context.Context, limit int, publish OutboxPublisher) (int, error) {
	if limit < 1 {
		limit = 1
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox relay transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, event_type, event_key, payload, created_at
		FROM outbox
		WHERE published = FALSE
		ORDER BY created_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("select pending outbox events: %w", err)
	}

	var events []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.EventKey, &event.Payload, &event.CreatedAt); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan pending outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate pending outbox events: %w", err)
	}
	rows.Close()

	for _, event := range events {
		if err := publish(ctx, event); err != nil {
			return 0, fmt.Errorf("publish outbox event %s: %w", event.ID, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published = TRUE WHERE id = $1`, event.ID); err != nil {
			return 0, fmt.Errorf("mark outbox event %s published: %w", event.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox relay transaction: %w", err)
	}
	return len(events), nil
}

func (r *PostgresBookingRepository) CountPendingOutbox(ctx context.Context) (int64, error) {
	var count int64
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE published = FALSE`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending outbox events: %w", err)
	}
	return count, nil
}

func (r *PostgresBookingRepository) BatchUpdateStatus(ctx context.Context, ids []uuid.UUID, status domain.BookingStatus) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT id,status,managed FROM bookings WHERE id=ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id uuid.UUID
		var current string
		var managed bool
		if err := rows.Scan(&id, &current, &managed); err != nil {
			rows.Close()
			return err
		}
		if current != string(status) && (managed || current != "PENDING" || status == domain.StatusPending) {
			rows.Close()
			return ErrLifecycleConflict
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if _, err = tx.Exec(ctx, `UPDATE bookings SET status=$2 WHERE id=ANY($1) AND NOT managed AND status='PENDING' AND $2<>'PENDING'`, ids, string(status)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
