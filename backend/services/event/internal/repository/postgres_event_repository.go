package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

// postgresPool keeps the existing pool operations injectable for read failures.
// Public constructors still receive the real pgx pool.
type postgresPool interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type PostgresEventRepository struct {
	pool postgresPool
}

func NewPostgresEventRepository(pool *pgxpool.Pool) *PostgresEventRepository {
	return &PostgresEventRepository{pool: pool}
}

func (r *PostgresEventRepository) Create(ctx context.Context, event *domain.Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if key := event.CreationRequest; key != nil {
		if key.ActorID == uuid.Nil || key.RequestID == "" || len(key.RequestID) > 128 || len(key.RequestHash) != sha256.Size {
			return ErrIdempotencyConflict
		}
		lockKey := "rushgo:event-create:" + key.ActorID.String() + ":" + key.RequestID
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
			return fmt.Errorf("lock event creation idempotency key: %w", err)
		}
		var priorHash []byte
		var priorEventID uuid.UUID
		err := tx.QueryRow(ctx, `SELECT request_hash,event_id FROM event_creation_requests WHERE actor_id=$1 AND request_id=$2`, key.ActorID, key.RequestID).Scan(&priorHash, &priorEventID)
		if err == nil {
			if !bytes.Equal(priorHash, key.RequestHash) {
				return ErrIdempotencyConflict
			}
			priorEvent, err := getEventByID(ctx, tx, priorEventID)
			if err != nil {
				return ErrIdempotencyConflict
			}
			*event = *priorEvent
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit idempotent event replay: %w", err)
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read event creation idempotency key: %w", err)
		}
	}

	query := `INSERT INTO events (id, title, description, category, venue, location, date, image_url, status, publication_status, sales_status, sales_start_at, sales_end_at, created_at, updated_at, sales_generation,first_sale_opened_at)
	            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
	                CASE WHEN $10::varchar(20)='published' AND $11::varchar(20) IN ('scheduled','selling') THEN 1 ELSE 0 END,
	                CASE WHEN $10::varchar(20)='published' AND $11::varchar(20)='selling' AND ($12::timestamptz IS NULL OR $12::timestamptz<=clock_timestamp())
	                  AND ($13::timestamptz IS NULL OR $13::timestamptz>clock_timestamp()) AND $7>clock_timestamp() THEN clock_timestamp() ELSE NULL END)`
	_, err = tx.Exec(ctx, query,
		event.ID, event.Title, event.Description, event.Category, event.Venue,
		event.Location, event.Date, event.ImageURL, event.Status, event.PublicationStatus, event.SalesStatus,
		event.SalesStartAt, event.SalesEndAt, event.CreatedAt, event.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	numFieldStepUpdate := 8
	insertValSQL := ""
	tierFieldValUpdate := []interface{}{}
	for tierIdx, tier := range event.Tiers {
		delta := tierIdx * numFieldStepUpdate
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
		if tierIdx < len(event.Tiers)-1 {
			insertValSQL += ",\n"
		}
		tierFieldValUpdate = append(tierFieldValUpdate, tier.ID, event.ID, tier.Name, tier.PriceCents, tier.TotalQuantity,
			tier.AvailableQuantity, 1, tier.CreatedAt)
	}

	tierQuery := `INSERT INTO ticket_tiers (id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at)
										VALUES
										` + insertValSQL
	_, err = tx.Exec(ctx, tierQuery, tierFieldValUpdate...)
	if err != nil {
		return fmt.Errorf("insert tier: %w", err)
	}

	if len(event.Seats) != 0 {
		proven, err := newSeatBatchBaselines(ctx, tx, event.Seats)
		if err != nil {
			return fmt.Errorf("verify generated seat inventory: %w", err)
		}
		batch := &pgx.Batch{}
		for _, seat := range event.Seats {
			positionJSON, err := json.Marshal(seat.Position)
			if err != nil {
				return fmt.Errorf("marshal generated seat position: %w", err)
			}
			var bookingID, orderID any
			if seat.BookingID != nil {
				bookingID = *seat.BookingID
			}
			if seat.OrderID != nil {
				orderID = *seat.OrderID
			} else {
				orderID = uuid.Nil
			}
			batch.Queue(`INSERT INTO seats (id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				seat.ID, seat.EventID, seat.TicketTierID, seat.Status, bookingID, orderID, positionJSON, seat.CreatedAt, seat.UpdatedAt)
		}
		results := tx.SendBatch(ctx, batch)
		for range event.Seats {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert generated seat: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("finish generated seat batch: %w", err)
		}
		for _, tierID := range proven {
			if _, err := tx.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, tierID); err != nil {
				return fmt.Errorf("record generated seat baseline: %w", err)
			}
		}
	}
	if key := event.CreationRequest; key != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO event_creation_requests(actor_id,request_id,request_hash,event_id) VALUES($1,$2,$3,$4)`,
			key.ActorID, key.RequestID, key.RequestHash, event.ID); err != nil {
			return fmt.Errorf("record event creation idempotency: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (r *PostgresEventRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Event, error) {
	return getEventByID(ctx, r.pool, id)
}

type eventQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func getEventByID(ctx context.Context, db eventQueryer, id uuid.UUID) (*domain.Event, error) {
	query := `SELECT id, title, description, category, venue, location, date, date_version, image_url, status, publication_status, sales_status, sales_start_at, sales_end_at, created_at, updated_at FROM events WHERE id = $1`
	event, err := scanEvent(db.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}

	tierQuery := `SELECT id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at
                FROM ticket_tiers WHERE event_id = $1`
	rows, err := db.Query(ctx, tierQuery, id)
	if err != nil {
		return nil, fmt.Errorf("get tiers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tier domain.TicketTier
		if err := rows.Scan(&tier.ID, &tier.EventID, &tier.Name, &tier.PriceCents,
			&tier.TotalQuantity, &tier.AvailableQuantity, &tier.Version, &tier.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan tier: %w", err)
		}
		event.Tiers = append(event.Tiers, tier)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tiers: %w", err)
	}

	return event, nil
}

func (r *PostgresEventRepository) List(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error) {
	return r.ListFiltered(ctx, EventListFilter{Category: category, Search: search}, page, pageSize)
}

func (r *PostgresEventRepository) ListAdmin(ctx context.Context, category, search string, page, pageSize int) ([]*domain.Event, int, error) {
	return r.ListAdminFiltered(ctx, EventListFilter{Category: category, Search: search}, page, pageSize)
}

func (r *PostgresEventRepository) ListFiltered(ctx context.Context, filter EventListFilter, page, pageSize int) ([]*domain.Event, int, error) {
	return r.listFiltered(ctx, filter, page, pageSize, true)
}

func (r *PostgresEventRepository) ListAdminFiltered(ctx context.Context, filter EventListFilter, page, pageSize int) ([]*domain.Event, int, error) {
	return r.listFiltered(ctx, filter, page, pageSize, false)
}

func (r *PostgresEventRepository) listFiltered(ctx context.Context, filter EventListFilter, page, pageSize int, publicOnly bool) ([]*domain.Event, int, error) {
	filter, page, pageSize, err := NormalizeEventListQuery(filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	where := make([]string, 0, 8)
	args := make([]any, 0, 12)
	appendFilter := func(expression string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(expression, len(args)))
	}
	if publicOnly {
		where = append(where, "e.publication_status = 'published'")
	}
	if filter.Category != "" {
		appendFilter("e.category = $%d", filter.Category)
	}
	if filter.Search != "" {
		args = append(args, "%"+filter.Search+"%")
		where = append(where, fmt.Sprintf("(e.title ILIKE $%d OR e.description ILIKE $%d)", len(args), len(args)))
	}
	if filter.DateFrom != nil {
		appendFilter("e.date >= $%d", *filter.DateFrom)
	}
	if filter.DateToExclusive != nil {
		appendFilter("e.date < $%d", *filter.DateToExclusive)
	}
	if filter.Location != "" {
		appendFilter("e.location ILIKE $%d", "%"+filter.Location+"%")
	}
	if filter.MinPriceCents != nil || filter.MaxPriceCents != nil {
		tierWhere := []string{"price_tier.event_id = e.id"}
		if filter.MinPriceCents != nil {
			args = append(args, *filter.MinPriceCents)
			tierWhere = append(tierWhere, fmt.Sprintf("price_tier.price_cents >= $%d", len(args)))
		}
		if filter.MaxPriceCents != nil {
			args = append(args, *filter.MaxPriceCents)
			tierWhere = append(tierWhere, fmt.Sprintf("price_tier.price_cents <= $%d", len(args)))
		}
		where = append(where, "EXISTS (SELECT 1 FROM ticket_tiers AS price_tier WHERE "+strings.Join(tierWhere, " AND ")+")")
	}
	whereSQL := ""
	if len(where) > 0 {
		whereSQL = " WHERE " + strings.Join(where, " AND ")
	}
	countQuery := "SELECT COUNT(*) FROM events AS e" + whereSQL
	var total int
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count events: %w", err)
	}
	orderBy := map[string]string{
		"date_asc":   "e.date ASC, e.id ASC",
		"date_desc":  "e.date DESC, e.id DESC",
		"price_asc":  "(SELECT MIN(sort_tier.price_cents) FROM ticket_tiers AS sort_tier WHERE sort_tier.event_id=e.id) ASC NULLS LAST, e.date ASC, e.id ASC",
		"price_desc": "(SELECT MIN(sort_tier.price_cents) FROM ticket_tiers AS sort_tier WHERE sort_tier.event_id=e.id) DESC NULLS LAST, e.date ASC, e.id ASC",
	}
	args = append(args, pageSize)
	limitParam := len(args)
	args = append(args, (page-1)*pageSize)
	offsetParam := len(args)
	query := fmt.Sprintf(`SELECT e.id, e.title, e.description, e.category, e.venue, e.location, e.date, e.date_version, e.image_url,
		e.status, e.publication_status, e.sales_status, e.sales_start_at, e.sales_end_at, e.created_at, e.updated_at
		FROM events AS e%s ORDER BY %s LIMIT $%d OFFSET $%d`, whereSQL, orderBy[filter.Sort], limitParam, offsetParam)
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []*domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate events: %w", err)
	}

	// Load tiers for each event
	for _, e := range events {
		tierQuery := `SELECT id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at
                    FROM ticket_tiers WHERE event_id = $1`
		tierRows, err := r.pool.Query(ctx, tierQuery, e.ID)
		if err != nil {
			return nil, 0, fmt.Errorf("get tiers: %w", err)
		}
		for tierRows.Next() {
			var tier domain.TicketTier
			if err := tierRows.Scan(&tier.ID, &tier.EventID, &tier.Name, &tier.PriceCents,
				&tier.TotalQuantity, &tier.AvailableQuantity, &tier.Version, &tier.CreatedAt); err != nil {
				tierRows.Close()
				return nil, 0, fmt.Errorf("scan tier: %w", err)
			}
			e.Tiers = append(e.Tiers, tier)
		}
		if err := tierRows.Err(); err != nil {
			tierRows.Close()
			return nil, 0, fmt.Errorf("iterate tiers: %w", err)
		}
		tierRows.Close()
	}

	return events, total, nil
}

// Event text is nullable in PostgreSQL but remains a string in the domain/API.
func scanEvent(row pgx.Row) (*domain.Event, error) {
	event := &domain.Event{}
	var description, imageURL *string
	var salesStartAt, salesEndAt *time.Time
	if err := row.Scan(
		&event.ID, &event.Title, &description, &event.Category, &event.Venue,
		&event.Location, &event.Date, &event.DateVersion, &imageURL, &event.Status, &event.PublicationStatus,
		&event.SalesStatus, &salesStartAt, &salesEndAt, &event.CreatedAt, &event.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if description != nil {
		event.Description = *description
	}
	if imageURL != nil {
		event.ImageURL = *imageURL
	}
	event.SalesStartAt = salesStartAt
	event.SalesEndAt = salesEndAt
	return event, nil
}

func (r *PostgresEventRepository) UpdateSales(ctx context.Context, event *domain.Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin event sales lifecycle: %w", err)
	}
	defer tx.Rollback(ctx)
	var current domain.Event
	var currentStart, currentEnd *time.Time
	var firstSaleOpenedAt *time.Time
	var generation, notifiedGeneration int64
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT id,title,category,venue,location,date,status,publication_status,sales_status,sales_start_at,sales_end_at,sales_generation,sale_notification_generation,first_sale_opened_at,clock_timestamp()
		FROM events WHERE id=$1 FOR UPDATE`, event.ID).Scan(&current.ID, &current.Title, &current.Category, &current.Venue, &current.Location, &current.Date, &current.Status, &current.PublicationStatus, &current.SalesStatus, &currentStart, &currentEnd, &generation, &notifiedGeneration, &firstSaleOpenedAt, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock event sales lifecycle: %w", err)
	}
	current.SalesStartAt, current.SalesEndAt = currentStart, currentEnd
	next := current
	next.PublicationStatus, next.SalesStatus = event.PublicationStatus, event.SalesStatus
	if event.UpdateSalesWindow {
		next.SalesStartAt, next.SalesEndAt = event.SalesStartAt, event.SalesEndAt
	}
	wasEligible, _ := domain.EventSaleEligibility(&current, now)
	isEligible, _ := domain.EventSaleEligibility(&next, now)
	windowChanged := event.UpdateSalesWindow && (!sameOptionalTime(current.SalesStartAt, next.SalesStartAt) || !sameOptionalTime(current.SalesEndAt, next.SalesEndAt))
	statusChanged := current.PublicationStatus != next.PublicationStatus || current.SalesStatus != next.SalesStatus
	newSaleGeneration := (statusChanged || windowChanged) &&
		(next.PublicationStatus == domain.PublicationPublished && (next.SalesStatus == domain.SalesScheduled || next.SalesStatus == domain.SalesSelling)) &&
		(!wasEligible || !isEligible || current.SalesStatus != next.SalesStatus)
	if newSaleGeneration {
		generation++
	}
	_, err = tx.Exec(ctx, `UPDATE events SET publication_status=$2,sales_status=$3,
		sales_start_at=CASE WHEN $6 THEN $4 ELSE sales_start_at END,
		sales_end_at=CASE WHEN $6 THEN $5 ELSE sales_end_at END,
		sales_generation=$7,first_sale_opened_at=CASE WHEN $8 THEN COALESCE(first_sale_opened_at,clock_timestamp()) ELSE first_sale_opened_at END,
		updated_at=clock_timestamp() WHERE id=$1`,
		event.ID, event.PublicationStatus, event.SalesStatus, event.SalesStartAt, event.SalesEndAt, event.UpdateSalesWindow, generation, isEligible)
	if err != nil {
		return fmt.Errorf("update event sales lifecycle: %w", err)
	}
	if isEligible && generation > notifiedGeneration {
		triggerID := saleGenerationTriggerID(event.ID, generation)
		if err := enqueueOnSaleAlerts(ctx, tx, event.ID, generation, triggerID); err != nil {
			return fmt.Errorf("enqueue on-sale reminder intents: %w", err)
		}
		notifiedGeneration = generation
		if _, err := tx.Exec(ctx, `UPDATE events SET sale_notification_generation=$2 WHERE id=$1`, event.ID, notifiedGeneration); err != nil {
			return fmt.Errorf("record on-sale notification generation: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event sales lifecycle and reminder intents: %w", err)
	}
	return nil
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func (r *PostgresEventRepository) Update(ctx context.Context, event *domain.Event) error {
	query := `UPDATE events SET title = $2, description = $3, category = $4, venue = $5, location = $6, image_url = $7, updated_at = $8
		WHERE id = $1 AND date_version = $9`
	tag, err := r.pool.Exec(ctx, query,
		event.ID, event.Title, event.Description, event.Category, event.Venue,
		event.Location, event.ImageURL, time.Now(), event.DateVersion)
	if err != nil {
		return mapEventDateMutationError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

func (r *PostgresEventRepository) UpdateDate(ctx context.Context, event *domain.Event, expectedVersion int64) error {
	if expectedVersion <= 0 {
		return ErrConflict
	}
	query := `UPDATE events SET title = $2, description = $3, category = $4, venue = $5, location = $6, date = $7, image_url = $8, updated_at = $9
		WHERE id = $1 AND date_version = $10`
	tag, err := r.pool.Exec(ctx, query,
		event.ID, event.Title, event.Description, event.Category, event.Venue,
		event.Location, event.Date, event.ImageURL, time.Now(), expectedVersion)
	if err != nil {
		return mapEventDateMutationError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

func (r *PostgresEventRepository) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `UPDATE events SET sales_status='cancelled',updated_at=clock_timestamp() WHERE id = $1`, id)
	if err != nil {
		return mapEventDateMutationError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func mapEventDateMutationError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.ConstraintName == "event_refund_time_permit_guard" {
		return fmt.Errorf("%w: %s", ErrActiveRefundTimePermit, pgErr.Message)
	}
	return err
}

// --- Ticket Tier Repository ---

type PostgresTicketTierRepository struct {
	pool postgresPool
}

func NewPostgresTicketTierRepository(pool *pgxpool.Pool) *PostgresTicketTierRepository {
	return &PostgresTicketTierRepository{pool: pool}
}

func (r *PostgresTicketTierRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.TicketTier, error) {
	query := `SELECT id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at
            FROM ticket_tiers WHERE id = $1`
	tier := &domain.TicketTier{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&tier.ID, &tier.EventID, &tier.Name, &tier.PriceCents,
		&tier.TotalQuantity, &tier.AvailableQuantity, &tier.Version, &tier.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get tier: %w", err)
	}
	return tier, nil
}

func (r *PostgresTicketTierRepository) GetByEventID(ctx context.Context, eventID uuid.UUID) ([]domain.TicketTier, error) {
	query := `SELECT id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at
            FROM ticket_tiers WHERE event_id = $1`
	rows, err := r.pool.Query(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tiers []domain.TicketTier
	for rows.Next() {
		var t domain.TicketTier
		if err := rows.Scan(&t.ID, &t.EventID, &t.Name, &t.PriceCents,
			&t.TotalQuantity, &t.AvailableQuantity, &t.Version, &t.CreatedAt); err != nil {
			return nil, err
		}
		tiers = append(tiers, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tiers: %w", err)
	}
	return tiers, nil
}

// Optimistic locking -- returns ErrConflict if version mismatch
func (r *PostgresTicketTierRepository) UpdateAvailability(ctx context.Context, tierID uuid.UUID, delta int32, expectedVersion int32) (*domain.TicketTier, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, `SELECT id FROM ticket_tiers WHERE id=$1 FOR UPDATE`, tierID).Scan(new(uuid.UUID)); err != nil {
		return nil, err
	}
	if err := rejectManagedTierMutation(ctx, tx, tierID); err != nil {
		return nil, err
	}
	query := `UPDATE ticket_tiers
            SET available_quantity = available_quantity + $2, version = version + 1
			WHERE id = $1 AND version = $3 AND available_quantity::bigint + $2 BETWEEN 0 AND total_quantity
            RETURNING id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at`
	tier := &domain.TicketTier{}
	err = tx.QueryRow(ctx, query, tierID, delta, expectedVersion).Scan(
		&tier.ID, &tier.EventID, &tier.Name, &tier.PriceCents,
		&tier.TotalQuantity, &tier.AvailableQuantity, &tier.Version, &tier.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return tier, tx.Commit(ctx)
}

// Pessimistic locking -- SELECT FOR UPDATE then UPDATE
func (r *PostgresTicketTierRepository) UpdateAvailabilityPessimistic(ctx context.Context, tierID uuid.UUID, delta int32) (*domain.TicketTier, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var available, total int64
	err = tx.QueryRow(ctx,
		`SELECT available_quantity,total_quantity FROM ticket_tiers WHERE id = $1 FOR UPDATE`, tierID).Scan(&available, &total)
	if err != nil {
		return nil, err
	}

	if err := rejectManagedTierMutation(ctx, tx, tierID); err != nil {
		return nil, err
	}
	if available+int64(delta) < 0 || available+int64(delta) > total {
		return nil, ErrInsufficientTickets
	}

	tier := &domain.TicketTier{}
	err = tx.QueryRow(ctx,
		`UPDATE ticket_tiers SET available_quantity = available_quantity + $2, version = version + 1
		WHERE id = $1
		RETURNING id, event_id, name, price_cents, total_quantity, available_quantity, version, created_at`,
		tierID, delta).Scan(
		&tier.ID, &tier.EventID, &tier.Name, &tier.PriceCents,
		&tier.TotalQuantity, &tier.AvailableQuantity, &tier.Version, &tier.CreatedAt)
	if err != nil {
		return nil, err
	}

	return tier, tx.Commit(ctx)
}

func (r *PostgresTicketTierRepository) UpdateBatchTicketAvailability(ctx context.Context, req *UpdateBatchTicketAvailabilityRequest) error {
	return ErrNotImplemented
}

// Deprecated naive adapter retains its signature but cannot bypass lifecycle
// evidence or quantity bounds.
func (r *PostgresTicketTierRepository) UpdateAvailabilityNoLock(ctx context.Context, tierID uuid.UUID, delta int32) error {
	_, err := r.UpdateAvailabilityPessimistic(ctx, tierID, delta)
	return err
}

// --- Seat Repository ---

type PostgresSeatRepository struct {
	pool postgresPool
}

func NewPostgresSeatRepository(pool *pgxpool.Pool) *PostgresSeatRepository {
	return &PostgresSeatRepository{pool: pool}
}

func (r *PostgresSeatRepository) Create(ctx context.Context, seat *domain.Seat) error {
	positionJSON, err := json.Marshal(seat.Position)
	if err != nil {
		return fmt.Errorf("marshal position: %w", err)
	}

	query := `INSERT INTO seats (id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	// Handle nullable UUID fields
	var bookingID, orderID uuid.UUID
	if seat.BookingID != nil {
		bookingID = *seat.BookingID
	}
	if seat.OrderID != nil {
		orderID = *seat.OrderID
	}

	_, err = r.pool.Exec(ctx, query,
		seat.ID, seat.EventID, seat.TicketTierID, seat.Status,
		bookingID, orderID, positionJSON, seat.CreatedAt, seat.UpdatedAt)
	return err
}

func (r *PostgresSeatRepository) CreateBatch(ctx context.Context, seats []*domain.Seat) error {
	if len(seats) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	proven, err := newSeatBatchBaselines(ctx, tx, seats)
	if err != nil {
		return err
	}

	query := `INSERT INTO seats (id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	for _, seat := range seats {
		positionJSON, err := json.Marshal(seat.Position)
		if err != nil {
			return fmt.Errorf("marshal position: %w", err)
		}

		// Handle nullable UUID fields
		var bookingID, orderID uuid.UUID
		if seat.BookingID != nil {
			bookingID = *seat.BookingID
		}
		if seat.OrderID != nil {
			orderID = *seat.OrderID
		}

		_, err = tx.Exec(ctx, query,
			seat.ID, seat.EventID, seat.TicketTierID, seat.Status,
			bookingID, orderID, positionJSON, seat.CreatedAt, seat.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert seat: %w", err)
		}
	}

	for _, tier := range proven {
		if _, err := tx.Exec(ctx, `UPDATE ticket_tiers SET lifecycle_accounting_baseline='verified' WHERE id=$1`, tier); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *PostgresSeatRepository) GetByEventID(ctx context.Context, eventID uuid.UUID, tierID *uuid.UUID) ([]*domain.Seat, error) {
	var rows pgx.Rows
	var err error

	if tierID != nil {
		query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
                FROM seats WHERE event_id = $1 AND ticket_tier_id = $2 AND deleted_at IS NULL
                ORDER BY position->>'sectionId', position->>'row', (position->>'seat')::int`
		rows, err = r.pool.Query(ctx, query, eventID, *tierID)
	} else {
		query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
                FROM seats WHERE event_id = $1 AND deleted_at IS NULL
                ORDER BY position->>'sectionId', position->>'row', (position->>'seat')::int`
		rows, err = r.pool.Query(ctx, query, eventID)
	}

	if err != nil {
		return nil, fmt.Errorf("get seats: %w", err)
	}
	defer rows.Close()

	var seats []*domain.Seat
	for rows.Next() {
		seat, err := r.scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("scan seat: %w", err)
		}
		seats = append(seats, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate seats: %w", err)
	}

	return seats, nil
}

func (r *PostgresSeatRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Seat, error) {
	query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
            FROM seats WHERE id = $1 AND deleted_at IS NULL`
	seat := &domain.Seat{}
	var positionJSON []byte
	var bookingID, orderID *uuid.UUID

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&seat.ID, &seat.EventID, &seat.TicketTierID, &seat.Status,
		&bookingID, &orderID, &positionJSON, &seat.CreatedAt, &seat.UpdatedAt, &seat.SeatRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get seat: %w", err)
	}

	// Handle nullable UUID fields
	if bookingID != nil && *bookingID != uuid.Nil {
		seat.BookingID = bookingID
	}
	if orderID != nil && *orderID != uuid.Nil {
		seat.OrderID = orderID
	}

	if err := json.Unmarshal(positionJSON, &seat.Position); err != nil {
		return nil, fmt.Errorf("unmarshal position: %w", err)
	}

	return seat, nil
}

func (r *PostgresSeatRepository) UpdateStatus(ctx context.Context, seatID uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) (SeatMutationResult, error) {
	result, err := r.UpdateStatusBatch(ctx, []uuid.UUID{seatID}, status, bookingID)
	if err != nil {
		return SeatMutationResult{}, err
	}
	if len(result.ChangedSeats) > 0 {
		result.Seat = result.ChangedSeats[0]
		return result, nil
	}
	seat, err := r.GetByID(ctx, seatID)
	if err != nil {
		return SeatMutationResult{}, err
	}
	result.Seat = seat
	return result, nil
}

// scanSeat is a helper to scan a seat from a database row
type seatRowScanner interface {
	Scan(dest ...any) error
}

func (r *PostgresSeatRepository) scanSeat(rows pgx.Rows) (*domain.Seat, error) {
	return scanSeat(rows)
}

func scanSeat(row seatRowScanner) (*domain.Seat, error) {
	seat := &domain.Seat{}
	var positionJSON []byte
	var bookingID, orderID *uuid.UUID

	err := row.Scan(
		&seat.ID, &seat.EventID, &seat.TicketTierID, &seat.Status,
		&bookingID, &orderID, &positionJSON, &seat.CreatedAt, &seat.UpdatedAt, &seat.SeatRevision)
	if err != nil {
		return nil, err
	}

	// Handle nullable UUID fields
	if bookingID != nil && *bookingID != uuid.Nil {
		seat.BookingID = bookingID
	}
	if orderID != nil && *orderID != uuid.Nil {
		seat.OrderID = orderID
	}

	if err := json.Unmarshal(positionJSON, &seat.Position); err != nil {
		return nil, fmt.Errorf("unmarshal position: %w", err)
	}

	return seat, nil
}

func (r *PostgresSeatRepository) GetByTierID(ctx context.Context, tierID uuid.UUID) ([]*domain.Seat, error) {
	query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
            FROM seats WHERE ticket_tier_id = $1 AND deleted_at IS NULL
            ORDER BY position->>'sectionId', position->>'row', (position->>'seat')::int`
	rows, err := r.pool.Query(ctx, query, tierID)
	if err != nil {
		return nil, fmt.Errorf("get seats by tier: %w", err)
	}
	defer rows.Close()

	var seats []*domain.Seat
	for rows.Next() {
		seat, err := r.scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("scan seat: %w", err)
		}
		seats = append(seats, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate seats by tier: %w", err)
	}

	return seats, nil
}

func (r *PostgresSeatRepository) UpdateStatusBatch(ctx context.Context, seatIDs []uuid.UUID, status domain.SeatStatus, bookingID *uuid.UUID) (SeatMutationResult, error) {
	if len(seatIDs) == 0 {
		return SeatMutationResult{}, nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return SeatMutationResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	saleEligible, saleReason := true, ""
	if status == domain.SeatStatusReserved {
		_, saleEligible, saleReason, err = lockEventSaleForSeats(ctx, tx, uuid.Nil, seatIDs)
		if err != nil {
			return SeatMutationResult{}, err
		}
	}
	if bookingID != nil {
		if err := lockSeatLifecycleIdentity(ctx, tx, *bookingID); err != nil {
			return SeatMutationResult{}, err
		}
	}
	locked, err := lockSeatStates(ctx, tx, seatIDs)
	if err != nil {
		return SeatMutationResult{}, err
	}
	if err := rejectManagedSeatMutation(ctx, tx, locked, bookingID); err != nil {
		return SeatMutationResult{}, err
	}
	if status == domain.SeatStatusReserved && !saleEligible && (bookingID == nil || !legacyReservationReplay(locked, *bookingID, time.Now())) {
		return SeatMutationResult{}, fmt.Errorf("%s: %w", saleReason, ErrSaleNotAllowed)
	}
	if err := validateSeatTransition(locked, status, bookingID, time.Now()); err != nil {
		return SeatMutationResult{}, err
	}
	mutationIDs := seatsNeedingTransition(locked, status, bookingID)
	if len(mutationIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return SeatMutationResult{}, err
		}
		return SeatMutationResult{Success: true}, nil
	}
	changedPublicIDs := make(map[uuid.UUID]struct{}, len(locked))
	for _, seat := range locked {
		if seat.status != status {
			changedPublicIDs[seat.id] = struct{}{}
		}
	}
	changedSeats, err := updateLockedSeatBatch(ctx, tx, mutationIDs, changedPublicIDs, status, *bookingID, 15)
	if err != nil {
		return SeatMutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SeatMutationResult{}, fmt.Errorf("commit seat status batch: %w", err)
	}
	return SeatMutationResult{Success: true, ChangedSeats: changedSeats}, nil
}

type lockedSeatState struct {
	id             uuid.UUID
	eventID        uuid.UUID
	tierID         uuid.UUID
	generation     *int64
	status         domain.SeatStatus
	bookingID      *uuid.UUID
	owner          *uuid.UUID
	expiresAt      *time.Time
	statusRevision int64
}

func lockSeatStates(ctx context.Context, tx pgx.Tx, seatIDs []uuid.UUID) ([]lockedSeatState, error) {
	unique := make(map[uuid.UUID]struct{}, len(seatIDs))
	for _, seatID := range seatIDs {
		if _, exists := unique[seatID]; exists {
			return nil, fmt.Errorf("duplicate seat id %s: %w", seatID, ErrSeatNotAvailable)
		}
		unique[seatID] = struct{}{}
	}

	rows, err := tx.Query(ctx, `
		SELECT id, status, booking_id, reserved_by_booking_id, reservation_expired_at, seat_revision, event_id, ticket_tier_id, lifecycle_generation
		FROM seats
		WHERE id = ANY($1) AND deleted_at IS NULL
		ORDER BY id
		FOR UPDATE`, seatIDs)
	if err != nil {
		return nil, fmt.Errorf("lock seats: %w", err)
	}
	defer rows.Close()

	locked := make([]lockedSeatState, 0, len(seatIDs))
	for rows.Next() {
		var seat lockedSeatState
		if err := rows.Scan(&seat.id, &seat.status, &seat.bookingID, &seat.owner, &seat.expiresAt, &seat.statusRevision, &seat.eventID, &seat.tierID, &seat.generation); err != nil {
			return nil, fmt.Errorf("scan locked seat: %w", err)
		}
		locked = append(locked, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate locked seats: %w", err)
	}
	rows.Close()
	if len(locked) != len(seatIDs) {
		return nil, ErrNotFound
	}
	return locked, nil
}

func validateSeatTransition(seats []lockedSeatState, status domain.SeatStatus, bookingID *uuid.UUID, now time.Time) error {
	if bookingID == nil || *bookingID == uuid.Nil {
		return fmt.Errorf("seat transition requires a booking owner: %w", ErrSeatNotAvailable)
	}
	for _, seat := range seats {
		switch status {
		case domain.SeatStatusReserved:
			if seat.status == domain.SeatStatusAvailable {
				continue
			}
			if seat.status == domain.SeatStatusReserved && seatOwnedBy(seat, bookingID) && reservationActive(seat, now) {
				continue
			}
		case domain.SeatStatusBooked:
			if seat.status == domain.SeatStatusBooked && seat.bookingID != nil && *seat.bookingID == *bookingID {
				continue
			}
			if seat.status == domain.SeatStatusReserved && seatOwnedBy(seat, bookingID) && reservationActive(seat, now) {
				continue
			}
		case domain.SeatStatusAvailable:
			if seat.status == domain.SeatStatusAvailable && seat.owner == nil && seat.bookingID == nil {
				continue
			}
			if seat.status == domain.SeatStatusReserved && seatOwnedBy(seat, bookingID) {
				continue
			}
			if seat.status == domain.SeatStatusBooked && seat.bookingID != nil && *seat.bookingID == *bookingID {
				continue
			}
		}
		return fmt.Errorf("seat %s is not owned by booking %s for transition to %s: %w", seat.id, bookingID, status, ErrSeatNotAvailable)
	}
	return nil
}

func seatsNeedingTransition(seats []lockedSeatState, status domain.SeatStatus, bookingID *uuid.UUID) []uuid.UUID {
	changed := make([]uuid.UUID, 0, len(seats))
	for _, seat := range seats {
		atTarget := false
		switch status {
		case domain.SeatStatusReserved:
			atTarget = seat.status == domain.SeatStatusReserved && seatOwnedBy(seat, bookingID)
		case domain.SeatStatusBooked:
			atTarget = seat.status == domain.SeatStatusBooked && seat.bookingID != nil && *seat.bookingID == *bookingID
		case domain.SeatStatusAvailable:
			atTarget = seat.status == domain.SeatStatusAvailable && seat.owner == nil && seat.expiresAt == nil && seat.bookingID == nil
		}
		if !atTarget {
			changed = append(changed, seat.id)
		}
	}
	return changed
}

func seatOwnedBy(seat lockedSeatState, bookingID *uuid.UUID) bool {
	return bookingID != nil && seat.owner != nil && *seat.owner == *bookingID
}

func reservationActive(seat lockedSeatState, now time.Time) bool {
	return seat.expiresAt != nil && seat.expiresAt.After(now)
}

func updateLockedSeatBatch(ctx context.Context, tx pgx.Tx, seatIDs []uuid.UUID, changedPublicIDs map[uuid.UUID]struct{}, status domain.SeatStatus, bookingID uuid.UUID, reservationMinutes int32) ([]*domain.Seat, error) {
	query := seatStatusBatchUpdateQuery(status)
	if query == "" {
		return nil, fmt.Errorf("unsupported seat transition status %q", status)
	}
	var rows pgx.Rows
	var err error
	if status == domain.SeatStatusReserved {
		reservationMinutes = domain.NormalizeReservationTimeInMinutes(reservationMinutes)
		rows, err = tx.Query(ctx, query, seatIDs, bookingID, reservationMinutes)
	} else {
		rows, err = tx.Query(ctx, query, seatIDs, bookingID)
	}
	if err != nil {
		return nil, fmt.Errorf("update seats: %w", err)
	}
	defer rows.Close()
	seats := make([]*domain.Seat, 0, len(seatIDs))
	var affected int
	for rows.Next() {
		affected++
		seat, scanErr := scanSeat(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan updated seat: %w", scanErr)
		}
		if _, changed := changedPublicIDs[seat.ID]; changed {
			seats = append(seats, seat)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate updated seats: %w", err)
	}
	if affected != len(seatIDs) {
		return nil, fmt.Errorf("seat transition affected %d of %d rows: %w", affected, len(seatIDs), ErrSeatNotAvailable)
	}
	return seats, nil
}

func seatStatusBatchUpdateQuery(status domain.SeatStatus) string {
	switch status {
	case domain.SeatStatusReserved:
		return `UPDATE seats
			SET status = 'reserved', booking_id = NULL,
			    reserved_by_booking_id = $2,
			    reservation_expired_at = NOW() + ($3 * INTERVAL '1 minute'),
			    seat_revision = seat_revision + 1,
			    updated_at = NOW()
			WHERE id = ANY($1) AND status = 'available' AND deleted_at IS NULL
			RETURNING id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision`
	case domain.SeatStatusBooked:
		return `UPDATE seats
			SET status = 'booked', booking_id = $2,
			    reserved_by_booking_id = NULL,
			    reservation_expired_at = NULL,
			    seat_revision = seat_revision + 1,
			    updated_at = NOW()
			WHERE id = ANY($1) AND status = 'reserved'
			  AND reserved_by_booking_id = $2
			  AND reservation_expired_at > NOW()
			  AND deleted_at IS NULL
			RETURNING id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision`
	case domain.SeatStatusAvailable:
		return `UPDATE seats
			SET status = 'available', booking_id = NULL,
			    reserved_by_booking_id = NULL,
			    reservation_expired_at = NULL,
			    seat_revision = seat_revision + CASE WHEN status <> 'available' THEN 1 ELSE 0 END,
			    updated_at = NOW()
			WHERE id = ANY($1) AND deleted_at IS NULL
			  AND ((status = 'reserved' AND reserved_by_booking_id = $2)
			       OR (status = 'booked' AND booking_id = $2)
			       OR status = 'available')
			RETURNING id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision`
	default:
		return ""
	}
}

func (r *PostgresSeatRepository) GetAvailableSeats(ctx context.Context, eventID uuid.UUID) ([]*domain.Seat, error) {
	query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
            FROM seats WHERE event_id = $1 AND status = 'available' AND deleted_at IS NULL
            ORDER BY position->>'sectionId', position->>'row', (position->>'seat')::int`
	rows, err := r.pool.Query(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("get available seats: %w", err)
	}
	defer rows.Close()

	var seats []*domain.Seat
	for rows.Next() {
		seat, err := r.scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("scan seat: %w", err)
		}
		seats = append(seats, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate available seats: %w", err)
	}

	return seats, nil
}

func (r *PostgresSeatRepository) GetAvailableSeatsByTier(ctx context.Context, tierID uuid.UUID) ([]*domain.Seat, error) {
	query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
            FROM seats WHERE ticket_tier_id = $1 AND status = 'available' AND deleted_at IS NULL
            ORDER BY position->>'sectionId', position->>'row', (position->>'seat')::int`
	rows, err := r.pool.Query(ctx, query, tierID)
	if err != nil {
		return nil, fmt.Errorf("get available seats by tier: %w", err)
	}
	defer rows.Close()

	var seats []*domain.Seat
	for rows.Next() {
		seat, err := r.scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("scan seat: %w", err)
		}
		seats = append(seats, seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate available seats by tier: %w", err)
	}

	return seats, nil
}

// Delete soft deletes a seat by setting deleted_at
func (r *PostgresSeatRepository) Delete(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE seats SET deleted_at = NOW() WHERE id = $1`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

// DeleteByEventID soft deletes all seats for an event
func (r *PostgresSeatRepository) DeleteByEventID(ctx context.Context, eventID uuid.UUID) error {
	query := `UPDATE seats SET deleted_at = NOW() WHERE event_id = $1 AND deleted_at IS NULL`
	_, err := r.pool.Exec(ctx, query, eventID)
	return err
}

func (r *PostgresSeatRepository) GetSeatsBySeatIds(ctx context.Context, seatIds []uuid.UUID) ([]domain.Seat, error) {
	query := `SELECT id, event_id, ticket_tier_id, status, booking_id, order_id, position, created_at, updated_at, seat_revision
            FROM seats WHERE id = ANY($1) AND deleted_at IS NULL`
	rows, err := r.pool.Query(ctx, query, seatIds)
	if err != nil {
		return nil, fmt.Errorf("fail to get seats by seatids: %w", err)
	}
	defer rows.Close()
	var seats []domain.Seat
	for rows.Next() {
		seat, err := r.scanSeat(rows)
		if err != nil {
			return nil, fmt.Errorf("[GetSeatsBySeatIds]: scan seat: %w", err)
		}
		if seat == nil {
			continue
		}
		seats = append(seats, *seat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("[GetSeatsBySeatIds]: iterate seats: %w", err)
	}
	return seats, nil
}

func (r *PostgresSeatRepository) ReservedOrCompensateBatchSeats(ctx context.Context, req *ReservedOrCompensateBatchSeats) (SeatMutationResult, error) {
	if len(req.SeatIds) == 0 {
		return SeatMutationResult{}, fmt.Errorf("seat ids are required: %w", ErrNotFound)
	}
	if req.ReservedByBookingId == uuid.Nil {
		return SeatMutationResult{}, fmt.Errorf("booking owner is required: %w", ErrSeatNotAvailable)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return SeatMutationResult{}, fmt.Errorf("begin seat transition: %w", err)
	}
	defer tx.Rollback(ctx)
	var saleEventID uuid.UUID
	saleEligible := true
	saleReason := ""
	if req.Action == domain.RESERVED_SEAT {
		saleEventID, saleEligible, saleReason, err = lockEventSaleForSeats(ctx, tx, req.EventID, req.SeatIds)
		if err != nil {
			return SeatMutationResult{}, err
		}
	}
	if err := lockSeatLifecycleIdentity(ctx, tx, req.ReservedByBookingId); err != nil {
		return SeatMutationResult{}, err
	}
	locked, err := lockSeatStates(ctx, tx, req.SeatIds)
	if err != nil {
		return SeatMutationResult{}, err
	}
	if saleEventID != uuid.Nil {
		for _, seat := range locked {
			if seat.eventID != saleEventID {
				return SeatMutationResult{}, ErrLifecycleConflict
			}
		}
	}
	if req.Action == domain.RESERVED_SEAT && !saleEligible && !legacyReservationReplay(locked, req.ReservedByBookingId, time.Now()) {
		return SeatMutationResult{}, fmt.Errorf("%s: %w", saleReason, ErrSaleNotAllowed)
	}
	if err := rejectManagedSeatMutation(ctx, tx, locked, &req.ReservedByBookingId); err != nil {
		return SeatMutationResult{}, err
	}

	var target domain.SeatStatus
	switch req.Action {
	case domain.RESERVED_SEAT:
		target = domain.SeatStatusReserved
	case domain.COMPENSATE_SEAT:
		target = domain.SeatStatusAvailable
	default:
		return SeatMutationResult{}, fmt.Errorf("unsupported seat action %q", req.Action)
	}
	if err := validateSeatTransition(locked, target, &req.ReservedByBookingId, time.Now()); err != nil {
		return SeatMutationResult{}, err
	}
	mutationIDs := seatsNeedingTransition(locked, target, &req.ReservedByBookingId)
	changedPublicIDs := make(map[uuid.UUID]struct{}, len(locked))
	for _, seat := range locked {
		if seat.status != target {
			changedPublicIDs[seat.id] = struct{}{}
		}
	}
	var changedSeats []*domain.Seat
	if len(mutationIDs) > 0 {
		changedSeats, err = updateLockedSeatBatch(ctx, tx, mutationIDs, changedPublicIDs, target, req.ReservedByBookingId, req.ReservedTimeInMinutes)
		if err != nil {
			return SeatMutationResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return SeatMutationResult{}, fmt.Errorf("commit seat transition: %w", err)
	}
	return SeatMutationResult{Success: true, ChangedSeats: changedSeats}, nil
}

// UndoReservedExpiredSeats releases all seats whose reservation has expired:
// status goes back to 'available' and every reservation marker is cleared,
// as if no user had ever held the seat. It returns the released seat IDs
// grouped by the booking that had reserved them, so the caller can mark
// those bookings EXPIRED.
//
// The CTE locks the expired rows with SKIP LOCKED so concurrent runs of the
// cleaner never block or fight over the same seats — a second run simply
// skips rows already locked by the first.
func (r *PostgresSeatRepository) UndoReservedExpiredSeats(ctx context.Context) (*ReservedExpiredSeatsResult, error) {
	query := `
		WITH expired AS (
			SELECT id, event_id, reserved_by_booking_id
			FROM seats
			WHERE status = 'reserved'
			  AND lifecycle_generation IS NULL
			  AND NOT EXISTS (SELECT 1 FROM booking_seat_lifecycles l WHERE l.booking_id = seats.reserved_by_booking_id)
			  AND deleted_at IS NULL
			  AND reservation_expired_at IS NOT NULL
			  AND reservation_expired_at <= NOW()
			FOR UPDATE SKIP LOCKED
		)
		UPDATE seats s
		SET status = 'available',
		    booking_id = NULL,
		    reserved_by_booking_id = NULL,
		    reservation_expired_at = NULL,
		    seat_revision = seat_revision + 1,
		    updated_at = NOW()
		FROM expired
		WHERE s.id = expired.id AND s.status = 'reserved' AND s.lifecycle_generation IS NULL
		RETURNING s.id, s.event_id, s.ticket_tier_id, s.status, s.booking_id,
		          s.order_id, s.position, s.created_at, s.updated_at,
		          s.seat_revision, expired.reserved_by_booking_id`

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin expired seat release: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("undo reserved expired seats: %w", err)
	}

	result := &ReservedExpiredSeatsResult{
		BookingIdSeatIdsMap: make(map[uuid.UUID][]uuid.UUID),
		EventBookingSeatIDs: make(map[uuid.UUID]map[uuid.UUID][]uuid.UUID),
	}
	for rows.Next() {
		var seat domain.Seat
		var positionJSON []byte
		var dbBookingID *uuid.UUID
		var dbOrderID *uuid.UUID
		var bookingID *uuid.UUID
		if err := rows.Scan(
			&seat.ID, &seat.EventID, &seat.TicketTierID, &seat.Status, &dbBookingID,
			&dbOrderID, &positionJSON, &seat.CreatedAt, &seat.UpdatedAt,
			&seat.SeatRevision, &bookingID,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("[UndoReservedExpiredSeats]: scan seat: %w", err)
		}
		if err := json.Unmarshal(positionJSON, &seat.Position); err != nil {
			rows.Close()
			return nil, fmt.Errorf("[UndoReservedExpiredSeats]: unmarshal seat position: %w", err)
		}
		// The status mutation clears booking_id; keep the pre-expiry owner only
		// in the separate bookingID variable returned by the CTE.
		if dbOrderID != nil && *dbOrderID != uuid.Nil {
			seat.OrderID = dbOrderID
		}
		seat.ReservedByBookingID = nil
		result.ChangedSeats = append(result.ChangedSeats, &seat)
		// Seats reserved before reserved_by_booking_id existed have no owner;
		// they are still released, but there is no booking to report back.
		if bookingID == nil {
			continue
		}
		result.BookingIdSeatIdsMap[*bookingID] = append(result.BookingIdSeatIdsMap[*bookingID], seat.ID)
		if result.EventBookingSeatIDs[seat.EventID] == nil {
			result.EventBookingSeatIDs[seat.EventID] = make(map[uuid.UUID][]uuid.UUID)
		}
		result.EventBookingSeatIDs[seat.EventID][*bookingID] = append(result.EventBookingSeatIDs[seat.EventID][*bookingID], seat.ID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("[UndoReservedExpiredSeats]: iterate seats: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("[UndoReservedExpiredSeats]: commit: %w", err)
	}

	return result, nil
}

func (r *PostgresSeatRepository) GetReservedExpiredSeats(ctx context.Context) (*ReservedExpiredSeatsResult, error) {
	// Read-only snapshot: RETURNING is not valid on SELECT, and row locks
	// taken here would be released at statement end anyway (no surrounding
	// tx), so locking is left to the status updates that act on these seats.
	query := `
		SELECT id, event_id, reserved_by_booking_id
		FROM seats
		WHERE status = 'reserved'
		AND deleted_at IS NULL
		AND reservation_expired_at IS NOT NULL
		AND reservation_expired_at <= NOW()`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get reserved expired seats: %w", err)
	}
	defer rows.Close()

	result := &ReservedExpiredSeatsResult{
		BookingIdSeatIdsMap: make(map[uuid.UUID][]uuid.UUID),
		EventBookingSeatIDs: make(map[uuid.UUID]map[uuid.UUID][]uuid.UUID),
	}
	for rows.Next() {
		var seatID, eventID uuid.UUID
		var bookingID *uuid.UUID
		if err := rows.Scan(&seatID, &eventID, &bookingID); err != nil {
			return nil, fmt.Errorf("[GetReservedExpiredSeats]: scan seat: %w", err)
		}
		// Seats reserved before reserved_by_booking_id existed have no owner;
		// they are still released, but there is no booking to report back.
		if bookingID == nil {
			continue
		}
		result.BookingIdSeatIdsMap[*bookingID] = append(result.BookingIdSeatIdsMap[*bookingID], seatID)
		if result.EventBookingSeatIDs[eventID] == nil {
			result.EventBookingSeatIDs[eventID] = make(map[uuid.UUID][]uuid.UUID)
		}
		result.EventBookingSeatIDs[eventID][*bookingID] = append(result.EventBookingSeatIDs[eventID][*bookingID], seatID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("[GetReservedExpiredSeats]: iterate seats: %w", err)
	}

	return result, nil
}
