package repository

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

func TestCreateEventInventoryIsAtomicAndIdempotentIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	repo := NewPostgresEventRepository(pool)

	event := newAtomicEventFixture(2)
	actorID := uuid.New()
	event.CreationRequest = &domain.EventCreationRequest{ActorID: actorID, RequestID: "create-atomic-1", RequestHash: []byte(strings.Repeat("a", 32))}
	if err := repo.Create(ctx, event); err != nil {
		t.Fatalf("create event with generated inventory: %v", err)
	}
	var seats int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM seats WHERE event_id=$1 AND deleted_at IS NULL`, event.ID).Scan(&seats); err != nil || seats != 2 {
		t.Fatalf("generated seat count=%d err=%v, want 2", seats, err)
	}

	replay := newAtomicEventFixture(2)
	replay.CreationRequest = &domain.EventCreationRequest{ActorID: actorID, RequestID: "create-atomic-1", RequestHash: []byte(strings.Repeat("a", 32))}
	if err := repo.Create(ctx, replay); err != nil {
		t.Fatalf("replay same event creation request: %v", err)
	}
	if replay.ID != event.ID || len(replay.Tiers) != 1 {
		t.Fatalf("idempotent replay event=%s tiers=%d, want original %s and one tier", replay.ID, len(replay.Tiers), event.ID)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id=$1`, event.ID).Scan(&seats); err != nil || seats != 1 {
		t.Fatalf("replayed event rows=%d err=%v, want 1", seats, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM seats WHERE event_id=$1 AND deleted_at IS NULL`, event.ID).Scan(&seats); err != nil || seats != 2 {
		t.Fatalf("replayed seat rows=%d err=%v, want 2", seats, err)
	}

	conflict := newAtomicEventFixture(2)
	conflict.CreationRequest = &domain.EventCreationRequest{ActorID: actorID, RequestID: "create-atomic-1", RequestHash: []byte(strings.Repeat("b", 32))}
	if err := repo.Create(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting key error=%v, want ErrIdempotencyConflict", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&seats); err != nil || seats != 1 {
		t.Fatalf("conflicting replay left event rows=%d err=%v, want 1", seats, err)
	}
}

func TestFailedSeatBatchRollsBackEventAndTiersIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	repo := NewPostgresEventRepository(pool)
	event := newAtomicEventFixture(2)
	event.Seats[1].ID = event.Seats[0].ID
	event.CreationRequest = &domain.EventCreationRequest{ActorID: uuid.New(), RequestID: "create-rollback-1", RequestHash: []byte(strings.Repeat("c", 32))}
	if err := repo.Create(ctx, event); err == nil {
		t.Fatal("duplicate generated seat identity unexpectedly committed")
	}
	for _, table := range []string{"events", "ticket_tiers", "seats", "event_creation_requests"} {
		query := `SELECT count(*) FROM ` + table
		var count int
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows=%d err=%v after failed seat batch, want 0", table, count, err)
		}
	}
}

func TestSaleRowLockRejectsNewReservationsButAllowsExistingLifecycleReplayIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	eventRepo := NewPostgresEventRepository(pool)
	seatRepo := NewPostgresSeatRepository(pool)
	event := newAtomicEventFixture(2)
	if err := eventRepo.Create(ctx, event); err != nil {
		t.Fatalf("create test event inventory: %v", err)
	}

	deadline := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Microsecond)
	existing := lifecycleReserveRequest(event.ID, event.Seats[0].ID, uuid.New(), deadline)
	if result, err := seatRepo.ApplyLifecycle(ctx, existing); err != nil || result.State != "held" {
		t.Fatalf("initial hold = %#v, %v", result, err)
	}

	pauseTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pauseTx.Exec(ctx, `UPDATE events SET sales_status='paused' WHERE id=$1`, event.ID); err != nil {
		_ = pauseTx.Rollback(ctx)
		t.Fatalf("pause event: %v", err)
	}
	newReservation := lifecycleReserveRequest(event.ID, event.Seats[1].ID, uuid.New(), deadline.Add(time.Second))
	resultCh := make(chan error, 1)
	go func() {
		_, reserveErr := seatRepo.ApplyLifecycle(ctx, newReservation)
		resultCh <- reserveErr
	}()
	select {
	case reserveErr := <-resultCh:
		_ = pauseTx.Rollback(ctx)
		t.Fatalf("reservation completed before pause transaction released event row: %v", reserveErr)
	case <-time.After(100 * time.Millisecond):
	}
	if err := pauseTx.Commit(ctx); err != nil {
		t.Fatalf("commit pause: %v", err)
	}
	select {
	case reserveErr := <-resultCh:
		if !errors.Is(reserveErr, ErrSaleNotAllowed) {
			t.Fatalf("new reservation after pause error=%v, want ErrSaleNotAllowed", reserveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reservation did not resolve after pause committed")
	}

	replayed, err := seatRepo.ApplyLifecycle(ctx, existing)
	if err != nil || replayed.Applied {
		t.Fatalf("idempotent existing hold replay = %#v, %v; want unchanged successful replay", replayed, err)
	}
	var status string
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT status,seat_revision FROM seats WHERE id=$1`, event.Seats[1].ID).Scan(&status, &revision); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.SeatStatusAvailable) || revision != 1 {
		t.Fatalf("denied reservation seat state=%s revision=%d, want available/1", status, revision)
	}
}

func TestDeleteEventClosesSalesAndRetainsTicketInventoryIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	eventRepo := NewPostgresEventRepository(pool)
	event := newAtomicEventFixture(2)
	if err := eventRepo.Create(ctx, event); err != nil {
		t.Fatalf("create test event inventory: %v", err)
	}
	if err := eventRepo.Delete(ctx, event.ID); err != nil {
		t.Fatalf("close legacy delete route: %v", err)
	}
	loaded, err := eventRepo.GetByID(ctx, event.ID)
	if err != nil {
		t.Fatalf("load retained cancelled event: %v", err)
	}
	if loaded.PublicationStatus != domain.PublicationPublished || loaded.SalesStatus != domain.SalesCancelled {
		t.Fatalf("retained lifecycle=%s/%s, want published/cancelled", loaded.PublicationStatus, loaded.SalesStatus)
	}
	var seats int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM seats WHERE event_id=$1 AND deleted_at IS NULL`, event.ID).Scan(&seats); err != nil || seats != 2 {
		t.Fatalf("cancelled event seat rows=%d err=%v, want 2", seats, err)
	}
}

func newAtomicEventFixture(seatCount int) *domain.Event {
	eventID, tierID := uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	event := &domain.Event{
		ID: eventID, Title: "Atomic event", Description: "fixture", Category: "music", Venue: "hall", Location: "city",
		Date: now.Add(24 * time.Hour), DateVersion: 1, Status: "active", PublicationStatus: domain.PublicationPublished,
		SalesStatus: domain.SalesSelling, CreatedAt: now, UpdatedAt: now,
		Tiers: []domain.TicketTier{{ID: tierID, EventID: eventID, Name: "General", PriceCents: 725, TotalQuantity: int32(seatCount), AvailableQuantity: int32(seatCount), Version: 1, CreatedAt: now}},
	}
	for i := 0; i < seatCount; i++ {
		event.Seats = append(event.Seats, &domain.Seat{
			ID: uuid.New(), EventID: eventID, TicketTierID: tierID, Status: domain.SeatStatusAvailable,
			Position: domain.Position{SectionID: "General", Row: "A", Seat: i + 1}, CreatedAt: now, UpdatedAt: now,
		})
	}
	return event
}

func lifecycleReserveRequest(eventID, seatID, bookingID uuid.UUID, deadline time.Time) domain.SeatLifecycleRequest {
	fingerprint, _ := domain.SeatSetFingerprint(eventID, []uuid.UUID{seatID})
	return domain.SeatLifecycleRequest{
		BookingID: bookingID, EventID: eventID, Generation: 1, SeatIDs: []uuid.UUID{seatID}, Fingerprint: fingerprint,
		OperationID: uuid.New(), Action: "reserve", ReservationDeadline: deadline,
	}
}

func eventSalesIntegrationPool(t *testing.T) *pgxpool.Pool {
	pool, _ := eventSalesIntegrationPoolAtVersion(t, 0)
	return pool
}

func eventSalesIntegrationPoolAtVersion(t *testing.T, version int) (*pgxpool.Pool, string) {
	t.Helper()
	databaseURL := os.Getenv("EVENT_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("EVENT_TEST_DATABASE_URL is not set; isolated Event sale integration test skipped")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil || !strings.HasPrefix(strings.Trim(parsed.Path, "/"), "rushgo_mvp_event_") {
		t.Fatal("refusing Event integration test outside a fresh rushgo_mvp_event_* database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("connect to isolated Event database: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		admin.Close()
		t.Fatalf("create UUID extension in isolated Event database: %v", err)
	}
	schema := "event_sales_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatalf("create isolated Event schema: %v", err)
	}
	t.Cleanup(func() {
		_, dropErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if dropErr != nil {
			t.Errorf("drop isolated Event schema: %v", dropErr)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	migrateArgs := []string{"-path", "../../migrations", "-database", parsed.String()}
	if version > 0 {
		migrateArgs = append(migrateArgs, "goto", strconv.Itoa(version))
	} else {
		migrateArgs = append(migrateArgs, "up")
	}
	if output, err := exec.Command("migrate", migrateArgs...).CombinedOutput(); err != nil {
		t.Fatalf("apply Event migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatalf("parse isolated Event database URL: %v", err)
	}
	config.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to isolated Event schema: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, parsed.String()
}

func TestLegacyStatusReservationHonorsSaleGateIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	events := NewPostgresEventRepository(pool)
	seats := NewPostgresSeatRepository(pool)
	event := newAtomicEventFixture(2)
	if err := events.Create(ctx, event); err != nil {
		t.Fatal(err)
	}
	owner := uuid.New()
	if _, err := seats.UpdateStatus(ctx, event.Seats[0].ID, domain.SeatStatusReserved, &owner); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET sales_status='paused' WHERE id=$1`, event.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := seats.UpdateStatus(ctx, event.Seats[1].ID, domain.SeatStatusReserved, &owner); !errors.Is(err, ErrSaleNotAllowed) {
		t.Fatalf("legacy reserve bypassed pause: %v", err)
	}
	if _, err := seats.UpdateStatus(ctx, event.Seats[0].ID, domain.SeatStatusReserved, &owner); err != nil {
		t.Fatalf("original hold replay denied: %v", err)
	}
}
