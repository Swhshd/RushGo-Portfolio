package repository

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

func TestSeatStatusBatchUpdateClearsReservationMetadataWhenAvailable(t *testing.T) {
	query := seatStatusBatchUpdateQuery(domain.SeatStatusAvailable)
	for _, fragment := range []string{"reserved_by_booking_id = $2", "status = 'reserved'", "reservation_expired_at = NULL", "booking_id = NULL", "seat_revision = seat_revision + CASE WHEN status <> 'available' THEN 1 ELSE 0 END"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("available-seat update must include %q, query=%q", fragment, query)
		}
	}
}

func TestSeatStatusBatchUpdateConfirmationRequiresOwnerAndActiveHold(t *testing.T) {
	query := seatStatusBatchUpdateQuery(domain.SeatStatusBooked)
	for _, fragment := range []string{"status = 'reserved'", "reserved_by_booking_id = $2", "reservation_expired_at > NOW()", "reserved_by_booking_id = NULL", "seat_revision = seat_revision + 1"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("booked-seat update must include %q, query=%q", fragment, query)
		}
	}
}

func TestSeatStatusBatchUpdateRevisionIncrementsOnlyWhenPublicStatusChanges(t *testing.T) {
	for _, status := range []domain.SeatStatus{domain.SeatStatusReserved, domain.SeatStatusBooked} {
		query := seatStatusBatchUpdateQuery(status)
		if !strings.Contains(query, "seat_revision = seat_revision + 1") {
			t.Errorf("%s query does not increment the public seat revision: %s", status, query)
		}
	}
	availableQuery := seatStatusBatchUpdateQuery(domain.SeatStatusAvailable)
	if !strings.Contains(availableQuery, "seat_revision = seat_revision + CASE WHEN status <> 'available' THEN 1 ELSE 0 END") {
		t.Errorf("available query must increment only for a public status change: %s", availableQuery)
	}
}

func TestSeatRevisionTransitionsAndSameOwnerRetryIntegration(t *testing.T) {
	pool, repo := newSeatRepositoryIntegrationFixture(t)
	ctx := context.Background()
	seatID := insertTestSeat(t, pool)
	owner := uuid.New()

	if revision := seatRevision(t, pool, seatID); revision != 1 {
		t.Fatalf("initial revision=%d, want 1", revision)
	}
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: owner, Action: domain.RESERVED_SEAT,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if revision := seatRevision(t, pool, seatID); revision != 2 {
		t.Fatalf("revision after reserve=%d, want 2", revision)
	}

	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: owner, Action: domain.RESERVED_SEAT,
	}); err != nil {
		t.Fatalf("same-owner retry: %v", err)
	}
	if revision := seatRevision(t, pool, seatID); revision != 2 {
		t.Fatalf("same-owner retry revision=%d, want unchanged revision 2", revision)
	}

	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: owner, Action: domain.COMPENSATE_SEAT,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if revision := seatRevision(t, pool, seatID); revision != 3 {
		t.Fatalf("revision after release=%d, want 3", revision)
	}
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: uuid.New(), Action: domain.RESERVED_SEAT,
	}); err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if revision := seatRevision(t, pool, seatID); revision != 4 {
		t.Fatalf("revision after second reserve=%d, want 4", revision)
	}
}

func TestSeatRevisionRollbackDoesNotBecomeVisibleIntegration(t *testing.T) {
	pool, _ := newSeatRepositoryIntegrationFixture(t)
	ctx := context.Background()
	seatID := insertTestSeat(t, pool)
	owner := uuid.New()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, seatStatusBatchUpdateQuery(domain.SeatStatusReserved), []uuid.UUID{seatID}, owner, 15); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("update inside rollback transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var status string
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT status, seat_revision FROM seats WHERE id=$1`, seatID).Scan(&status, &revision); err != nil {
		t.Fatal(err)
	}
	if status != "available" || revision != 1 {
		t.Fatalf("after rollback status=%q revision=%d, want available/1", status, revision)
	}
}

func TestSeatRevisionFollowsMutationOrderNotTransactionStartIntegration(t *testing.T) {
	pool, repo := newSeatRepositoryIntegrationFixture(t)
	ctx := context.Background()
	seatID := insertTestSeat(t, pool)
	owner := uuid.New()
	olderTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer olderTx.Rollback(ctx)
	var olderTransactionTimestamp time.Time
	if err := olderTx.QueryRow(ctx, `SELECT NOW()`).Scan(&olderTransactionTimestamp); err != nil {
		t.Fatalf("start earlier transaction timestamp: %v", err)
	}
	time.Sleep(25 * time.Millisecond)

	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: owner, Action: domain.RESERVED_SEAT,
	}); err != nil {
		t.Fatalf("later-started reserve: %v", err)
	}
	reservedRevision := seatRevision(t, pool, seatID)
	var reservedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM seats WHERE id=$1`, seatID).Scan(&reservedAt); err != nil {
		t.Fatal(err)
	}

	// This earlier-started transaction performs its valid release only after
	// the later transaction's reservation committed.
	query := seatStatusBatchUpdateQuery(domain.SeatStatusAvailable)
	command, err := olderTx.Exec(ctx, query, []uuid.UUID{seatID}, owner)
	if err != nil {
		t.Fatalf("earlier-started later mutation: %v", err)
	}
	if command.RowsAffected() != 1 {
		t.Fatalf("release rows affected=%d, want 1", command.RowsAffected())
	}
	if err := olderTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var status string
	var revision int64
	var releasedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT status, seat_revision, updated_at FROM seats WHERE id=$1`, seatID).Scan(&status, &revision, &releasedAt); err != nil {
		t.Fatal(err)
	}
	if status != "available" || revision != reservedRevision+1 {
		t.Fatalf("final status=%q revision=%d, want available/%d", status, revision, reservedRevision+1)
	}
	if !reservedAt.After(olderTransactionTimestamp) || !releasedAt.Equal(olderTransactionTimestamp) {
		t.Fatalf("test did not invert transaction timestamp ordering: older tx timestamp %s, reserve updated_at %s, release updated_at %s", olderTransactionTimestamp, reservedAt, releasedAt)
	}
}

func TestConcurrentSeatWinnerAdvancesRevisionOnceIntegration(t *testing.T) {
	pool, repo := newSeatRepositoryIntegrationFixture(t)
	ctx := context.Background()
	seatID := insertTestSeat(t, pool)
	start := make(chan struct{})
	var winners atomic.Int32
	var conflicts atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		owner := uuid.New()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
				SeatIds: []uuid.UUID{seatID}, ReservedByBookingId: owner, Action: domain.RESERVED_SEAT,
			})
			if err == nil {
				winners.Add(1)
			} else if errors.Is(err, ErrSeatNotAvailable) {
				conflicts.Add(1)
			} else {
				t.Errorf("unexpected reservation result: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 || conflicts.Load() != 99 {
		t.Fatalf("winners=%d conflicts=%d, want 1/99", winners.Load(), conflicts.Load())
	}
	if revision := seatRevision(t, pool, seatID); revision != 2 {
		t.Fatalf("final revision=%d, want exactly one increment from 1", revision)
	}
}

func TestSeatReservationConcurrencyAndOwnershipIntegration(t *testing.T) {
	pool, repo := newSeatRepositoryIntegrationFixture(t)
	ctx := context.Background()

	seatID := insertTestSeat(t, pool)
	bookingIDs := make([]uuid.UUID, 100)
	for i := range bookingIDs {
		bookingIDs[i] = uuid.New()
	}

	start := make(chan struct{})
	var winners atomic.Int32
	var conflicts atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup
	for _, bookingID := range bookingIDs {
		wg.Add(1)
		go func(bookingID uuid.UUID) {
			defer wg.Done()
			<-start
			_, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
				SeatIds:               []uuid.UUID{seatID},
				ReservedByBookingId:   bookingID,
				Action:                domain.RESERVED_SEAT,
				ReservedTimeInMinutes: 15,
			})
			switch {
			case err == nil:
				winners.Add(1)
			case errors.Is(err, ErrSeatNotAvailable):
				conflicts.Add(1)
			default:
				t.Logf("unexpected reserve error: %v", err)
				unexpected.Add(1)
			}
		}(bookingID)
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 || conflicts.Load() != int32(len(bookingIDs)-1) || unexpected.Load() != 0 {
		t.Fatalf("acquisition winners=%d conflicts=%d unexpected=%d; want 1/%d/0",
			winners.Load(), conflicts.Load(), unexpected.Load(), len(bookingIDs)-1)
	}

	var owner uuid.UUID
	var status string
	if err := pool.QueryRow(ctx, `SELECT status, reserved_by_booking_id FROM seats WHERE id = $1`, seatID).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" {
		t.Fatalf("seat status=%q, want reserved", status)
	}
	if owner == uuid.Nil {
		t.Fatal("reserved seat has no owner")
	}

	// A partially available batch must roll back completely when one requested
	// seat belongs to another booking.
	secondSeatID := insertTestSeat(t, pool)
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds:             []uuid.UUID{secondSeatID, seatID},
		ReservedByBookingId: uuid.New(),
		Action:              domain.RESERVED_SEAT,
	}); !errors.Is(err, ErrSeatNotAvailable) {
		t.Fatalf("conflicting multi-seat reserve error=%v, want ErrSeatNotAvailable", err)
	}
	var secondStatus string
	var secondOwner *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT status, reserved_by_booking_id FROM seats WHERE id = $1`, secondSeatID).Scan(&secondStatus, &secondOwner); err != nil {
		t.Fatal(err)
	}
	if secondStatus != "available" || secondOwner != nil {
		t.Fatalf("partial batch mutation: status=%s owner=%v", secondStatus, secondOwner)
	}

	// Same-owner retry succeeds without extending or changing the original hold.
	var firstExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT reservation_expired_at FROM seats WHERE id = $1`, seatID).Scan(&firstExpiry); err != nil {
		t.Fatal(err)
	}
	if result, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds:               []uuid.UUID{seatID},
		ReservedByBookingId:   owner,
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 15,
	}); err != nil || !result.Success || len(result.ChangedSeats) != 0 {
		t.Fatalf("same-owner reserve retry result=%+v err=%v, want success with no changed seats", result, err)
	}
	var retriedExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT reservation_expired_at FROM seats WHERE id = $1`, seatID).Scan(&retriedExpiry); err != nil {
		t.Fatal(err)
	}
	if !firstExpiry.Equal(retriedExpiry) {
		t.Fatalf("same-owner retry changed expiry: before=%s after=%s", firstExpiry, retriedExpiry)
	}

	// Release A, let B acquire, then ensure A's stale duplicate release cannot
	// release B's hold.
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds:             []uuid.UUID{seatID},
		ReservedByBookingId: owner,
		Action:              domain.COMPENSATE_SEAT,
	}); err != nil {
		t.Fatalf("release original owner: %v", err)
	}
	newOwner := uuid.New()
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds:               []uuid.UUID{seatID},
		ReservedByBookingId:   newOwner,
		Action:                domain.RESERVED_SEAT,
		ReservedTimeInMinutes: 15,
	}); err != nil {
		t.Fatalf("new owner acquisition: %v", err)
	}
	if _, err := repo.ReservedOrCompensateBatchSeats(ctx, &ReservedOrCompensateBatchSeats{
		SeatIds:             []uuid.UUID{seatID},
		ReservedByBookingId: owner,
		Action:              domain.COMPENSATE_SEAT,
	}); !errors.Is(err, ErrSeatNotAvailable) {
		t.Fatalf("stale release error=%v, want ErrSeatNotAvailable", err)
	}
	if err := pool.QueryRow(ctx, `SELECT reserved_by_booking_id FROM seats WHERE id = $1`, seatID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != newOwner {
		t.Fatalf("stale release changed owner to %s, want %s", owner, newOwner)
	}

	// Neither another booking nor the owner of an expired hold may confirm it.
	if _, err := repo.UpdateStatusBatch(ctx, []uuid.UUID{seatID}, domain.SeatStatusBooked, ptrUUID(uuid.New())); !errors.Is(err, ErrSeatNotAvailable) {
		t.Fatalf("wrong-owner confirmation error=%v, want ErrSeatNotAvailable", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE seats SET reservation_expired_at = NOW() - INTERVAL '1 second' WHERE id = $1`, seatID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.UpdateStatusBatch(ctx, []uuid.UUID{seatID}, domain.SeatStatusBooked, &newOwner); !errors.Is(err, ErrSeatNotAvailable) {
		t.Fatalf("expired-hold confirmation error=%v, want ErrSeatNotAvailable", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE seats SET reservation_expired_at = NOW() + INTERVAL '1 minute' WHERE id = $1`, seatID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.UpdateStatusBatch(ctx, []uuid.UUID{seatID}, domain.SeatStatusBooked, &newOwner); err != nil {
		t.Fatalf("owner confirmation: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status, booking_id FROM seats WHERE id = $1`, seatID).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if status != "booked" || owner != newOwner {
		t.Fatalf("confirmed seat status=%s owner=%s, want booked/%s", status, owner, newOwner)
	}
}

func newSeatRepositoryIntegrationFixture(t *testing.T) (*pgxpool.Pool, *PostgresSeatRepository) {
	t.Helper()
	pool := eventLifecycleFixture(t)
	seedLifecycleSeats(t, pool, 1)
	return pool, NewPostgresSeatRepository(pool)
}

func seatRevision(t *testing.T, pool *pgxpool.Pool, seatID uuid.UUID) int64 {
	t.Helper()
	var revision int64
	if err := pool.QueryRow(context.Background(), `SELECT seat_revision FROM seats WHERE id=$1`, seatID).Scan(&revision); err != nil {
		t.Fatalf("read seat revision: %v", err)
	}
	return revision
}

func insertTestSeat(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var eventID, tierID uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT e.id,t.id FROM events e JOIN ticket_tiers t ON t.event_id=e.id
		WHERE e.publication_status='published' AND e.sales_status='selling' ORDER BY e.created_at DESC LIMIT 1`).Scan(&eventID, &tierID); err != nil {
		t.Fatalf("find the explicitly selling event fixture: %v", err)
	}
	id := uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO seats (id,event_id,ticket_tier_id,status) VALUES ($1,$2,$3,'available')`, id, eventID, tierID); err != nil {
		t.Fatalf("insert test seat: %v", err)
	}
	return id
}

func ptrUUID(id uuid.UUID) *uuid.UUID { return &id }
