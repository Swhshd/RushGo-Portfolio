package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ticketbox/booking/internal/domain"
)

func TestBookingAdmissionMigrationUpgradeAndRollbackBoundaryIntegration(t *testing.T) {
	pool, _ := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	f := seedTicketRepositoryRows(t, pool, domain.TicketIssued, nil, true)
	var before, after string
	snapshot := `SELECT (to_jsonb(t)-'admission_request_id')::text FROM tickets t WHERE id=$1`
	if err := pool.QueryRow(ctx, snapshot, f.ticketID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/000007_admission_fence.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, err := os.ReadFile("../../migrations/000007_admission_fence.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Empty inactive coordination state permits rollback; existing business rows
	// are retained. Reapplying models an upgrade with pre-existing tickets.
	if _, err = pool.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(up)); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, snapshot, f.ticketID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("admission upgrade changed historical ticket data")
	}
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", pool.Config().ConnString(), "up").CombinedOutput(); err != nil {
		t.Fatalf("repeat migration failed (output bytes=%d)", len(output))
	}
	assertBookingAdmissionRollbackSerializesActivation(t, pool, down)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, downErr := tx.Exec(ctx, string(down))
	_ = tx.Rollback(ctx)
	if downErr == nil {
		t.Fatal("activated Booking admission allowed destructive rollback")
	}
	var active bool
	if err = pool.QueryRow(ctx, `SELECT active FROM booking_admission_mode WHERE singleton=TRUE`).Scan(&active); err != nil || !active {
		t.Fatal("failed rollback did not preserve activation")
	}
}

// Hold activation uncommitted, observe rollback's database lock wait, then
// commit activation. The rollback must re-evaluate the latch after its wait.
func assertBookingAdmissionRollbackSerializesActivation(t *testing.T, pool *pgxpool.Pool, down []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	activation, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer activation.Rollback(context.Background())
	if _, err = activation.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	result := make(chan error, 1)
	go func() { _, e := conn.Exec(ctx, string(down)); result <- e }()
	deadline := time.Now().Add(time.Second)
	for {
		var waiting bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND relation='booking_admission_mode'::regclass AND mode='AccessExclusiveLock' AND NOT granted)`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rollback did not wait on activation barrier")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = activation.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	downErr := <-result
	_, _ = conn.Exec(context.Background(), "ROLLBACK")
	if downErr == nil || !strings.Contains(downErr.Error(), "cannot remove Booking admission fence") {
		t.Fatalf("rollback failed to refuse activation committed during lock wait: %v", downErr)
	}
}
