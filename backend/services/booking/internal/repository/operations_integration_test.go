package repository

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func operationsFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MVP_BOOKING_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated MVP_BOOKING_DATABASE_URL required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/rushgo_mvp_") {
		t.Fatal("refusing non-isolated database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	name := "rushgo_mvp_operations_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, `DROP DATABASE "`+name+`"`); admin.Close() })
	dir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("migrate", "-path", ".", "-database", dsn, "up")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migration: %v %s", err, output)
	}
	return pool
}

func TestOperationsAndNotificationTransaction(t *testing.T) {
	pool := operationsFixture(t)
	ctx := context.Background()
	booking, event := seedAdminBookingDetail(t, pool, true)
	repo := NewPostgresBookingRepository(pool)
	rows, more, err := repo.ListOperationsBookings(ctx, domain.OperationsQuery{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour), Limit: 50})
	if err != nil || more || len(rows) != 1 || rows[0].BookingID != booking || rows[0].TicketCount != 1 || rows[0].RefundStatus != "REQUESTED" {
		t.Fatalf("projection rows=%+v err=%v", rows, err)
	}
	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type='BusinessNotificationV1' AND event_key=$1`, booking.String()).Scan(&count)
	if err != nil || count != 2 {
		t.Fatalf("notifications count=%d err=%v", count, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE booking_refund_requests SET status=status WHERE booking_id=$1`, booking); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `SELECT enqueue_booking_notification('refund_requested',request_id,booking_id,status) FROM booking_refund_requests WHERE booking_id=$1`, booking); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type='BusinessNotificationV1' AND event_key=$1`, booking.String()).Scan(&count)
	if count != 2 {
		t.Fatal("duplicate business notification")
	}
	actor := uuid.New()
	_, err = repo.SetEventRefundApprover(ctx, event, actor, uuid.New(), true)
	if err != nil {
		t.Fatal(err)
	}
	work, _, err := repo.ListStaffWork(ctx, actor, false, 50, 0)
	if err != nil || len(work) != 1 || work[0].Access != "approver" || work[0].PendingRefunds != 1 {
		t.Fatalf("approver work %+v %v", work, err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, true, 50, 0)
	if err != nil || len(work) != 1 || work[0].Access != "approver" {
		t.Fatalf("admin approver duplicated %+v %v", work, err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at)VALUES($1,$2,true,$3,clock_timestamp())`, event, actor, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, false, 50, 0)
	if err != nil || len(work) != 2 || work[0].Access != "approver" || work[0].PendingRefunds != 1 || work[1].Access != "scanner" || work[1].PendingRefunds != 0 {
		t.Fatalf("independent dual capabilities %+v %v", work, err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, true, 50, 0)
	if err != nil || len(work) != 2 || work[0].Access != "approver" || work[1].Access != "scanner" {
		t.Fatalf("admin dual capabilities duplicated %+v %v", work, err)
	}
	if _, err = repo.SetScannerAssignment(ctx, event, actor, uuid.New(), false); err != nil {
		t.Fatal(err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, false, 50, 0)
	if err != nil || len(work) != 1 || work[0].Access != "approver" || work[0].PendingRefunds != 1 {
		t.Fatalf("scanner revocation removed approval %+v %v", work, err)
	}
	if _, err = repo.SetScannerAssignment(ctx, event, actor, uuid.New(), true); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.SetEventRefundApprover(ctx, event, actor, uuid.New(), false); err != nil {
		t.Fatal(err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, false, 50, 0)
	if err != nil || len(work) != 1 || work[0].Access != "scanner" || work[0].PendingRefunds != 0 {
		t.Fatalf("approver revocation removed scanner or granted refunds %+v %v", work, err)
	}
	work, _, err = repo.ListStaffWork(ctx, actor, true, 50, 0)
	if err != nil || len(work) != 2 || work[0].Access != "admin" || work[0].PendingRefunds != 1 || work[1].Access != "scanner" {
		t.Fatalf("scanner assignment hid current administrator refunds %+v %v", work, err)
	}
	work, _, err = repo.ListStaffWork(ctx, uuid.New(), false, 50, 0)
	if err != nil || len(work) != 0 {
		t.Fatal("unassigned work leaked")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `SELECT enqueue_booking_notification('manual_attention',$1,$2,'diagnostic')`, uuid.New(), booking)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type='BusinessNotificationV1' AND event_key=$1`, booking.String()).Scan(&count)
	if count != 2 {
		t.Fatal("outbox survived transaction rollback")
	}
}
