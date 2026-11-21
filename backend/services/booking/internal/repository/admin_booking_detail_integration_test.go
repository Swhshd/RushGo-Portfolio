package repository

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func adminBookingDetailFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("BOOKING_TEST_DATABASE_URL")
	parsed, err := url.Parse(raw)
	if raw == "" || err != nil || parsed.Hostname() != "127.0.0.1" || parsed.Port() != "25435" || parsed.Path != "/ticketbox_booking_test" {
		t.Fatal("BLOCKED: isolated Booking PG required (127.0.0.1:25435/ticketbox_booking_test)")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatalf("connect Booking test PG: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "admin_booking_detail_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, dropErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if dropErr != nil {
			t.Errorf("drop disposable schema: %v", dropErr)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	isolated := parsed.String()
	command := exec.Command("migrate", "-path", "../../migrations", "-database", isolated, "up")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("booking migrations failed: %s", strings.ReplaceAll(string(output), isolated, "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(isolated)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func seedAdminBookingDetail(t *testing.T, pool *pgxpool.Pool, withFacts bool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	bookingID, userID, eventID, paymentID, sagaID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	itemID, tierID, seatID, ticketID, requestID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',1000,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+interval '1 hour',$5)`,
		bookingID, userID, eventID, uuid.New(), sagaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,1,1000,$4)`,
		itemID, bookingID, tierID, []uuid.UUID{seatID}); err != nil {
		t.Fatal(err)
	}
	if !withFacts {
		return bookingID, eventID
	}
	issuedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status,issued_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'ISSUED',$9)`, ticketID, bookingID, itemID, sagaID, paymentID, eventID, tierID, seatID, issuedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count)
		VALUES($1,$2,$3,$4,$5,1,'issued','eligible','provider_observed_no_dispute','provider_observed_no_refund','stripe_test_api',clock_timestamp(),1)`,
		uuid.New(), sagaID, bookingID, paymentID, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO booking_refund_requests(request_id,booking_id,event_id,buyer_user_id,payment_id,generation,amount_cents,currency,reason,status)
		VALUES($1,$2,$3,$4,$5,1,1000,'usd','Changed plans','REQUESTED')`, requestID, bookingID, eventID, userID, paymentID); err != nil {
		t.Fatal(err)
	}
	return bookingID, eventID
}

func TestPostgresAdminBookingDetailReadsExactBookingFacts(t *testing.T) {
	pool := adminBookingDetailFixture(t)
	bookingID, _ := seedAdminBookingDetail(t, pool, true)
	repo := NewPostgresBookingRepository(pool)
	detail, err := repo.GetAdminBookingDetail(context.Background(), bookingID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Booking == nil || detail.Booking.ID != bookingID || detail.Booking.Status != "CONFIRMED" || detail.Booking.Currency != "usd" ||
		detail.Booking.LifecyclePhase != "TERMINAL" || len(detail.Booking.Items) != 1 {
		t.Fatalf("unexpected booking: %+v", detail.Booking)
	}
	if len(detail.Tickets) != 1 || detail.Tickets[0].Status != "ISSUED" || detail.Tickets[0].RevokedAt != nil {
		t.Fatalf("unexpected tickets: %+v", detail.Tickets)
	}
	if detail.Receipt == nil || detail.Receipt.Outcome != "issued" || detail.Receipt.ReceiptCount != 1 || detail.Receipt.TicketCount != 1 {
		t.Fatalf("unexpected receipt: %+v", detail.Receipt)
	}
	if detail.Refund == nil || detail.Refund.Status != "REQUESTED" || detail.Refund.PaymentRefundState != "unknown" || detail.Refund.RequestedAt == nil {
		t.Fatalf("unexpected refund: %+v", detail.Refund)
	}
	if detail.ObservedAt.IsZero() {
		t.Fatal("observed_at is zero")
	}

	emptyID, _ := seedAdminBookingDetail(t, pool, false)
	empty, err := repo.GetAdminBookingDetail(context.Background(), emptyID)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Tickets) != 0 || empty.Receipt != nil || empty.Refund != nil {
		t.Fatalf("empty booking returned facts: %+v", empty)
	}
	if _, err := repo.GetAdminBookingDetail(context.Background(), uuid.New()); err != ErrNotFound {
		t.Fatalf("missing booking error = %v", err)
	}
}

func TestPostgresAdminBookingDetailUsesNewestReceipt(t *testing.T) {
	pool := adminBookingDetailFixture(t)
	bookingID, eventID := seedAdminBookingDetail(t, pool, true)
	if _, err := pool.Exec(context.Background(), `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count,created_at)
		VALUES($1,$2,$3,$4,$5,1,'denied','not_eligible','unknown','unknown','',NULL,0,clock_timestamp()+interval '1 second')`,
		uuid.New(), uuid.New(), bookingID, uuid.New(), eventID); err != nil {
		t.Fatal(err)
	}
	detail, err := NewPostgresBookingRepository(pool).GetAdminBookingDetail(context.Background(), bookingID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Receipt == nil || detail.Receipt.Outcome != "denied" || detail.Receipt.ReceiptCount != 2 {
		t.Fatalf("newest receipt not selected: %+v", detail.Receipt)
	}
}
