package cronjob

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/testfixture"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type failedSettlement struct{}

func (failedSettlement) SettleOrderSaga(context.Context, *sagav1.SettleOrderSagaRequest, ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	return nil, errors.New("settlement wake unavailable")
}

func TestSchedulerExpiryRetainsBookingDispatchAfterWakeFailureIntegration(t *testing.T) {
	u, err := url.Parse(os.Getenv("BOOKING_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25435" || u.Path != "/ticketbox_booking_test" {
		t.Fatal("BLOCKED: isolated Booking PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "scheduler_expiry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Error(err)
		}
	}()
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../../booking/migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual Booking migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline,dispatch_kind,dispatch_state,dispatch_due_at,dispatch_version) VALUES($1,$2,$3,'PENDING',725,1,true,1,'OPEN','usd','pessimistic',clock_timestamp()-interval '1 minute','start','pending',clock_timestamp(),1)`, id, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	bookingv1.RegisterBookingServiceServer(server, testfixture.SagaReplayServer(p))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///scheduler-expiry", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	job := NewReservationCleanerJob(nil, bookingv1.NewBookingServiceClient(conn), nil, failedSettlement{}, nil, zap.NewNop())
	if err := job.Run(ctx); err == nil {
		t.Fatal("failed wake did not remain observable")
	}
	var status, kind, state string
	var needed bool
	var version, count int
	if err := p.QueryRow(ctx, `SELECT status,version,dispatch_kind,dispatch_state,terminal_dispatch_needed FROM bookings WHERE id=$1`, id).Scan(&status, &version, &kind, &state, &needed); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_key=$1`, id.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if status != "EXPIRED" || version != 2 || kind != "settle" || state != "pending" || !needed || count != 1 {
		t.Fatal("wake failure lost durable Booking settlement", status, version, kind, state, needed, count)
	}
}
