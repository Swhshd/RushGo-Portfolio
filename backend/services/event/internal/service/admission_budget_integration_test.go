package service

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/event/internal/admission"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"go.uber.org/zap"
)

func TestRedisUnavailableFallsBackToActualPGAndRejectsPGFailureIntegration(t *testing.T) {
	u, err := url.Parse(os.Getenv("EVENT_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25434" || u.Path != "/ticketbox_event_test" {
		t.Fatal("BLOCKED: isolated Event PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "admission_fallback_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_, err := admin.Exec(cleanup, `DROP SCHEMA `+schema+` CASCADE`)
		if err != nil {
			t.Error(err)
		}
	}()
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	event, tier, seat := uuid.New(), uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date,publication_status,sales_status)
		VALUES($1,'fixture','concert','fixture','fixture',NOW()+INTERVAL '1 day','published','selling')`, event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,lifecycle_accounting_baseline) VALUES($1,$2,'fixture',725,1,1,'verified')`, tier, event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id) VALUES($1,$2,$3)`, seat, event, tier); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 8)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	defer func() {
		listener.Close()
		<-acceptDone
		close(accepted)
		for c := range accepted {
			c.Close()
		}
	}()
	cache := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), ContextTimeoutEnabled: true})
	defer cache.Close()
	svc := NewEventServiceWithAdmissionAndRealtimePublisherAndMetrics(nil, nil, repository.NewPostgresSeatRepository(p), zap.NewNop(), admission.NewRedisAdmission(cache), admission.ModeRedisAdmission, nil, nil)
	fingerprint, _ := domain.SeatSetFingerprint(event, []uuid.UUID{seat})
	req := domain.SeatLifecycleRequest{BookingID: uuid.New(), EventID: event, Generation: 1, SeatIDs: []uuid.UUID{seat}, Fingerprint: fingerprint, OperationID: uuid.New(), Action: "reserve", ReservationDeadline: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if result, err := svc.ApplyLifecycle(ctx, req); err != nil || result == nil || result.State != "held" {
		t.Fatal("blackhole did not fall back to PG", result, err)
	}
	req.BookingID = uuid.New()
	req.OperationID = uuid.New()
	if _, err := svc.ApplyLifecycle(ctx, req); err == nil {
		t.Fatal("PG conflict reported Redis-only success")
	}
}
