package service_scheduler

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/scheduler/internal/domain"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

func TestSchedulerRepeatedTargetRetainsConfigVersionAndEventIntegration(t *testing.T) {
	u, err := url.Parse(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25439" || u.Path != "/ticketbox_scheduler_test" {
		t.Fatal("BLOCKED: isolated Scheduler PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "scheduler_service_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Error(err)
		}
	}()
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migration: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression) VALUES($1,'cleaner','reservation-cleaner-job',10,1,'*/5 * * * * *')`, id); err != nil {
		t.Fatal(err)
	}
	address, committed := serviceCommitProxy(t)
	proxyURL := *u
	proxyURL.Host = address
	pcfg, err := pgxpool.ParseConfig(proxyURL.String())
	if err != nil {
		t.Fatal(err)
	}
	pcfg.MaxConns = 1
	pp, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pp.Close()
	service := NewSchedulerService(zap.NewNop(), repository.NewSchedulerConfigRepo(p), repository.NewOutboundEventRepository(p), repository.NewUnitOfWork(pp), nil)
	target := domain.SchedulerConfig{IsEnabled: false, Timeout: 20 * time.Second, IntervalExpression: "*/10 * * * * *", Version: 1}
	if err := service.UpdateSchedulerConfigById(ctx, id, target); err != nil {
		t.Fatal("service failed to reread committed pair after ACK loss", err)
	}
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("service COMMIT boundary not observed")
	}
	service = NewSchedulerService(zap.NewNop(), repository.NewSchedulerConfigRepo(p), repository.NewOutboundEventRepository(p), repository.NewUnitOfWork(p), nil)
	for range 3 {
		if err := service.UpdateSchedulerConfigById(ctx, id, target); err != nil {
			t.Fatal(err)
		}
	}
	target.Version = 0 // compatibility request repeats latest target with durable pair.
	if err := service.UpdateSchedulerConfigById(ctx, id, target); err != nil {
		t.Fatal(err)
	}
	var version, count int
	var actual uuid.UUID
	if err := p.QueryRow(ctx, `SELECT version FROM scheduler_configs WHERE id=$1`, id).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*),min(id::text)::uuid FROM outbound_events`).Scan(&count, &actual); err != nil {
		t.Fatal(err)
	}
	want := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:scheduler:"+id.String()+":2"))
	if version != 2 || count != 1 || actual != want {
		t.Fatal("replay incremented version or changed identity", version, count, actual)
	}
	target.Version = 1
	target.Timeout = 30 * time.Second
	if err := service.UpdateSchedulerConfigById(ctx, id, target); err == nil {
		t.Fatal("mismatched replay accepted")
	}
	target.Version = 2
	if err := service.UpdateSchedulerConfigById(ctx, id, target); err != nil {
		t.Fatal(err)
	}
	target.Version = 0
	target.Timeout = 20 * time.Second
	if err := service.UpdateSchedulerConfigById(ctx, id, target); err == nil {
		t.Fatal("unversioned old target replay created a second decision")
	}
}

func serviceCommitProxy(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	committed := make(chan struct{}, 1)
	go func() {
		client, err := l.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		server, err := net.Dial("tcp", "127.0.0.1:25439")
		if err != nil {
			return
		}
		defer server.Close()
		go func() { _, _ = io.Copy(server, client) }()
		for {
			var h [5]byte
			if _, err := io.ReadFull(server, h[:]); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint32(h[1:])) - 4
			if size < 0 || size > 16*1024*1024 {
				return
			}
			body := make([]byte, size)
			if _, err := io.ReadFull(server, body); err != nil {
				return
			}
			if h[0] == 'C' && string(body) == "COMMIT\x00" {
				committed <- struct{}{}
				return
			}
			if _, err := client.Write(append(h[:], body...)); err != nil {
				return
			}
		}
	}()
	return l.Addr().String(), committed
}
