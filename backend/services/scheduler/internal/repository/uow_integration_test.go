package repository

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
)

func schedulerPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25439" || u.Path != "/ticketbox_scheduler_test" {
		t.Fatal("BLOCKED: isolated Scheduler PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "scheduler_uow_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
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
	t.Cleanup(p.Close)
	return p
}

func schedulerCommitProxy(t *testing.T) (string, <-chan struct{}) {
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

func TestSchedulerCommitUncertaintyAndNullableReadsIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := uuid.New()
	nullable := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable)
		VALUES($1,'valid','reservation-cleaner-job',10,1,'*/5 * * * * *',true),($2,'legacy','legacy-unsupported',NULL,NULL,'bad expression',NULL)`, id, nullable); err != nil {
		t.Fatal(err)
	}
	address, committed := schedulerCommitProxy(t)
	u, err := url.Parse(p.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	u.Host = address
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	proxyPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer proxyPool.Close()
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:scheduler:"+id.String()+":2"))
	target := domain.SchedulerConfig{IsEnabled: false, Timeout: 20 * time.Second, IntervalExpression: "*/10 * * * * *"}
	err = NewUnitOfWork(proxyPool).Execute(ctx, func(repos *UowRepos) error {
		updated, err := repos.SchedulerConfigRepo().UpdateByVersion(ctx, id, 1, target)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(updated)
		if err != nil {
			return err
		}
		return repos.OutboundRepo().Create(ctx, outbound.OutboundEvent{Id: eventID, Topic: topics.TopicSchedulerConfigEvent, EventType: topics.TypeSchedulerConfigChanged, Status: outbound.OutboundStatusPending, Payload: payload})
	})
	if !errors.Is(err, ErrCommitUnknown) {
		t.Fatal("commit transport loss not classified unknown", err)
	}
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("no real COMMIT barrier")
	}
	configs, err := NewSchedulerConfigRepo(p).ListSchedulersConfig(ctx)
	if err != nil || len(configs) != 2 {
		t.Fatal("nullable config disappeared", configs, err)
	}
	c, err := NewSchedulerConfigRepo(p).GetById(ctx, nullable)
	if err != nil || c.Valid || c.ValidationError == "" {
		t.Fatal("legacy NULL invented valid config", c, err)
	}
	c, err = NewSchedulerConfigRepo(p).GetById(ctx, id)
	if err != nil || !c.Valid || c.Version != 2 || c.IsEnabled {
		t.Fatal("config half missing", c, err)
	}
	e, err := NewOutboundEventRepository(p).GetByID(ctx, eventID)
	if err != nil || e.Id != eventID || e.PublishAt != nil {
		t.Fatal("outbound half missing/false publication", e, err)
	}
	if _, err := NewSchedulerConfigRepo(p).UpdateByVersion(ctx, id, 1, target); !errors.Is(err, ErrConfigConflict) {
		t.Fatal("old expected version incremented twice", err)
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbound_events`).Scan(&count); err != nil || count != 1 {
		t.Fatal("uncertainty invented second identity", count, err)
	}
	legacyEvent := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO outbound_events(id,topic,event_type,status,payload) VALUES($1,'legacy','unknown',NULL,NULL)`, legacyEvent); err != nil {
		t.Fatal(err)
	}
	e, err = NewOutboundEventRepository(p).GetByID(ctx, legacyEvent)
	if err != nil || e.PublishAt != nil || e.Status != "" || e.Payload != nil {
		t.Fatal("legacy nullable outbound cannot be read", e, err)
	}
	// A stream error after Next must propagate instead of returning partial rows.
	broken := NewSchedulerConfigRepo(failingSchedulerQuery{Querier: p})
	if _, err := broken.ListSchedulersConfig(ctx); err == nil {
		t.Fatal("rows iteration error hidden")
	}
	if _, err := p.Exec(ctx, `UPDATE scheduler_configs SET version=7,is_enable=true WHERE id=$1`, nullable); err != nil {
		t.Fatal(err)
	}
	c, err = NewSchedulerConfigRepo(p).GetById(ctx, nullable)
	if err != nil || c.Valid || c.Version != 7 || !c.IsEnabled {
		t.Fatal("NULL timeout discarded known sibling fields", c, err)
	}
}

type failingSchedulerQuery struct{ Querier }

func (q failingSchedulerQuery) Query(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
	rows, err := q.Querier.Query(ctx, `SELECT `+configColumns+` FROM scheduler_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return &lateFailureRows{Rows: rows}, nil
}

type lateFailureRows struct {
	pgx.Rows
	seen bool
}

func (r *lateFailureRows) Next() bool {
	if r.seen {
		r.Rows.Close()
		return false
	}
	r.seen = r.Rows.Next()
	return r.seen
}
func (r *lateFailureRows) Err() error {
	if r.seen {
		return errors.New("injected transport loss after first config row")
	}
	return r.Rows.Err()
}

func TestSchedulerSecondRepositoryFailureRollsBackIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	id := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable) VALUES($1,'fixture-cleaner','reservation-cleaner-job',10,1,'*/5 * * * * *',true)`, id); err != nil {
		t.Fatal(err)
	}
	var original error
	err := NewUnitOfWork(p).Execute(ctx, func(repos *UowRepos) error {
		if _, err := repos.SchedulerConfigRepo().UpdateById(ctx, id, domain.SchedulerConfig{IsEnabled: false, Timeout: 20 * time.Second, IntervalExpression: "*/10 * * * * *"}); err != nil {
			return err
		}
		original = repos.OutboundRepo().Create(ctx, outbound.OutboundEvent{Id: uuid.New(), Topic: "scheduler.config.events", EventType: "changed", Status: outbound.OutboundStatusPending, Payload: []byte(`{"invalid":`)})
		return original
	})
	if original == nil {
		t.Fatal("constraint fault did not reach production outbound repository")
	}
	if err == nil || !errors.Is(err, original) {
		t.Fatal("UOW swallowed required outbound failure", err)
	}
	var enabled bool
	var version, count int
	if err := p.QueryRow(ctx, `SELECT is_enable,version FROM scheduler_configs WHERE id=$1`, id).Scan(&enabled, &version); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbound_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if !enabled || version != 1 || count != 0 {
		t.Fatal("second repository failure committed partial config", enabled, version, count)
	}
	sentinel := errors.New("original canceled callback")
	cctx, stop := context.WithCancel(ctx)
	err = NewUnitOfWork(p).Execute(cctx, func(*UowRepos) error { stop(); return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatal("cancellation replaced original callback error", err)
	}
	err = NewUnitOfWork(p).Execute(ctx, func(repos *UowRepos) error {
		if _, err := repos.SchedulerConfigRepo().UpdateById(ctx, id, domain.SchedulerConfig{IsEnabled: false, Timeout: 20 * time.Second, IntervalExpression: "*/10 * * * * *"}); err != nil {
			return err
		}
		pid := repos.schedulerConfigRepo.q.(pgx.Tx).Conn().PgConn().PID()
		if _, err := p.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "scheduler rollback") {
		t.Fatal("rollback failure replaced original callback", err)
	}
	if err := p.QueryRow(ctx, `SELECT is_enable,version FROM scheduler_configs WHERE id=$1`, id).Scan(&enabled, &version); err != nil || !enabled || version != 1 {
		t.Fatal("terminated rollback committed writes", enabled, version, err)
	}
}
