package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
)

func TestSchedulerUpgradeDrainsProvenLegacyIDAndQuarantinesUnknownIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Exercise the actual old schema, writes made before deployment, then upgrade.
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", p.Config().ConnString(), "down", "1").CombinedOutput(); err != nil {
		t.Fatalf("old schema: %s", out)
	}
	config, original, unknown := uuid.New(), uuid.New(), uuid.New()
	cfg := domain.SchedulerConfig{Id: config, Name: "legacy-cleaner", Version: 2, Timeout: 10 * time.Second, IntervalExpression: "*/5 * * * * *", IsEnabled: true}
	raw, _ := json.Marshal(cfg)
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,timeout,version,interval_expression,is_enable) VALUES($1,$2,10,2,$3,true)`, config, cfg.Name, cfg.IntervalExpression); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO outbound_events(id,topic,event_type,status,payload) VALUES($1,$2,$3,'pending',$4),($5,'unproven','unknown','pending','{}')`, original, topics.TopicSchedulerConfigEvent, topics.TypeSchedulerConfigChanged, raw, unknown); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", p.Config().ConnString(), "up").CombinedOutput(); err != nil {
		t.Fatalf("upgrade: %s", out)
	}
	for _, version := range []int64{2, 2147483648} {
		payload := map[string]any{"Id": uuid.NewString(), "Name": cfg.Name, "Version": version, "Timeout": int64(10 * time.Second), "IntervalExpression": cfg.IntervalExpression}
		if version > 2 {
			payload["IsEnabled"] = true
		}
		poison, _ := json.Marshal(payload)
		if _, err := p.Exec(ctx, `INSERT INTO outbound_events(id,topic,event_type,status,payload) VALUES($1,$2,$3,'pending',$4)`, uuid.New(), topics.TopicSchedulerConfigEvent, topics.TypeSchedulerConfigChanged, poison); err != nil {
			t.Fatal(err)
		}
	}
	r := NewOutboundEventRepository(p)
	claims, err := r.ClaimOutbound(ctx, 1, time.Second)
	if err != nil || len(claims) != 1 || claims[0].Event.Id != original {
		t.Fatal("upgrade stranded or replaced proven legacy UUID", claims, err)
	}
	var category string
	if err := p.QueryRow(ctx, `SELECT last_error FROM outbound_events WHERE id=$1`, unknown).Scan(&category); err != nil || category != "legacy_outbound_attention" {
		t.Fatal("unproven history disappeared", category, err)
	}
	if ok, err := r.FinishOutbound(ctx, claims[0], true, "", time.Time{}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	event, err := r.GetByConfigVersion(ctx, config, 2)
	if err != nil || event.Id != original || event.Status != outbound.OutboundStatusPublished {
		t.Fatal("upgrade identity/result lost", event, err)
	}
}

func TestSchedulerMarkerACKLossAndExpiredLockWaitIntegration(t *testing.T) {
	p := schedulerPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := NewOutboundEventRepository(p)
	config := uuid.New()
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:scheduler:"+config.String()+":2"))
	raw, _ := json.Marshal(domain.SchedulerConfig{Id: config, Name: "marker-fixture", Version: 2, Timeout: time.Second, IntervalExpression: "*/5 * * * * *"})
	if err := r.Create(ctx, outbound.OutboundEvent{Id: id, Topic: topics.TopicSchedulerConfigEvent, EventType: topics.TypeSchedulerConfigChanged, Status: outbound.OutboundStatusPending, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimOutbound(ctx, 1, time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	address, committed := schedulerCommitProxy(t)
	u, _ := url.Parse(p.Config().ConnString())
	u.Host = address
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	proxy, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if ok, err := NewOutboundEventRepository(proxy).FinishOutbound(ctx, claims[0], true, "", time.Time{}); err == nil || ok {
		t.Fatal("marker ACK not dropped", ok, err)
	}
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("marker COMMIT barrier absent")
	}
	if ok, err := r.FinishOutbound(ctx, claims[0], true, "", time.Time{}); err != nil || !ok {
		t.Fatal("real marker ACK loss reread failed", ok, err)
	}
	// A second row is held across expiry while Finish waits for its lock.
	config = uuid.New()
	id = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:scheduler:"+config.String()+":2"))
	raw, _ = json.Marshal(domain.SchedulerConfig{Id: config, Name: "lock-fixture", Version: 2, Timeout: time.Second, IntervalExpression: "*/5 * * * * *"})
	if err := r.Create(ctx, outbound.OutboundEvent{Id: id, Topic: topics.TopicSchedulerConfigEvent, EventType: topics.TypeSchedulerConfigChanged, Status: outbound.OutboundStatusPending, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	claims, err = r.ClaimOutbound(ctx, 1, time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `UPDATE outbound_events SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ok, err := r.FinishOutbound(ctx, claims[0], true, "", time.Time{})
		if ok {
			err = fmt.Errorf("expired publisher finished after row lock")
		}
		done <- err
	}()
	for {
		var waiting bool
		if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatal("no observed lock wait", err)
		case <-ctx.Done():
			t.Fatal("lock wait missing")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
