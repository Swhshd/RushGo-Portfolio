package service_outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
	cronjob "github.com/ticketbox/scheduler/internal/jobs"
	"github.com/ticketbox/scheduler/internal/kafka"
	"github.com/ticketbox/scheduler/internal/repository"
	service_scheduler "github.com/ticketbox/scheduler/internal/service/scheduler"
	"go.uber.org/zap"
)

func relayPool(t *testing.T) *pgxpool.Pool {
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
	schema := "scheduler_relay_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
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
	t.Cleanup(p.Close)
	return p
}

func TestSchedulerRelayReloadAndExpiryAuthorityIntegration(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker != "127.0.0.1:29093" {
		t.Fatal("BLOCKED: isolated Kafka required")
	}
	p := relayPool(t)
	r := repository.NewOutboundEventRepository(p)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configID := uuid.New()
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:scheduler:"+configID.String()+":2"))
	payload, _ := json.Marshal(domain.SchedulerConfig{Id: configID, Name: "relay-fixture", Version: 2, IsEnabled: true, Timeout: time.Second, IntervalExpression: "*/5 * * * * *", Valid: true})
	if err := r.Create(ctx, outbound.OutboundEvent{Id: id, Topic: topics.TopicSchedulerConfigEvent, EventType: topics.TypeSchedulerConfigChanged, Status: outbound.OutboundStatusPending, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	producer := kafka.NewOutboundEventProducer(zap.NewNop(), []string{broker})
	defer producer.Producer().Close()
	fault := &markerFault{OutboundWorkRepository: r}
	if n, err := NewRelay(fault, producer.Producer(), zap.NewNop()).RunOnce(ctx, 1); err == nil || n != 1 {
		t.Fatal("lost marker not retained", n, err)
	}
	if _, err := p.Exec(ctx, `UPDATE outbound_events SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if n, err := NewRelay(r, producer.Producer(), zap.NewNop()).RunOnce(ctx, 1); err != nil || n != 1 {
		t.Fatal("restart failed to drain", n, err)
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{broker}, Topic: topics.TopicSchedulerConfigEvent, Partition: 0, MinBytes: 1, MaxBytes: 1e6, MaxWait: 100 * time.Millisecond})
	defer reader.Close()
	var first []byte
	found := 0
	for found < 2 {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Key) != id.String() {
			continue
		}
		var envelope pkgkafka.Event
		if err := json.Unmarshal(msg.Value, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.ID != id.String() {
			t.Fatalf("scheduler relay lost stable envelope ID: got %q want %s", envelope.ID, id)
		}
		if first == nil {
			first = msg.Value
		} else if !bytes.Equal(first, msg.Value) {
			t.Fatal("retry changed frozen envelope")
		}
		found++
	}
	if ok, err := r.FinishOutbound(ctx, fault.claim, false, "late", time.Now()); err != nil || ok {
		t.Fatal("stale publisher overwrote marker", ok, err)
	}
	if err := r.MarkEventAsPublished(ctx, id); err == nil {
		t.Fatal("managed relay bypass accepted")
	}
	// Restart loads the authoritative config; periodic Reload applies a newer
	// valid version even without receiving its Kafka wake.
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable) VALUES($1,'reload-fixture','reservation-cleaner-job',2,1,'* * * * * *',false)`, configID); err != nil {
		t.Fatal(err)
	}
	manager := cronjob.NewCronjobManager(zap.NewNop(), repository.NewSchedulerConfigRepo(p), cronjob.WithSecond)
	if err := manager.LoadJobSchedulerConfigs(ctx); err != nil {
		t.Fatal(err)
	}
	job := reloadFixture{runs: make(chan struct{}, 1)}
	if err := manager.RegisterJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	manager.Start()
	defer manager.Stop()
	if _, err := p.Exec(ctx, `UPDATE scheduler_configs SET version=2,is_enable=true WHERE id=$1`, configID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-job.runs:
	case <-ctx.Done():
		t.Fatal("authoritative reload never scheduled valid newer config")
	}
}

type reloadFixture struct{ runs chan struct{} }

func (j reloadFixture) Name() string { return "reservation-cleaner-job" }
func (j reloadFixture) Run(context.Context) error {
	select {
	case j.runs <- struct{}{}:
	default:
	}
	return nil
}

type markerFault struct {
	repository.OutboundWorkRepository
	claim repository.OutboundClaim
}

func (f *markerFault) FinishOutbound(_ context.Context, c repository.OutboundClaim, _ bool, _ string, _ time.Time) (bool, error) {
	f.claim = c
	return false, errors.New("injected marker unavailable")
}

func TestSchedulerOutboundClaimsRetainVersionIdentityIntegration(t *testing.T) {
	p := relayPool(t)
	r := repository.NewOutboundEventRepository(p)
	ctx := context.Background()
	config := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression) VALUES($1,'relay-config','reservation-cleaner-job',10,1,'*/5 * * * * *')`, config); err != nil {
		t.Fatal(err)
	}
	svc := service_scheduler.NewSchedulerService(zap.NewNop(), repository.NewSchedulerConfigRepo(p), r, repository.NewUnitOfWork(p), nil)
	if err := svc.UpdateSchedulerConfigById(ctx, config, domain.SchedulerConfig{Version: 1, Timeout: 20 * time.Second, IntervalExpression: "*/10 * * * * *", IsEnabled: false}); err != nil {
		t.Fatal(err)
	}
	c, err := r.ClaimOutbound(ctx, 1, time.Second)
	if err != nil || len(c) != 1 {
		t.Fatal(c, err)
	}
	competing, err := repository.NewOutboundEventRepository(p).ClaimOutbound(ctx, 1, time.Second)
	if err != nil || len(competing) != 0 {
		t.Fatal("concurrent publisher stole claim", competing, err)
	}
	e, err := r.GetByConfigVersion(ctx, config, 2)
	if err != nil || e.Id != c[0].Event.Id {
		t.Fatal("config version identity drift", e, err)
	}
	if ok, err := r.FinishOutbound(ctx, c[0], true, "", time.Time{}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := r.FinishOutbound(ctx, c[0], true, "", time.Time{}); err != nil || !ok {
		t.Fatal("published marker ACK reread failed", ok, err)
	}
}
