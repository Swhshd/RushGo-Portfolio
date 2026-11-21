package admission

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func repairPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(os.Getenv("EVENT_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25434" || u.Path != "/ticketbox_event_test" {
		t.Fatal("BLOCKED: isolated Event PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "admission_repair_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
func repairRedis(t *testing.T) *redis.Client {
	t.Helper()
	raw := os.Getenv("REDIS_TEST_URL")
	if raw != "redis://127.0.0.1:16379/0" {
		t.Fatal("BLOCKED: dedicated local Redis database0 required")
	}
	opt, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	opt.ContextTimeoutEnabled = true
	c := redis.NewClient(opt)
	t.Cleanup(func() { c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return c
}
func repairSeats(t *testing.T, p *pgxpool.Pool, count int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	event, tier := uuid.New(), uuid.New()
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date,publication_status,sales_status)
		VALUES($1,'fixture','concert','fixture','fixture',NOW()+INTERVAL '1 day','published','selling')`, event); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity,lifecycle_accounting_baseline) VALUES($1,$2,'fixture',725,$3,$3,'verified')`, tier, event, count); err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, count)
	for i := range ids {
		ids[i] = uuid.New()
		if _, err := p.Exec(ctx, `INSERT INTO seats(id,event_id,ticket_tier_id) VALUES($1,$2,$3)`, ids[i], event, tier); err != nil {
			t.Fatal(err)
		}
	}
	return event, ids
}

type countedAuthority struct {
	SeatAuthorityReader
	queries atomic.Int32
	largest atomic.Int32
	fail    bool
	after   func()
}

func (a *countedAuthority) GetSeatAuthorities(ctx context.Context, ids []uuid.UUID) ([]domain.SeatAuthority, error) {
	a.queries.Add(1)
	for {
		old := a.largest.Load()
		if int32(len(ids)) <= old || a.largest.CompareAndSwap(old, int32(len(ids))) {
			break
		}
	}
	if a.fail {
		return nil, errors.New("authority unavailable")
	}
	rows, err := a.SeatAuthorityReader.GetSeatAuthorities(ctx, ids)
	if err == nil && a.after != nil {
		a.after()
	}
	return rows, err
}

type deleteBarrier struct {
	ReconciliationStore
	before func(CapturedAdmission)
}

type firstScanFailure struct {
	ReconciliationStore
	calls atomic.Int32
}

func (s *firstScanFailure) ScanAdmissions(ctx context.Context, cursor uint64, count int64) ([]CapturedAdmission, uint64, error) {
	if s.calls.Add(1) == 1 {
		return nil, cursor, errors.New("transient private owner failure")
	}
	return s.ReconciliationStore.ScanAdmissions(ctx, cursor, count)
}
func TestRepairWorkerReportsFailureAndRecoversIntegration(t *testing.T) {
	p := repairPool(t)
	c := repairRedis(t)
	event, ids := repairSeats(t, p, 1)
	putAdmissions(t, c, event, ids, uuid.New())
	core, logs := observer.New(zap.WarnLevel)
	worker := NewReconcilerWithLogger(&firstScanFailure{ReconciliationStore: NewRedisAdmission(c)}, repository.NewPostgresSeatRepository(p), zap.New(core))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Start(ctx) }()
	for c.Exists(context.Background(), Key(event, ids[0])).Val() != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("startup scan failure stopped repair")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 1 {
		t.Fatal("transient repair failure lacked diagnostic", logs.Len())
	}
	if strings.Contains(logs.All()[0].Message, "private") {
		t.Fatal("repair error exposed raw data")
	}
}

func (s deleteBarrier) CompareDeleteAdmission(ctx context.Context, c CapturedAdmission) (bool, error) {
	if s.before != nil {
		s.before(c)
	}
	return s.ReconciliationStore.CompareDeleteAdmission(ctx, c)
}
func putAdmissions(t *testing.T, c *redis.Client, event uuid.UUID, ids []uuid.UUID, owner uuid.UUID) {
	t.Helper()
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = Key(event, id)
		if err := c.Set(context.Background(), keys[i], owner.String(), 15*time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = c.Del(context.Background(), keys...).Err() })
}

func TestStaleAdmissionExactTokenRepairAndFairnessIntegration(t *testing.T) {
	p := repairPool(t)
	c := repairRedis(t)
	event, ids := repairSeats(t, p, 600)
	owner, b := uuid.New(), uuid.New()
	putAdmissions(t, c, event, ids, owner)
	// A valid legacy B hold is stale for captured A; contradictory legacy shapes
	// and matching healthy/expired owners must be preserved conservatively.
	for _, index := range []int{0, 1} {
		if _, err := p.Exec(context.Background(), `UPDATE seats SET status='reserved',reserved_by_booking_id=$2,reservation_expired_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, ids[index], owner); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Exec(context.Background(), `UPDATE seats SET status='reserved',reserved_by_booking_id=$2,reservation_expired_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, ids[2], b); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE seats SET status='reserved',reserved_by_booking_id=$2,booking_id=$3,reservation_expired_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, ids[3], owner, b); err != nil {
		t.Fatal(err)
	}
	reader := &countedAuthority{SeatAuthorityReader: repository.NewPostgresSeatRepository(p)}
	trace := &authoritySQLTrace{}
	tracedConfig := p.Config().Copy()
	tracedConfig.ConnConfig.Tracer = trace
	tracedPool, err := pgxpool.NewWithConfig(context.Background(), tracedConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer tracedPool.Close()
	reader.SeatAuthorityReader = repository.NewPostgresSeatRepository(tracedPool)
	r := NewReconciler(NewRedisAdmission(c), reader)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.Exists(context.Background(), Key(event, ids[599])).Val() == 0 && r.cursor == 0 {
			break
		}
	}
	for _, index := range []int{0, 1, 3} {
		if c.Get(context.Background(), Key(event, ids[index])).Val() != owner.String() {
			t.Fatalf("reconciliation deleted matching or unknown authority at index %d", index)
		}
	}
	for _, index := range []int{2, 4, 599} {
		if c.Exists(context.Background(), Key(event, ids[index])).Val() != 0 {
			t.Fatal("stale admission waited for TTL", index)
		}
	}
	for index, id := range ids {
		if index == 0 || index == 1 || index == 3 {
			continue
		}
		if c.Exists(context.Background(), Key(event, id)).Val() != 0 {
			t.Fatal("full stale inventory not repaired", index)
		}
	}
	if trace.queries.Load() != reader.queries.Load() {
		t.Fatal("method count differs from independently traced SQL", trace.queries.Load(), reader.queries.Load())
	}
	if reader.largest.Load() > 256 || reader.queries.Load() > 12 {
		t.Fatal("unbounded PG reconciliation", reader.largest.Load(), reader.queries.Load())
	}
	// Capture A, install B before compare-delete: B survives.
	putAdmissions(t, c, event, []uuid.UUID{ids[4]}, owner)
	barrier := deleteBarrier{ReconciliationStore: NewRedisAdmission(c), before: func(capture CapturedAdmission) {
		if capture.SeatID == ids[4] {
			if err := c.Set(context.Background(), capture.Key, b.String(), time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}}
	replacement := NewReconciler(barrier, reader)
	for i := 0; i < 10; i++ {
		if _, err := replacement.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if replacement.cursor == 0 {
			break
		}
	}
	if c.Get(context.Background(), Key(event, ids[4])).Val() != b.String() {
		t.Fatal("compare-delete removed replacement B")
	}
	// Same A token can still be deleted after its PG snapshot becomes stale.
	// Verify PG lifecycle authority survives that efficiency-only race.
	putAdmissions(t, c, event, []uuid.UUID{ids[5]}, owner)
	provisionalReader := &countedAuthority{SeatAuthorityReader: repository.NewPostgresSeatRepository(p)}
	provisionalReader.after = func() {
		provisionalReader.after = nil
		fingerprint, _ := domain.SeatSetFingerprint(event, []uuid.UUID{ids[5]})
		_, err := repository.NewPostgresSeatRepository(p).ApplyLifecycle(context.Background(), domain.SeatLifecycleRequest{BookingID: owner, EventID: event, Generation: 1, SeatIDs: []uuid.UUID{ids[5]}, Fingerprint: fingerprint, OperationID: uuid.New(), Action: "reserve", ReservationDeadline: time.Now().Add(time.Minute).Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
	}
	provisional := NewReconciler(NewRedisAdmission(c), provisionalReader)
	for i := 0; i < 10; i++ {
		if _, err := provisional.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if provisional.cursor == 0 {
			break
		}
	}
	var pgOwner uuid.UUID
	var revision int64
	var generation int
	var receipt string
	if err := p.QueryRow(context.Background(), `SELECT reserved_by_booking_id,seat_revision,lifecycle_generation FROM seats WHERE id=$1`, ids[5]).Scan(&pgOwner, &revision, &generation); err != nil || pgOwner != owner || generation != 1 {
		t.Fatal("same-token repair changed PG authority", pgOwner, err)
	}
	if err := p.QueryRow(context.Background(), `SELECT state FROM booking_seat_lifecycles WHERE booking_id=$1`, owner).Scan(&receipt); err != nil || receipt != "held" {
		t.Fatal("managed receipt changed", receipt, err)
	}
	fingerprint, _ := domain.SeatSetFingerprint(event, []uuid.UUID{ids[5]})
	if _, err := repository.NewPostgresSeatRepository(p).ApplyLifecycle(context.Background(), domain.SeatLifecycleRequest{BookingID: b, EventID: event, Generation: 1, SeatIDs: []uuid.UUID{ids[5]}, Fingerprint: fingerprint, OperationID: uuid.New(), Action: "reserve", ReservationDeadline: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("B stole managed PG owner after same-token deletion")
	}
	var afterRevision int64
	if err := p.QueryRow(context.Background(), `SELECT seat_revision FROM seats WHERE id=$1`, ids[5]).Scan(&afterRevision); err != nil || afterRevision != revision {
		t.Fatal("competing owner changed PG revision", afterRevision, revision, err)
	}
	if c.Exists(context.Background(), Key(event, ids[5])).Val() != 0 {
		t.Fatal("same-token provisional race was not exercised")
	}
	if _, err := NewRedisAdmission(c).Acquire(context.Background(), event, []uuid.UUID{ids[0], ids[6]}, b, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("mixed real conflict was not rejected all-or-none", err)
	}
	if c.Exists(context.Background(), Key(event, ids[6])).Val() != 0 {
		t.Fatal("mixed conflict created partial admission")
	}
}

type authoritySQLTrace struct{ queries atomic.Int32 }

func (t *authoritySQLTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM seats s LEFT JOIN booking_seat_lifecycles") {
		t.queries.Add(1)
	}
	return ctx
}
func (t *authoritySQLTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestRepairWorkerCadenceAndUnknownReceiptsIntegration(t *testing.T) {
	p := repairPool(t)
	c := repairRedis(t)
	event, ids := repairSeats(t, p, 600)
	owner, b := uuid.New(), uuid.New()
	putAdmissions(t, c, event, ids, owner)
	fingerprint, _ := domain.SeatSetFingerprint(event, []uuid.UUID{ids[0]})
	repo := repository.NewPostgresSeatRepository(p)
	if _, err := repo.ApplyLifecycle(context.Background(), domain.SeatLifecycleRequest{BookingID: b, EventID: event, Generation: 1, SeatIDs: []uuid.UUID{ids[0]}, Fingerprint: fingerprint, OperationID: uuid.New(), Action: "close", ReservationDeadline: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE seats SET status='reserved',reserved_by_booking_id=$2,reservation_expired_at=clock_timestamp()+interval '1 minute',lifecycle_generation=NULL WHERE id=$1`, ids[0], b); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	worker := NewReconciler(NewRedisAdmission(c), repo)
	done := make(chan error, 1)
	go func() { done <- worker.Start(ctx) }()
	for {
		left := c.Exists(context.Background(), makeKeys(event, ids[1:])...).Val()
		if left == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("600-key healthy-worker cycle exceeded5s", left)
		case <-time.After(25 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.Get(context.Background(), Key(event, ids[0])).Val() != owner.String() {
		t.Fatal("closed managed receipt misclassified as coherent legacy owner")
	}
}

func TestRedisBlackholeBudgetAndMultiworkerReconciliationIntegration(t *testing.T) {
	p := repairPool(t)
	c := repairRedis(t)
	event, ids := repairSeats(t, p, 4)
	owner := uuid.New()
	putAdmissions(t, c, event, ids, owner)
	reader := &countedAuthority{SeatAuthorityReader: repository.NewPostgresSeatRepository(p), fail: true}
	if _, err := NewReconciler(NewRedisAdmission(c), reader).RunOnce(context.Background()); err == nil {
		t.Fatal("PG failure silently accepted")
	}
	if c.Exists(context.Background(), makeKeys(event, ids)...).Val() != 4 {
		t.Fatal("PG failure removed cache owners")
	}
	reader.fail = false
	done := make(chan error, 2)
	for range 2 {
		r := NewReconciler(NewRedisAdmission(c), reader)
		go func() { _, err := r.RunOnce(context.Background()); done <- err }()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if c.Exists(context.Background(), makeKeys(event, ids)...).Val() != 0 {
		t.Fatal("two sweepers failed to drain stale batch")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var conn net.Conn
	accepted := make(chan struct{})
	go func() {
		conn, _ = listener.Accept()
		close(accepted)
		if conn != nil {
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	blackhole := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), ContextTimeoutEnabled: true, MaxRetries: -1})
	defer blackhole.Close()
	started := time.Now()
	_, err = NewRedisAdmission(blackhole).Acquire(context.Background(), event, ids, owner, 1)
	if err == nil || time.Since(started) > 500*time.Millisecond {
		t.Fatal("Redis blackhole escaped admission budget", time.Since(started), err)
	}
	if time.Since(started) > 180*time.Millisecond {
		t.Fatal("100ms admission deadline exceeded tolerance", time.Since(started))
	}
	// Connection refusal uses production-equivalent default retries.
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := refused.Addr().String()
	refused.Close()
	refusal := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer refusal.Close()
	started = time.Now()
	if _, err := NewRedisAdmission(refusal).Acquire(context.Background(), event, ids, owner, 1); err == nil || time.Since(started) > 180*time.Millisecond {
		t.Fatal("Redis refusal exceeded100ms budget", time.Since(started), err)
	}
	<-accepted
	if conn != nil {
		conn.Close()
	}
}
