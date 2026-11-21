package repository

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/topics"
)

type bookingLifecyclePersistence interface {
	GetLifecycle(context.Context, domain.LifecycleRef) (*domain.LifecycleSnapshot, error)
	DecideLifecycle(context.Context, domain.LifecycleDecision, *OutboxEvent) (*domain.LifecycleSnapshot, bool, error)
}
type legacyBookingDecisionAdapter struct{ repo *PostgresBookingRepository }

func (a legacyBookingDecisionAdapter) GetLifecycle(ctx context.Context, ref domain.LifecycleRef) (*domain.LifecycleSnapshot, error) {
	b, err := a.repo.GetByID(ctx, ref.BookingID)
	return &domain.LifecycleSnapshot{Booking: b, Generation: 1, Phase: "OPEN"}, err
}
func (a legacyBookingDecisionAdapter) DecideLifecycle(ctx context.Context, d domain.LifecycleDecision, event *OutboxEvent) (*domain.LifecycleSnapshot, bool, error) {
	status := domain.StatusCancelled
	switch d.Kind {
	case "claim_confirmation":
		status = domain.StatusPending
	case "confirm":
		status = domain.StatusConfirmed
	case "expire":
		status = domain.StatusExpired
	case "fail", "abort_confirmation":
		status = domain.StatusFailed
	}
	var changed bool
	var err error
	if d.Kind == "confirm" && event != nil {
		changed, err = a.repo.ConfirmBookingWithOutbox(ctx, d.Ref.BookingID, *event)
	} else {
		err = a.repo.UpdateStatus(ctx, d.Ref.BookingID, status)
		changed = err == nil
	}
	v, loadErr := a.GetLifecycle(ctx, d.Ref)
	if err == nil {
		err = loadErr
	}
	return v, changed, err
}
func lifecycleCapability(repo *PostgresBookingRepository) bookingLifecyclePersistence {
	if actual, ok := any(repo).(bookingLifecyclePersistence); ok {
		return actual
	}
	return legacyBookingDecisionAdapter{repo}
}

func isIsolatedBookingTestDatabaseURL(dsn string) bool {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != "postgres" || parsed.Path != "/ticketbox_booking_test" {
		return false
	}
	for _, key := range []string{"host", "hostaddr", "port", "database", "dbname", "service", "servicefile"} {
		if _, ok := parsed.Query()[key]; ok {
			return false
		}
	}
	if parsed.Hostname() == "127.0.0.1" {
		return parsed.Port() == "25435"
	}
	address := net.ParseIP(parsed.Hostname())
	return parsed.Port() == "5432" && address != nil && address.To4() != nil && address.IsPrivate()
}

func bookingLifecycleFixture(t *testing.T, version ...string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if !isIsolatedBookingTestDatabaseURL(dsn) {
		t.Fatal("BLOCKED: isolated Booking PG required")
	}
	requireVerifiedTicketE2EPostgres(t, dsn)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse isolated Booking DSN")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("connect isolated Booking PG")
	}
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "lifecycle_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	migrationArgs := []string{"-path", "../../migrations", "-database", parsed.String(), "up"}
	if len(version) == 1 {
		migrationArgs = append(migrationArgs[:len(migrationArgs)-1], "goto", version[0])
	}
	if output, err := exec.Command("migrate", migrationArgs...).CombinedOutput(); err != nil {
		t.Fatalf("actual Booking migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal("parse isolated Booking schema")
	}
	config.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("connect isolated Booking schema")
	}
	t.Cleanup(pool.Close)
	return pool
}
func seedLifecycleBooking(t *testing.T, p *pgxpool.Pool, deadline time.Time) domain.LifecycleRef {
	t.Helper()
	ref := domain.LifecycleRef{BookingID: uuid.New(), Generation: 1}
	ctx := context.Background()
	if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version) VALUES($1,$2,$3,'PENDING',725,1)`, ref.BookingID, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	var hasManaged bool
	if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='bookings' AND column_name='managed')`).Scan(&hasManaged); err != nil {
		t.Fatal(err)
	}
	if hasManaged {
		if _, err := p.Exec(ctx, `UPDATE bookings SET managed=true,lifecycle_generation=1,lifecycle_phase='OPEN',currency='usd',booking_mode='pessimistic',reservation_deadline=$2 WHERE id=$1`, ref.BookingID, deadline); err != nil {
			t.Fatal(err)
		}
	}
	return ref
}
func fixtureDecision(ref domain.LifecycleRef, kind string, version int32) domain.LifecycleDecision {
	return domain.LifecycleDecision{Ref: ref, Kind: kind, ExpectedVersion: version, Reason: "fixture",
		DecisionID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+ref.BookingID.String()+":g1:"+kind))}
}
func lifecycleFixtureEvent(ref domain.LifecycleRef, kind string) *OutboxEvent {
	typeName := topics.TypeBookingCancelled
	if kind == "confirm" {
		typeName = topics.TypeBookingConfirmed
	}
	if kind == "fail" || kind == "expire" || kind == "abort_confirmation" {
		typeName = topics.TypeBookingFailed
	}
	identity := "ticketbox:booking-event:" + ref.BookingID.String() + ":g1:" + kind
	if kind == "confirm" {
		identity = "ticketbox:booking_confirmation:" + ref.BookingID.String()
	}
	return &OutboxEvent{ID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)),
		EventType: typeName, EventKey: ref.BookingID.String(), Payload: []byte(`{"booking_id":"` + ref.BookingID.String() + `"}`), CreatedAt: time.Now()}
}

func TestBookingDecisionRaceAndGenericGuardsIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	repo := NewPostgresBookingRepository(p)
	life := lifecycleCapability(repo)
	ctx := context.Background()
	t.Run("claim_cancel_expire_same_row", func(t *testing.T) {
		ref := seedLifecycleBooking(t, p, time.Now().Add(-time.Minute))
		blocker, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		if _, err = blocker.Exec(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, ref.BookingID); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan bool, 3)
		errs := make(chan error, 3)
		var wg sync.WaitGroup
		for _, kind := range []string{"claim_confirmation", "cancel", "expire"} {
			wg.Add(1)
			go func(kind string) {
				defer wg.Done()
				<-start
				var event *OutboxEvent
				if kind != "claim_confirmation" {
					event = lifecycleFixtureEvent(ref, kind)
				}
				_, changed, err := life.DecideLifecycle(ctx, fixtureDecision(ref, kind, 1), event)
				results <- changed
				errs <- err
			}(kind)
		}
		close(start)
		// The held row provides the deterministic decision barrier; observe all three waiters.
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			var waiters int
			if err := p.QueryRow(waitCtx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%bookings%'`).Scan(&waiters); err != nil {
				t.Fatal(err)
			}
			if waiters >= 3 {
				break
			}
			if waitCtx.Err() != nil {
				t.Fatal("decision contenders did not reach held-row barrier")
			}
		}
		if err = blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
		winners := 0
		for changed := range results {
			if changed {
				winners++
			}
		}
		if winners != 1 {
			t.Errorf("same-row race had %d decision winners, want 1", winners)
		}
		v, err := life.GetLifecycle(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if v.Booking.Version != 2 {
			t.Errorf("winner version=%d, want 2", v.Booking.Version)
		}
	})
	t.Run("repeat_cancel_and_generic_bypass", func(t *testing.T) {
		ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
		d := fixtureDecision(ref, "cancel", 1)
		for i := 0; i < 5; i++ {
			v, changed, err := life.DecideLifecycle(ctx, d, lifecycleFixtureEvent(ref, "cancel"))
			if err != nil {
				t.Fatal(err)
			}
			if changed != (i == 0) || v.Booking.Status != domain.StatusCancelled {
				t.Errorf("cancel replay %d changed=%v status=%s", i, changed, v.Booking.Status)
			}
		}
		if err := repo.UpdateStatus(ctx, ref.BookingID, domain.StatusPending); err == nil {
			t.Error("managed single setter silently accepted illegal terminal mutation")
		}
		if changed, err := repo.CompareAndSetStatus(ctx, ref.BookingID, domain.StatusCancelled, domain.StatusConfirmed); err == nil || changed {
			t.Error("managed generic CAS did not reject bypass")
		}
		if err := repo.BatchUpdateStatus(ctx, []uuid.UUID{ref.BookingID}, domain.StatusFailed); err == nil {
			t.Error("managed batch setter silently accepted illegal terminal mutation")
		}
		_, _ = repo.ConfirmBookingWithOutbox(ctx, ref.BookingID, *lifecycleFixtureEvent(ref, "confirm"))
		v, err := life.GetLifecycle(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if v.Booking.Status != domain.StatusCancelled || v.Booking.Version != 2 {
			t.Errorf("generic setter overwrote terminal cancellation: %s/version%d", v.Booking.Status, v.Booking.Version)
		}
		var intents int
		if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_key=$1`, ref.BookingID.String()).Scan(&intents); err != nil {
			t.Fatal(err)
		}
		if intents != 1 {
			t.Errorf("repeated cancel produced %d atomic outbox intents, want 1", intents)
		}
	})
	t.Run("confirmation_atomic_and_late_cancel_rejected", func(t *testing.T) {
		ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
		claim := fixtureDecision(ref, "claim_confirmation", 1)
		v, changed, err := life.DecideLifecycle(ctx, claim, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !changed || v.Phase != "CONFIRMING" {
			t.Error("confirmation claim did not enter CONFIRMING")
		}
		d := fixtureDecision(ref, "confirm", v.Booking.Version)
		d.ConfirmationDecisionID = &claim.DecisionID
		event := lifecycleFixtureEvent(ref, "confirm")
		if _, err := p.Exec(ctx, `INSERT INTO outbox(id,event_type,event_key,payload) VALUES($1,$2,$3,'{}')`, event.ID, event.EventType, event.EventKey); err != nil {
			t.Fatal(err)
		}
		_, changed, err = life.DecideLifecycle(ctx, d, event)
		if err == nil || changed {
			t.Error("failed outbox insert committed confirmation")
		}
		if _, err := p.Exec(ctx, `DELETE FROM outbox WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		v, changed, err = life.DecideLifecycle(ctx, d, event)
		if err != nil {
			t.Fatal(err)
		}
		if !changed || v.Booking.Status != domain.StatusConfirmed {
			t.Error("confirmation did not commit")
		}
		v, changed, err = life.DecideLifecycle(ctx, fixtureDecision(ref, "cancel", v.Booking.Version), lifecycleFixtureEvent(ref, "cancel"))
		if err != nil {
			t.Fatal(err)
		}
		if changed || v.Booking.Status != domain.StatusConfirmed {
			t.Error("post-confirm cancellation overwrote sale")
		}
	})
	t.Run("abort_races_confirm", func(t *testing.T) {
		ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
		claim := fixtureDecision(ref, "claim_confirmation", 1)
		v, _, err := life.DecideLifecycle(ctx, claim, nil)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan bool, 2)
		var wg sync.WaitGroup
		for _, kind := range []string{"confirm", "abort_confirmation"} {
			wg.Add(1)
			go func(kind string) {
				defer wg.Done()
				<-start
				d := fixtureDecision(ref, kind, v.Booking.Version)
				d.ConfirmationDecisionID = &claim.DecisionID
				_, changed, err := life.DecideLifecycle(ctx, d, lifecycleFixtureEvent(ref, kind))
				if err != nil {
					t.Error(err)
				}
				results <- changed
			}(kind)
		}
		close(start)
		wg.Wait()
		close(results)
		winners := 0
		for changed := range results {
			if changed {
				winners++
			}
		}
		if winners != 1 {
			t.Errorf("abort/confirm had %d winners, want 1", winners)
		}
	})
}

func TestLifecycleRejectsMissingOutboxAndConfirmationEvidenceIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	life := lifecycleCapability(NewPostgresBookingRepository(p))
	ctx := context.Background()
	for _, kind := range []string{"cancel", "expire", "fail"} {
		ref := seedLifecycleBooking(t, p, time.Now().Add(-time.Hour))
		if _, _, err := life.DecideLifecycle(ctx, fixtureDecision(ref, kind, 1), nil); err == nil {
			t.Errorf("%s accepted without required atomic intent", kind)
		}
	}
	ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
	claim := fixtureDecision(ref, "claim_confirmation", 1)
	v, _, err := life.DecideLifecycle(ctx, claim, nil)
	if err != nil {
		t.Fatal(err)
	}
	d := fixtureDecision(ref, "confirm", v.Booking.Version)
	if _, changed, err := life.DecideLifecycle(ctx, d, lifecycleFixtureEvent(ref, "confirm")); err == nil && changed {
		t.Error("confirmation without original claim evidence accepted")
	}
}

func TestBookingDecisionCommitACKLossReloadsOriginalIntentIntegration(t *testing.T) {
	p := bookingLifecycleFixture(t)
	repo := NewPostgresBookingRepository(p)
	ctx := context.Background()
	ref := seedLifecycleBooking(t, p, time.Now().Add(time.Hour))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	committed := make(chan bool, 1)
	go func() {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		server, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(p.Config().ConnConfig.Port))))
		if err != nil {
			return
		}
		defer server.Close()
		go func() { io.Copy(server, client) }()
		for {
			var header [5]byte
			if _, err := io.ReadFull(server, header[:]); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint32(header[1:])) - 4
			if length < 0 || length > 16*1024*1024 {
				return
			}
			body := make([]byte, length)
			if _, err := io.ReadFull(server, body); err != nil {
				return
			}
			if header[0] == 'C' && string(body) == "COMMIT\x00" {
				committed <- true
				return
			}
			if _, err := client.Write(append(header[:], body...)); err != nil {
				return
			}
		}
	}()
	config := p.Config().Copy()
	config.ConnConfig.Host = "127.0.0.1"
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	var proxyPort uint16
	for _, digit := range port {
		proxyPort = proxyPort*10 + uint16(digit-'0')
	}
	config.ConnConfig.Port = proxyPort
	config.MaxConns = 1
	proxy, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("connect Booking COMMIT proxy")
	}
	defer proxy.Close()
	d := fixtureDecision(ref, "cancel", 1)
	event := lifecycleFixtureEvent(ref, "cancel")
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, _, err := NewPostgresBookingRepository(proxy).DecideLifecycle(bounded, d, event); err == nil {
		t.Fatal("Booking COMMIT acknowledgement not lost")
	}
	select {
	case <-committed:
	case <-bounded.Done():
		t.Fatal("Booking COMMIT barrier absent")
	}
	v, changed, err := repo.DecideLifecycle(ctx, d, event)
	if err != nil {
		t.Fatal(err)
	}
	if changed || v.Booking.Status != domain.StatusCancelled || v.Booking.Version != 2 {
		t.Fatal("COMMIT uncertainty replay changed terminal decision")
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_key=$1`, ref.BookingID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("COMMIT uncertainty duplicated terminal intent")
	}
}
