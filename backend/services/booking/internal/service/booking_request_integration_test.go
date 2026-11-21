package service

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func isIsolatedBookingTestDatabaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgres" || u.Path != "/ticketbox_booking_test" {
		return false
	}
	for _, key := range []string{"host", "hostaddr", "port", "database", "dbname", "service", "servicefile"} {
		if _, ok := u.Query()[key]; ok {
			return false
		}
	}
	if u.Hostname() == "127.0.0.1" {
		return u.Port() == "25435"
	}
	address := net.ParseIP(u.Hostname())
	return u.Port() == "5432" && address != nil && address.To4() != nil && address.IsPrivate()
}

func bookingRequestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if !isIsolatedBookingTestDatabaseURL(dsn) {
		t.Fatal("BLOCKED: isolated Booking PG required")
	}
	requireVerifiedTicketE2EPostgres(t, dsn)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse isolated Booking DSN")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "booking_request_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.Command("migrate", "-path", "../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

type keyedBookingCreator interface {
	CreateBooking(context.Context, uuid.UUID, uuid.UUID, []domain.BookingItem, string, string) (*CreateBookingResponse, error)
}
type oldBookingCreator interface {
	CreateBooking(context.Context, uuid.UUID, uuid.UUID, []domain.BookingItem, string) (*CreateBookingResponse, error)
}

func createKeyed(t *testing.T, s *BookingService, user, event uuid.UUID, items []domain.BookingItem, key string) (*CreateBookingResponse, error) {
	t.Helper()
	if c, ok := any(s).(keyedBookingCreator); ok {
		return c.CreateBooking(context.Background(), user, event, items, "pessimistic", key)
	}
	return any(s).(oldBookingCreator).CreateBooking(context.Background(), user, event, items, "pessimistic")
}

type requestEvent struct {
	eventv1.EventServiceClient
	mu                sync.Mutex
	event, tier, seat uuid.UUID
	unavailable       bool
	price             int64
	accountingReady   *bool
}

func (e *requestEvent) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	return &eventv1.EventDetail{Id: e.event.String()}, nil
}

func (e *requestEvent) CheckEventSaleEligibility(context.Context, *eventv1.CheckEventSaleEligibilityRequest, ...grpc.CallOption) (*eventv1.CheckEventSaleEligibilityResponse, error) {
	return &eventv1.CheckEventSaleEligibilityResponse{State: eventv1.EventSaleEligibilityState_EVENT_SALE_ELIGIBILITY_STATE_ELIGIBLE}, nil
}
func (e *requestEvent) GetSeatsBySeatIds(context.Context, *eventv1.GetSeatsBySeatIdsRequest, ...grpc.CallOption) (*eventv1.GetSeatsResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := "available"
	if e.unavailable {
		state = "reserved"
	}
	return &eventv1.GetSeatsResponse{Seats: []*eventv1.Seat{{Id: e.seat.String(), EventId: e.event.String(), TicketTierId: e.tier.String(), Status: state}}}, nil
}
func (e *requestEvent) GetTicketAvailability(context.Context, *eventv1.GetTicketAvailabilityRequest, ...grpc.CallOption) (*eventv1.TicketAvailabilityResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ready := true
	if e.accountingReady != nil {
		ready = *e.accountingReady
	}
	return &eventv1.TicketAvailabilityResponse{TierId: e.tier.String(), PriceCents: e.price, AvailableQuantity: 1, AccountingReady: ready}, nil
}

type requestSaga struct {
	sagav1.SagaOrchestratorServiceClient
	mu               sync.Mutex
	e                *requestEvent
	err              error
	entered, release chan struct{}
	once             sync.Once
	ids              map[string]string
	db               *pgxpool.Pool
	calls            int
	malformed        bool
	ackLost          bool
}

func (s *requestSaga) StartOrderSaga(ctx context.Context, req *sagav1.StartOrderSagaRequest, _ ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	if s.db != nil {
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE NOWAIT`, req.BookingId); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if s.ids == nil {
		s.ids = make(map[string]string)
	}
	id := s.ids[req.BookingId]
	if id == "" {
		id = uuid.NewString()
		s.ids[req.BookingId] = id
	}
	if s.malformed {
		id = "invalid-saga-id"
	}
	if s.ackLost {
		s.ackLost = false
		return nil, errors.New("committed Saga ACK lost")
	}
	return &sagav1.OrderSagaResponse{SagaId: id, Status: "success", PaymentIntentClientSecret: "fixture_original_secret"}, nil
}
func (s *requestSaga) SettleOrderSaga(_ context.Context, req *sagav1.SettleOrderSagaRequest, _ ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = make(map[string]string)
	}
	id := s.ids[req.BookingId]
	if id == "" {
		id = uuid.NewString()
		s.ids[req.BookingId] = id
	}
	return &sagav1.OrderSagaResponse{SagaId: id, Status: "ROLLING_BACK"}, nil
}

func TestKeyedReplayCancelExpiryDispatchRaceIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	saga := &requestSaga{e: e}
	s := NewBookingService(r, e, saga, zap.NewNop())
	user := uuid.New()
	items := []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}
	first, err := createKeyed(t, s, user, e.event, items, "lost-response")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.unavailable = true
	e.price = 900
	e.mu.Unlock()
	again, err := createKeyed(t, s, user, e.event, items, "lost-response")
	if err != nil || again.Booking.ID != first.Booking.ID || again.Booking.TotalAmountCents != 725 || again.PaymentClientSecret != first.PaymentClientSecret {
		t.Fatalf("lost response could not recover original Booking before availability check: err=%v", err)
	}
	var count int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM bookings`).Scan(&count); err != nil || count != 1 {
		t.Fatal("request duplicated durable Booking", count, err)
	}
	changed := []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}}
	if _, err := createKeyed(t, s, user, e.event, changed, "lost-response"); err == nil {
		t.Fatal("same key with conflicting request accepted")
	}
	saga.mu.Lock()
	saga.err = errors.New("Saga transport unavailable")
	saga.mu.Unlock()
	e.mu.Lock()
	e.unavailable = false
	e.mu.Unlock()
	unknown, err := createKeyed(t, s, user, e.event, items, "unknown-start")
	if err != nil || unknown.Booking.Status != domain.StatusPending {
		t.Fatal("ambiguous dispatch destroyed committed Booking", err)
	}
	snapshot, err := r.GetLifecycle(context.Background(), domain.LifecycleRef{BookingID: unknown.Booking.ID, Generation: 1})
	if err != nil || !snapshot.Booking.Managed || snapshot.Phase != "OPEN" || snapshot.DispatchState != "pending" || snapshot.ReservationDeadline == nil {
		t.Fatal("commit did not retain managed start responsibility", err)
	}
	deadline := *snapshot.ReservationDeadline
	if deadline.Before(time.Now().Add(14*time.Minute)) || deadline.After(time.Now().Add(16*time.Minute)) {
		t.Fatal("reservation deadline invalid")
	}
	t.Run("concurrent_same_key_and_user_isolation", func(t *testing.T) {
		saga.mu.Lock()
		saga.err = nil
		saga.mu.Unlock()
		const callers = 16
		results := make(chan *CreateBookingResponse, callers)
		failures := make(chan error, callers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got, err := createKeyed(t, s, user, e.event, items, "concurrent-key")
				results <- got
				failures <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		var id uuid.UUID
		for result := range results {
			if id == uuid.Nil {
				id = result.Booking.ID
			}
			if result.Booking.ID != id {
				t.Fatal("concurrent key allocated multiple Bookings")
			}
		}
		var bookings, itemCount int
		if err := p.QueryRow(context.Background(), `SELECT count(*),(SELECT count(*) FROM booking_items WHERE booking_id=$1) FROM bookings WHERE user_id=$2 AND request_id='concurrent-key'`, id, user).Scan(&bookings, &itemCount); err != nil || bookings != 1 || itemCount != 1 {
			t.Fatal("request/item duplication", bookings, itemCount, err)
		}
		other, err := createKeyed(t, s, uuid.New(), e.event, items, "concurrent-key")
		if err != nil || other.Booking.ID == id {
			t.Fatal("key was not user scoped", err)
		}
	})
	for _, kind := range []string{"cancel", "expire"} {
		t.Run(kind+"_during_start", func(t *testing.T) {
			barrier := &requestSaga{e: e, db: p, entered: make(chan struct{}), release: make(chan struct{})}
			svc := NewBookingService(r, e, barrier, zap.NewNop())
			done := make(chan *CreateBookingResponse, 1)
			errs := make(chan error, 1)
			go func() { got, err := createKeyed(t, svc, user, e.event, items, kind+"-race"); done <- got; errs <- err }()
			select {
			case <-barrier.entered:
			case <-time.After(time.Second):
				t.Fatal("start barrier not reached")
			}
			var id uuid.UUID
			if err := p.QueryRow(context.Background(), `SELECT id FROM bookings WHERE user_id=$1 AND request_id=$2`, user, kind+"-race").Scan(&id); err != nil {
				t.Fatal(err)
			}
			if kind == "expire" {
				if _, err := p.Exec(context.Background(), `UPDATE bookings SET reservation_deadline=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := svc.DecideLifecycle(context.Background(), domain.LifecycleDecision{Ref: domain.LifecycleRef{BookingID: id, Generation: 1}, DecisionID: domain.BookingDecisionID(id, kind), Kind: kind, ExpectedVersion: 1}); err != nil {
				t.Fatal(err)
			}
			close(barrier.release)
			result := <-done
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			if result.CreationState != "terminal" || result.PaymentClientSecret != "" {
				t.Fatal("late Start revived terminal payment", result)
			}
			v, err := r.GetLifecycle(context.Background(), domain.LifecycleRef{BookingID: id, Generation: 1})
			if err != nil || v.DispatchState != "pending" {
				t.Fatal("old ACK erased terminal settle intent", err)
			}
			claims, err := r.ClaimDispatch(context.Background(), 4, 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, c := range claims {
				if c.Ref.BookingID == id {
					found = true
					if c.Kind != "settle" {
						t.Fatal("terminal work still starts")
					}
				}
			}
			if !found {
				t.Fatal("terminal settlement not discoverable")
			}
		})
	}
}

func TestBookingCommitBeforeDispatchRestartIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	saga := &requestSaga{e: e, db: p, ackLost: true}
	s := NewBookingService(r, e, saga, zap.NewNop())
	user := uuid.New()
	items := []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}
	first, err := createKeyed(t, s, user, e.event, items, "restart")
	if err != nil || first.CreationState != "starting" {
		t.Fatal("ambiguous start lost durable request", err)
	}
	v, err := r.GetLifecycle(context.Background(), domain.LifecycleRef{BookingID: first.Booking.ID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	deadline := *v.ReservationDeadline
	saga.mu.Lock()
	originalSaga := saga.ids[first.Booking.ID.String()]
	saga.mu.Unlock()
	if originalSaga == "" {
		t.Fatal("test did not commit Saga before losing ACK")
	}
	saga.mu.Lock()
	saga.err = nil
	saga.mu.Unlock()
	if _, err := p.Exec(context.Background(), `UPDATE bookings SET dispatch_due_at=clock_timestamp() WHERE id=$1`, first.Booking.ID); err != nil {
		t.Fatal(err)
	}
	restarted := NewBookingService(r, e, saga, zap.NewNop())
	worker, err := NewDispatchWorker(r, restarted, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.RunOnce(context.Background(), 1); err != nil || n != 1 {
		t.Fatal("restart did not dispatch original Booking", n, err)
	}
	e.mu.Lock()
	e.unavailable = true
	e.price = 900
	e.mu.Unlock()
	again, err := createKeyed(t, restarted, user, e.event, items, "restart")
	if err != nil || again.Booking.ID != first.Booking.ID || again.Booking.TotalAmountCents != 725 || again.PaymentClientSecret != "fixture_original_secret" {
		t.Fatal("restart lost original session/price", err)
	}
	v, err = r.GetLifecycle(context.Background(), domain.LifecycleRef{BookingID: first.Booking.ID, Generation: 1})
	if err != nil || !deadline.Equal(*v.ReservationDeadline) {
		t.Fatal("replay extended reservation", err)
	}
	var storedSaga string
	if err := p.QueryRow(context.Background(), `SELECT saga_id::text FROM bookings WHERE id=$1`, first.Booking.ID).Scan(&storedSaga); err != nil || storedSaga != originalSaga {
		t.Fatal("lost ACK recreated Saga identity", storedSaga, originalSaga, err)
	}
}

type dispatchCapacitySaga struct {
	*requestSaga
	active, maximum atomic.Int64
	entered         chan struct{}
	barrier         <-chan struct{}
}

func (s *dispatchCapacitySaga) StartOrderSaga(ctx context.Context, req *sagav1.StartOrderSagaRequest, opts ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		old := s.maximum.Load()
		if n <= old || s.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	s.entered <- struct{}{}
	select {
	case <-s.barrier:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.requestSaga.StartOrderSaga(ctx, req, opts...)
}
func TestBookingDispatchBudgetIncludesRequestRecoveryIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	base := &requestSaga{e: e}
	s := NewBookingService(r, e, base, zap.NewNop())
	user := uuid.New()
	items := []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}
	if _, err := createKeyed(t, s, user, e.event, items, "accepted"); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	saga := &dispatchCapacitySaga{requestSaga: base, entered: make(chan struct{}, 32), barrier: release}
	s.sagaClient = saga
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.CreateBooking(ctx, user, e.event, items, "pessimistic", "accepted") }()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-saga.entered:
		case <-time.After(time.Second):
			t.Fatal("capacity barrier not reached")
		}
	}
	done := make(chan *CreateBookingResponse, 1)
	errs := make(chan error, 1)
	go func() {
		got, err := s.CreateBooking(ctx, user, e.event, items, "pessimistic", "new-at-capacity")
		done <- got
		errs <- err
	}()
	select {
	case <-saga.entered:
		cancel()
		wg.Wait()
		t.Fatal("request dispatch exceeded shared concurrency budget")
	case got := <-done:
		if err := <-errs; err != nil || got.CreationState != "starting" {
			t.Fatal("capacity did not retain starting response", err)
		}
	case <-time.After(time.Second):
		cancel()
		wg.Wait()
		t.Fatal("full budget blocked request instead of returning starting")
	}
	worker, err := NewDispatchWorker(r, s, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.RunOnce(ctx, 1); err != nil || n != 0 {
		t.Fatal("background worker bypassed shared budget", n, err)
	}
	if saga.maximum.Load() > 4 {
		t.Fatal("aggregate dispatch concurrency exceeded 4")
	}
	cancel()
	wg.Wait()
}

func TestBookingUnknownSagaResponseCannotExposeSecretIntegration(t *testing.T) {
	p := bookingRequestPool(t)
	r := repository.NewPostgresBookingRepository(p)
	e := &requestEvent{event: uuid.New(), tier: uuid.New(), seat: uuid.New(), price: 725}
	saga := &requestSaga{e: e, malformed: true}
	s := NewBookingService(r, e, saga, zap.NewNop())
	got, err := createKeyed(t, s, uuid.New(), e.event, []domain.BookingItem{{TicketTierID: e.tier, Quantity: 1, SeatIDs: []uuid.UUID{e.seat}}}, "malformed-response")
	if err != nil || got.CreationState != "starting" || got.PaymentClientSecret != "" {
		t.Fatal("unaccepted/malformed Saga response exposed secret", err, got)
	}
}

func TestBookingRequiredKeyRejectsMissingBeforeEvent(t *testing.T) {
	s := NewBookingService(nil, nil, nil, zap.NewNop())
	s.RequireRequestID = true
	_, err := s.CreateBooking(context.Background(), uuid.New(), uuid.New(), []domain.BookingItem{{TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}}, "", "")
	if !errors.Is(err, repository.ErrInvalidBookingRequest) {
		t.Fatal("missing key accepted", err)
	}
}
