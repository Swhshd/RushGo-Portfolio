package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type retryBookingReader struct {
	bookingv1.BookingServiceClient
	view *bookingv1.LifecycleDisposition
}

func (b *retryBookingReader) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if b.view != nil && b.view.Booking != nil && len(b.view.Booking.Items) == 0 {
		b.view.Booking.Items = []*bookingv1.BookingItem{{TicketTierId: uuid.NewString(), Quantity: 1}}
	}
	return b.view, nil
}

type readyAccounting struct{}

func (readyAccounting) Check(context.Context, []uuid.UUID) error { return nil }

func withReadyAccounting(s *PaymentService) *PaymentService {
	s.SetAccountingReadiness(readyAccounting{})
	return s
}

func retryPaymentFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("PAYMENT_TEST_DATABASE_URL"))
	if err != nil || parsed.Port() != "25439" || parsed.Path != "/ticketbox_payment_test" {
		t.Fatal("BLOCKED: isolated Payment PG required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "retry_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := parsed.Query()
	q.Set("search_path", schema+",public")
	parsed.RawQuery = q.Encode()
	if output, err := exec.Command("migrate", "-path", "../../migrations", "-database", parsed.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual Payment migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	p, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestCreateAcceptedThenLocalDeathRecoversOriginalIntent(t *testing.T) {
	p := retryPaymentFixture(t)
	ctx := context.Background()
	booking, user := uuid.New(), uuid.New()
	var mu sync.Mutex
	keys := []string{}
	bodies := []string{}
	byKey := map[string]string{}
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "unexpected request", 500)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		key := r.Header.Get("Idempotency-Key")
		body := r.PostForm.Encode()
		mu.Lock()
		keys = append(keys, key)
		bodies = append(bodies, body)
		id := byKey[key]
		if id == "" {
			mutations++
			id = fmt.Sprintf("pi_fixture_%d", mutations)
			byKey[key] = id
		}
		first := len(keys) == 1
		mu.Unlock()
		if first {
			// The provider has accepted and remembers the intent before the
			// response connection is lost; no local correlation can be written.
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "payment_intent", "amount": 725, "currency": "usd", "livemode": false, "status": "requires_payment_method", "client_secret": "fixture_transient_secret", "metadata": map[string]string{"ticketbox_payment_id": r.PostForm.Get("metadata[ticketbox_payment_id]"), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}})
	}))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), MaxNetworkRetries: stripego.Int64(0), HTTPClient: server.Client()})
	old := stripego.GetBackend(stripego.APIBackend)
	stripego.SetBackend(stripego.APIBackend, backend)
	defer stripego.SetBackend(stripego.APIBackend, old)
	stripego.Key = "sk_test_fixture"
	client := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, client)
	s := withReadyAccounting(NewPaymentService(repository.NewPostgresPaymentRepository(p), zap.NewNop(), provider))
	reader := &retryBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: "PENDING"}, Generation: 1, Phase: "OPEN", Currency: "usd", ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour))}}
	if managed, ok := any(s).(interface {
		SetFinancialAuthority(bookingv1.BookingServiceClient, string, bool)
	}); ok {
		managed.SetFinancialAuthority(reader, "acct_fixture", false)
	}
	request := func() *domain.CreatePaymentRequest {
		return &domain.CreatePaymentRequest{Payment: domain.Payment{ID: uuid.New(), UserId: user, BookingId: &booking, Price: 725, Currency: "usd", Status: domain.PaymentPending}, UserEmail: "original@example.test"}
	}
	_, _ = s.CreatePayment(ctx, request())
	// Simulate a short worker lease ending after the interrupted first attempt.
	if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW(),lease_until=NOW()-INTERVAL '1 second' WHERE managed`); err != nil {
		t.Fatal(err)
	}
	res, err := s.CreatePayment(ctx, request())
	if err != nil {
		t.Fatalf("same identity replay: %v", err)
	}
	var rows int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM payments WHERE booking_id=$1`, booking).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if rows != 1 || mutations != 1 {
		t.Errorf("retry forked lifecycle: local rows=%d provider intents=%d", rows, mutations)
	}
	if len(keys) != 2 || keys[0] != keys[1] || !strings.HasPrefix(keys[0], "ticketbox:payment:") {
		t.Errorf("unstable create identity: %v", keys)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Error("retry changed frozen provider parameters")
	}
	if res == nil || res.PaymentItentId != "pi_fixture_1" {
		t.Errorf("did not recover original intent: %+v", res)
	}
	if len(bodies) > 0 {
		values, _ := url.ParseQuery(bodies[0])
		if values.Get("metadata[ticketbox_payment_id]") == "" || values.Get("metadata[ticketbox_booking_id]") != booking.String() || values.Get("metadata[ticketbox_generation]") != "1" {
			t.Error("durable correlation metadata missing")
		}
	}
	mu.Unlock()
	// A current server-owned snapshot change must conflict before another POST.
	reader.view.Booking.TotalAmountCents = 800
	changed := request()
	changed.Price = 800
	if _, err := s.CreatePayment(ctx, changed); err == nil {
		t.Error("changed authoritative amount accepted")
	}
	mu.Lock()
	if len(keys) != 2 {
		t.Error("conflicting authoritative replay sent provider mutation")
	}
	mu.Unlock()
}

func TestConcurrentCreateCallersShareFrozenProviderOperationIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	ctx := context.Background()
	booking, user := uuid.New(), uuid.New()
	reader := &retryBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725}, Generation: 1, Phase: "OPEN", Currency: "usd", ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour))}}
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		mu.Lock()
		posts++
		first := posts == 1
		mu.Unlock()
		if first {
			close(entered)
		}
		<-release
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "pi_concurrent_fixture", "amount": 725, "currency": "usd", "status": "requires_payment_method", "livemode": false, "metadata": map[string]string{"ticketbox_payment_id": r.PostForm.Get("metadata[ticketbox_payment_id]"), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}})
	}))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), MaxNetworkRetries: stripego.Int64(0), HTTPClient: server.Client(), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	s := withReadyAccounting(NewPaymentService(repository.NewPostgresPaymentRepository(pool), zap.NewNop(), provider))
	s.SetFinancialAuthority(reader, "acct_fixture", false)
	request := func() *domain.CreatePaymentRequest {
		return &domain.CreatePaymentRequest{Payment: domain.Payment{ID: uuid.New(), UserId: user, BookingId: &booking, Price: 725, Currency: "usd"}, UserEmail: "original@example.test"}
	}
	first := make(chan *domain.CreatePaymentResponse, 1)
	go func() {
		response, err := s.CreatePayment(ctx, request())
		if err != nil {
			t.Error(err)
		}
		first <- response
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("provider boundary absent")
	}
	var wg sync.WaitGroup
	responses := make(chan *domain.CreatePaymentResponse, 15)
	for range 15 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := s.CreatePayment(ctx, request())
			if err != nil {
				t.Error(err)
			}
			responses <- response
		}()
	}
	wg.Wait()
	close(responses)
	var id uuid.UUID
	for response := range responses {
		if response == nil {
			t.Error("missing concurrent response")
			continue
		}
		if id == uuid.Nil {
			id = response.PaymentID
		}
		if id != response.PaymentID || response.PaymentIntentClientSecret != "" || response.CreateState != "create_claimed" {
			t.Error("concurrent replay lost original durable identity")
		}
	}
	close(release)
	response := <-first
	if response == nil || response.PaymentID != id || response.PaymentItentId != "pi_concurrent_fixture" {
		t.Fatal("winner returned another financial lifecycle")
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 {
		t.Fatalf("concurrent provider creates=%d", posts)
	}
}

// This wrapper is the deterministic failure boundary between the actual HTTP
// provider result and the real PostgreSQL result transaction.
type interruptedResultRepository struct {
	repository.PaymentRepositoryInterface
	entered chan struct{}
	release chan struct{}
}

func (r *interruptedResultRepository) FinishFinancialWork(ctx context.Context, c domain.FinancialClaim, result domain.FinancialResult) (bool, error) {
	if result.Observation != nil {
		close(r.entered)
		<-r.release
		return false, errors.New("fixture local result write interrupted")
	}
	return r.PaymentRepositoryInterface.FinishFinancialWork(ctx, c, result)
}

func TestCreateResultDeathAndRetentionBoundaryIntegration(t *testing.T) {
	for _, scenario := range []string{"local_result_death", "unknown_beyond_safe_retention"} {
		t.Run(scenario, func(t *testing.T) {
			pool := retryPaymentFixture(t)
			ctx := context.Background()
			booking, user := uuid.New(), uuid.New()
			reader := &retryBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725}, Generation: 1, Phase: "OPEN", Currency: "usd", ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour))}}
			var mu sync.Mutex
			var keys, bodies []string
			searches, retrieves := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					mu.Lock()
					if strings.HasSuffix(r.URL.Path, "/search") {
						searches++
						mu.Unlock()
						_, _ = w.Write([]byte(`{"object":"search_result","data":[],"has_more":false}`))
						return
					}
					retrieves++
					mu.Unlock()
				} else {
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
					mu.Lock()
					keys = append(keys, r.Header.Get("Idempotency-Key"))
					bodies = append(bodies, r.PostForm.Encode())
					mu.Unlock()
					if scenario == "unknown_beyond_safe_retention" {
						connection, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = connection.Close()
						return
					}
				}
				var paymentID string
				if err := pool.QueryRow(ctx, `SELECT id::text FROM payments WHERE booking_id=$1`, booking).Scan(&paymentID); err != nil {
					t.Error(err)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "pi_result_gap", "object": "payment_intent", "amount": 725, "currency": "usd", "livemode": false, "status": "requires_payment_method", "client_secret": "fixture_transient_secret", "metadata": map[string]string{"ticketbox_payment_id": paymentID, "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}})
			}))
			defer server.Close()
			backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), MaxNetworkRetries: stripego.Int64(0), HTTPClient: server.Client(), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
			provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
			repo := repository.NewPostgresPaymentRepository(pool)
			var firstRepo repository.PaymentRepositoryInterface = repo
			interrupted := &interruptedResultRepository{PaymentRepositoryInterface: repo, entered: make(chan struct{}), release: make(chan struct{})}
			if scenario == "local_result_death" {
				firstRepo = interrupted
			}
			service := withReadyAccounting(NewPaymentService(firstRepo, zap.NewNop(), provider))
			service.SetFinancialAuthority(reader, "acct_fixture", false)
			request := func() *domain.CreatePaymentRequest {
				return &domain.CreatePaymentRequest{Payment: domain.Payment{ID: uuid.New(), UserId: user, BookingId: &booking, Price: 725, Currency: "usd"}, UserEmail: "original@example.test"}
			}
			if scenario == "local_result_death" {
				done := make(chan error, 1)
				go func() { _, err := service.CreatePayment(ctx, request()); done <- err }()
				select {
				case <-interrupted.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("post-provider boundary not reached")
				}
				// Prove no Payment row lock/transaction spans the remote effect.
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, lockErr := tx.Exec(ctx, `SELECT id FROM payments WHERE booking_id=$1 FOR UPDATE NOWAIT`, booking)
				_ = tx.Rollback(ctx)
				close(interrupted.release)
				if lockErr != nil {
					t.Fatalf("row lock held across provider boundary: %v", lockErr)
				}
				if err := <-done; err == nil {
					t.Fatal("interrupted local result was reported as stored")
				}
			} else {
				response, err := service.CreatePayment(ctx, request())
				if err != nil || response.CreateState != "create_unknown" || response.PaymentIntentClientSecret != "" {
					t.Fatalf("lost response erased uncertainty: %+v %v", response, err)
				}
				if _, err := pool.Exec(ctx, `UPDATE payments SET create_first_attempt_at=NOW()-INTERVAL '20 hours 1 second' WHERE booking_id=$1`, booking); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE booking_id=$1`, booking); err != nil {
				t.Fatal(err)
			}
			// A fresh service instance has no in-memory operation identity.
			restarted := withReadyAccounting(NewPaymentService(repo, zap.NewNop(), provider))
			restarted.SetFinancialAuthority(reader, "acct_fixture", false)
			response, err := restarted.CreatePayment(ctx, request())
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "local_result_death" {
				if response.PaymentItentId != "pi_result_gap" || response.CreateState != "create_ready" {
					t.Fatalf("result gap recovery: %+v", response)
				}
				// Even beyond retention a proven intent is retrieved, never recreated.
				if _, err := pool.Exec(ctx, `UPDATE payments SET create_first_attempt_at=NOW()-INTERVAL '21 hours' WHERE booking_id=$1`, booking); err != nil {
					t.Fatal(err)
				}
				known, err := restarted.CreatePayment(ctx, request())
				if err != nil || known.PaymentID != response.PaymentID || known.PaymentIntentClientSecret == "" {
					t.Fatalf("known intent retrieval: %+v %v", known, err)
				}
				reader.view.ReservationDeadline = timestamppb.New(time.Now().Add(-time.Second))
				expired, err := restarted.CreatePayment(ctx, request())
				if err != nil || expired.PaymentIntentClientSecret != "" {
					t.Error("expired authoritative hold returned payable secret")
				}
				reader.view.Phase = "TERMINAL"
				terminal, err := restarted.CreatePayment(ctx, request())
				if err != nil || terminal.PaymentIntentClientSecret != "" {
					t.Fatal("terminal replay returned payable secret")
				}
			} else if response.CreateState != "create_attention" || response.PaymentIntentClientSecret != "" {
				t.Fatalf("retention ambiguity not durable attention: %+v", response)
			}
			mu.Lock()
			defer mu.Unlock()
			if scenario == "local_result_death" {
				if len(keys) != 2 || keys[0] != keys[1] || bodies[0] != bodies[1] || retrieves != 1 {
					t.Fatalf("frozen recovery identity: posts=%d retrieves=%d", len(keys), retrieves)
				}
			} else if len(keys) != 1 || searches != 1 {
				t.Fatalf("retention caused new mutation: posts=%d searches=%d", len(keys), searches)
			}
		})
	}
}

func TestCreateRejectsInvalidAuthoritativeFinancialSnapshotIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	booking, user := uuid.New(), uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid snapshot reached provider")
		http.Error(w, "unexpected", 500)
	}))
	defer server.Close()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	reader := &retryBookingReader{}
	s := withReadyAccounting(NewPaymentService(repository.NewPostgresPaymentRepository(pool), zap.NewNop(), provider))
	s.SetFinancialAuthority(reader, "acct_fixture", false)
	for _, test := range []struct {
		name            string
		amount          int64
		caller          int32
		currency, owner string
		generation      int64
	}{
		{"overflow", 2147483648, -2147483648, "usd", user.String(), 1},
		{"overflow_wraps_to_valid", 4294968021, 725, "usd", user.String(), 1},
		{"zero", 0, 0, "usd", user.String(), 1},
		{"wrong_user", 725, 725, "usd", uuid.NewString(), 1},
		{"wrong_currency", 725, 725, "eur", user.String(), 1},
		{"wrong_generation", 725, 725, "usd", user.String(), 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader.view = &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: test.owner, TotalAmountCents: test.amount}, Generation: 1, Phase: "OPEN", Currency: test.currency, ReservationDeadline: timestamppb.New(time.Now().Add(time.Hour))}
			_, err := s.CreatePayment(context.Background(), &domain.CreatePaymentRequest{Payment: domain.Payment{UserId: user, BookingId: &booking, Price: test.caller, Currency: test.currency}, PaymentGeneration: test.generation})
			if err == nil {
				t.Fatal("invalid authoritative snapshot accepted")
			}
		})
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM payments`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid snapshot stored: %d %v", count, err)
	}
}
