package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
)

func TestLateCollectionAfterReallocationTwoRefundWorkersIntegration(t *testing.T) {
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	assertEventUnchanged := reallocatedEventFixture(t, booking)
	defer assertEventUnchanged()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_late_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:expire"))
	if _, err := repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: id, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + id.String() + ":g1:refund"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	posts := 0
	refundStatus := "succeeded"
	keys := []string{}
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata := map[string]string{"ticketbox_payment_id": id.String(), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}
		if r.Method == "POST" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.PostForm.Get("payment_intent") != "pi_late_fixture" || r.PostForm.Get("amount") != "725" {
				t.Error("refund full target changed")
			}
			mu.Lock()
			posts++
			keys = append(keys, r.Header.Get("Idempotency-Key"))
			first := posts == 1
			mu.Unlock()
			if first {
				close(entered)
				<-release
			}
		}
		if strings.Contains(r.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "pi_late_fixture", "amount": 725, "amount_received": 725, "currency": "usd", "status": "succeeded", "livemode": false, "metadata": metadata})
			return
		}
		mu.Lock()
		status := refundStatus
		count := posts
		mu.Unlock()
		refund := map[string]any{"id": "re_late_fixture", "object": "refund", "payment_intent": "pi_late_fixture", "amount": 725, "currency": "usd", "status": status, "metadata": metadata}
		if r.Method == "GET" && r.URL.Path == "/v1/refunds" {
			data := []any{}
			if count > 0 {
				data = append(data, refund)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
			return
		}
		_ = json.NewEncoder(w).Encode(refund)
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	reader := &resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: "EXPIRED"}, Generation: 1, Phase: "TERMINAL", Currency: "usd", DecisionId: decision.String()}}
	s := NewPaymentService(repo, zap.NewNop(), provider)
	s.SetFinancialAuthority(reader, "acct_fixture", false)
	worker1, worker2 := NewFinancialWorker(s), NewFinancialWorker(s)
	done := make(chan error, 1)
	go func() { _, err := worker1.RunOnce(ctx, 1); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Error("durable refund worker never executed provider mutation")
		close(release)
		<-done
		return
	}
	// The first request is accepted at the provider boundary but its local
	// worker is still waiting. Expiry permits a new fenced executor.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, lockErr := tx.Exec(ctx, `SELECT id FROM payments WHERE id=$1 FOR UPDATE NOWAIT`, id)
	_ = tx.Rollback(ctx)
	if lockErr != nil {
		t.Fatal("provider mutation holds Payment row lock", lockErr)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET lease_until=NOW()-INTERVAL '1 second',due_at=NOW() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := worker2.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	view, err := repo.GetFinancial(ctx, id)
	if err != nil || view.RefundState != "succeeded" || view.RefundAmountCents != 725 || view.ProviderOutcome != "collected" {
		t.Fatalf("full refund not proven durably: %+v %v", view, err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, key := range keys {
		if key != "ticketbox:payment:"+id.String()+":g1:refund" {
			t.Error("refund worker allocated another operation")
		}
	}
}

// The actual migrated Event database contains a closed A generation and B's
// booked seat/accounting. Payment recovery must leave that entire snapshot intact.
func reallocatedEventFixture(t *testing.T, a uuid.UUID) func() {
	t.Helper()
	u, err := url.Parse(os.Getenv("EVENT_TEST_DATABASE_URL"))
	if err != nil || u.Port() != "25439" || u.Path != "/ticketbox_event_test" {
		t.Fatal("BLOCKED: isolated Event PG required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "refund_event_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if output, err := exec.Command("migrate", "-path", "../../../event/migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("Event migrations: %s", strings.ReplaceAll(string(output), u.String(), "[isolated DSN]"))
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	event, tier, seat, b := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO events(id,title,category,venue,location,date) VALUES($1,'fixture','concert','fixture','fixture',NOW())`, []any{event}},
		{`INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity) VALUES($1,$2,'fixture',725,1,0)`, []any{tier, event}},
		{`INSERT INTO seats(id,event_id,ticket_tier_id,status,booking_id,lifecycle_generation) VALUES($1,$2,$3,'booked',$4,1)`, []any{seat, event, tier, b}},
		{`INSERT INTO booking_seat_lifecycles(booking_id,generation,event_id,seat_ids,seat_set_hash,reservation_deadline,state,accounting_state,last_operation_id,debits,accounted_at) VALUES($1,1,$2,ARRAY[$3::uuid],decode(repeat('ab',32),'hex'),NOW()-INTERVAL '1 hour','closed','not_applied',$4,NULL,NULL),($5,1,$2,ARRAY[$3::uuid],decode(repeat('ab',32),'hex'),NOW()+INTERVAL '1 hour','booked','applied',$6,jsonb_build_object($7::text,1),NOW())`, []any{a, event, seat, uuid.New(), b, uuid.New(), tier}},
	} {
		if _, err = p.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	query := `SELECT jsonb_build_object('seats',(SELECT jsonb_agg(to_jsonb(s)) FROM seats s),'tiers',(SELECT jsonb_agg(to_jsonb(t)) FROM ticket_tiers t),'lifecycles',(SELECT jsonb_agg(to_jsonb(l) ORDER BY booking_id) FROM booking_seat_lifecycles l))::text`
	var before string
	if err = p.QueryRow(ctx, query).Scan(&before); err != nil {
		t.Fatal(err)
	}
	return func() {
		var after string
		if err := p.QueryRow(ctx, query).Scan(&after); err != nil {
			t.Error(err)
		} else if after != before {
			t.Error("late A refund changed B's seat/accounting or reopened A")
		}
	}
}

func refundWorkerFixture(t *testing.T, status string, refund bool) (*pgxpool.Pool, *repository.PostgresPaymentRepository, domain.CreateOperation, *FinancialWorker, *httptest.Server) {
	t.Helper()
	p := retryPaymentFixture(t)
	r := repository.NewPostgresPaymentRepository(p)
	ctx := context.Background()
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if status != "unknown" {
		if err := r.RecordObservation(ctx, domain.ProviderObservation{PaymentID: id, BookingID: booking, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_fixture", AmountCents: 725, Currency: "usd", Status: status, Collected: status == "succeeded", ObservedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:expire"))
	if refund {
		if _, err := r.RequireRefund(ctx, domain.RefundObligation{PaymentID: id, DecisionID: decision, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: "ticketbox:payment:" + id.String() + ":g1:refund"}); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := r.RequireCancel(ctx, id, decision); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{URL: stripego.String(server.URL), HTTPClient: server.Client(), MaxNetworkRetries: stripego.Int64(0), LeveledLogger: &stripego.LeveledLogger{Level: stripego.LevelNull}})
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend})))
	s := NewPaymentService(r, zap.NewNop(), provider)
	s.SetFinancialAuthority(&resolutionBookingReader{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), TotalAmountCents: 725, Status: "EXPIRED"}, Generation: 1, Phase: "TERMINAL", Currency: "usd", DecisionId: decision.String()}}, "acct_fixture", false)
	return p, r, op, NewFinancialWorker(s), server
}

func workerIntentJSON(op domain.CreateOperation, status string) map[string]any {
	amountReceived := 0
	if status == "succeeded" {
		amountReceived = 725
	}
	return map[string]any{"id": "pi_worker_fixture", "object": "payment_intent", "amount": 725, "amount_received": amountReceived, "currency": "usd", "status": status, "livemode": false, "metadata": map[string]string{"ticketbox_payment_id": op.PaymentID.String(), "ticketbox_booking_id": op.Identity.BookingID.String(), "ticketbox_generation": "1"}}
}

func TestCancelDiscoveryOfCollectionProgressesToRefundIntegration(t *testing.T) {
	p, r, op, worker, server := refundWorkerFixture(t, "requires_payment_method", false)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" {
			t.Error("cancel mutated a collected intent")
		}
		_ = json.NewEncoder(w).Encode(workerIntentJSON(op, "succeeded"))
	})
	ctx := context.Background()
	for range 2 {
		if _, err := worker.RunOnce(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "pending" || v.RefundAmountCents != 725 || v.ProviderOutcome != "collected" {
		t.Fatalf("cancel discovery starved refund responsibility: %+v %v", v, err)
	}
}

func TestCanceledUnknownCreateRecoversWithoutReplacementIntegration(t *testing.T) {
	p, r, op, worker, server := refundWorkerFixture(t, "unknown", false)
	posts := 0
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/payment_intents/search" {
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "search_result", "data": []any{workerIntentJSON(op, "requires_payment_method")}, "has_more": false})
			return
		}
		status := "requires_payment_method"
		if req.Method == "POST" {
			posts++
			if req.URL.Path != "/v1/payment_intents/pi_worker_fixture/cancel" || req.Header.Get("Idempotency-Key") != "ticketbox:payment:"+op.PaymentID.String()+":g1:cancel" {
				t.Error("canceled unknown create replaced the original operation")
			}
			status = "canceled"
		}
		_ = json.NewEncoder(w).Encode(workerIntentJSON(op, status))
	})
	ctx := context.Background()
	for range 3 {
		if _, err := worker.RunOnce(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.CancelState != "cancelled" || v.ReconciliationState != "resolved" || v.Payment.PaymentIntentId != "pi_worker_fixture" || posts != 1 {
		t.Fatalf("original uncertain create not settled: %+v posts=%d %v", v, posts, err)
	}
}

func TestRefundStatusesRequireCumulativeSucceededTargetIntegration(t *testing.T) {
	for _, tc := range []struct {
		name, status, want string
		amount             int
		old                bool
	}{
		{"pending", "pending", "provider_pending", 725, false},
		{"failed", "failed", "attention", 725, false},
		{"requires_action", "requires_action", "attention", 725, false},
		{"partial", "succeeded", "attention", 300, false},
		{"full_cumulative", "succeeded", "succeeded", 725, false},
		{"old_unknown", "none", "attention", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, r, op, worker, server := refundWorkerFixture(t, "succeeded", true)
			if tc.old {
				if _, err := p.Exec(context.Background(), `UPDATE payments SET refund_first_attempt_at=NOW()-INTERVAL '21 hours' WHERE id=$1`, op.PaymentID); err != nil {
					t.Fatal(err)
				}
			}
			posts := 0
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.Contains(req.URL.Path, "payment_intents") {
					_ = json.NewEncoder(w).Encode(workerIntentJSON(op, "succeeded"))
					return
				}
				if req.Method == "POST" {
					posts++
					t.Error("existing refund evidence issued new mutation")
				}
				data := []any{}
				if tc.status != "none" {
					if tc.name == "full_cumulative" {
						for i, amount := range []int{300, 425} {
							data = append(data, map[string]any{"id": []string{"re_part1", "re_part2"}[i], "object": "refund", "payment_intent": "pi_worker_fixture", "amount": amount, "currency": "usd", "status": "succeeded"})
						}
					} else {
						data = append(data, map[string]any{"id": "re_status_fixture", "object": "refund", "payment_intent": "pi_worker_fixture", "amount": tc.amount, "currency": "usd", "status": tc.status})
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
			})
			if _, err := worker.RunOnce(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			v, err := r.GetFinancial(context.Background(), op.PaymentID)
			if err != nil || v.RefundState != tc.want || v.ProviderOutcome != "collected" || posts != 0 {
				t.Fatalf("refund state=%+v %v", v, err)
			}
		})
	}
}

func TestRefundAcceptedLostResponseRecoversSameOperationIntegration(t *testing.T) {
	p, r, op, worker, server := refundWorkerFixture(t, "succeeded", true)
	posts := 0
	accepted := false
	hide := true
	var body string
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.Contains(req.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(workerIntentJSON(op, "succeeded"))
			return
		}
		refund := map[string]any{"id": "re_unknown_fixture", "object": "refund", "payment_intent": "pi_worker_fixture", "amount": 725, "currency": "usd", "status": "succeeded"}
		if req.Method == "POST" {
			_ = req.ParseForm()
			posts++
			if req.Header.Get("Idempotency-Key") != "ticketbox:payment:"+op.PaymentID.String()+":g1:refund" || req.PostForm.Get("amount") != "725" {
				t.Error("refund replay identity changed")
			}
			if body != "" && body != req.PostForm.Encode() {
				t.Error("refund replay parameters changed")
			}
			body = req.PostForm.Encode()
			accepted = true
			if posts == 1 {
				http.Error(w, `{"error":{"type":"api_error","message":"fixture lost response"}}`, 500)
				return
			}
			_ = json.NewEncoder(w).Encode(refund)
			return
		}
		data := []any{}
		if accepted && !hide {
			data = append(data, refund)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
	})
	ctx := context.Background()
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "retryable" {
		t.Fatal("lost response erased refund uncertainty", err)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	v, err = r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "provider_pending" || v.RefundID != "re_unknown_fixture" || posts != 2 {
		t.Fatalf("same-key replay not retained: %+v posts=%d %v", v, posts, err)
	}
	hide = false
	if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	v, err = r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "succeeded" || posts != 2 {
		t.Fatal("provider retrieval did not verify original full refund", err)
	}
}

func TestLegacyRefundAcceptsOnlyDurableFullObligationIntegration(t *testing.T) {
	_, r, op, worker, server := refundWorkerFixture(t, "requires_payment_method", false)
	if err := r.RecordObservation(context.Background(), domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	posts := 0
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		posts++
		http.Error(w, "unexpected direct mutation", 500)
	})
	s := NewPaymentService(r, zap.NewNop(), worker.provider.(paymentgateway.PaymentGatewayInterface))
	s.SetFinancialAuthority(worker.booking, "acct_fixture", false)
	ctx := context.Background()
	if err := s.MakePaymentRefund(ctx, &domain.MakePaymentRefundRequest{PaymentIntentId: "pi_worker_fixture", Amount: 300}); err == nil {
		t.Fatal("legacy partial refund accepted")
	}
	if err := s.MakePaymentRefund(ctx, &domain.MakePaymentRefundRequest{PaymentIntentId: "pi_worker_fixture", Amount: 725}); err != nil {
		t.Fatal("full durable refund acceptance failed", err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "pending" || v.RefundAmountCents != 725 || posts != 0 {
		t.Fatalf("legacy adapter skipped durable responsibility: %+v posts=%d %v", v, posts, err)
	}
}

func TestRefundProviderReadsCannotAssertUncollectedFullRefundIntegration(t *testing.T) {
	_, r, op, worker, server := refundWorkerFixture(t, "succeeded", true)
	posts := 0
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" {
			posts++
		}
		_ = json.NewEncoder(w).Encode(workerIntentJSON(op, "requires_payment_method"))
	})
	if _, err := worker.RunOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	v, err := r.GetFinancial(context.Background(), op.PaymentID)
	if err != nil || v.RefundState != "attention" || posts != 0 {
		t.Fatalf("uncollected provider read mutated refund: %+v posts=%d %v", v, posts, err)
	}
}

func TestFinancialInfrastructureExhaustionRetainsAttentionIntegration(t *testing.T) {
	for _, kind := range []string{"refund", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			status := "requires_payment_method"
			if kind == "refund" {
				status = "succeeded"
			}
			p, r, op, worker, server := refundWorkerFixture(t, status, kind == "refund")
			if _, err := p.Exec(context.Background(), `UPDATE payments SET attempts=32,refund_first_attempt_at=CASE WHEN $2='refund' THEN NOW()-INTERVAL '1 hour' ELSE refund_first_attempt_at END,cancel_first_attempt_at=CASE WHEN $2='cancel' THEN NOW()-INTERVAL '1 hour' ELSE cancel_first_attempt_at END WHERE id=$1`, op.PaymentID, kind); err != nil {
				t.Fatal(err)
			}
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"error":{"type":"api_error","message":"fixture unavailable"}}`, 503)
			})
			if _, err := worker.RunOnce(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			v, err := r.GetFinancial(context.Background(), op.PaymentID)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "refund" && (v.RefundState != "attention" || v.RefundAmountCents != 725) || kind == "cancel" && v.CancelState != "cancel_attention" {
				t.Fatalf("exhausted uncertainty lost diagnosable obligation: %+v", v)
			}
		})
	}
}

func TestHealthyReconciliationDoesNotExhaustNewRefundIntegration(t *testing.T) {
	p, r, op, worker, server := refundWorkerFixture(t, "requires_payment_method", false)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `UPDATE payments SET cancel_state='not_requested',booking_disposition=NULL WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	reader := worker.booking.(*resolutionBookingReader)
	reader.view.Phase = "OPEN"
	reader.view.Booking.Status = "PENDING"
	for range 34 {
		if _, err := worker.RunOnce(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
			t.Fatal(err)
		}
	}
	reader.view.Phase = "TERMINAL"
	reader.view.Booking.Status = "EXPIRED"
	if err := r.RecordObservation(ctx, domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"type":"api_error","message":"fixture unavailable"}}`, 503)
	})
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "retryable" {
		t.Fatalf("healthy polls consumed new refund retry budget: %+v %v", v, err)
	}
}

func TestReceiptAfterCompletedRefundRemainsResolvedIntegration(t *testing.T) {
	p, r, op, worker, server := refundWorkerFixture(t, "succeeded", true)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" {
			t.Error("completed refund mutated again")
		}
		if strings.Contains(req.URL.Path, "payment_intents") {
			_ = json.NewEncoder(w).Encode(workerIntentJSON(op, "succeeded"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []any{map[string]any{"id": "re_completed", "object": "refund", "payment_intent": "pi_worker_fixture", "amount": 725, "currency": "usd", "status": "succeeded"}}, "has_more": false})
	})
	ctx := context.Background()
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	safe := json.RawMessage(`{"id":"re_completed","payment_intent":"pi_worker_fixture","amount":725,"currency":"usd","status":"succeeded"}`)
	receipt := domain.ProviderReceipt{EventType: "refund.updated", Observation: domain.ProviderObservation{EventID: "evt_completed_refund", PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_fixture", RefundID: "re_completed", AmountCents: 725, Currency: "usd", Status: "succeeded", ObservedAt: time.Now(), SafeObject: safe, EvidenceHash: sha256.Sum256(safe)}}
	receipt.ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:acct_fixture:test:evt_completed_refund"))
	receipt.EnvelopeID = receipt.ID.String()
	receipt.CorrelationState, receipt.RelayState = "pending", "not_applicable"
	if _, _, err := r.AcceptProviderReceipt(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if v, err := r.GetFinancial(ctx, op.PaymentID); err != nil || v.ReconciliationState != "resolved" {
		t.Fatal("refund receipt stranded completion", err)
	}
	if err := r.RecordObservation(ctx, domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: "acct_fixture", IntentID: "pi_worker_fixture", AmountCents: 725, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.RefundState != "succeeded" || v.ReconciliationState != "resolved" {
		t.Fatalf("later receipt stranded completed refund: %+v %v", v, err)
	}
	if _, err := p.Exec(ctx, `UPDATE payments SET due_at=NOW() WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	if count, err := worker.RunOnce(ctx, 1); err != nil || count != 0 {
		t.Fatal("completed refund still claims work", count, err)
	}
}
