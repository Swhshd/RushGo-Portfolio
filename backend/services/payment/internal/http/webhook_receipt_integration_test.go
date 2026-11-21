package payment_http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/stripe/stripe-go/v84/webhook"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/kafka"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"go.uber.org/zap"
)

func webhookFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("PAYMENT_TEST_DATABASE_URL"))
	if err != nil || parsed.Port() != "25439" || parsed.Path != "/ticketbox_payment_test" {
		t.Fatal("isolated Payment PG required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "webhook_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
		t.Fatalf("actual migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type beforeAckRecorder struct {
	*httptest.ResponseRecorder
	check func(int)
}

type lostAcknowledgementWriter struct {
	http.ResponseWriter
	check func(int)
}

func (w lostAcknowledgementWriter) WriteHeader(code int) {
	w.check(code)
	connection, _, err := w.ResponseWriter.(http.Hijacker).Hijack()
	if err == nil {
		_ = connection.Close()
	}
}

func (r *beforeAckRecorder) WriteHeader(code int) {
	r.check(code)
	r.ResponseRecorder.WriteHeader(code)
}

func TestVerifiedReceiptCommitsBeforeAcknowledgementIntegration(t *testing.T) {
	if os.Getenv("KAFKA_TEST_BROKERS") != "127.0.0.1:29093" {
		t.Fatal("isolated Kafka required")
	}
	pool := webhookFixture(t)
	ctx := context.Background()
	repo := repository.NewPostgresPaymentRepository(pool)
	id, booking, user := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen, Identity: domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, AmountCents: 725, Currency: "usd", ProviderAccount: "acct_fixture"}}
	op.Identity.Fingerprint = sha256.Sum256([]byte(fmt.Sprintf("v1\n%s\n%s\n1\n725\n\"usd\"\n\"acct_fixture\"\ntest\ntrue\n\"always\"\n\"\"\n", booking, user)))
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET status='timeout',create_state='create_unknown' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	producer := kafka.NewPaymentProducer([]string{"127.0.0.1:29093"}, zap.NewNop())
	t.Cleanup(func() { _ = producer.Close() })
	provider := stripegateway.NewStripePaymentGateway(zap.NewNop(), "whsec_fixture", producer, stripego.NewClient("sk_test_fixture"))
	if scoped, ok := any(provider).(interface{ SetWebhookScope(string, bool) }); ok {
		scoped.SetWebhookScope("acct_fixture", false)
	}
	s := service.NewPaymentService(repo, zap.NewNop(), provider)
	h := NewPaymentHttpHandler(s, zap.NewNop(), provider)
	object := map[string]any{"object": "payment_intent", "id": "pi_receipt_fixture", "amount": 725, "amount_received": 725, "currency": "usd", "livemode": false, "status": "succeeded", "client_secret": "fixture_private_secret", "receipt_email": "fixture-private@example.test", "payment_method": map[string]any{"card": map[string]any{"last4": "4242"}}, "metadata": map[string]string{"ticketbox_payment_id": id.String(), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1", "private_extra": "omit"}}
	payload, _ := json.Marshal(map[string]any{"object": "event", "id": "evt_receipt_fixture_" + uuid.NewString(), "type": "payment_intent.succeeded", "api_version": stripego.APIVersion, "created": time.Now().Unix(), "livemode": false, "data": map[string]any{"object": object}})
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: payload, Secret: "whsec_fixture"})
	assertDurable := func(code int) {
		if code != http.StatusOK {
			return
		}
		var outcome string
		if err := pool.QueryRow(ctx, `SELECT provider_outcome FROM payments WHERE id=$1`, id).Scan(&outcome); err != nil || outcome != "collected" {
			t.Errorf("HTTP200 before durable collection: outcome=%s err=%v", outcome, err)
		}
		var receiptTable *string
		if err := pool.QueryRow(ctx, `SELECT to_regclass(current_schema()||'.payment_provider_events')::text`).Scan(&receiptTable); err != nil {
			t.Error(err)
		}
		if receiptTable == nil {
			t.Error("HTTP200 without durable provider receipt")
			return
		}
		var count int
		var safe string
		if err := pool.QueryRow(ctx, `SELECT count(*),COALESCE(max(normalized_object::text),'') FROM payment_provider_events`).Scan(&count, &safe); err != nil {
			t.Error(err)
		} else if count != 1 {
			t.Errorf("durable receipt count=%d", count)
		}
		for _, private := range []string{"fixture_private_secret", "fixture-private@example.test", "4242", "private_extra"} {
			if strings.Contains(safe, private) {
				t.Error("receipt retained a disallowed private field")
			}
		}
	}
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(payload))
		req.Header.Set("Stripe-Signature", signed.Header)
		res := &beforeAckRecorder{ResponseRecorder: httptest.NewRecorder(), check: assertDurable}
		h.WebhookHandler(res, req)
		if res.Code != http.StatusOK {
			t.Errorf("verified receipt HTTP=%d", res.Code)
		}
	}
	// Lose the actual response connection at the post-commit/header barrier.
	lostServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.WebhookHandler(lostAcknowledgementWriter{ResponseWriter: w, check: assertDurable}, r)
	}))
	lost, err := http.NewRequest(http.MethodPost, lostServer.URL, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	lost.Header.Set("Stripe-Signature", signed.Header)
	if response, err := lostServer.Client().Do(lost); err == nil {
		response.Body.Close()
		t.Error("HTTP acknowledgement loss boundary not exercised")
	}
	lostServer.Close()
	retry := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(payload))
	retry.Header.Set("Stripe-Signature", signed.Header)
	h.WebhookHandler(&beforeAckRecorder{ResponseRecorder: httptest.NewRecorder(), check: assertDurable}, retry)
	for _, invalid := range []struct {
		name      string
		body      []byte
		signature string
		want      int
	}{
		{"bad_signature", payload, "t=1,v1=bad", 400},
		{"bad_body", []byte(`{`), signed.Header, 400},
		{"oversize", bytes.Repeat([]byte("x"), 65537), signed.Header, 400},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(invalid.body))
			req.Header.Set("Stripe-Signature", invalid.signature)
			res := httptest.NewRecorder()
			h.WebhookHandler(res, req)
			if res.Code != invalid.want {
				t.Fatalf("HTTP=%d", res.Code)
			}
		})
	}
	for _, scope := range []struct {
		name    string
		account string
		live    bool
	}{{"wrong_account", "acct_other", false}, {"live_mode", "", true}} {
		t.Run(scope.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"object": "event", "id": "evt_scope_" + uuid.NewString(), "type": "payment_intent.succeeded", "api_version": stripego.APIVersion, "account": scope.account, "created": time.Now().Unix(), "livemode": scope.live, "data": map[string]any{"object": object}})
			signedScope := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: body, Secret: "whsec_fixture"})
			req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
			req.Header.Set("Stripe-Signature", signedScope.Header)
			res := httptest.NewRecorder()
			h.WebhookHandler(res, req)
			if res.Code != 400 {
				t.Fatalf("wrong scope HTTP=%d", res.Code)
			}
		})
	}
	for _, event := range []struct {
		name, kind string
		object     map[string]any
		want       int
	}{
		{"irrelevant", "customer.created", map[string]any{"object": "customer", "id": "cus_fixture"}, 204},
		{"unexpected_object", "payment_intent.succeeded", map[string]any{"object": "unexpected", "id": "obj_fixture", "livemode": false}, 200},
		{"refund_pending", "refund.updated", map[string]any{"object": "refund", "id": "re_fixture", "payment_intent": "pi_receipt_fixture", "amount": 725, "currency": "usd", "status": "pending", "livemode": false}, 200},
		{"older_failure", "payment_intent.payment_failed", map[string]any{"object": "payment_intent", "id": "pi_receipt_fixture", "amount": 725, "currency": "usd", "status": "requires_payment_method", "livemode": false, "metadata": map[string]string{"ticketbox_payment_id": id.String(), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}}, 200},
	} {
		t.Run(event.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"object": "event", "id": "evt_" + event.name + "_" + uuid.NewString(), "type": event.kind, "api_version": stripego.APIVersion, "created": time.Now().Add(-time.Minute).Unix(), "livemode": false, "data": map[string]any{"object": event.object}})
			signedEvent := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: body, Secret: "whsec_fixture"})
			req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(body))
			req.Header.Set("Stripe-Signature", signedEvent.Header)
			res := httptest.NewRecorder()
			h.WebhookHandler(res, req)
			if res.Code != event.want {
				t.Fatalf("HTTP=%d", res.Code)
			}
		})
	}
	var total, attention int
	if err := pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE correlation_state='attention') FROM payment_provider_events`).Scan(&total, &attention); err != nil || total != 4 || attention != 1 {
		t.Fatalf("receipt classifications: total=%d attention=%d %v", total, attention, err)
	}
	view, err := repo.GetFinancial(ctx, id)
	if err != nil || view.ProviderOutcome != "collected" || view.Payment.Status != domain.PaymentSuccess || view.RefundState == "succeeded" {
		t.Fatalf("callback regressed collection or fabricated full refund: %+v %v", view, err)
	}
	var refundRelay string
	if err := pool.QueryRow(ctx, `SELECT relay_state FROM payment_provider_events WHERE event_type='refund.updated'`).Scan(&refundRelay); err != nil || refundRelay != "not_applicable" {
		t.Fatal("refund callback created a Kafka event responsibility")
	}
	// A new valid signed event must not be acknowledged if storage is unavailable.
	pool.Close()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(payload))
	req.Header.Set("Stripe-Signature", signed.Header)
	res := httptest.NewRecorder()
	h.WebhookHandler(res, req)
	if res.Code != http.StatusServiceUnavailable {
		t.Errorf("storage failure HTTP=%d want503", res.Code)
	}
}
