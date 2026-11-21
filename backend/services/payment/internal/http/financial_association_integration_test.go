//go:build reliability_financial_chain

package payment_http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/paymentruntime"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"go.uber.org/zap"
)

func financialAssociationFixture(t *testing.T, pool *pgxpool.Pool) (domain.CreateOperation, *service.PaymentService, *atomic.Int64) {
	t.Helper()
	ctx := context.Background()
	repo := repository.NewPostgresPaymentRepository(pool)
	paymentID, bookingID, userID := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{PaymentID: paymentID, FrozenParams: frozen,
		Identity: domain.FinancialIdentity{BookingID: bookingID, UserID: userID, Generation: 1,
			ProviderAccount: "acct_fixture_test", AmountCents: 725, Currency: "usd"}}
	op.Key = "ticketbox:payment:" + paymentID.String() + ":g1:create"
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: "acct_fixture_test", Status: "succeeded", IntentID: "pi_fin_assoc",
		PaymentID: paymentID, BookingID: bookingID, Generation: 1,
		AmountCents: 725, Currency: "usd", Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:expire"))
	if _, err := repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: paymentID, DecisionID: decision,
		Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED",
		Key: "ticketbox:payment:" + paymentID.String() + ":g1:refund"}); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{
		"ticketbox_payment_id": paymentID.String(),
		"ticketbox_booking_id": bookingID.String(),
		"ticketbox_generation": "1",
	}
	expectedKey := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	var posts atomic.Int64
	var created atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "payment_intents") {
			if r.Method != http.MethodGet || r.URL.Path != "/v1/payment_intents/pi_fin_assoc" {
				http.Error(w, "unexpected intent request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "pi_fin_assoc", "object": "payment_intent",
				"status": "succeeded", "amount": 725, "amount_received": 725, "currency": "usd",
				"livemode": false, "latest_charge": "ch_fin_assoc", "metadata": metadata})
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/refunds" {
			if got := r.Header.Get("Idempotency-Key"); got != expectedKey {
				t.Errorf("refund idempotency key=%q want %q", got, expectedKey)
				http.Error(w, "unexpected idempotency key", http.StatusBadRequest)
				return
			}
			if err := r.ParseForm(); err != nil ||
				r.PostForm.Get("amount") != "725" ||
				r.PostForm.Get("payment_intent") != "pi_fin_assoc" ||
				r.PostForm.Get("metadata[ticketbox_payment_id]") != paymentID.String() ||
				r.PostForm.Get("metadata[ticketbox_booking_id]") != bookingID.String() ||
				r.PostForm.Get("metadata[ticketbox_generation]") != "1" {
				t.Errorf("unexpected refund form: %v", r.PostForm)
				http.Error(w, "unexpected refund request", http.StatusBadRequest)
				return
			}
			posts.Add(1)
			created.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "re_fin_assoc", "object": "refund",
				"status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_fin_assoc",
				"charge": "ch_fin_assoc", "livemode": false, "metadata": metadata})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/refunds") {
			if r.Method != http.MethodGet || r.URL.Query().Get("payment_intent") != "pi_fin_assoc" {
				http.Error(w, "unexpected refund list request", http.StatusBadRequest)
				return
			}
			data := []any{}
			if created.Load() {
				data = append(data, map[string]any{"id": "re_fin_assoc", "object": "refund",
					"status": "succeeded", "amount": 725, "currency": "usd", "payment_intent": "pi_fin_assoc",
					"charge": "ch_fin_assoc", "livemode": false, "metadata": metadata})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": false})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(),
		MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger:     &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	client := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	gateway := stripegateway.NewStripePaymentGateway(zap.NewNop(), "", nil, client)
	svc := service.NewPaymentService(repo, zap.NewNop(), gateway)
	svc.SetFinancialAuthority(nil, "acct_fixture_test", false)
	return op, svc, &posts
}

func waitFinancialState(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID, state string, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		var got string
		if err := pool.QueryRow(context.Background(), `SELECT COALESCE(refund_state,'') FROM payments WHERE id=$1`, paymentID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == state {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("refund_state=%q want %q", got, state)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFinancialAssociationProductTerminalThenLateEvents(t *testing.T) {
	pool, ctx := webhookDB(t)
	op, svc, posts := financialAssociationFixture(t, pool)
	worker := service.NewFinancialWorker(svc)
	group := paymentruntime.Start(context.Background(), worker.Start)
	waitFinancialState(t, pool, op.PaymentID, "succeeded", 15*time.Second)
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := group.StopAndWait(stopCtx); err != nil {
		t.Fatal(err)
	}
	var state, providerID, providerStatus, reconciliation string
	if err := pool.QueryRow(ctx, `SELECT refund_state,COALESCE(refund_provider_id,''),COALESCE(refund_provider_status,''),reconciliation_state FROM payments WHERE id=$1`, op.PaymentID).
		Scan(&state, &providerID, &providerStatus, &reconciliation); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" || providerID != "re_fin_assoc" || providerStatus != "succeeded" {
		t.Fatalf("product terminal state = %s/%s/%s", state, providerID, providerStatus)
	}
	if posts.Load() != 1 {
		t.Fatalf("refund create calls=%d want 1", posts.Load())
	}
	// Restart a new worker on the same service/repository and force one real
	// claim cycle. The succeeded terminal must not be re-claimed or create
	// another refund.
	worker2 := service.NewFinancialWorker(svc)
	claimed, err := worker2.RunOnce(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != 0 {
		t.Fatalf("restart claimed %d terminal financial rows", claimed)
	}
	if posts.Load() != 1 {
		t.Fatalf("restart created another refund: calls=%d", posts.Load())
	}
	// A real HTTP delivery, a replay of the identical signed event, and an
	// earlier-created but later-delivered event, all on the same payment.
	secret := os.Getenv("REFUND_HTTP_WEBHOOK_SECRET")
	if secret == "" {
		t.Fatal("REFUND_HTTP_WEBHOOK_SECRET required")
	}
	server := newWebhookServer(secret, pool)
	defer server.Close()
	// Both synthetic events must postdate the actual provider refund
	// completion. Wait until the clock has moved past that second so
	// created/updated are temporally possible and still strictly ordered.
	var refundCreatedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT COALESCE(refund_completed_at, updated_at) FROM payments WHERE id=$1`, op.PaymentID).Scan(&refundCreatedAt); err != nil {
		t.Fatal(err)
	}
	dbNow := func() int64 {
		t.Helper()
		var value int64
		if err := pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM clock_timestamp())::bigint`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	deadline := time.Now().Add(5 * time.Second)
	for dbNow() <= refundCreatedAt.Unix()+1 {
		if time.Now().After(deadline) {
			t.Fatalf("refund completion time %s is not in the past", refundCreatedAt)
		}
		time.Sleep(50 * time.Millisecond)
	}
	now := dbNow()
	earlierTime := now - 1
	eventID := "evt_fin_assoc_updated"
	body := financialRefundEventBody(eventID, "refund.updated", "re_fin_assoc", "succeeded", now, op)
	signed, _ := signedWebhook(t, body)
	readState := func() (string, string, string, string) {
		t.Helper()
		var gotState, gotProvider, gotStatus, gotReconciliation string
		if err := pool.QueryRow(ctx, `SELECT refund_state,COALESCE(refund_provider_id,''),COALESCE(refund_provider_status,''),reconciliation_state FROM payments WHERE id=$1`, op.PaymentID).
			Scan(&gotState, &gotProvider, &gotStatus, &gotReconciliation); err != nil {
			t.Fatal(err)
		}
		return gotState, gotProvider, gotStatus, gotReconciliation
	}
	readReceipt := func(eventID string) (string, string, string) {
		t.Helper()
		var correlation, receivedPayment, receivedBooking string
		if err := pool.QueryRow(ctx, `SELECT correlation_state,COALESCE(payment_id::text,''),COALESCE(booking_id::text,'')
			FROM payment_provider_events WHERE event_id=$1`, eventID).
			Scan(&correlation, &receivedPayment, &receivedBooking); err != nil {
			t.Fatal(err)
		}
		return correlation, receivedPayment, receivedBooking
	}
	readReceiptHistory := func(eventID string) string {
		t.Helper()
		var history string
		if err := pool.QueryRow(ctx, `SELECT COALESCE(received_at::text,'')||'|'||
			COALESCE(handled_at::text,'')||'|'||COALESCE(due_at::text,'')||'|'||COALESCE(last_error,'')||'|'||
			COALESCE(attempts,0)||'|'||COALESCE(claim_token::text,'')||'|'||COALESCE(lease_until::text,'')
			FROM payment_provider_events WHERE event_id=$1`, eventID).Scan(&history); err != nil {
			t.Fatal(err)
		}
		return history
	}
	if reconciliation != "resolved" {
		t.Fatalf("worker terminal reconciliation_state=%q want resolved", reconciliation)
	}
	if got := postWebhook(t, server, body, signed.Header); got != http.StatusOK {
		t.Fatalf("updated webhook status=%d", got)
	}
	afterFirstState, afterFirstProvider, afterFirstStatus, afterFirstReconciliation := readState()
	if afterFirstState != state || afterFirstProvider != providerID || afterFirstStatus != providerStatus {
		t.Fatalf("first event regressed financial state: %s/%s/%s", afterFirstState, afterFirstProvider, afterFirstStatus)
	}
	if afterFirstReconciliation != "pending" {
		t.Fatalf("first event reconciliation_state=%q want pending", afterFirstReconciliation)
	}
	if correlation, receivedPayment, receivedBooking := readReceipt(eventID); correlation != "applied" ||
		receivedPayment != op.PaymentID.String() || receivedBooking != op.Identity.BookingID.String() {
		t.Fatalf("first event receipt = %s/%s/%s", correlation, receivedPayment, receivedBooking)
	}
	var updatedReceipts, updatedAudits int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_provider_events WHERE event_id=$1`, eventID).Scan(&updatedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_refund_audit WHERE record_kind='VERIFIED_EVENT' AND provider_event_id=$1`, eventID).Scan(&updatedAudits); err != nil {
		t.Fatal(err)
	}
	if updatedReceipts != 1 || updatedAudits != 1 {
		t.Fatalf("first event receipts=%d audits=%d want 1/1", updatedReceipts, updatedAudits)
	}
	historyBeforeDuplicate := readReceiptHistory(eventID)
	// Exact replay: the stored receipt suppresses a second row and leaves the
	// financial and reconciliation state and receipt work timestamps untouched.
	if got := postWebhook(t, server, body, signed.Header); got != http.StatusOK {
		t.Fatalf("duplicate webhook status=%d", got)
	}
	dupState, dupProvider, dupStatus, dupReconciliation := readState()
	if dupState != state || dupProvider != providerID || dupStatus != providerStatus ||
		dupReconciliation != afterFirstReconciliation {
		t.Fatalf("duplicate changed state: %s/%s/%s/%s want %s/%s", dupState, dupProvider, dupStatus, dupReconciliation, state, afterFirstReconciliation)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_provider_events WHERE event_id=$1`, eventID).Scan(&updatedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_refund_audit WHERE record_kind='VERIFIED_EVENT' AND provider_event_id=$1`, eventID).Scan(&updatedAudits); err != nil {
		t.Fatal(err)
	}
	if updatedReceipts != 1 || updatedAudits != 1 {
		t.Fatalf("duplicate persisted extra rows: receipts=%d audits=%d", updatedReceipts, updatedAudits)
	}
	if correlation, receivedPayment, receivedBooking := readReceipt(eventID); correlation != "applied" ||
		receivedPayment != op.PaymentID.String() || receivedBooking != op.Identity.BookingID.String() {
		t.Fatalf("duplicate receipt = %s/%s/%s", correlation, receivedPayment, receivedBooking)
	}
	if history := readReceiptHistory(eventID); history != historyBeforeDuplicate {
		t.Fatalf("duplicate rewrote receipt work state: %q want %q", history, historyBeforeDuplicate)
	}
	earlier := financialRefundEventBody("evt_fin_assoc_created", "refund.created", "re_fin_assoc", "pending", earlierTime, op)
	earlierSigned, _ := signedWebhook(t, earlier)
	if got := postWebhook(t, server, earlier, earlierSigned.Header); got != http.StatusOK {
		t.Fatalf("earlier webhook status=%d", got)
	}
	afterState, afterProvider, afterStatus, afterReconciliation := readState()
	if afterState != state || afterProvider != providerID || afterStatus != providerStatus {
		t.Fatalf("late event regressed financial state: %s/%s/%s", afterState, afterProvider, afterStatus)
	}
	if afterReconciliation != "pending" {
		t.Fatalf("late event reconciliation_state=%q want pending", afterReconciliation)
	}
	if correlation, receivedPayment, receivedBooking := readReceipt("evt_fin_assoc_created"); correlation != "applied" ||
		receivedPayment != op.PaymentID.String() || receivedBooking != op.Identity.BookingID.String() {
		t.Fatalf("earlier event receipt = %s/%s/%s", correlation, receivedPayment, receivedBooking)
	}
	var receipts, audits int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_provider_events WHERE payment_id=$1`, op.PaymentID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	const earlierID = "evt_fin_assoc_created"
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_refund_audit
		WHERE record_kind='VERIFIED_EVENT' AND provider_event_id IN ($1,$2)`, eventID, earlierID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if receipts != 2 || audits != 2 {
		t.Fatalf("receipts=%d audits=%d want 2/2", receipts, audits)
	}
	rows, err := pool.Query(ctx, `SELECT provider_event_id, COALESCE(payment_id::text,'NULL'),
		COALESCE(payload->>'refund_id',''), COALESCE(payload->>'payment_intent_id',''),
		COALESCE(payload->>'link_classification','')
		FROM payment_refund_audit WHERE record_kind='VERIFIED_EVENT'
		AND provider_event_id IN ($1,$2) ORDER BY provider_event_id`, eventID, earlierID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	auditLinks := map[string]string{}
	for rows.Next() {
		var gotEvent, payment, refund, intent, link string
		if err := rows.Scan(&gotEvent, &payment, &refund, &intent, &link); err != nil {
			t.Fatal(err)
		}
		// The Stripe refund object exposes no observed provider account, so
		// the production correlation must remain uncorrelated and unlinked.
		// A regression to conflict/ambiguous or an invented link is visible.
		if payment != "NULL" || refund != "re_fin_assoc" || intent != "pi_fin_assoc" ||
			link != "uncorrelated" {
			t.Fatalf("event audit identity = %s/%s/%s/%s/%s", gotEvent, payment, refund, intent, link)
		}
		auditLinks[gotEvent] = link
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 {
		t.Fatalf("late event created a refund: calls=%d", posts.Load())
	}
	fmt.Printf("FINANCIAL_ASSOC_EVIDENCE %s\n", mustJSON(map[string]any{
		"paymentId": op.PaymentID.String(), "bookingId": op.Identity.BookingID.String(),
		"terminal":                     state + "/" + providerID + "/" + providerStatus,
		"reconciliationBefore":         reconciliation,
		"reconciliationAfterFirst":     afterFirstReconciliation,
		"reconciliationAfterDuplicate": dupReconciliation,
		"reconciliationAfterEarlier":   afterReconciliation,
		"providerCreateCalls":          posts.Load(), "receipts": receipts, "audits": audits,
		"duplicateReceipts": updatedReceipts, "duplicateAudits": updatedAudits,
		"auditLinkClassification": auditLinks,
	}))
}

// TestFinancialAssociationUnmatchedClaimIsQuarantined covers the two
// fail-closed lookup paths: metadata payment ID pointing at a different
// managed payment (identity conflict) and a payment ID with no managed row
// (receipt stays pending). Neither may mutate either payment.
func TestFinancialAssociationUnmatchedClaimIsQuarantined(t *testing.T) {
	pool, ctx := webhookDB(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	newPayment := func() domain.CreateOperation {
		booking := uuid.New()
		op := domain.CreateOperation{PaymentID: uuid.New(), FrozenParams: frozen,
			Identity: domain.FinancialIdentity{BookingID: booking, UserID: uuid.New(), Generation: 1,
				ProviderAccount: "acct_fixture_test", AmountCents: 725, Currency: "usd"}}
		op.Key = "ticketbox:payment:" + op.PaymentID.String() + ":g1:create"
		op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, op.FrozenParams)
		if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
			t.Fatal(err)
		}
		if err := repo.RecordObservation(ctx, domain.ProviderObservation{
			Account: "acct_fixture_test", Status: "succeeded",
			IntentID:  "pi_unmatched_" + uuid.NewString(),
			PaymentID: op.PaymentID, BookingID: booking, Generation: 1,
			AmountCents: 725, Currency: "usd", Collected: true, ObservedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		return op
	}
	op := newPayment()
	other := newPayment()
	readState := func(id uuid.UUID) string {
		t.Helper()
		var state string
		if err := pool.QueryRow(ctx, `SELECT refund_state||'|'||reconciliation_state||'|'||
			COALESCE(refund_provider_id,'')||'|'||COALESCE(refund_provider_status,'')||'|'||
			COALESCE(due_at::text,'')||'|'||COALESCE(updated_at::text,'')||'|'||
			COALESCE(attempts,0)||'|'||COALESCE(claim_token::text,'')||'|'||COALESCE(lease_until::text,'')
			FROM payments WHERE id=$1`, id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='resolved'
		WHERE id=$1 OR id=$2`, op.PaymentID, other.PaymentID); err != nil {
		t.Fatal(err)
	}
	opBefore := readState(op.PaymentID)
	otherBefore := readState(other.PaymentID)
	secret := os.Getenv("REFUND_HTTP_WEBHOOK_SECRET")
	if secret == "" {
		t.Fatal("REFUND_HTTP_WEBHOOK_SECRET required")
	}
	server := newWebhookServer(secret, pool)
	defer server.Close()
	now := time.Now().Unix()

	// A managed row exists at the claimed payment ID, but the claimed booking
	// does not match. The receipt must be marked conflict/attention.
	conflictID := "evt_fin_assoc_conflict"
	conflict := financialRefundEventBodyFor(conflictID, "refund.updated", "re_fin_assoc",
		"succeeded", now, other.PaymentID, op.Identity.BookingID, "acct_fixture_test")
	conflictSigned, _ := signedWebhook(t, conflict)
	if got := postWebhook(t, server, conflict, conflictSigned.Header); got != http.StatusOK {
		t.Fatalf("conflict webhook status=%d", got)
	}
	var conflictState, conflictError string
	if err := pool.QueryRow(ctx, `SELECT correlation_state,COALESCE(last_error,'') FROM payment_provider_events WHERE event_id=$1`, conflictID).
		Scan(&conflictState, &conflictError); err != nil {
		t.Fatal(err)
	}
	if conflictState != "attention" || conflictError != "provider_identity_conflict" {
		t.Fatalf("conflict receipt = %s/%s", conflictState, conflictError)
	}
	if state := readState(op.PaymentID); state != opBefore {
		t.Fatalf("conflict mutated original payment: %s want %s", state, opBefore)
	}
	if state := readState(other.PaymentID); state != otherBefore {
		t.Fatalf("conflict mutated claimed payment: %s want %s", state, otherBefore)
	}

	// No managed row exists at the claimed payment ID. The receipt is retained
	// as pending and no business row changes.
	pendingID := "evt_fin_assoc_pending"
	pending := financialRefundEventBodyFor(pendingID, "refund.updated", "re_fin_assoc",
		"succeeded", now, uuid.New(), uuid.New(), "acct_fixture_test")
	pendingSigned, _ := signedWebhook(t, pending)
	if got := postWebhook(t, server, pending, pendingSigned.Header); got != http.StatusOK {
		t.Fatalf("pending webhook status=%d", got)
	}
	var pendingState string
	var handled bool
	if err := pool.QueryRow(ctx, `SELECT correlation_state,handled_at IS NOT NULL FROM payment_provider_events WHERE event_id=$1`, pendingID).
		Scan(&pendingState, &handled); err != nil {
		t.Fatal(err)
	}
	if pendingState != "pending" || handled {
		t.Fatalf("unmatched receipt = %s handled=%t", pendingState, handled)
	}
	if state := readState(op.PaymentID); state != opBefore {
		t.Fatalf("unmatched event mutated original payment: %s want %s", state, opBefore)
	}
	if state := readState(other.PaymentID); state != otherBefore {
		t.Fatalf("unmatched event mutated other payment: %s want %s", state, otherBefore)
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM payment_refund_audit
		WHERE record_kind='VERIFIED_EVENT' AND provider_event_id IN ($1,$2)`, conflictID, pendingID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 2 {
		t.Fatalf("audits=%d want 2", audits)
	}
	fmt.Printf("FINANCIAL_ASSOC_UNMATCHED_EVIDENCE %s\n", mustJSON(map[string]any{
		"conflictReceipt": conflictState + "/" + conflictError,
		"pendingReceipt":  pendingState, "pendingHandled": handled,
		"audits": audits,
	}))
}

// financialRefundEventBody binds the signed event to the same payment intent
// and account as the worker-produced response record. The Stripe refund
// object does not expose an observed provider account, so the production
// correlation is still expected to be uncorrelated; the test asserts that
// exact outcome rather than promoting an invented link.
func financialRefundEventBody(eventID, eventType, refundID, status string, created int64, op domain.CreateOperation) []byte {
	return financialRefundEventBodyFor(eventID, eventType, refundID, status, created,
		op.PaymentID, op.Identity.BookingID, "acct_fixture_test")
}

func financialRefundEventBodyFor(eventID, eventType, refundID, status string, created int64,
	paymentID, bookingID uuid.UUID, account string) []byte {
	object := map[string]any{
		"id": refundID, "object": "refund", "status": status, "amount": 725,
		"currency": "usd", "livemode": false, "payment_intent": "pi_fin_assoc",
		"metadata": map[string]string{
			"ticketbox_payment_id": paymentID.String(),
			"ticketbox_booking_id": bookingID.String(),
			"ticketbox_generation": "1",
		},
	}
	body, _ := json.Marshal(map[string]any{
		"id": eventID, "object": "event", "type": eventType,
		"livemode": false, "account": account, "created": created,
		"api_version": "2026-02-25.clover",
		"data":        map[string]any{"object": object},
	})
	return body
}

func mustJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
