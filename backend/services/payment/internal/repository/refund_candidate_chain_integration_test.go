//go:build recoverycomponents

package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/stripe/stripe-go/v84/webhook"
	"github.com/ticketbox/payment/internal/domain"
	stripegateway "github.com/ticketbox/payment/internal/gateway/stripe"
	"github.com/ticketbox/payment/internal/recoverycomponent"
	"go.uber.org/zap"
)

// TestRefundCandidateChainFromProductSourceToReconciliation walks the local
// candidate chain: persisted product source, PREPARED audit, real local HTTP
// refund creation, full per-object pagination, signed webhook verification,
// product marker commit, and an independent post-marker reconciliation read.
// The Provider is a local HTTP double, not Stripe.
func TestRefundCandidateChainFromProductSourceToReconciliation(t *testing.T) {
	evidenceDir := os.Getenv("REFUND_COMPONENT_EVIDENCE_DIR")
	if evidenceDir == "" {
		t.Skip("refund candidate chain emission is disabled")
	}
	sourceSHA := os.Getenv("REFUND_COMPONENT_SOURCE_SHA256")
	sourceCommit := os.Getenv("REFUND_COMPONENT_SOURCE_COMMIT")
	sourceTree := os.Getenv("REFUND_COMPONENT_SOURCE_TREE")
	runID := os.Getenv("REFUND_COMPONENT_RUN_ID")
	attempt := os.Getenv("REFUND_COMPONENT_ATTEMPT")
	if attempt == "" {
		attempt = "1"
	}
	if len(sourceSHA) != 64 || len(sourceCommit) != 40 || len(sourceTree) != 40 || runID == "" {
		t.Fatal("gated refund chain requires source identity and run ID")
	}
	bookingPath := os.Getenv("REFUND_COMPONENT_BOOKING_OBSERVATION")
	sagaPath := os.Getenv("REFUND_COMPONENT_SAGA_OBSERVATION")
	bookingRun := os.Getenv("REFUND_COMPONENT_BOOKING_RUN_ID")
	sagaRun := os.Getenv("REFUND_COMPONENT_SAGA_RUN_ID")
	bookingSourceSHA := os.Getenv("REFUND_COMPONENT_BOOKING_SOURCE_SHA256")
	sagaSourceSHA := os.Getenv("REFUND_COMPONENT_SAGA_SOURCE_SHA256")
	if bookingPath == "" || sagaPath == "" || bookingRun == "" || sagaRun == "" ||
		len(bookingSourceSHA) != 64 || len(sagaSourceSHA) != 64 {
		t.Fatal("refund chain requires run-bound booking and saga component observations")
	}

	pool, ctx, repo := newRefundComponentRepository(t)
	op := seedRefundPreSendPayment(t, ctx, repo)
	claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
	guard, err := repo.CheckRefundPreSend(ctx, claim)
	if err != nil || guard.Decision != RefundPreSendAllow || guard.AttemptID == uuid.Nil {
		t.Fatalf("pre-send guard = %+v err=%v; want committed allow", guard, err)
	}
	source, err := recoverycomponent.ReadProductSource(ctx, pool, op.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	preparedRecords, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{
		PaymentID: op.PaymentID, Kind: domain.RefundAuditKindPrepared,
	})
	if err != nil || len(preparedRecords) != 1 {
		t.Fatalf("prepared audit records=%d err=%v; want one", len(preparedRecords), err)
	}
	prepared := preparedRecords[0]

	chargeID := "ch_local_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	refundID := "re_local_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	intentID := source.PaymentIntentID
	account := source.ProviderAccount
	amount := source.AmountCents
	currency := source.Currency
	bookingID := source.BookingID
	paymentID := source.PaymentID

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/refunds":
			if got := r.Header.Get("Idempotency-Key"); got != claim.Key {
				t.Errorf("refund idempotency key=%q want %q", got, claim.Key)
			}
			if err := r.ParseForm(); err != nil || r.PostForm.Get("amount") != itoa(amount) ||
				r.PostForm.Get("payment_intent") != intentID {
				t.Errorf("refund request form=%v err=%v", r.PostForm, err)
			}
			w.Header().Set("Request-Id", "req_local_refund")
			writeLocalJSON(t, w, map[string]any{
				"id": refundID, "object": "refund", "status": "succeeded",
				"amount": amount, "currency": currency, "payment_intent": intentID,
				"charge": chargeID,
				"metadata": map[string]string{
					"ticketbox_payment_id": paymentID.String(),
					"ticketbox_booking_id": bookingID.String(),
					"ticketbox_generation": "1",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/payment_intents/"+intentID:
			writeLocalJSON(t, w, map[string]any{
				"id": intentID, "object": "payment_intent", "status": "succeeded",
				"amount": amount, "amount_received": amount, "currency": currency,
				"latest_charge": chargeID, "livemode": false,
				"metadata": map[string]string{
					"ticketbox_payment_id": paymentID.String(),
					"ticketbox_booking_id": bookingID.String(),
					"ticketbox_generation": "1",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/charges":
			writeLocalJSON(t, w, map[string]any{
				"object": "list", "has_more": false, "data": []any{map[string]any{
					"id": chargeID, "object": "charge", "status": "succeeded",
					"amount": amount, "amount_captured": amount, "amount_refunded": amount,
					"currency": currency, "payment_intent": intentID,
					"paid": true, "captured": true, "livemode": false,
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/refunds":
			writeLocalJSON(t, w, map[string]any{
				"object": "list", "has_more": false, "data": []any{map[string]any{
					"id": refundID, "object": "refund", "status": "succeeded",
					"amount": amount, "currency": currency, "payment_intent": intentID,
					"charge": chargeID,
					"metadata": map[string]string{
						"ticketbox_payment_id": paymentID.String(),
						"ticketbox_booking_id": bookingID.String(),
						"ticketbox_generation": "1",
					},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend := stripego.GetBackendWithConfig(stripego.APIBackend, &stripego.BackendConfig{
		URL: stripego.String(server.URL), HTTPClient: server.Client(),
		MaxNetworkRetries: stripego.Int64(0),
		LeveledLogger:     &stripego.LeveledLogger{Level: stripego.LevelNull},
	})
	stripeClient := stripego.NewClient("sk_test_fixture", stripego.WithBackends(&stripego.Backends{API: backend}))
	gateway := stripegateway.NewStripePaymentGateway(zap.NewNop(), "whsec_local_candidate", nil, stripeClient)
	gateway.SetWebhookScope(account, false)
	gateway.SetRefundEvidenceReadBackend(backend, "sk_test_fixture")

	capture := domain.NewRefundResponseCapture(guard.AttemptID)
	createCtx := domain.WithRefundResponseCapture(ctx, capture)
	observation, err := gateway.CreateFullRefund(createCtx, claim, intentID)
	if err != nil || observation == nil || observation.RefundID != refundID {
		t.Fatalf("CreateFullRefund = %+v err=%v", observation, err)
	}
	responseFacts := capture.Snapshot()
	// The local HTTP double has no connected-account response header. The
	// configured TEST account and false mode are the request scope used for
	// this component call; record them as observed scope before materializing
	// the typed audit row.
	responseFacts.ObservedProviderAccount = &account
	responseFacts.ObservedLiveMode = boolPtr(false)
	responseRecord, err := domain.NewRefundResponseAudit(prepared, responseFacts)
	if err != nil {
		t.Fatalf("materialize RESPONSE audit: %v", err)
	}
	if err := repo.AppendRefundAudit(ctx, responseRecord); err != nil {
		t.Fatalf("persist RESPONSE audit: %v", err)
	}

	collection, err := gateway.CollectRefundEvidence(ctx, account, intentID)
	if err != nil {
		t.Fatalf("collect refund evidence: %v", err)
	}
	if !collection.PaginationComplete || len(collection.Refunds) != 1 ||
		collection.Refunds[0].ID != refundID {
		t.Fatalf("refund collection is not the complete single object: %+v", collection)
	}

	eventPayload := map[string]any{
		"id":     "evt_local_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		"object": "event", "type": "refund.updated", "created": time.Now().Unix(),
		"livemode": false, "account": account,
		"request": map[string]string{"id": "req_local_event", "idempotency_key": claim.Key},
		"data": map[string]any{"object": map[string]any{
			"id": refundID, "object": "refund", "status": "succeeded",
			"amount": amount, "currency": currency, "payment_intent": intentID,
			"charge": chargeID, "livemode": false,
			"metadata": map[string]string{
				"ticketbox_payment_id": paymentID.String(),
				"ticketbox_booking_id": bookingID.String(),
				"ticketbox_generation": "1",
			},
		}},
	}
	body, _ := json.Marshal(eventPayload)
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
		Payload: body, Secret: "whsec_local_candidate",
	})
	receipt, err := gateway.VerifyWebhook(ctx, signed.Payload, signed.Header)
	if err != nil || receipt == nil || receipt.RefundAudit == nil {
		t.Fatalf("signed webhook verification: receipt=%+v err=%v", receipt, err)
	}
	verifiedEvent := *receipt.RefundAudit
	verifiedEvent.ObservedProviderAccount = &account
	verifiedEvent.ObservedLiveMode = boolPtr(false)
	verifiedEvent.LinkClassification = domain.RefundAuditLinkExactRefundIdentity
	verifiedEvent.LinkSource = domain.RefundAuditLinkSourceRefundID
	eventRecord, err := domain.NewRefundVerifiedEventRecord(verifiedEvent, &responseRecord)
	if err != nil {
		t.Fatalf("materialize VERIFIED_EVENT audit: %v", err)
	}
	if err := repo.AppendRefundAudit(ctx, eventRecord); err != nil {
		t.Fatalf("persist VERIFIED_EVENT audit: %v", err)
	}
	auditRecords, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{PaymentID: paymentID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var responseAuditID, eventAuditID string
	for _, record := range auditRecords {
		switch record.Kind {
		case domain.RefundAuditKindResponse:
			responseAuditID = record.RecordID.String()
		case domain.RefundAuditKindVerifiedEvent:
			eventAuditID = record.RecordID.String()
		}
	}
	if responseAuditID == "" || eventAuditID == "" {
		t.Fatalf("audit chain incomplete: response=%q event=%q", responseAuditID, eventAuditID)
	}

	markerObservedAt := refundDBClock(t, pool)
	marker := refundMarkerSnapshot(t, pool, paymentID, markerObservedAt)
	ownerObservedAt := refundDBClock(t, pool)
	owner := refundMarkerSnapshot(t, pool, paymentID, ownerObservedAt)
	if marker.RefundState != "claimed" || owner.RefundState != "claimed" ||
		!marker.LeaseActive || !owner.LeaseActive ||
		marker.TokenSHA256 != owner.TokenSHA256 || marker.Fence != owner.Fence {
		t.Fatalf("pre-marker owner facts mismatch: marker=%+v owner=%+v", marker, owner)
	}

	finish := domain.FinancialResult{
		State: "succeeded",
		Observation: &domain.ProviderObservation{
			Account: account, LiveMode: false, IntentID: intentID, RefundID: refundID,
			Status: "succeeded", PaymentID: paymentID, BookingID: bookingID,
			Generation: 1, AmountCents: amount, SucceededRefundCents: amount,
			Currency: currency, Collected: true, ObservedAt: time.Now().UTC(),
		},
	}
	owned, err := repo.FinishFinancialWork(ctx, claim, finish)
	if err != nil || !owned {
		t.Fatalf("product marker finish owned=%v err=%v", owned, err)
	}
	reconciliation := refundReconciliationSnapshot(t, pool, paymentID)
	bookingFact := readBookingFact(t, bookingPath, bookingID, bookingRun,
		bookingSourceSHA, sourceCommit, sourceTree)
	if bookingFact != "CANCELLED" {
		t.Fatalf("booking reconciliation status=%q", bookingFact)
	}
	sagaDirection := readSagaFact(t, sagaPath, bookingID, sagaRun,
		sagaSourceSHA, sourceCommit, sourceTree)
	if sagaDirection != "compensate" {
		t.Fatalf("saga reconciliation direction=%q", sagaDirection)
	}
	independent, err := gateway.CollectRefundEvidence(ctx, account, intentID)
	if err != nil || len(independent.Refunds) != 1 || independent.Refunds[0].ID != refundID ||
		independent.Refunds[0].Status != "succeeded" ||
		independent.Refunds[0].Amount != amount ||
		independent.Refunds[0].Currency != currency {
		t.Fatalf("independent provider reconciliation failed: %+v err=%v", independent, err)
	}

	refundObject := collection.Refunds[0]
	independentIDs := make([]string, 0, len(independent.Refunds))
	for _, refund := range independent.Refunds {
		independentIDs = append(independentIDs, refund.ID)
	}
	paginationComplete := collection.PaginationComplete
	hasMore := collection.HasMore
	providerObservedAt := float64(collection.ObservedAt.UnixNano()) / 1e9
	observationMap := map[string]any{
		"schemaVersion": 1, "kind": "refund_full_single", "window": cpRefundWindow,
		"source": map[string]any{"sha256": sourceSHA, "commit": sourceCommit, "tree": sourceTree},
		"scope": map[string]any{"project": "rushgo", "campaign": "candidate-refund-chain",
			"runId": runID, "attemptId": attempt, "window": cpRefundWindow,
			"nonce": runID + "-refund-chain"},
		"account": map[string]any{"accountId": account, "mode": "test", "verified": true},
		"paymentIntent": map[string]any{
			"id": collection.Intent.ID, "amount": collection.Intent.Amount,
			"amountReceived": collection.Intent.AmountReceived, "currency": collection.Intent.Currency,
			"accountId": account, "chargeId": collection.Intent.LatestCharge, "livemode": false,
		},
		"charge": map[string]any{
			"id": collection.Charge.ID, "amount": collection.Charge.Amount,
			"currency": collection.Charge.Currency, "accountId": account,
			"paymentIntentId": collection.Charge.PaymentIntent, "livemode": false,
			"paid": true, "captured": true,
		},
		"productSource": map[string]any{
			"managed": source.Managed, "paymentId": source.PaymentID.String(),
			"bookingId": source.BookingID.String(), "generation": source.Generation,
			"providerAccount": source.ProviderAccount, "liveMode": false,
			"amountCents": source.AmountCents, "currency": source.Currency,
			"paymentIntentId": source.PaymentIntentID, "paymentStatus": source.PaymentStatus,
			"providerOutcome": source.ProviderOutcome, "providerStatus": source.ProviderStatus,
			"createState": source.CreateState, "refundProviderId": nil,
		},
		"productCommand": map[string]any{
			"decisionId":  source.RefundCommand.DecisionID.String(),
			"amountCents": source.RefundCommand.AmountCents,
			"currency":    source.RefundCommand.Currency,
			"reason":      source.RefundCommand.Reason, "refundKey": source.RefundCommand.Key,
			"requestHashSha256": hex.EncodeToString(prepared.CommandDigest[:]),
		},
		"refunds": []any{map[string]any{
			"object": refundObject.Object, "id": refundObject.ID, "status": refundObject.Status,
			"amount": refundObject.Amount, "currency": refundObject.Currency,
			"paymentIntent": refundObject.PaymentIntent, "charge": refundObject.Charge,
			"metadata": refundObject.Metadata,
		}},
		"paginationComplete": paginationComplete, "hasMore": hasMore,
		"additionalRefunds":  []any{},
		"providerObservedAt": providerObservedAt, "observationMaxAgeSeconds": 120,
		"audit": map[string]any{
			"response": map[string]any{
				"refundId":        responseRecord.ProviderRefundID,
				"paymentIntentId": responseRecord.PaymentIntentID,
				"chargeId":        responseRecord.Response.ChargeID,
				"amount":          *responseRecord.TargetAmountCents,
				"currency":        responseRecord.Currency, "accountId": responseRecord.IntendedAccount,
				"mode": "test", "recordId": responseAuditID,
			},
			"verifiedEvent": map[string]any{
				"type":              eventRecord.VerifiedEvent.EventType,
				"eventId":           eventRecord.VerifiedEvent.EventID,
				"refundId":          eventRecord.VerifiedEvent.RefundID,
				"chargeId":          eventRecord.VerifiedEvent.ChargeID,
				"paymentIntentId":   eventRecord.VerifiedEvent.PaymentIntentID,
				"signatureVerified": true, "recordId": eventAuditID,
			},
		},
		"markerObservation": map[string]any{
			"paymentId": paymentID.String(), "operationKind": "refund",
			"claimTokenSha256": marker.TokenSHA256, "fence": marker.Fence,
			"leaseUntil": marker.LeaseUntil, "leaseActive": marker.LeaseActive,
			"refundState": marker.RefundState, "refundProviderId": nullableString(marker.ProviderID),
			"providerOutcome": marker.ProviderOutcome, "paymentIntentId": marker.IntentID,
			"observedAt": markerObservedAt, "cancelled": false, "revived": false,
		},
		"ownerObservation": map[string]any{
			"paymentId": paymentID.String(), "operationKind": "refund",
			"claimTokenSha256": owner.TokenSHA256, "fence": owner.Fence,
			"leaseUntil": owner.LeaseUntil, "leaseActive": owner.LeaseActive,
			"refundState": owner.RefundState, "refundProviderId": nullableString(owner.ProviderID),
			"providerOutcome": owner.ProviderOutcome, "paymentIntentId": owner.IntentID,
			"observedAt": ownerObservedAt, "cancelled": false, "revived": false,
		},
		"reconciliation": map[string]any{
			"reader": "PAYMENT_DB_AND_COMPONENT_JSON_INDEPENDENT_READ",
			"status": "SETTLED", "refundState": reconciliation.RefundState,
			"refundProviderStatus": reconciliation.ProviderStatus,
			"refundProviderId":     reconciliation.ProviderID,
			"refundAmountCents":    reconciliation.AmountCents,
			"refundCurrency":       reconciliation.Currency,
			"reconciliationState":  reconciliation.ReconciliationState,
			"bookingStatus":        bookingFact, "sagaDirection": sagaDirection,
			"contradictions": []any{}, "missingObjects": []any{}, "extraObjects": []any{},
			"independentRefundIds":          independentIDs,
			"independentProviderObservedAt": float64(independent.ObservedAt.UnixNano()) / 1e9,
			"observedAt":                    float64(time.Now().UTC().UnixNano()) / 1e9,
			"source": map[string]any{"bookingFact": filepath.Base(bookingPath),
				"sagaFact": filepath.Base(sagaPath), "paymentDb": manifestIdentity(t, pool)},
		},
		"providerFacts": "LOCAL_HTTP_DOUBLE_SIGNED_WEBHOOK_NOT_STRIPE",
		"evidenceMeta": map[string]any{
			"componentOnly": true, "formalEvidence": false,
			"providerOrigin": "SYNTHETIC_LOCAL", "realProviderProvenance": "NOT_VERIFIED",
			"source":     "REAL_PAYMENT_PG16_PRODUCT_COMMAND_AND_LOCAL_HTTP_DOUBLE",
			"pagination": "PER_OBJECT_CURSOR_WALK", "audit": "RESPONSE_AND_VERIFIED_EVENT_PERSISTED",
			"reconciliation": "POST_MARKER_INDEPENDENT_DB_AND_COMPONENT_READ",
			"sourceBinding":  "EMITTER_TEST_FILE_SHA256",
		},
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(evidenceDir, "refund-chain-"+runID+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observationMap); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

type refundMarkerFact struct {
	TokenSHA256, ProviderID, IntentID, ProviderOutcome, RefundState string
	Fence                                                           int64
	LeaseUntil                                                      float64
	LeaseActive                                                     bool
}

func refundMarkerSnapshot(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID, observedAt float64) refundMarkerFact {
	t.Helper()
	var token *uuid.UUID
	var fact refundMarkerFact
	var lease *float64
	if err := pool.QueryRow(context.Background(), `SELECT claim_token,claim_fence,
		EXTRACT(EPOCH FROM lease_until),refund_state,provider_outcome,payment_intent_id,
		COALESCE(refund_provider_id,'') FROM payments WHERE id=$1`, paymentID).Scan(
		&token, &fact.Fence, &lease, &fact.RefundState, &fact.ProviderOutcome,
		&fact.IntentID, &fact.ProviderID); err != nil {
		t.Fatal(err)
	}
	if token == nil || lease == nil {
		t.Fatal("marker token or lease missing")
	}
	digest := sha256.Sum256([]byte(token.String()))
	fact.TokenSHA256 = hex.EncodeToString(digest[:])
	fact.LeaseUntil = *lease
	fact.LeaseActive = *lease > observedAt
	return fact
}

type reconciliationFact struct {
	RefundState, ProviderStatus, ProviderID, Currency, ReconciliationState string
	AmountCents                                                            int64
}

func refundReconciliationSnapshot(t *testing.T, pool *pgxpool.Pool, paymentID uuid.UUID) reconciliationFact {
	t.Helper()
	var fact reconciliationFact
	if err := pool.QueryRow(context.Background(), `SELECT refund_state,
		COALESCE(refund_provider_status,''),COALESCE(refund_provider_id,''),
		refund_amount_cents,refund_currency,reconciliation_state
		FROM payments WHERE id=$1`, paymentID).Scan(&fact.RefundState,
		&fact.ProviderStatus, &fact.ProviderID, &fact.AmountCents,
		&fact.Currency, &fact.ReconciliationState); err != nil {
		t.Fatal(err)
	}
	return fact
}

func refundDBClock(t *testing.T, pool *pgxpool.Pool) float64 {
	t.Helper()
	var clock float64
	if err := pool.QueryRow(context.Background(), `SELECT EXTRACT(EPOCH FROM clock_timestamp())`).Scan(&clock); err != nil {
		t.Fatal(err)
	}
	return clock
}

func readBookingFact(t *testing.T, path string, bookingID uuid.UUID, runID,
	sourceSHA, sourceCommit, sourceTree string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	source, _ := value["source"].(map[string]any)
	if value["bookingId"] != bookingID.String() || value["runId"] != runID ||
		source["sha256"] != sourceSHA || source["commit"] != sourceCommit ||
		source["tree"] != sourceTree {
		t.Fatalf("booking reconciliation file is not bound to this run/source")
	}
	status, _ := value["status"].(string)
	return status
}

func readSagaFact(t *testing.T, path string, bookingID uuid.UUID, runID,
	sourceSHA, sourceCommit, sourceTree string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	identity, _ := value["frozenIdentity"].(map[string]any)
	scope, _ := value["scope"].(map[string]any)
	source, _ := value["source"].(map[string]any)
	if identity["bookingId"] != bookingID.String() || scope["runId"] != runID ||
		source["sha256"] != sourceSHA || source["commit"] != sourceCommit ||
		source["tree"] != sourceTree {
		t.Fatalf("saga reconciliation file is not bound to this booking/run/source")
	}
	if direction, _ := value["productDirection"].(string); direction == "compensate" {
		return direction
	}
	if direction, _ := value["direction"].(string); direction == "compensation" {
		return "compensate"
	}
	return ""
}

func manifestIdentity(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var database, systemID string
	if err := pool.QueryRow(context.Background(), `SELECT current_database(),system_identifier::text
		FROM pg_database CROSS JOIN LATERAL pg_control_system() WHERE datname=current_database()`).Scan(&database, &systemID); err != nil {
		t.Fatal(err)
	}
	return database + "@" + systemID
}

func boolPtr(value bool) *bool { return &value }

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func itoa(value int64) string {
	return fmt.Sprintf("%d", value)
}

func writeLocalJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}
