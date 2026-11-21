//go:build recoverycomponents

package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/recoverycomponent"
)

// TestRefundCandidateEvidenceFromProductSourceAndPreSendAudit emits a
// candidate observation from the real persisted Payment source, the real
// PREPARED audit, and a synthetic Provider object. It deliberately does not
// invent a RESPONSE/VERIFIED_EVENT or a reconciliation: the candidate
// validator must keep those facts UNKNOWN. The emitted file is component-only
// and never formal evidence.
func TestRefundCandidateEvidenceFromProductSourceAndPreSendAudit(t *testing.T) {
	evidenceDir := os.Getenv("REFUND_COMPONENT_EVIDENCE_DIR")
	if evidenceDir == "" {
		t.Skip("refund candidate evidence emission is disabled")
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
		t.Fatal("gated refund evidence requires REFUND_COMPONENT_SOURCE_SHA256/COMMIT/TREE and RUN_ID")
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
	records, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{
		PaymentID: op.PaymentID, Kind: domain.RefundAuditKindPrepared,
	})
	if err != nil || len(records) != 1 {
		t.Fatalf("prepared audit records = %d err=%v; want one", len(records), err)
	}
	prepared := records[0]
	provider := syntheticRefundForSource(source)

	var markerToken *uuid.UUID
	var markerFence int64
	var markerLease *time.Time
	var markerState, markerOutcome, markerIntent, markerRefundID string
	if err := pool.QueryRow(ctx, `SELECT claim_token,claim_fence,lease_until,refund_state,
		provider_outcome,payment_intent_id,COALESCE(refund_provider_id,'')
		FROM payments WHERE id=$1`, op.PaymentID).Scan(
		&markerToken, &markerFence, &markerLease, &markerState, &markerOutcome,
		&markerIntent, &markerRefundID); err != nil {
		t.Fatal(err)
	}
	if markerToken == nil || markerLease == nil {
		t.Fatal("pre-send marker did not retain claim token and lease")
	}
	tokenDigest := sha256.Sum256([]byte(markerToken.String()))
	observedAt := float64(time.Now().UTC().UnixNano()) / 1e9
	leaseUntil := float64(markerLease.UTC().UnixNano()) / 1e9
	metadata := map[string]string{
		"ticketbox_payment_id": source.PaymentID.String(),
		"ticketbox_booking_id": source.BookingID.String(),
		"ticketbox_generation": "1",
	}
	refund := provider.Refund
	liveMode := false
	observation := map[string]any{
		"schemaVersion": 1,
		"kind":          "refund_full_single",
		"window":        cpRefundWindow,
		"source":        map[string]any{"sha256": sourceSHA, "commit": sourceCommit, "tree": sourceTree},
		"scope": map[string]any{
			"project": "rushgo", "campaign": "candidate-refund-component",
			"runId": runID, "attemptId": attempt, "window": cpRefundWindow,
			"nonce": runID + "-refund-source",
		},
		"account": map[string]any{
			"accountId": source.ProviderAccount, "mode": "test", "verified": true,
		},
		"paymentIntent": map[string]any{
			"id": source.PaymentIntentID, "amount": source.AmountCents,
			"amountReceived": provider.PaymentIntent.AmountReceivedCents,
			"currency":       source.Currency, "accountId": source.ProviderAccount,
			"chargeId": provider.Charge.ID, "livemode": liveMode,
		},
		"charge": map[string]any{
			"id": provider.Charge.ID, "amount": provider.Charge.AmountCents,
			"currency": provider.Charge.Currency, "accountId": source.ProviderAccount,
			"paymentIntentId": provider.Charge.PaymentIntentID, "livemode": liveMode,
			"paid": true, "captured": true,
		},
		"productSource": map[string]any{
			"managed": source.Managed, "paymentId": source.PaymentID.String(),
			"bookingId": source.BookingID.String(), "generation": source.Generation,
			"providerAccount": source.ProviderAccount, "liveMode": liveMode,
			"amountCents": source.AmountCents, "currency": source.Currency,
			"paymentIntentId": source.PaymentIntentID, "paymentStatus": source.PaymentStatus,
			"providerOutcome": source.ProviderOutcome, "providerStatus": source.ProviderStatus,
			"createState": source.CreateState, "refundProviderId": markerRefundID,
		},
		"productCommand": map[string]any{
			"decisionId":        source.RefundCommand.DecisionID.String(),
			"amountCents":       source.RefundCommand.AmountCents,
			"currency":          source.RefundCommand.Currency,
			"reason":            source.RefundCommand.Reason,
			"refundKey":         source.RefundCommand.Key,
			"requestHashSha256": hex.EncodeToString(prepared.CommandDigest[:]),
		},
		"refunds": []any{map[string]any{
			"object": "refund", "id": refund.ID, "status": refund.Status,
			"amount": refund.AmountCents, "currency": refund.Currency,
			"paymentIntent": refund.PaymentIntentID, "charge": refund.ChargeID,
			"metadata": metadata,
		}},
		// The product path does not expose a complete Refund list or has_more.
		// Keep both facts explicitly unproven.
		"paginationComplete":       false,
		"hasMore":                  true,
		"additionalRefunds":        []any{},
		"providerObservedAt":       observedAt,
		"observationMaxAgeSeconds": 120,
		"markerObservation": map[string]any{
			"paymentId": source.PaymentID.String(), "operationKind": "refund",
			"claimTokenSha256": hex.EncodeToString(tokenDigest[:]),
			"fence":            markerFence, "leaseUntil": leaseUntil,
			"leaseActive": leaseUntil > observedAt, "refundState": markerState,
			"refundProviderId": markerRefundID, "providerOutcome": markerOutcome,
			"paymentIntentId": markerIntent, "observedAt": observedAt,
			"cancelled": false, "revived": false,
		},
		"ownerObservation": map[string]any{
			"paymentId": source.PaymentID.String(), "operationKind": "refund",
			"claimTokenSha256": hex.EncodeToString(tokenDigest[:]),
			"fence":            markerFence, "leaseUntil": leaseUntil,
			"leaseActive": leaseUntil > observedAt, "refundState": markerState,
			"refundProviderId": markerRefundID, "providerOutcome": markerOutcome,
			"paymentIntentId": markerIntent, "observedAt": observedAt,
			"cancelled": false, "revived": false,
		},
		"evidenceMeta": map[string]any{
			"emitter": "refund_component_presend_audit", "componentOnly": true,
			"formalEvidence": false, "providerFacts": "SYNTHETIC_SINGLE_OBJECT",
			"pagination": "NOT_EXPOSED_BY_PRODUCT", "audit": "PREPARED_ONLY",
			"reconciliation": "NOT_IN_PAYMENT_DB", "source": "REAL_PG16_COMPONENT",
			"sourceBinding":         "EMITTER_TEST_FILE_SHA256",
			"preparedAuditRecordId": prepared.RecordID.String(),
			"preparedAttemptId":     prepared.AttemptID.String(),
		},
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(evidenceDir, "refund-source-"+runID+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create refund candidate evidence: %v", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observation); err != nil {
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

const cpRefundWindow = "saga.late_collection.compensation_verified_full_refund"
