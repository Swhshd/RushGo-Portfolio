package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v84/webhook"
	payment_gateway "github.com/ticketbox/payment/internal/gateway"
	"go.uber.org/zap"
)

func TestVerifiedRefundWebhookCapturesSeparateSignedSidecar(t *testing.T) {
	const secret = "whsec_synthetic_fixture"
	requestKey := "ticketbox:payment:synthetic:refund"
	payload := syntheticRefundWebhookPayload(t, map[string]any{
		"id": "evt_synthetic_refund", "object": "event", "type": "refund.created", "created": time.Now().Unix(), "livemode": false,
		"request": map[string]string{"id": "req_synthetic_event", "idempotency_key": requestKey},
		"data": map[string]any{"object": map[string]any{
			"id": "re_synthetic_event", "object": "refund", "payment_intent": "pi_synthetic_event", "charge": "ch_synthetic_event",
			"amount": 725, "currency": "usd", "status": "pending", "livemode": false,
			"metadata": map[string]string{"ticketbox_payment_id": "synthetic-hint"},
		}},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), secret, nil, nil)
	provider.SetWebhookScope("acct_configured", false)

	receipt, err := provider.VerifyWebhook(context.Background(), payload.Payload, payload.Header)
	if err != nil || receipt == nil {
		t.Fatalf("VerifyWebhook returned receipt=%+v err=%v", receipt, err)
	}
	sidecar := receipt.RefundAudit
	if sidecar == nil {
		t.Fatal("verified refund event had no audit sidecar")
	}
	if sidecar.ReceiptID != receipt.ID || sidecar.EventID != receipt.Observation.EventID || sidecar.EventType != "refund.created" || sidecar.ObjectType != "refund" || sidecar.ObjectID != "re_synthetic_event" || sidecar.RefundID != "re_synthetic_event" || sidecar.ChargeID != "ch_synthetic_event" || sidecar.PaymentIntentID != "pi_synthetic_event" || sidecar.ProviderRequestID != "req_synthetic_event" {
		t.Fatalf("signed event fields not captured: %+v", sidecar)
	}
	if sidecar.ObservedProviderAccount != nil || sidecar.ObservedLiveMode == nil || *sidecar.ObservedLiveMode {
		t.Fatalf("configured account was promoted or signed mode was lost: %+v", sidecar)
	}
	if sidecar.IdempotencyKeyDigestHex == nil || *sidecar.IdempotencyKeyDigestHex != func() string { sum := sha256.Sum256([]byte(requestKey)); return hex.EncodeToString(sum[:]) }() {
		t.Fatalf("request idempotency key was not safely digested: %+v", sidecar.IdempotencyKeyDigestHex)
	}
	if receipt.Observation.Account != "acct_configured" {
		t.Fatalf("existing receipt account changed: %q", receipt.Observation.Account)
	}
	if receipt.Observation.EvidenceHash != sha256.Sum256(receipt.Observation.SafeObject) || strings.Contains(string(receipt.Observation.SafeObject), "req_synthetic_event") || strings.Contains(string(receipt.Observation.SafeObject), requestKey) {
		t.Fatalf("sidecar changed normalized receipt evidence: object=%s", receipt.Observation.SafeObject)
	}
	serialized, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(serialized), requestKey) || strings.Contains(string(serialized), "refund_audit") {
		t.Fatalf("sidecar or raw request key leaked through receipt serialization: %s err=%v", serialized, err)
	}
}

func TestVerifiedRefundWebhookKeepsMissingAccountModeAndRequestUnknown(t *testing.T) {
	payload := syntheticRefundWebhookPayload(t, map[string]any{
		"id": "evt_missing_scope", "object": "event", "type": "refund.updated", "created": time.Now().Unix(),
		"data": map[string]any{"object": map[string]any{
			"id": "re_missing_scope", "object": "refund", "payment_intent": "pi_missing_scope", "amount": 725, "currency": "usd", "status": "succeeded", "livemode": false,
		}},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "whsec_synthetic_fixture", nil, nil)
	provider.SetWebhookScope("acct_configured", false)

	receipt, err := provider.VerifyWebhook(context.Background(), payload.Payload, payload.Header)
	if err != nil || receipt == nil || receipt.RefundAudit == nil {
		t.Fatalf("VerifyWebhook returned receipt=%+v err=%v", receipt, err)
	}
	if receipt.RefundAudit.ObservedProviderAccount != nil || receipt.RefundAudit.ObservedLiveMode != nil || receipt.RefundAudit.ProviderRequestID != "" || receipt.RefundAudit.IdempotencyKeyDigestHex != nil {
		t.Fatalf("missing signed fields were inferred: %+v", receipt.RefundAudit)
	}
	if receipt.Observation.Account != "acct_configured" {
		t.Fatalf("existing Observation.Account unexpectedly changed: %q", receipt.Observation.Account)
	}
}

func TestVerifiedNonRefundWebhookDoesNotGainRefundAuditSidecar(t *testing.T) {
	payload := syntheticRefundWebhookPayload(t, map[string]any{
		"id": "evt_payment_succeeded", "object": "event", "type": "payment_intent.succeeded", "created": time.Now().Unix(), "livemode": false,
		"data": map[string]any{"object": map[string]any{
			"id": "pi_verified_payment", "object": "payment_intent", "status": "succeeded", "amount": 725, "amount_received": 725, "currency": "usd", "livemode": false,
			"metadata": map[string]string{"ticketbox_payment_id": "11111111-1111-4111-8111-111111111111", "ticketbox_booking_id": "22222222-2222-4222-8222-222222222222", "ticketbox_generation": "1"},
		}},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "whsec_synthetic_fixture", nil, nil)
	provider.SetWebhookScope("acct_configured", false)

	receipt, err := provider.VerifyWebhook(context.Background(), payload.Payload, payload.Header)
	if err != nil || receipt == nil {
		t.Fatalf("VerifyWebhook returned receipt=%+v err=%v", receipt, err)
	}
	if receipt.RefundAudit != nil {
		t.Fatalf("non-refund event gained refund sidecar: %+v", receipt.RefundAudit)
	}
}

func TestInvalidWebhookSignatureDoesNotProduceRefundSidecar(t *testing.T) {
	payload := syntheticRefundWebhookPayload(t, map[string]any{
		"id": "evt_invalid_signature", "object": "event", "type": "refund.created", "created": time.Now().Unix(), "livemode": false,
		"data": map[string]any{"object": map[string]any{"id": "re_invalid", "object": "refund", "payment_intent": "pi_invalid", "amount": 725, "currency": "usd", "status": "pending", "livemode": false}},
	})
	provider := NewStripePaymentGateway(zap.NewNop(), "whsec_synthetic_fixture", nil, nil)
	provider.SetWebhookScope("acct_configured", false)

	receipt, err := provider.VerifyWebhook(context.Background(), payload.Payload, "t=1,v1=invalid")
	if receipt != nil || err == nil || err != payment_gateway.ErrInvalidWebhook {
		t.Fatalf("invalid signature returned receipt=%+v err=%v", receipt, err)
	}
}

func syntheticRefundWebhookPayload(t *testing.T, event map[string]any) *webhook.SignedPayload {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: body, Secret: "whsec_synthetic_fixture"})
}

func TestRefundWebhookUsesExplicitSignedEnvelopeMode(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		envelope                 any
		objectMode               any
		wantError, wantAttention bool
	}{
		{"refund omits mode", false, nil, false, false},
		{"missing both stays unknown", nil, nil, false, true},
		{"live envelope rejected", true, nil, true, false},
		{"conflicting object rejected", false, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := map[string]any{"id": "re_real_shape", "object": "refund", "payment_intent": "pi_real_shape", "amount": 100, "currency": "usd", "status": "succeeded"}
			event := map[string]any{"id": "evt_real_shape", "object": "event", "type": "refund.created", "created": time.Now().Unix(), "data": map[string]any{"object": obj}}
			if tc.envelope != nil {
				event["livemode"] = tc.envelope
			}
			if tc.objectMode != nil {
				obj["livemode"] = tc.objectMode
			}
			signed := syntheticRefundWebhookPayload(t, event)
			provider := NewStripePaymentGateway(zap.NewNop(), "whsec_synthetic_fixture", nil, nil)
			provider.SetWebhookScope("acct_configured", false)
			receipt, err := provider.VerifyWebhook(context.Background(), signed.Payload, signed.Header)
			if tc.wantError {
				if err == nil || receipt != nil {
					t.Fatal("expected rejection")
				}
				return
			}
			if err != nil || receipt == nil {
				t.Fatalf("receipt=%+v err=%v", receipt, err)
			}
			if (receipt.CorrelationState == "attention") != tc.wantAttention {
				t.Fatalf("correlation=%s", receipt.CorrelationState)
			}
			if !tc.wantAttention {
				var normalized map[string]any
				if err := json.Unmarshal(receipt.Observation.SafeObject, &normalized); err != nil {
					t.Fatal(err)
				}
				if normalized["livemode"] != false {
					t.Fatal("signed TEST mode not normalized")
				}
			}
		})
	}
}
