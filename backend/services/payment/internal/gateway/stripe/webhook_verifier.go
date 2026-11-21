package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/stripe/stripe-go/v84/webhook"
	"github.com/ticketbox/payment/internal/domain"
	gateways "github.com/ticketbox/payment/internal/gateway"
)

func (s *StripePaymentGateway) SetWebhookScope(account string, live bool) {
	s.webhookAccount, s.webhookLive = account, live
}

func (s *StripePaymentGateway) VerifyWebhook(ctx context.Context, body []byte, signature string) (*domain.ProviderReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(body) > 65536 || s.webhookAccount == "" || s.webhookLive || s.webhookSecret == "" {
		return nil, gateways.ErrInvalidWebhook
	}
	event, err := webhook.ConstructEventWithOptions(body, signature, s.webhookSecret, webhook.ConstructEventOptions{IgnoreAPIVersionMismatch: true})
	if err != nil || event.ID == "" || event.Object != "event" || event.Livemode != s.webhookLive || (event.Account != "" && event.Account != s.webhookAccount) {
		return nil, gateways.ErrInvalidWebhook
	}
	kind := string(event.Type)
	switch kind {
	case "payment_intent.succeeded", "payment_intent.payment_failed", "payment_intent.canceled", "refund.created", "refund.updated", "refund.failed", "charge.refunded":
	default:
		return nil, gateways.ErrIrrelevantWebhook
	}
	o := &domain.ProviderObservation{Account: s.webhookAccount, LiveMode: s.webhookLive, EventID: event.ID, ObservedAt: time.Unix(event.Created, 0)}
	if o.ObservedAt.Unix() <= 0 {
		return nil, gateways.ErrInvalidWebhook
	}
	r := &domain.ProviderReceipt{Observation: *o, EventType: kind, CorrelationState: "pending", RelayState: "not_applicable", ReceivedAt: time.Now()}
	if kind == "payment_intent.succeeded" || kind == "payment_intent.payment_failed" {
		r.RelayState = "pending"
	}
	mode := "test"
	r.ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+s.webhookAccount+":"+mode+":"+event.ID))
	r.EnvelopeID = r.ID.String()
	r.RefundAudit = verifiedRefundEventAudit(body, r.ID, event.ID, kind, event.Created, event.Account, event.Request)
	var object struct {
		Object, ID, Status, Currency string
		Livemode                     *bool             `json:"livemode"`
		Amount                       int64             `json:"amount"`
		AmountRefunded               int64             `json:"amount_refunded"`
		AmountReceived               int64             `json:"amount_received"`
		PaymentIntent                json.RawMessage   `json:"payment_intent"`
		Charge                       json.RawMessage   `json:"charge"`
		Metadata                     map[string]string `json:"metadata"`
	}
	if event.Data == nil || json.Unmarshal(event.Data.Raw, &object) != nil {
		r.CorrelationState = "attention"
		r.RelayState = "not_applicable"
		r.Observation.SafeObject = json.RawMessage(`{"diagnostic":"unexpected_provider_object"}`)
		r.Observation.EvidenceHash = sha256.Sum256(r.Observation.SafeObject)
		return r, nil
	}
	if object.Livemode != nil && *object.Livemode != s.webhookLive {
		return nil, gateways.ErrInvalidWebhook
	}
	o = &r.Observation
	o.Status, o.Currency, o.AmountCents = object.Status, object.Currency, object.Amount
	o.PaymentID, _ = uuid.Parse(object.Metadata["ticketbox_payment_id"])
	o.BookingID, _ = uuid.Parse(object.Metadata["ticketbox_booking_id"])
	o.Generation, _ = strconv.ParseInt(object.Metadata["ticketbox_generation"], 10, 64)
	expected := "payment_intent"
	if kind == "charge.refunded" {
		expected = "charge"
	} else if kind == "refund.created" || kind == "refund.updated" || kind == "refund.failed" {
		expected = "refund"
	}
	// Refund objects omit livemode in Stripe's schema. Only use an explicitly
	// present mode from the signature-verified event envelope as fallback.
	if expected == "refund" && object.Livemode == nil {
		var scope struct {
			Livemode *bool `json:"livemode"`
		}
		if json.Unmarshal(body, &scope) == nil && scope.Livemode != nil {
			object.Livemode = scope.Livemode
		}
	}
	if expected == "payment_intent" {
		o.IntentID = object.ID
		o.Collected = kind == "payment_intent.succeeded" && object.Status == "succeeded" && object.AmountReceived == object.Amount
	} else {
		_ = json.Unmarshal(object.PaymentIntent, &o.IntentID)
		if o.IntentID == "" {
			var expanded struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(object.PaymentIntent, &expanded)
			o.IntentID = expanded.ID
		}
		if expected == "refund" {
			o.RefundID = object.ID
		}
	}
	safe := map[string]any{"id": object.ID, "object": object.Object, "status": object.Status, "amount": object.Amount, "currency": object.Currency, "livemode": object.Livemode, "metadata": map[string]string{"ticketbox_payment_id": object.Metadata["ticketbox_payment_id"], "ticketbox_booking_id": object.Metadata["ticketbox_booking_id"], "ticketbox_generation": object.Metadata["ticketbox_generation"]}}
	if expected == "payment_intent" {
		safe["amount_received"] = object.AmountReceived
	} else {
		safe["payment_intent"] = o.IntentID
		if expected == "charge" {
			safe["amount_refunded"] = object.AmountRefunded
		}
	}
	o.SafeObject, _ = json.Marshal(safe)
	o.EvidenceHash = sha256.Sum256(o.SafeObject)
	if object.Object != expected || object.ID == "" || object.Livemode == nil || o.IntentID == "" || object.Amount < 1 || object.Currency != "usd" || (kind == "payment_intent.succeeded" && !o.Collected) {
		r.CorrelationState = "attention"
		r.RelayState = "not_applicable"
	}
	return r, nil
}

func verifiedRefundEventAudit(body []byte, receiptID uuid.UUID, eventID, eventType string, created int64, account string, request *stripego.EventRequest) *domain.VerifiedRefundEventAudit {
	switch eventType {
	case "refund.created", "refund.updated", "refund.failed", "charge.refunded":
	default:
		return nil
	}
	audit := &domain.VerifiedRefundEventAudit{
		ReceiptID: receiptID, EventID: eventID, EventType: eventType,
		LinkClassification: domain.RefundAuditLinkUncorrelated,
	}
	if eventID == "" || receiptID == uuid.Nil {
		return nil
	}
	if account != "" {
		observedAccount := account
		audit.ObservedProviderAccount = &observedAccount
	}
	var eventScope struct {
		Livemode *bool `json:"livemode"`
	}
	if json.Unmarshal(body, &eventScope) == nil && eventScope.Livemode != nil {
		mode := *eventScope.Livemode
		audit.ObservedLiveMode = &mode
	}
	if request != nil {
		audit.ProviderRequestID = request.ID
		if request.IdempotencyKey != "" {
			digest := sha256.Sum256([]byte(request.IdempotencyKey))
			value := hex.EncodeToString(digest[:])
			audit.IdempotencyKeyDigestHex = &value
		}
	}
	if created > 0 {
		eventTime := time.Unix(created, 0).UTC()
		audit.EventCreatedAt = &eventTime
	}
	observedAt := time.Now().UTC()
	audit.ObservedAt = &observedAt
	var object struct {
		Object         *string         `json:"object"`
		ID             *string         `json:"id"`
		Status         *string         `json:"status"`
		Currency       *string         `json:"currency"`
		Amount         *int64          `json:"amount"`
		AmountRefunded *int64          `json:"amount_refunded"`
		PaymentIntent  json.RawMessage `json:"payment_intent"`
		Charge         json.RawMessage `json:"charge"`
	}
	var envelope struct {
		Data struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		_ = json.Unmarshal(envelope.Data.Object, &object)
	}
	if object.Object != nil {
		audit.ObjectType = *object.Object
	}
	if object.ID != nil {
		audit.ObjectID = *object.ID
		if audit.ObjectType == "refund" {
			audit.RefundID = *object.ID
		}
		if audit.ObjectType == "charge" {
			audit.ChargeID = *object.ID
		}
	}
	if object.Status != nil {
		audit.ProviderStatus = *object.Status
	}
	if object.Currency != nil {
		audit.Currency = *object.Currency
	}
	if object.Amount != nil {
		amount := *object.Amount
		if eventType == "charge.refunded" && object.AmountRefunded != nil {
			amount = *object.AmountRefunded
		}
		audit.AmountCents = &amount
	} else if eventType == "charge.refunded" && object.AmountRefunded != nil {
		amount := *object.AmountRefunded
		audit.AmountCents = &amount
	}
	audit.PaymentIntentID = stripeObjectID(object.PaymentIntent)
	if object.Charge != nil && audit.ChargeID == "" {
		audit.ChargeID = stripeObjectID(object.Charge)
	}
	return audit
}
