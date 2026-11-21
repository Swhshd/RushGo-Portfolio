package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
)

func (s *StripePaymentGateway) CancelIntent(ctx context.Context, c domain.FinancialClaim, id string) (*domain.ProviderObservation, error) {
	if c.Kind != "cancel" || c.Key != "ticketbox:payment:"+c.PaymentID.String()+":g1:cancel" || c.Operation.Identity.LiveMode {
		return nil, errors.New("invalid cancel operation")
	}
	params := &stripego.PaymentIntentCancelParams{}
	params.SetIdempotencyKey(c.Key)
	pi, err := s.stripeClient.V1PaymentIntents.Cancel(ctx, id, params)
	if err != nil {
		return nil, errors.New("provider cancel outcome unknown")
	}
	o, err := intentObservation(pi, c.Operation.Identity.ProviderAccount, false)
	if err != nil {
		return nil, err
	}
	if o.PaymentID != c.PaymentID || o.BookingID != c.Operation.Identity.BookingID || o.AmountCents != c.Operation.Identity.AmountCents || o.Currency != c.Operation.Identity.Currency {
		return nil, errors.New("cancel identity mismatch")
	}
	return o, nil
}

func (s *StripePaymentGateway) CreateFullRefund(ctx context.Context, c domain.FinancialClaim, id string) (*domain.ProviderObservation, error) {
	if c.Kind != "refund" || c.Refund == nil || c.Refund.AmountCents != c.Operation.Identity.AmountCents || c.Refund.Currency != c.Operation.Identity.Currency || c.Key != c.Refund.Key || c.Key != "ticketbox:payment:"+c.PaymentID.String()+":g1:refund" || c.Operation.Identity.LiveMode {
		if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
			capture.SetResult(domain.RefundAuditAdapterFailure, "invalid_refund_operation")
		}
		return nil, errors.New("invalid frozen full-refund operation")
	}
	params := &stripego.RefundCreateParams{Amount: stripego.Int64(c.Refund.AmountCents), PaymentIntent: stripego.String(id), Metadata: map[string]string{"ticketbox_payment_id": c.PaymentID.String(), "ticketbox_booking_id": c.Operation.Identity.BookingID.String(), "ticketbox_generation": "1"}}
	params.SetIdempotencyKey(c.Key)
	r, err := s.stripeClient.V1Refunds.Create(ctx, params)
	recordRefundSDKResponse(ctx, r)
	if err != nil {
		if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
			capture.SetResult(domain.RefundAuditOutcomeUnknown, "provider_refund_unknown")
		}
		return nil, errors.New("provider refund outcome unknown")
	}
	if r == nil {
		if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
			capture.SetResult(domain.RefundAuditAdapterFailure, "incomplete_response")
		}
		return nil, errors.New("provider refund response incomplete")
	}
	o, err := s.refundObservation(ctx, c.Operation.Identity.ProviderAccount, false, r)
	if err != nil {
		if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
			capture.SetResult(domain.RefundAuditAdapterFailure, "refund_association_failed")
		}
		return nil, err
	}
	if o.PaymentID != c.PaymentID || o.BookingID != c.Operation.Identity.BookingID || o.IntentID != id || r.Amount != c.Refund.AmountCents || o.Currency != c.Refund.Currency {
		if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
			capture.SetResult(domain.RefundAuditAdapterFailure, "refund_identity_mismatch")
		}
		return nil, errors.New("refund identity mismatch")
	}
	if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
		capture.SetResult(domain.RefundAuditResponseObserved, "")
	}
	// A created object is not cumulative full-refund verification.
	return o, nil
}

// recordRefundSDKResponse preserves SDK object facts before follow-up mapping
// can fail. A configured transport observer takes precedence because it can
// distinguish absent fields and capture each actual HTTP RoundTrip.
func recordRefundSDKResponse(ctx context.Context, refund *stripego.Refund) {
	capture := domain.RefundResponseCaptureFromContext(ctx)
	if capture == nil || refund == nil {
		return
	}
	if source := capture.Snapshot().SummarySource; source == "http_response_allowlist" || source == "mixed" {
		return
	}
	response := domain.RefundAuditResponse{SummarySource: "sdk_refund_object"}
	observedAt := time.Now().UTC()
	response.ObservedAt = &observedAt
	if refund.LastResponse != nil {
		response.ProviderRequestID = refund.LastResponse.RequestID
	}
	response.RefundID = refund.ID
	response.ProviderStatus = string(refund.Status)
	response.Currency = string(refund.Currency)
	if refund.Amount > 0 {
		amount := refund.Amount
		response.AmountCents = &amount
	}
	if refund.PaymentIntent != nil {
		response.PaymentIntentID = refund.PaymentIntent.ID
	}
	if refund.Charge != nil {
		response.ChargeID = refund.Charge.ID
	}
	capture.RecordResponse(response)
}

func (s *StripePaymentGateway) RetrieveRefund(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, error) {
	if live || id == "" {
		return nil, errors.New("invalid refund retrieval scope")
	}
	r, err := s.stripeClient.V1Refunds.Retrieve(ctx, id, nil)
	if err != nil {
		return nil, errors.New("provider refund retrieval unavailable")
	}
	return s.refundObservation(ctx, account, live, r)
}

func (s *StripePaymentGateway) refundObservation(ctx context.Context, account string, live bool, r *stripego.Refund) (*domain.ProviderObservation, error) {
	if r == nil || r.ID == "" || r.PaymentIntent == nil || r.PaymentIntent.ID == "" {
		return nil, errors.New("refund provider association missing")
	}
	o, _, err := s.RetrieveIntent(ctx, account, live, r.PaymentIntent.ID)
	if err != nil {
		return nil, err
	}
	if !o.Collected || r.Amount < 1 || r.Amount > o.AmountCents || string(r.Currency) != o.Currency {
		return nil, errors.New("refund amount or collection mismatch")
	}
	for key, want := range map[string]string{"ticketbox_payment_id": o.PaymentID.String(), "ticketbox_booking_id": o.BookingID.String(), "ticketbox_generation": "1"} {
		if actual := r.Metadata[key]; actual != "" && actual != want {
			return nil, errors.New("refund metadata conflict")
		}
	}
	o.RefundID, o.Status = r.ID, string(r.Status)
	o.SafeObject, _ = json.Marshal(map[string]any{"id": r.ID, "payment_intent": o.IntentID, "status": r.Status, "amount": r.Amount, "currency": r.Currency, "livemode": live})
	o.EvidenceHash = sha256.Sum256(o.SafeObject)
	return o, nil
}

func (s *StripePaymentGateway) InspectSucceededRefunds(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, error) {
	o, _, err := s.RetrieveIntent(ctx, account, live, id)
	if err != nil {
		return nil, err
	}
	if !o.Collected {
		return o, nil
	}
	sum := int64(0)
	count := 0
	pendingFull := false
	bad := false
	partial := false
	ids := make(map[string]bool)
	safe := []map[string]any{}
	for r, err := range s.stripeClient.V1Refunds.List(ctx, &stripego.RefundListParams{PaymentIntent: stripego.String(id), ListParams: stripego.ListParams{Limit: stripego.Int64(100)}}) {
		if err != nil {
			return nil, errors.New("provider cumulative refunds unavailable")
		}
		count++
		if count > 500 {
			return nil, errors.New("provider cumulative refund bound exceeded")
		}
		if r == nil || r.ID == "" || ids[r.ID] || r.PaymentIntent == nil || r.PaymentIntent.ID != id || string(r.Currency) != o.Currency || r.Amount < 1 || r.Amount > o.AmountCents {
			return nil, errors.New("provider cumulative refund mismatch")
		}
		ids[r.ID] = true
		for key, want := range map[string]string{"ticketbox_payment_id": o.PaymentID.String(), "ticketbox_booking_id": o.BookingID.String(), "ticketbox_generation": "1"} {
			if actual := r.Metadata[key]; actual != "" && actual != want {
				return nil, errors.New("provider cumulative refund metadata conflict")
			}
		}
		if r.Status == stripego.RefundStatusSucceeded {
			if sum > o.AmountCents-r.Amount {
				return nil, errors.New("provider cumulative refund overflow")
			}
			sum += r.Amount
		}
		if r.Amount < o.AmountCents {
			partial = true
		}
		if r.Amount == o.AmountCents && r.Status == stripego.RefundStatusPending {
			pendingFull = true
			o.RefundID = r.ID
		}
		if r.Status == stripego.RefundStatusFailed || r.Status == stripego.RefundStatusCanceled || r.Status == stripego.RefundStatusRequiresAction {
			bad = true
			o.RefundID = r.ID
		}
		if r.Status == stripego.RefundStatusSucceeded && r.Amount == o.AmountCents {
			o.RefundID = r.ID
		}
		safe = append(safe, map[string]any{"id": r.ID, "status": r.Status, "amount": r.Amount, "currency": r.Currency})
	}
	o.SucceededRefundCents = sum
	switch {
	case sum == o.AmountCents:
		o.Status = "succeeded"
	case partial:
		o.Status = "partial_attention"
	case bad:
		o.Status = "provider_attention"
	case pendingFull:
		o.Status = "pending"
	case count == 0:
		o.Status = "no_refunds"
	default:
		o.Status = "provider_attention"
	}
	o.SafeObject, _ = json.Marshal(map[string]any{"payment_intent": id, "succeeded_refund_cents": sum, "currency": o.Currency, "livemode": live, "refunds": safe})
	o.EvidenceHash = sha256.Sum256(o.SafeObject)
	return o, nil
}
