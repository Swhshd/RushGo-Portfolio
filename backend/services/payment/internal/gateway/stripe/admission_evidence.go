package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
)

const admissionEvidencePageLimit = 100

// SetAdmissionReadBackend configures the read-only Stripe TEST surface. It is
// deliberately separate from webhook verification and is not set on the
// webhook-only gateway.
func (s *StripePaymentGateway) SetAdmissionReadBackend(backend stripego.Backend, apiKey string) {
	if backend == nil || (!strings.HasPrefix(apiKey, "sk_test_") && !strings.HasPrefix(apiKey, "rk_test_")) {
		s.admissionBackend, s.admissionAPIKey = nil, ""
		return
	}
	s.admissionBackend, s.admissionAPIKey = backend, apiKey
}

type admissionPaymentIntent struct {
	stripego.APIResource
	ID             *string           `json:"id"`
	Object         *string           `json:"object"`
	Status         *string           `json:"status"`
	Amount         *int64            `json:"amount"`
	AmountReceived *int64            `json:"amount_received"`
	Currency       *string           `json:"currency"`
	Livemode       *bool             `json:"livemode"`
	LatestCharge   json.RawMessage   `json:"latest_charge"`
	Metadata       map[string]string `json:"metadata"`
}

type admissionCharge struct {
	ID             *string         `json:"id"`
	Object         *string         `json:"object"`
	PaymentIntent  json.RawMessage `json:"payment_intent"`
	Amount         *int64          `json:"amount"`
	AmountCaptured *int64          `json:"amount_captured"`
	AmountRefunded *int64          `json:"amount_refunded"`
	Currency       *string         `json:"currency"`
	Livemode       *bool           `json:"livemode"`
	Status         *string         `json:"status"`
	Paid           *bool           `json:"paid"`
	Captured       *bool           `json:"captured"`
	Disputed       *bool           `json:"disputed"`
}

type admissionChargeList struct {
	stripego.APIResource
	Object  *string             `json:"object"`
	HasMore *bool               `json:"has_more"`
	Data    *[]*admissionCharge `json:"data"`
}

type admissionDispute struct {
	ID            *string         `json:"id"`
	Object        *string         `json:"object"`
	Charge        json.RawMessage `json:"charge"`
	PaymentIntent json.RawMessage `json:"payment_intent"`
	Status        *string         `json:"status"`
	Livemode      *bool           `json:"livemode"`
}

type admissionDisputeList struct {
	stripego.APIResource
	Object  *string              `json:"object"`
	HasMore *bool                `json:"has_more"`
	Data    *[]*admissionDispute `json:"data"`
}

func (s *StripePaymentGateway) CheckAdmissionEvidence(ctx context.Context, identity domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	unknown := func(reason string) domain.AdmissionProviderEvidence {
		return domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityUnknown,
			DisputeState:     domain.DisputeEvidenceUnknown,
			RefundState:      domain.RefundEvidenceUnknown,
			ReasonCode:       reason,
		}
	}
	if s.admissionBackend == nil || s.admissionAPIKey == "" {
		return unknown("provider_read_unconfigured")
	}
	if identity.PaymentID == uuid.Nil || identity.BookingID == uuid.Nil || identity.Generation != 1 ||
		identity.ProviderAccount == "" || identity.ProviderAccount != s.webhookAccount || identity.LiveMode ||
		!safeStripeObjectID(identity.PaymentIntentID, "pi_") || identity.AmountCents < 1 || identity.AmountCents > 2147483647 || identity.Currency != "usd" {
		return unknown("payment_identity_invalid")
	}

	var intent admissionPaymentIntent
	params := &stripego.Params{Context: ctx}
	if err := s.admissionBackend.Call("GET", "/v1/payment_intents/"+identity.PaymentIntentID, s.admissionAPIKey, params, &intent); err != nil {
		return unknown("provider_read_unavailable")
	}
	if intent.ID == nil || intent.Object == nil || intent.Status == nil || intent.Amount == nil || intent.AmountReceived == nil ||
		intent.Currency == nil || intent.Livemode == nil || len(intent.LatestCharge) == 0 || intent.Metadata == nil ||
		*intent.ID != identity.PaymentIntentID || *intent.Object != "payment_intent" || *intent.Status != "succeeded" ||
		*intent.Amount != identity.AmountCents || *intent.AmountReceived != identity.AmountCents || *intent.Currency != identity.Currency || *intent.Livemode ||
		intent.Metadata["ticketbox_payment_id"] != identity.PaymentID.String() ||
		intent.Metadata["ticketbox_booking_id"] != identity.BookingID.String() || intent.Metadata["ticketbox_generation"] != strconv.FormatInt(identity.Generation, 10) {
		return unknown("provider_payment_mismatch")
	}
	latestChargeID, ok := providerObjectID(intent.LatestCharge)
	if !ok || !safeStripeObjectID(latestChargeID, "ch_") {
		return unknown("provider_payment_mismatch")
	}

	chargeIDs := make(map[string]struct{})
	var successfulChargeID string
	var successfulCharges int
	var sawRefund, sawDispute bool
	if err := s.walkAdmissionCharges(ctx, identity, chargeIDs, func(charge *admissionCharge) error {
		if charge.AmountRefunded != nil && *charge.AmountRefunded > 0 {
			sawRefund = true
		}
		if charge.Disputed != nil && *charge.Disputed {
			sawDispute = true
		}
		if *charge.Status == "succeeded" {
			if !*charge.Paid || !*charge.Captured || *charge.AmountCaptured != identity.AmountCents || *charge.Amount != identity.AmountCents {
				return errors.New("charge capture mismatch")
			}
			successfulCharges++
			successfulChargeID = *charge.ID
		} else if *charge.Paid || *charge.Captured || *charge.AmountCaptured != 0 {
			return errors.New("unexpected captured charge")
		}
		return nil
	}); err != nil || successfulCharges != 1 || successfulChargeID != latestChargeID {
		return unknown("provider_charge_evidence_incomplete")
	}

	if err := s.walkAdmissionDisputes(ctx, identity.PaymentIntentID, chargeIDs, func(*admissionDispute) { sawDispute = true }); err != nil {
		return unknown("provider_dispute_evidence_incomplete")
	}

	evidence := domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(),
		Source:           "stripe_test_api",
		ReasonCode:       "provider_observed_clear",
	}
	if sawDispute {
		evidence.EligibilityState = domain.AdmissionEligibilityDenied
		evidence.DisputeState = domain.DisputeEvidencePresent
		evidence.ReasonCode = "provider_dispute_present"
	}
	if sawRefund {
		evidence.EligibilityState = domain.AdmissionEligibilityDenied
		evidence.RefundState = domain.RefundEvidencePresent
		if !sawDispute {
			evidence.ReasonCode = "provider_refund_present"
		}
	}
	return evidence
}

func (s *StripePaymentGateway) walkAdmissionCharges(ctx context.Context, identity domain.AdmissionFinancialIdentity, ids map[string]struct{}, visit func(*admissionCharge) error) error {
	cursor := ""
	seenCursors := map[string]struct{}{}
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		values := url.Values{"payment_intent": {identity.PaymentIntentID}, "limit": {strconv.Itoa(admissionEvidencePageLimit)}}
		if cursor != "" {
			values.Set("starting_after", cursor)
		}
		params := &stripego.Params{Context: ctx, Extra: &stripego.ExtraValues{Values: values}}
		var page admissionChargeList
		if err := s.admissionBackend.Call("GET", "/v1/charges", s.admissionAPIKey, params, &page); err != nil {
			return err
		}
		if page.Object == nil || *page.Object != "list" || page.HasMore == nil || page.Data == nil || len(*page.Data) > admissionEvidencePageLimit {
			return errors.New("malformed charge page")
		}
		if *page.Data == nil {
			return errors.New("null charge page")
		}
		for _, charge := range *page.Data {
			if charge == nil || charge.ID == nil || charge.Object == nil || len(charge.PaymentIntent) == 0 || charge.Amount == nil ||
				charge.AmountCaptured == nil || charge.AmountRefunded == nil || charge.Currency == nil || charge.Livemode == nil ||
				charge.Status == nil || charge.Paid == nil || charge.Captured == nil || charge.Disputed == nil ||
				!safeStripeObjectID(*charge.ID, "ch_") || *charge.Object != "charge" || *charge.Livemode || *charge.Amount < 0 ||
				*charge.Amount != identity.AmountCents || *charge.AmountCaptured < 0 || *charge.AmountCaptured > *charge.Amount ||
				*charge.AmountRefunded < 0 || *charge.AmountRefunded > *charge.Amount || *charge.Currency != identity.Currency ||
				(*charge.Status != "succeeded" && *charge.Status != "pending" && *charge.Status != "failed") {
				return errors.New("malformed charge")
			}
			piID, ok := providerObjectID(charge.PaymentIntent)
			if !ok || piID != identity.PaymentIntentID {
				return errors.New("charge identity mismatch")
			}
			if _, duplicate := ids[*charge.ID]; duplicate {
				return errors.New("duplicate charge")
			}
			ids[*charge.ID] = struct{}{}
			if err := visit(charge); err != nil {
				return err
			}
		}
		if !*page.HasMore {
			return nil
		}
		if len(*page.Data) == 0 {
			return errors.New("empty charge page with has_more")
		}
		cursor = *(*page.Data)[len(*page.Data)-1].ID
		if _, repeated := seenCursors[cursor]; repeated {
			return errors.New("charge cursor did not advance")
		}
		seenCursors[cursor] = struct{}{}
	}
	return errors.New("charge pagination bound exceeded")
}

func (s *StripePaymentGateway) walkAdmissionDisputes(ctx context.Context, paymentIntentID string, chargeIDs map[string]struct{}, visit func(*admissionDispute)) error {
	cursor := ""
	seenCursors, disputeIDs := map[string]struct{}{}, map[string]struct{}{}
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		values := url.Values{"payment_intent": {paymentIntentID}, "limit": {strconv.Itoa(admissionEvidencePageLimit)}}
		if cursor != "" {
			values.Set("starting_after", cursor)
		}
		params := &stripego.Params{Context: ctx, Extra: &stripego.ExtraValues{Values: values}}
		var page admissionDisputeList
		if err := s.admissionBackend.Call("GET", "/v1/disputes", s.admissionAPIKey, params, &page); err != nil {
			return err
		}
		if page.Object == nil || *page.Object != "list" || page.HasMore == nil || page.Data == nil || len(*page.Data) > admissionEvidencePageLimit {
			return errors.New("malformed dispute page")
		}
		if *page.Data == nil {
			return errors.New("null dispute page")
		}
		for _, dispute := range *page.Data {
			if dispute == nil || dispute.ID == nil || dispute.Object == nil || len(dispute.Charge) == 0 || len(dispute.PaymentIntent) == 0 ||
				dispute.Status == nil || dispute.Livemode == nil || !safeStripeObjectID(*dispute.ID, "du_") || *dispute.Object != "dispute" || *dispute.Livemode || *dispute.Status == "" {
				return errors.New("malformed dispute")
			}
			chargeID, ok := providerObjectID(dispute.Charge)
			if !ok {
				return errors.New("dispute charge missing")
			}
			if _, linked := chargeIDs[chargeID]; !linked {
				return errors.New("dispute charge is not linked to payment")
			}
			if string(dispute.PaymentIntent) != "null" {
				linkedPI, ok := providerObjectID(dispute.PaymentIntent)
				if !ok || linkedPI != paymentIntentID {
					return errors.New("dispute payment intent mismatch")
				}
			}
			if _, duplicate := disputeIDs[*dispute.ID]; duplicate {
				return errors.New("duplicate dispute")
			}
			disputeIDs[*dispute.ID] = struct{}{}
			visit(dispute)
		}
		if !*page.HasMore {
			return nil
		}
		if len(*page.Data) == 0 {
			return errors.New("empty dispute page with has_more")
		}
		cursor = *(*page.Data)[len(*page.Data)-1].ID
		if _, repeated := seenCursors[cursor]; repeated {
			return errors.New("dispute cursor did not advance")
		}
		seenCursors[cursor] = struct{}{}
	}
	return errors.New("dispute pagination bound exceeded")
}

func providerObjectID(raw json.RawMessage) (string, bool) {
	var id string
	if err := json.Unmarshal(raw, &id); err == nil && id != "" {
		return id, true
	}
	var object struct {
		ID *string `json:"id"`
	}
	if err := json.Unmarshal(raw, &object); err != nil || object.ID == nil || *object.ID == "" {
		return "", false
	}
	return *object.ID, true
}

func safeStripeObjectID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) <= len(prefix) {
		return false
	}
	for _, r := range value[len(prefix):] {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' {
			return false
		}
	}
	return true
}
