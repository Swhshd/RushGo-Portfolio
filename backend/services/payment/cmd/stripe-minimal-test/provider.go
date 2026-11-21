//go:build stripe_minimal_test

package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	pgateway "github.com/ticketbox/payment/internal/gateway"
)

type boundedProvider struct {
	next                 pgateway.FinancialProvider
	state                *stateStore
	approvedUnusedRefund approvedUnusedRefundAcceptor
	verifyAppliedReceipt func(context.Context, runState) error
}

type approvedUnusedRefundAcceptor interface {
	AcceptApprovedUnusedRefundDecision(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*domain.FinancialView, error)
}

func (p *boundedProvider) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, errors.New("persisted TEST financial operation required")
}
func (p *boundedProvider) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, errors.New("durable signed TEST webhook handler required")
}
func (p *boundedProvider) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return errors.New("persisted TEST refund obligation required")
}

func (p *boundedProvider) CreateIntent(ctx context.Context, op domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	if err := p.state.reserve(op); err != nil {
		return nil, "", err
	}
	o, secret, err := p.next.CreateIntent(ctx, op)
	if err != nil {
		_ = p.state.unknownCreate()
		return nil, "", errors.New("TEST payment outcome unknown; run stopped")
	}
	if o == nil {
		_ = p.state.unknownCreate()
		return nil, "", errors.New("TEST payment evidence incomplete; run stopped")
	}
	if err := p.state.created(o); err != nil {
		return nil, "", err
	}
	return o, secret, nil
}
func (p *boundedProvider) RetrieveIntent(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, string, error) {
	if err := p.state.permitIntent(id); err != nil {
		return nil, "", err
	}
	if account != approvedAccount || live {
		return nil, "", errors.New("outside approved TEST scope")
	}
	return p.next.RetrieveIntent(ctx, account, live, id)
}
func (p *boundedProvider) FindIntent(ctx context.Context, op domain.CreateOperation) ([]domain.ProviderObservation, error) {
	if err := p.state.permitOperation(op); err != nil {
		return nil, err
	}
	return p.next.FindIntent(ctx, op)
}
func (p *boundedProvider) CancelIntent(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error) {
	return nil, errors.New("cancel operation blocked by one-charge TEST harness")
}
func (p *boundedProvider) CreateFullRefund(ctx context.Context, c domain.FinancialClaim, id string) (*domain.ProviderObservation, error) {
	mode, ok := configuredRuntimeMode(p.state.snapshot().RuntimeMode)
	if !ok {
		return nil, errors.New("TEST refund runtime mode invalid")
	}
	authorityValidated := false
	if mode == runtimeModeDefaultWorker {
		if err := p.validateApprovedUnusedRefund(ctx, c, id); err != nil {
			return nil, err
		}
		authorityValidated = true
	}
	if err := p.state.reserveRefund(c, id, authorityValidated); err != nil {
		return nil, err
	}
	o, err := p.next.CreateFullRefund(ctx, c, id)
	if err != nil {
		_ = p.state.unknownRefund()
		return nil, errors.New("TEST refund outcome unknown; run stopped")
	}
	if o == nil {
		_ = p.state.unknownRefund()
		return nil, errors.New("TEST refund evidence incomplete; run stopped")
	}
	if err := p.state.refundCreated(o); err != nil {
		return nil, err
	}
	return o, nil
}

func (p *boundedProvider) validateApprovedUnusedRefund(ctx context.Context, c domain.FinancialClaim, intentID string) error {
	s := p.state.snapshot()
	if err := validateDefaultWorkerRefundClaim(s, c, intentID); err != nil {
		return err
	}
	if p.verifyAppliedReceipt == nil {
		return errors.New("durable signed TEST receipt verifier unavailable")
	}
	if err := p.verifyAppliedReceipt(ctx, s); err != nil {
		return errors.New("durable signed TEST receipt is not applied")
	}
	if p.approvedUnusedRefund == nil {
		return errors.New("approved-unused refund authority unavailable")
	}
	decisionID := c.Refund.DecisionID
	view, err := p.approvedUnusedRefund.AcceptApprovedUnusedRefundDecision(ctx,
		c.Operation.Identity.BookingID, decisionID, c.PaymentID, c.Operation.Identity.Generation)
	if err != nil || !approvedUnusedRefundAcceptedViewMatches(s, c, view) {
		return errors.New("durable approved-unused refund decision does not match the worker claim")
	}
	if err := p.state.confirmBackgroundReceiptApplied(view); err != nil {
		return errors.New("durable TEST payment receipt could not be confirmed")
	}
	return nil
}

func approvedUnusedRefundAcceptedViewMatches(s runState, c domain.FinancialClaim, view *domain.FinancialView) bool {
	if c.Refund == nil || view == nil || view.Payment == nil || view.Payment.BookingId == nil || view.LiveMode == nil {
		return false
	}
	return view.Payment.ID.String() == s.PaymentID && view.Payment.BookingId.String() == s.BookingID &&
		view.Payment.UserId.String() == s.ExpectedUserID && view.Payment.Status == domain.PaymentSuccess &&
		view.Payment.Price == 100 && view.Payment.Currency == "usd" && view.Payment.PaymentIntentId == s.PaymentIntentID &&
		view.Generation == 1 && view.Managed && view.ProviderAccount == s.StripeAccount && !*view.LiveMode &&
		view.ProviderOutcome == "collected" && view.BookingDisposition == "CONFIRMED" &&
		view.RefundState == "claimed" && view.RefundAmountCents == 100 && view.RefundReason == domain.RefundReasonConfirmedUnusedTicket &&
		view.RefundDecisionID != nil && *view.RefundDecisionID == c.Refund.DecisionID &&
		view.RefundID == "" && view.RefundStatus == "" && view.ReconciliationState == "refund_required"
}
func (p *boundedProvider) RetrieveRefund(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, error) {
	if err := p.state.permitRefund(id); err != nil {
		return nil, err
	}
	if account != approvedAccount || live {
		return nil, errors.New("outside approved TEST scope")
	}
	return p.next.RetrieveRefund(ctx, account, live, id)
}
func (p *boundedProvider) InspectSucceededRefunds(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, error) {
	if err := p.state.permitIntent(id); err != nil {
		return nil, err
	}
	if account != approvedAccount || live {
		return nil, errors.New("outside approved TEST scope")
	}
	return p.next.InspectSucceededRefunds(ctx, account, live, id)
}
func (p *boundedProvider) VerifyWebhook(ctx context.Context, body []byte, signature string) (*domain.ProviderReceipt, error) {
	r, err := p.next.VerifyWebhook(ctx, body, signature)
	if err != nil || r == nil {
		return r, err
	}
	if err = p.state.webhook(*r); err != nil {
		return nil, pgateway.ErrInvalidWebhook
	}
	return r, nil
}
func (p *boundedProvider) CheckAdmissionEvidence(ctx context.Context, i domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	unknown := func(reason string) domain.AdmissionProviderEvidence {
		return domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityUnknown,
			DisputeState:     domain.DisputeEvidenceUnknown,
			RefundState:      domain.RefundEvidenceUnknown,
			ReasonCode:       reason,
		}
	}
	if i.ProviderAccount != approvedAccount || i.LiveMode || i.AmountCents != 100 || i.Currency != "usd" || i.Generation != 1 {
		return unknown("outside_approved_test_scope")
	}
	if err := p.state.permitAdmission(i); err != nil {
		return unknown("outside_one_payment_scope")
	}
	reader, ok := p.next.(pgateway.AdmissionEvidenceProvider)
	if !ok {
		return unknown("provider_evidence_unavailable")
	}
	return reader.CheckAdmissionEvidence(ctx, i)
}
