package service

import (
	"context"
	"github.com/ticketbox/pkg/providerevidence"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

func (p *PaymentService) CheckAdmissionFinancialEligibility(ctx context.Context, paymentID, bookingID uuid.UUID, generation int64) domain.AdmissionProviderEvidence {
	unknown := func(reason string) domain.AdmissionProviderEvidence {
		return domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityUnknown,
			DisputeState:     domain.DisputeEvidenceUnknown,
			RefundState:      domain.RefundEvidenceUnknown,
			ReasonCode:       reason,
		}
	}
	denied := func(reason string) domain.AdmissionProviderEvidence {
		return domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityDenied,
			DisputeState:     domain.DisputeEvidenceUnknown,
			RefundState:      domain.RefundEvidenceUnknown,
			ReasonCode:       reason,
		}
	}
	if paymentID == uuid.Nil || bookingID == uuid.Nil || generation != 1 {
		return denied("request_identity_invalid")
	}
	if p.repo == nil {
		return unknown("local_payment_unavailable")
	}
	view, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		if err == repository.ErrFinancialConflict || err == repository.ErrInvalidFinancialIdentity {
			return denied("local_identity_conflict")
		}
		return unknown("local_payment_unavailable")
	}
	if view == nil || view.Payment == nil {
		return unknown("local_payment_unavailable")
	}
	if view.Payment.ID != paymentID || view.Payment.BookingId == nil || *view.Payment.BookingId != bookingID || view.Generation != generation {
		return denied("local_identity_conflict")
	}
	if !view.Managed || view.ProviderAccount == "" || view.LiveMode == nil {
		return unknown("local_financial_identity_unverified")
	}
	if p.account == "" || view.ProviderAccount != p.account || *view.LiveMode {
		return denied("local_provider_scope_mismatch")
	}
	if view.RefundState == "succeeded" || view.RefundAmountCents > 0 {
		return denied("local_refund_present")
	}
	if view.RefundState != "not_required" {
		return unknown("local_refund_state_unverified")
	}
	if view.ReconciliationState == "refund_required" {
		return denied("local_refund_present")
	}
	if view.ReconciliationState != "sale_verified" || view.ProviderOutcome != "collected" || view.CreateState != "create_ready" || view.CancelState != "not_requested" {
		return unknown("local_financial_state_unverified")
	}
	if view.Payment.Status != domain.PaymentSuccess {
		return denied("local_payment_not_collected")
	}
	if view.Payment.Price < 1 || view.Payment.Currency != "usd" || view.Payment.PaymentIntentId == "" || p.admission == nil {
		return unknown("provider_read_unavailable")
	}

	identity := domain.AdmissionFinancialIdentity{
		PaymentID:       paymentID,
		BookingID:       bookingID,
		Generation:      generation,
		ProviderAccount: view.ProviderAccount,
		LiveMode:        *view.LiveMode,
		PaymentIntentID: view.Payment.PaymentIntentId,
		AmountCents:     int64(view.Payment.Price),
		Currency:        view.Payment.Currency,
	}
	providerCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	evidence := p.admission.CheckAdmissionEvidence(providerCtx, identity)
	if evidence.EligibilityState == domain.AdmissionEligibilityUnknown {
		if evidence.ReasonCode == "" {
			evidence.ReasonCode = "provider_evidence_unknown"
		}
		return evidence
	}
	if evidence.DisputeState == domain.DisputeEvidencePresent {
		evidence.EligibilityState = domain.AdmissionEligibilityDenied
		if evidence.ReasonCode == "" || evidence.ReasonCode == "provider_observed_clear" {
			evidence.ReasonCode = "provider_dispute_present"
		}
		return evidence
	}
	if evidence.RefundState == domain.RefundEvidencePresent {
		evidence.EligibilityState = domain.AdmissionEligibilityDenied
		if evidence.ReasonCode == "" || evidence.ReasonCode == "provider_observed_clear" {
			evidence.ReasonCode = "provider_refund_present"
		}
		return evidence
	}
	if evidence.EligibilityState == domain.AdmissionEligibilityEligible &&
		(evidence.DisputeState != domain.DisputeEvidenceNoDispute || evidence.RefundState != domain.RefundEvidenceNoRefund ||
			evidence.ObservedAt.IsZero() || !providerevidence.AllowedSource(evidence.Source)) {
		return unknown("provider_evidence_incomplete")
	}
	if evidence.EligibilityState == domain.AdmissionEligibilityDenied {
		return evidence
	}
	if evidence.EligibilityState != domain.AdmissionEligibilityEligible {
		return unknown("provider_evidence_incomplete")
	}
	return evidence
}
