package service

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

var ErrPaidRecoverySnapshotUnavailable = errors.New("paid recovery Payment snapshot unavailable")

// PaidRecoveryPaymentSnapshot is a read-only, Saga-only projection. Admission
// evidence is current only for the exact Payment facts read before and after
// the provider check; a concurrent local change downgrades it to UNKNOWN or
// DENIED so Saga recovery cannot rely on stale collection evidence.
type PaidRecoveryPaymentSnapshot struct {
	Financial                *domain.FinancialView
	ProviderIdentityVerified bool
	Admission                domain.AdmissionProviderEvidence
}

func (p *PaymentService) GetPaidRecoveryPaymentSnapshot(ctx context.Context, paymentID, bookingID uuid.UUID, generation int64) (*PaidRecoveryPaymentSnapshot, error) {
	if paymentID == uuid.Nil || bookingID == uuid.Nil || generation != 1 {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	if p == nil || p.repo == nil {
		return nil, ErrPaidRecoverySnapshotUnavailable
	}
	initial, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	if initial == nil || initial.Payment == nil {
		return nil, ErrPaidRecoverySnapshotUnavailable
	}

	var evidence domain.AdmissionProviderEvidence
	if p.liveMode {
		evidence = deniedPaidRecoveryEvidence("local_provider_scope_mismatch")
	} else {
		evidence = p.CheckAdmissionFinancialEligibility(ctx, paymentID, bookingID, generation)
	}

	latest, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	if latest == nil || latest.Payment == nil {
		return nil, ErrPaidRecoverySnapshotUnavailable
	}
	identityVerified := paidRecoveryProviderIdentityVerified(latest, paymentID, bookingID, generation, p.account, p.liveMode)
	if !samePaidRecoveryEligibilityBasis(initial, latest) {
		evidence = paidRecoveryEvidenceAfterLocalChange(latest, paymentID, bookingID, generation, p.account, p.liveMode)
	}
	return &PaidRecoveryPaymentSnapshot{
		Financial: latest, ProviderIdentityVerified: identityVerified, Admission: evidence,
	}, nil
}

func paidRecoveryProviderIdentityVerified(view *domain.FinancialView, paymentID, bookingID uuid.UUID, generation int64, account string, liveMode bool) bool {
	if view == nil || view.Payment == nil || view.Payment.ID != paymentID || view.Payment.BookingId == nil || *view.Payment.BookingId != bookingID ||
		generation != 1 || view.Generation != generation || !view.Managed || account == "" || view.ProviderAccount != account || liveMode ||
		view.LiveMode == nil || *view.LiveMode || view.Payment.Price < 1 || view.Payment.Currency != "usd" || view.Payment.PaymentIntentId == "" {
		return false
	}
	return true
}

func samePaidRecoveryEligibilityBasis(a, b *domain.FinancialView) bool {
	if a == nil || b == nil || a.Payment == nil || b.Payment == nil {
		return false
	}
	return a.Payment.ID == b.Payment.ID && a.Payment.UserId == b.Payment.UserId && sameUUIDPointer(a.Payment.BookingId, b.Payment.BookingId) &&
		a.Payment.Status == b.Payment.Status && a.Payment.Price == b.Payment.Price && a.Payment.Currency == b.Payment.Currency &&
		a.Payment.PaymentIntentId == b.Payment.PaymentIntentId && a.Generation == b.Generation && a.Managed == b.Managed &&
		a.ProviderAccount == b.ProviderAccount && sameBoolPointer(a.LiveMode, b.LiveMode) &&
		a.ProviderOutcome == b.ProviderOutcome && a.ProviderStatus == b.ProviderStatus && a.BookingDisposition == b.BookingDisposition &&
		a.CreateState == b.CreateState && a.CancelState == b.CancelState && a.ReconciliationState == b.ReconciliationState &&
		a.RefundState == b.RefundState && a.RefundID == b.RefundID && a.RefundStatus == b.RefundStatus &&
		a.RefundAmountCents == b.RefundAmountCents && sameUUIDPointer(a.RefundDecisionID, b.RefundDecisionID)
}

func sameUUIDPointer(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameBoolPointer(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func paidRecoveryEvidenceAfterLocalChange(view *domain.FinancialView, paymentID, bookingID uuid.UUID, generation int64, account string, liveMode bool) domain.AdmissionProviderEvidence {
	if !paidRecoveryProviderIdentityVerified(view, paymentID, bookingID, generation, account, liveMode) {
		return deniedPaidRecoveryEvidence("local_identity_conflict")
	}
	if view.RefundDecisionID != nil || view.RefundAmountCents > 0 || view.RefundState == "succeeded" || view.RefundID != "" || view.RefundStatus != "" ||
		view.ReconciliationState == "refund_required" {
		return deniedPaidRecoveryEvidence("local_refund_present")
	}
	if view.RefundState != "not_required" {
		return unknownPaidRecoveryEvidence("local_refund_state_unverified")
	}
	return unknownPaidRecoveryEvidence("local_financial_state_changed_during_provider_check")
}

func deniedPaidRecoveryEvidence(reason string) domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityDenied,
		DisputeState:     domain.DisputeEvidenceUnknown,
		RefundState:      domain.RefundEvidenceUnknown,
		ReasonCode:       reason,
	}
}

func unknownPaidRecoveryEvidence(reason string) domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityUnknown,
		DisputeState:     domain.DisputeEvidenceUnknown,
		RefundState:      domain.RefundEvidenceUnknown,
		ReasonCode:       reason,
	}
}
