package service

import (
	"context"
	"errors"
	"github.com/ticketbox/pkg/providerevidence"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
)

var ErrApprovedUnusedRefundAuthorityUnavailable = errors.New("approved unused refund authority unavailable")

const unusedRefundDecisionStatusApproved = "APPROVED"

// ApprovedUnusedRefundDecisionReader is the narrow Booking-owned authority
// used for this refund type. It deliberately does not reuse lifecycle terminal
// decisions or the legacy RequestPaymentResolution contract.
type ApprovedUnusedRefundDecisionReader interface {
	GetApprovedUnusedRefundDecision(context.Context, uuid.UUID, uuid.UUID) (*bookingv1.ApprovedUnusedRefundDecision, error)
}

type bookingApprovedUnusedRefundDecisionReader struct {
	client bookingv1.BookingServiceClient
}

func (r bookingApprovedUnusedRefundDecisionReader) GetApprovedUnusedRefundDecision(ctx context.Context, bookingID, decisionID uuid.UUID) (*bookingv1.ApprovedUnusedRefundDecision, error) {
	return r.client.GetApprovedUnusedRefundDecision(ctx, &bookingv1.GetApprovedUnusedRefundDecisionRequest{
		BookingId: bookingID.String(), RefundDecisionId: decisionID.String(),
	})
}

// AcceptApprovedUnusedRefundDecision consumes a Booking decision only after
// re-reading Booking's immutable approval proof and matching it to Payment's
// managed TEST collection. The repository then serializes the unique refund
// obligation against the current Payment row.
func (p *PaymentService) AcceptApprovedUnusedRefundDecision(ctx context.Context, bookingID, decisionID, paymentID uuid.UUID, generation int64) (*domain.FinancialView, error) {
	if p == nil || p.repo == nil || bookingID == uuid.Nil || decisionID == uuid.Nil || paymentID == uuid.Nil || generation != 1 {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	if p.approvedUnusedRefundDecisionReader == nil {
		return nil, ErrApprovedUnusedRefundAuthorityUnavailable
	}

	authorityCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	proof, err := p.approvedUnusedRefundDecisionReader.GetApprovedUnusedRefundDecision(authorityCtx, bookingID, decisionID)
	cancel()
	if err != nil {
		return nil, err
	}

	financial, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	obligation, identity, err := approvedUnusedRefundObligation(proof, financial, bookingID, decisionID, paymentID, generation, p.account, p.liveMode)
	if err != nil {
		return nil, err
	}
	acceptor, ok := p.repo.(repository.ApprovedUnusedRefundRepository)
	if !ok {
		return nil, ErrApprovedUnusedRefundAuthorityUnavailable
	}
	return acceptor.RequireApprovedUnusedRefund(ctx, obligation, identity)
}

// GetUnusedRefundPaymentSnapshot returns Payment-owned identity and refund
// state to Booking. It is safe for recovery polling: a recorded refund
// decision disables approval eligibility without making another provider
// read. The only provider call is the existing read-only eligibility check
// while no refund obligation exists.
func (p *PaymentService) GetUnusedRefundPaymentSnapshot(ctx context.Context, bookingID, paymentID uuid.UUID, generation int64, forApproval bool) (*domain.FinancialView, bool, error) {
	if p == nil || p.repo == nil || bookingID == uuid.Nil || paymentID == uuid.Nil || generation != 1 {
		return nil, false, repository.ErrInvalidFinancialIdentity
	}
	view, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		return nil, false, err
	}
	if !validUnusedRefundPaymentSnapshot(view, bookingID, paymentID, generation, p.account, p.liveMode) {
		return nil, false, repository.ErrFinancialConflict
	}
	if !forApproval || hasRefundObligation(view) {
		return view, false, nil
	}

	evidence := p.CheckAdmissionFinancialEligibility(ctx, paymentID, bookingID, generation)
	if evidence.EligibilityState != domain.AdmissionEligibilityEligible || evidence.DisputeState != domain.DisputeEvidenceNoDispute ||
		evidence.RefundState != domain.RefundEvidenceNoRefund || evidence.ObservedAt.IsZero() || !providerevidence.AllowedSource(evidence.Source) {
		return view, false, nil
	}

	// The provider read is external to the Payment row transaction. Re-read
	// local state after it so a concurrently created refund cannot still be
	// represented as eligible for approval.
	latest, err := p.repo.GetFinancial(ctx, paymentID)
	if err != nil {
		return nil, false, err
	}
	if !validUnusedRefundPaymentSnapshot(latest, bookingID, paymentID, generation, p.account, p.liveMode) || hasRefundObligation(latest) {
		return latest, false, nil
	}
	return latest, true, nil
}

func approvedUnusedRefundObligation(proof *bookingv1.ApprovedUnusedRefundDecision, financial *domain.FinancialView, bookingID, decisionID, paymentID uuid.UUID, generation int64, configuredAccount string, configuredLiveMode bool) (domain.RefundObligation, repository.ApprovedUnusedRefundIdentity, error) {
	invalid := func() (domain.RefundObligation, repository.ApprovedUnusedRefundIdentity, error) {
		return domain.RefundObligation{}, repository.ApprovedUnusedRefundIdentity{}, repository.ErrFinancialConflict
	}
	if proof == nil || proof.GetStatus() != unusedRefundDecisionStatusApproved || financial == nil || financial.Payment == nil {
		return invalid()
	}
	proofDecision, err := uuid.Parse(proof.GetRefundDecisionId())
	if err != nil || proofDecision == uuid.Nil || proofDecision != decisionID {
		return invalid()
	}
	proofBooking, err := uuid.Parse(proof.GetBookingId())
	if err != nil || proofBooking != bookingID {
		return invalid()
	}
	proofBuyer, err := uuid.Parse(proof.GetBuyerId())
	if err != nil || proofBuyer == uuid.Nil {
		return invalid()
	}
	proofEvent, err := uuid.Parse(proof.GetEventId())
	if err != nil || proofEvent == uuid.Nil {
		return invalid()
	}
	proofPayment, err := uuid.Parse(proof.GetPaymentId())
	if err != nil || proofPayment != paymentID {
		return invalid()
	}
	approver, err := uuid.Parse(proof.GetApproverId())
	if err != nil || approver == uuid.Nil || approver == proofBuyer {
		return invalid()
	}
	if proof.GetGeneration() != generation || proof.GetAmountCents() < 1 || proof.GetCurrency() != "usd" ||
		proof.GetPaymentIntentId() == "" || proof.GetProviderAccount() == "" || proof.GetLivemode() || configuredLiveMode || configuredAccount == "" {
		return invalid()
	}
	approvedAt, eventStartsAt := proof.GetApprovedAt(), proof.GetEventStartsAt()
	if approvedAt == nil || eventStartsAt == nil || approvedAt.CheckValid() != nil || eventStartsAt.CheckValid() != nil ||
		approvedAt.AsTime().IsZero() || eventStartsAt.AsTime().IsZero() {
		return invalid()
	}
	qualificationAttemptID, eventQualifiedAt := proof.GetQualificationAttemptId(), proof.GetEventQualifiedAt()
	if qualificationAttemptID == "" && eventQualifiedAt == nil {
		if !approvedAt.AsTime().UTC().Before(eventStartsAt.AsTime().UTC()) {
			return invalid()
		}
	} else {
		attemptID, err := uuid.Parse(qualificationAttemptID)
		if qualificationAttemptID == "" || eventQualifiedAt == nil || err != nil || attemptID == uuid.Nil || attemptID != decisionID ||
			eventQualifiedAt.CheckValid() != nil || eventQualifiedAt.AsTime().IsZero() ||
			!eventQualifiedAt.AsTime().UTC().Before(eventStartsAt.AsTime().UTC()) || !isCanonicalPositiveInteger(proof.GetEventAuthorityVersion()) {
			return invalid()
		}
	}
	if financial.Payment.ID != paymentID || financial.Payment.BookingId == nil || *financial.Payment.BookingId != bookingID ||
		financial.Payment.UserId != proofBuyer || financial.Generation != generation || !financial.Managed ||
		financial.ProviderOutcome != "collected" || financial.Payment.Status != domain.PaymentSuccess ||
		financial.Payment.Price < 1 || proof.GetAmountCents() != int64(financial.Payment.Price) ||
		financial.Payment.Currency != "usd" || proof.GetCurrency() != financial.Payment.Currency ||
		financial.Payment.PaymentIntentId == "" || proof.GetPaymentIntentId() != financial.Payment.PaymentIntentId ||
		financial.ProviderAccount != configuredAccount || proof.GetProviderAccount() != financial.ProviderAccount ||
		financial.LiveMode == nil || *financial.LiveMode || proof.GetLivemode() != *financial.LiveMode {
		return invalid()
	}
	if financial.BookingDisposition != "CONFIRMED" {
		return invalid()
	}

	identity := repository.ApprovedUnusedRefundIdentity{
		BookingID: bookingID, UserID: proofBuyer, Generation: generation,
		ProviderAccount: financial.ProviderAccount, LiveMode: *financial.LiveMode,
		AmountCents: int64(financial.Payment.Price), Currency: financial.Payment.Currency,
		PaymentIntentID: financial.Payment.PaymentIntentId,
	}
	obligation := domain.RefundObligation{
		PaymentID: paymentID, DecisionID: decisionID, Generation: generation,
		AmountCents: identity.AmountCents, Currency: identity.Currency,
		Reason: domain.RefundReasonConfirmedUnusedTicket,
		Key:    "ticketbox:payment:" + paymentID.String() + ":g1:refund",
	}
	return obligation, identity, nil
}

func isCanonicalPositiveInteger(value string) bool {
	if len(value) == 0 || value[0] < '1' || value[0] > '9' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func validUnusedRefundPaymentSnapshot(view *domain.FinancialView, bookingID, paymentID uuid.UUID, generation int64, configuredAccount string, configuredLiveMode bool) bool {
	return view != nil && view.Payment != nil && view.Payment.ID == paymentID && view.Payment.BookingId != nil &&
		*view.Payment.BookingId == bookingID && view.Generation == generation && view.Managed &&
		view.Payment.UserId != uuid.Nil && view.ProviderAccount != "" && view.ProviderAccount == configuredAccount &&
		view.LiveMode != nil && *view.LiveMode == configuredLiveMode && !configuredLiveMode &&
		view.BookingDisposition == "CONFIRMED" && view.Payment.Price > 0 && view.Payment.Currency == "usd" && view.Payment.PaymentIntentId != ""
}

func hasRefundObligation(view *domain.FinancialView) bool {
	return view == nil || view.RefundDecisionID != nil || view.RefundState != "not_required" || view.RefundAmountCents != 0 ||
		view.RefundReason != "" || view.RefundID != "" || view.RefundStatus != ""
}
