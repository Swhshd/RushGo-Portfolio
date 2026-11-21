package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

type admissionPaymentRepository struct {
	repository.PaymentRepositoryInterface
	view *domain.FinancialView
	err  error
}

func (r admissionPaymentRepository) GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error) {
	return r.view, r.err
}

type admissionEvidenceProvider struct {
	evidence domain.AdmissionProviderEvidence
	identity domain.AdmissionFinancialIdentity
	calls    int
}

func (p *admissionEvidenceProvider) CheckAdmissionEvidence(_ context.Context, identity domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	p.calls++
	p.identity = identity
	return p.evidence
}

func TestCheckAdmissionFinancialEligibilityRequiresManagedCollectedRefundClearPayment(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	mode := false
	provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(), Source: "stripe_test_api", ReasonCode: "provider_observed_clear",
	}}
	view := &domain.FinancialView{
		Payment:    &domain.Payment{ID: paymentID, BookingId: &bookingID, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", PaymentIntentId: "pi_admission_fixture"},
		Generation: 1, Managed: true, ProviderAccount: "acct_fixture", LiveMode: &mode,
		ProviderOutcome: "collected", CreateState: "create_ready", ReconciliationState: "sale_verified",
		RefundState: "not_required", CancelState: "not_requested",
	}
	s := &PaymentService{repo: admissionPaymentRepository{view: view}, admission: provider, account: "acct_fixture"}
	got := s.CheckAdmissionFinancialEligibility(context.Background(), paymentID, bookingID, 1)
	if got.EligibilityState != domain.AdmissionEligibilityEligible || got.DisputeState != domain.DisputeEvidenceNoDispute || provider.calls != 1 {
		t.Fatalf("eligible managed Payment did not reach provider evidence: result=%+v calls=%d", got, provider.calls)
	}
	if provider.identity.PaymentID != paymentID || provider.identity.BookingID != bookingID || provider.identity.Generation != 1 || provider.identity.PaymentIntentID != "pi_admission_fixture" {
		t.Fatalf("provider did not receive trusted local identity: %+v", provider.identity)
	}
}

func TestCheckAdmissionFinancialEligibilityFailsClosedBeforeProviderForLocalDenialOrUnknown(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*domain.FinancialView)
		wantState  domain.AdmissionEligibilityState
		wantReason string
	}{
		{name: "refund already recorded", mutate: func(v *domain.FinancialView) { v.RefundState = "succeeded" }, wantState: domain.AdmissionEligibilityDenied, wantReason: "local_refund_present"},
		{name: "legacy unmanaged", mutate: func(v *domain.FinancialView) { v.Managed = false }, wantState: domain.AdmissionEligibilityUnknown, wantReason: "local_financial_identity_unverified"},
		{name: "missing live mode", mutate: func(v *domain.FinancialView) { v.LiveMode = nil }, wantState: domain.AdmissionEligibilityUnknown, wantReason: "local_financial_identity_unverified"},
	} {
		t.Run(test.name, func(t *testing.T) {
			paymentID, bookingID := uuid.New(), uuid.New()
			mode := false
			view := &domain.FinancialView{
				Payment:    &domain.Payment{ID: paymentID, BookingId: &bookingID, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", PaymentIntentId: "pi_admission_fixture"},
				Generation: 1, Managed: true, ProviderAccount: "acct_fixture", LiveMode: &mode,
				ProviderOutcome: "collected", CreateState: "create_ready", ReconciliationState: "sale_verified",
				RefundState: "not_required", CancelState: "not_requested",
			}
			test.mutate(view)
			provider := &admissionEvidenceProvider{}
			s := &PaymentService{repo: admissionPaymentRepository{view: view}, admission: provider, account: "acct_fixture"}
			got := s.CheckAdmissionFinancialEligibility(context.Background(), paymentID, bookingID, 1)
			if got.EligibilityState != test.wantState || got.ReasonCode != test.wantReason || provider.calls != 0 {
				t.Fatalf("local gate=%+v calls=%d, want state=%s reason=%s calls=0", got, provider.calls, test.wantState, test.wantReason)
			}
		})
	}
}

func TestCheckAdmissionFinancialEligibilityRejectsMismatchedRequestIdentity(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	provider := &admissionEvidenceProvider{}
	s := &PaymentService{repo: admissionPaymentRepository{}, admission: provider, account: "acct_fixture"}
	got := s.CheckAdmissionFinancialEligibility(context.Background(), paymentID, bookingID, 2)
	if got.EligibilityState != domain.AdmissionEligibilityDenied || got.ReasonCode != "request_identity_invalid" || provider.calls != 0 {
		t.Fatalf("invalid request was not denied before provider read: result=%+v calls=%d", got, provider.calls)
	}
}
