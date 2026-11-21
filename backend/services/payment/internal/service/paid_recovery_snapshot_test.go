package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

type paidRecoverySnapshotRepository struct {
	repository.PaymentRepositoryInterface
	views []*domain.FinancialView
	calls int
}

func (r *paidRecoverySnapshotRepository) GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error) {
	index := r.calls
	r.calls++
	if index >= len(r.views) {
		index = len(r.views) - 1
	}
	return r.views[index], nil
}

func TestPaidRecoverySnapshotReturnsFreshProviderEligibilityAndVerifiedTestIdentity(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	view := paidRecoveryFinancialView(paymentID, bookingID)
	repo := &paidRecoverySnapshotRepository{views: []*domain.FinancialView{view, view, view}}
	provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(), Source: "stripe_test_api", ReasonCode: "provider_observed_clear",
	}}
	service := &PaymentService{repo: repo, admission: provider, account: "acct_fixture"}

	snapshot, err := service.GetPaidRecoveryPaymentSnapshot(context.Background(), paymentID, bookingID, 1)
	if err != nil {
		t.Fatalf("GetPaidRecoveryPaymentSnapshot() error = %v", err)
	}
	if snapshot == nil || snapshot.Financial == nil || snapshot.Financial.Payment == nil || snapshot.Financial.Payment.ID != paymentID ||
		!snapshot.ProviderIdentityVerified || snapshot.Admission.EligibilityState != domain.AdmissionEligibilityEligible ||
		snapshot.Admission.DisputeState != domain.DisputeEvidenceNoDispute || snapshot.Admission.RefundState != domain.RefundEvidenceNoRefund {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if repo.calls != 3 || provider.calls != 1 {
		t.Fatalf("snapshot reads/provider reads = %d/%d, want fresh local reads around one provider read", repo.calls, provider.calls)
	}
}

func TestPaidRecoverySnapshotDowngradesEligibilityWhenRefundAppearsDuringProviderRead(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	initial := paidRecoveryFinancialView(paymentID, bookingID)
	latest := paidRecoveryFinancialView(paymentID, bookingID)
	decisionID := uuid.New()
	latest.RefundDecisionID = &decisionID
	latest.RefundState = "pending"
	repo := &paidRecoverySnapshotRepository{views: []*domain.FinancialView{initial, initial, latest}}
	provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Now().UTC(), Source: "stripe_test_api", ReasonCode: "provider_observed_clear",
	}}
	service := &PaymentService{repo: repo, admission: provider, account: "acct_fixture"}

	snapshot, err := service.GetPaidRecoveryPaymentSnapshot(context.Background(), paymentID, bookingID, 1)
	if err != nil {
		t.Fatalf("GetPaidRecoveryPaymentSnapshot() error = %v", err)
	}
	if snapshot.ProviderIdentityVerified != true || snapshot.Financial.RefundDecisionID == nil ||
		snapshot.Admission.EligibilityState != domain.AdmissionEligibilityDenied || snapshot.Admission.ReasonCode != "local_refund_present" {
		t.Fatalf("concurrent refund was not visible in the final snapshot: %+v", snapshot)
	}
	if provider.calls != 1 {
		t.Fatalf("provider was queried %d times, want exactly one read", provider.calls)
	}
}

func TestPaidRecoverySnapshotDoesNotCallProviderForUnverifiedTestScope(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	view := paidRecoveryFinancialView(paymentID, bookingID)
	view.ProviderAccount = "acct_other"
	repo := &paidRecoverySnapshotRepository{views: []*domain.FinancialView{view, view, view}}
	provider := &admissionEvidenceProvider{}
	service := &PaymentService{repo: repo, admission: provider, account: "acct_fixture"}

	snapshot, err := service.GetPaidRecoveryPaymentSnapshot(context.Background(), paymentID, bookingID, 1)
	if err != nil {
		t.Fatalf("GetPaidRecoveryPaymentSnapshot() error = %v", err)
	}
	if snapshot.ProviderIdentityVerified || snapshot.Admission.EligibilityState != domain.AdmissionEligibilityDenied || provider.calls != 0 {
		t.Fatalf("mismatched provider scope was not denied before provider access: %+v calls=%d", snapshot, provider.calls)
	}
}

func paidRecoveryFinancialView(paymentID, bookingID uuid.UUID) *domain.FinancialView {
	mode := false
	return &domain.FinancialView{
		Payment:    &domain.Payment{ID: paymentID, UserId: uuid.New(), BookingId: &bookingID, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", PaymentIntentId: "pi_recovery_fixture"},
		Generation: 1, Managed: true, ProviderAccount: "acct_fixture", LiveMode: &mode,
		ProviderOutcome: "collected", CreateState: "create_ready", ReconciliationState: "sale_verified",
		RefundState: "not_required", CancelState: "not_requested",
	}
}
