package grpc

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionRPCGateway struct {
	response domain.AdmissionProviderEvidence
}

func (admissionRPCGateway) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, nil
}
func (admissionRPCGateway) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, nil
}
func (admissionRPCGateway) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return nil
}
func (g admissionRPCGateway) CheckAdmissionEvidence(context.Context, domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence {
	return g.response
}

func TestAdmissionFinancialEligibilityRPCUsesExplicitFailClosedEnums(t *testing.T) {
	paymentID, bookingID := uuid.New(), uuid.New()
	mode := false
	repositoryStub := &financialReadRepository{view: &domain.FinancialView{
		Payment:    &domain.Payment{ID: paymentID, BookingId: &bookingID, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", PaymentIntentId: "pi_admission_fixture"},
		Generation: 1, Managed: true, ProviderAccount: "acct_fixture", LiveMode: &mode,
		ProviderOutcome: "collected", CreateState: "create_ready", ReconciliationState: "sale_verified", RefundState: "not_required", CancelState: "not_requested",
	}}
	serviceStub := service.NewPaymentService(repositoryStub, zap.NewNop(), admissionRPCGateway{response: domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState:     domain.DisputeEvidenceNoDispute,
		RefundState:      domain.RefundEvidenceNoRefund,
		ObservedAt:       time.Date(2026, 10, 22, 10, 0, 0, 0, time.FixedZone("+08", 8*60*60)),
		Source:           "stripe_test_api", ReasonCode: "provider_observed_clear",
	}})
	serviceStub.SetFinancialAuthority(nil, "acct_fixture", false)
	server := NewPaymentServer(serviceStub, zap.NewNop())
	response, err := server.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: paymentID.String(), BookingId: bookingID.String(), Generation: 1,
	})
	if err != nil || response.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE ||
		response.DisputeState != paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE ||
		response.RefundState != paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND ||
		response.Source != "stripe_test_api" || response.ReasonCode != "provider_observed_clear" || response.ObservedAt == nil || response.ObservedAt.AsTime().Location().String() != "UTC" {
		t.Fatalf("eligibility contract did not preserve normalized provider evidence: %+v err=%v", response, err)
	}
	response, err = server.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: paymentID.String(), BookingId: uuid.NewString(), Generation: 1,
	})
	if err != nil || response.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_DENIED ||
		response.DisputeState != paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_UNKNOWN || response.ObservedAt != nil {
		t.Fatalf("local identity mismatch was not an explicit denial without fabricated provider evidence: %+v err=%v", response, err)
	}
	if _, err := server.CheckAdmissionFinancialEligibility(context.Background(), &paymentv1.AdmissionFinancialEligibilityRequest{PaymentId: "bad", BookingId: bookingID.String(), Generation: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid request identity returned %v", err)
	}
}

var _ repository.PaymentRepositoryInterface = (*financialReadRepository)(nil)
