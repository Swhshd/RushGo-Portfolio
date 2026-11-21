package payment_gateway

import (
	"context"
	"errors"
	"net/http"

	"github.com/ticketbox/payment/internal/domain"
)

var (
	ErrInvalidWebhook    = errors.New("invalid verified-provider webhook")
	ErrIrrelevantWebhook = errors.New("verified irrelevant provider event")
)

type WebhookVerifier interface {
	VerifyWebhook(context.Context, []byte, string) (*domain.ProviderReceipt, error)
}

type PaymentGatewayInterface interface {
	CreatePaymentIntent(ctx context.Context, req *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error)
	HandleWebhookAfterPayment(ctx context.Context, req *http.Request, res http.ResponseWriter) (*domain.WebhookPaymentResponse, error)
	MakePaymentRefund(ctx context.Context, req *domain.MakePaymentRefundRequest) error
}

// FinancialProvider grows only with the owning tasks' verified operation APIs.
type FinancialProvider interface {
	CreateIntent(context.Context, domain.CreateOperation) (*domain.ProviderObservation, string, error)
	RetrieveIntent(context.Context, string, bool, string) (*domain.ProviderObservation, string, error)
	FindIntent(context.Context, domain.CreateOperation) ([]domain.ProviderObservation, error)
	CancelIntent(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error)
	CreateFullRefund(context.Context, domain.FinancialClaim, string) (*domain.ProviderObservation, error)
	RetrieveRefund(context.Context, string, bool, string) (*domain.ProviderObservation, error)
	InspectSucceededRefunds(context.Context, string, bool, string) (*domain.ProviderObservation, error)
	WebhookVerifier
}

// AdmissionEvidenceProvider reads fresh, provider-side refund and dispute facts
// for ticket issuance and check-in admission decisions.
type AdmissionEvidenceProvider interface {
	CheckAdmissionEvidence(context.Context, domain.AdmissionFinancialIdentity) domain.AdmissionProviderEvidence
}
