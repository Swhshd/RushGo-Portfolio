package stripe

import (
	"context"
	"errors"
	"net/http"

	"github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/kafka"
	"github.com/ticketbox/pkg/observability"
	"go.uber.org/zap"
)

type StripePaymentGateway struct {
	logger                *zap.Logger
	webhookSecret         string
	paymentProducer       *kafka.PaymentProducer
	stripeClient          *stripe.Client
	metrics               *observability.Metrics
	webhookAccount        string
	webhookLive           bool
	admissionBackend      stripe.Backend
	admissionAPIKey       string
	refundEvidenceBackend stripe.Backend
	refundEvidenceAPIKey  string
}

func NewStripePaymentGateway(logger *zap.Logger, webhookSecret string, paymentProducer *kafka.PaymentProducer, stripeClient *stripe.Client, metrics ...*observability.Metrics) *StripePaymentGateway {
	var domainMetrics *observability.Metrics
	if len(metrics) > 0 {
		domainMetrics = metrics[0]
	}
	return &StripePaymentGateway{logger: logger, webhookSecret: webhookSecret, paymentProducer: paymentProducer, stripeClient: stripeClient, metrics: domainMetrics}
}

// Compatibility signatures fail closed: remote creates require the persisted
// operation and verified webhooks require the Payment receipt transaction.
func (s *StripePaymentGateway) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, errors.New("persisted financial create operation required")
}

func (s *StripePaymentGateway) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, errors.New("durable Payment receipt ingress required")
}

func (s *StripePaymentGateway) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return errors.New("persisted full-refund obligation required")
}
