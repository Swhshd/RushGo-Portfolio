package payment_http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ticketbox/payment/internal/domain"
	"go.uber.org/zap"
)

type failingWebhookGateway struct{}

func (failingWebhookGateway) CreatePaymentIntent(context.Context, *domain.CreatePaymentIntentRequest) (*domain.CreatePaymentIntentResponse, error) {
	return nil, nil
}

func (failingWebhookGateway) HandleWebhookAfterPayment(context.Context, *http.Request, http.ResponseWriter) (*domain.WebhookPaymentResponse, error) {
	return nil, errWebhookTest
}

func (failingWebhookGateway) MakePaymentRefund(context.Context, *domain.MakePaymentRefundRequest) error {
	return nil
}

var errWebhookTest = &webhookTestError{}

type webhookTestError struct{}

func (*webhookTestError) Error() string { return "webhook test failure" }

func TestWebhookHandlerReturnsServerErrorWhenWebhookProcessingFails(t *testing.T) {
	h := NewPaymentHttpHandler(nil, zap.NewNop(), failingWebhookGateway{})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", nil)
	res := httptest.NewRecorder()

	h.WebhookHandler(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected503 for unavailable verified receipt storage, got %d", res.Code)
	}
}
