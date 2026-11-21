package payment_http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	payment_gateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/service"
	"go.uber.org/zap"
)

type PaymentHttpHandler struct {
	service        *service.PaymentService
	logger         *zap.Logger
	webhookHandler payment_gateway.PaymentGatewayInterface
}

func NewPaymentHttpHandler(service *service.PaymentService, logger *zap.Logger, webhookHandler payment_gateway.PaymentGatewayInterface) *PaymentHttpHandler {
	return &PaymentHttpHandler{
		service:        service,
		logger:         logger,
		webhookHandler: webhookHandler,
	}
}

func (p *PaymentHttpHandler) WebhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	verifier, ok := p.webhookHandler.(payment_gateway.WebhookVerifier)
	if !ok {
		http.Error(w, "webhook storage unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
	if err != nil {
		http.Error(w, "invalid webhook body", http.StatusBadRequest)
		return
	}
	receipt, err := verifier.VerifyWebhook(ctx, body, r.Header.Get("Stripe-Signature"))
	if errors.Is(err, payment_gateway.ErrIrrelevantWebhook) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, payment_gateway.ErrInvalidWebhook) {
		http.Error(w, "invalid webhook", http.StatusBadRequest)
		return
	}
	if err != nil || receipt == nil || p.service == nil {
		http.Error(w, "webhook storage unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := p.service.AcceptProviderReceipt(ctx, *receipt); err != nil {
		p.logger.Error("verified webhook responsibility storage failed")
		http.Error(w, "webhook storage unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
