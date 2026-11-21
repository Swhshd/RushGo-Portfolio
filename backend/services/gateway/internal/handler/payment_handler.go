package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PaymentHandler struct {
	client  paymentv1.PaymentServiceClient
	booking bookingv1.BookingServiceClient
}

func NewPaymentHandler(client paymentv1.PaymentServiceClient, booking ...bookingv1.BookingServiceClient) *PaymentHandler {
	h := &PaymentHandler{client: client}
	if len(booking) > 0 {
		h.booking = booking[0]
	}
	return h
}

func paymentUser(c *gin.Context) (string, bool) {
	value, exists := c.Get("user_id")
	user, ok := value.(string)
	id, err := uuid.Parse(user)
	if !exists || !ok || err != nil || id == uuid.Nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return "", false
	}
	return id.String(), true
}

func paymentHTTPError(c *gin.Context, err error) {
	code, message := http.StatusServiceUnavailable, "financial responsibility unavailable; retry same identity"
	switch status.Code(err) {
	case codes.NotFound:
		code, message = http.StatusNotFound, "payment not found"
	case codes.InvalidArgument:
		code, message = http.StatusBadRequest, "invalid financial request"
	case codes.PermissionDenied:
		code, message = http.StatusForbidden, "forbidden"
	case codes.FailedPrecondition, codes.AlreadyExists:
		code, message = http.StatusConflict, "financial_state_protected"
		if status.Convert(err).Message() == "accounting_not_ready" {
			message = "ticket accounting is not ready for sale"
		}
	}
	c.JSON(code, gin.H{"error": message})
}

func (p *PaymentHandler) SearchPayments(c *gin.Context) {
	user, ok := paymentUser(c)
	if !ok {
		return
	}
	var req struct {
		UserID    string `json:"user_id"`
		Status    string `json:"status"`
		BookingID string `json:"booking_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid financial request"})
		return
	}
	if req.UserID != "" && req.UserID != user {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}
	if req.BookingID != "" {
		id, err := uuid.Parse(req.BookingID)
		if err != nil || id == uuid.Nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid booking id"})
			return
		}
		req.BookingID = id.String()
	}
	res, err := p.client.GetPayments(c.Request.Context(), &paymentv1.GetPaymentsReqeust{UserId: user, Status: req.Status, BookingId: req.BookingID})
	if err != nil {
		paymentHTTPError(c, err)
		return
	}
	if res == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "financial read unavailable"})
		return
	}
	entries := make([]gin.H, 0, len(res.Payments))
	for _, entry := range res.Payments {
		if entry == nil || entry.UserId != user {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		entries = append(entries, paymentJSON(entry))
	}
	c.JSON(http.StatusOK, gin.H{"payments": entries})
}

func (p *PaymentHandler) CreatePayment(c *gin.Context) {
	user, ok := paymentUser(c)
	if !ok {
		return
	}
	var req struct {
		BookingID     string `json:"booking_id" binding:"required"`
		Price         int32  `json:"price" binding:"required"`
		Currency      string `json:"currency" binding:"required"`
		PaymentMethod string `json:"payment_method" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid financial request"})
		return
	}
	id, err := uuid.Parse(req.BookingID)
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid booking id"})
		return
	}
	if p.booking == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Booking authority unavailable"})
		return
	}
	view, err := p.booking.GetLifecycleDisposition(c.Request.Context(), &bookingv1.LifecycleRequest{BookingId: id.String(), Generation: 1})
	if err != nil {
		paymentHTTPError(c, err)
		return
	}
	if view == nil || view.Booking == nil || view.Booking.Id != id.String() || view.Generation != 1 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Booking authority unavailable"})
		return
	}
	if view.Booking.UserId != user {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}
	if view.Booking.TotalAmountCents < 1 || view.Booking.TotalAmountCents > 2147483647 || int64(req.Price) != view.Booking.TotalAmountCents || req.Currency != "usd" || view.Currency != "usd" {
		c.JSON(http.StatusConflict, gin.H{"error": "financial_identity_conflict"})
		return
	}
	email, _ := c.Get("email")
	userEmail, _ := email.(string)
	res, err := p.client.CreatePayment(c.Request.Context(), &paymentv1.CreatePaymentRequest{UserId: user, BookingId: id.String(), Price: int32(view.Booking.TotalAmountCents), Currency: view.Currency, PaymentMethod: req.PaymentMethod, UserEmail: userEmail, PaymentGeneration: 1})
	if err != nil {
		paymentHTTPError(c, err)
		return
	}
	if res == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "financial create unavailable"})
		return
	}
	code := http.StatusCreated
	if res.CreateState != "create_ready" {
		code = http.StatusAccepted
		if res.RetryAfterSeconds > 0 {
			c.Header("Retry-After", strconv.Itoa(int(res.RetryAfterSeconds)))
		}
	}
	if view.Phase == "TERMINAL" {
		code = http.StatusOK
		res.PaymentIntentClientSecret = ""
	}
	c.JSON(code, gin.H{"id": res.Id, "status": res.Status, "create_state": res.CreateState, "retry_after_seconds": res.RetryAfterSeconds, "payment_intent_id": res.PaymentIntentId, "payment_intent_client_secret": res.PaymentIntentClientSecret})
}

func (p *PaymentHandler) ownedPayment(c *gin.Context) (*paymentv1.PaymentEntry, bool) {
	user, ok := paymentUser(c)
	if !ok {
		return nil, false
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payment id"})
		return nil, false
	}
	res, err := p.client.GetPaymentById(c.Request.Context(), &paymentv1.GetPaymentByIdReq{Id: id.String()})
	if err != nil {
		paymentHTTPError(c, err)
		return nil, false
	}
	if res == nil || res.Id != id.String() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "financial read unavailable"})
		return nil, false
	}
	if res.UserId != user {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return nil, false
	}
	return res, true
}

func (p *PaymentHandler) GetPaymentById(c *gin.Context) {
	if res, ok := p.ownedPayment(c); ok {
		c.JSON(http.StatusOK, paymentJSON(res))
	}
}
func (p *PaymentHandler) UpdatePayment(c *gin.Context) {
	if _, ok := p.ownedPayment(c); ok {
		c.JSON(http.StatusConflict, gin.H{"error": "financial_state_protected"})
	}
}
func (p *PaymentHandler) DeletePayment(c *gin.Context) {
	if _, ok := p.ownedPayment(c); ok {
		c.JSON(http.StatusConflict, gin.H{"error": "financial_state_protected"})
	}
}

func paymentJSON(p *paymentv1.PaymentEntry) gin.H {
	var created, updated any
	if p.CreatedAt != nil {
		created = p.CreatedAt.AsTime().Format(time.RFC3339)
	}
	if p.UpdatedAt != nil {
		updated = p.UpdatedAt.AsTime().Format(time.RFC3339)
	}
	return gin.H{"id": p.Id, "booking_id": p.BookingId, "user_id": p.UserId, "transaction_id": p.TransactionId, "price": p.Price, "price_method": p.PaymentMethod, "status": p.Status, "currency": p.Currency, "created_at": created, "updated_at": updated, "provider_outcome": p.ProviderOutcome, "provider_status": p.ProviderStatus, "reconciliation_state": p.ReconciliationState, "refund_state": p.RefundState, "refund_amount_cents": p.RefundAmountCents, "refund_id": p.RefundId, "refund_status": p.RefundStatus, "payment_generation": p.PaymentGeneration, "booking_disposition": p.BookingDisposition, "attention_reason": p.AttentionReason}
}
