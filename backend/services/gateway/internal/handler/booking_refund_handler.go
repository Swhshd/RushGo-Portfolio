package handler

import (
	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"strconv"
	"unicode/utf8"
)

// RefundHandler exposes only buyer/approver projections, never provider identity.
type RefundHandler struct {
	client bookingv1.BookingServiceClient
}

func NewRefundHandler(c bookingv1.BookingServiceClient) *RefundHandler {
	return &RefundHandler{client: c}
}
func refundKey(c *gin.Context) (string, bool) {
	keys := c.Request.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !ticketScanKeyPattern.MatchString(keys[0]) {
		refundError(c, status.Error(codes.InvalidArgument, "key"))
		return "", false
	}
	c.Header("Idempotency-Key", keys[0])
	return keys[0], true
}
func refundError(c *gin.Context, err error) {
	code, httpCode, message, retry := "refund_unavailable", 503, "Refund authority is temporarily unavailable", true
	switch status.Code(err) {
	case codes.InvalidArgument:
		code, httpCode, message, retry = "invalid_refund_request", 400, "Refund request is invalid", false
	case codes.Unauthenticated:
		code, httpCode, message, retry = "unauthenticated", 401, "Authentication required", false
	case codes.PermissionDenied:
		code, httpCode, message, retry = "refund_forbidden", 403, "This refund action is not permitted", false
	case codes.NotFound:
		code, httpCode, message, retry = "refund_not_found", 404, "Refund request is not available", false
	case codes.AlreadyExists:
		code, httpCode, message, retry = "idempotency_conflict", 409, "This key was used for a different operation", false
	case codes.FailedPrecondition:
		code, httpCode, message, retry = "refund_not_eligible", 409, "The order does not currently qualify for this action", false
	case codes.Aborted:
		code, httpCode, message, retry = "refund_in_progress", 409, "Retry this operation with the same key", true
	}
	c.JSON(httpCode, gin.H{"code": code, "error": message, "retryable": retry})
}
func refundJSON(v *bookingv1.BookingRefund) gin.H {
	return gin.H{"request_id": v.RequestId, "booking_id": v.BookingId, "event_id": v.EventId, "status": v.Status, "reason": v.Reason, "decision_reason": v.DecisionReason, "refund_decision_id": v.RefundDecisionId, "amount_cents": v.AmountCents, "currency": v.Currency, "requested_at": timestampString(v.RequestedAt), "decided_at": timestampString(v.DecidedAt), "payment_refund_state": v.PaymentRefundState, "ticket_revocation_state": v.TicketRevocationState, "can_withdraw": v.CanWithdraw, "approval_enabled": v.ApprovalEnabled, "eligibility_code": v.EligibilityCode}
}
func refundReply(c *gin.Context, v *bookingv1.BookingRefund, err error) {
	if err != nil {
		refundError(c, err)
		return
	}
	if v == nil {
		refundError(c, status.Error(codes.Unavailable, "empty"))
		return
	}
	c.JSON(http.StatusOK, refundJSON(v))
}
func (h *RefundHandler) Request(c *gin.Context) {
	key, ok := refundKey(c)
	if !ok {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if c.ShouldBindJSON(&body) != nil || utf8.RuneCountInString(body.Reason) > 500 {
		refundError(c, status.Error(codes.InvalidArgument, "reason"))
		return
	}
	v, err := h.client.RequestBookingRefund(ticketIdentityContext(c), &bookingv1.RequestBookingRefundRequest{BookingId: c.Param("id"), Reason: body.Reason, IdempotencyKey: key})
	refundReply(c, v, err)
}
func (h *RefundHandler) Get(c *gin.Context) {
	v, err := h.client.GetBookingRefund(ticketIdentityContext(c), &bookingv1.GetBookingRefundRequest{BookingId: c.Param("id")})
	refundReply(c, v, err)
}
func (h *RefundHandler) Withdraw(c *gin.Context) {
	key, ok := refundKey(c)
	if !ok {
		return
	}
	v, err := h.client.WithdrawBookingRefund(ticketIdentityContext(c), &bookingv1.WithdrawBookingRefundRequest{RequestId: c.Param("requestId"), IdempotencyKey: key})
	refundReply(c, v, err)
}
func (h *RefundHandler) Decide(c *gin.Context) {
	key, ok := refundKey(c)
	if !ok {
		return
	}
	var body struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if c.ShouldBindJSON(&body) != nil || (body.Decision != "APPROVE" && body.Decision != "REJECT") || utf8.RuneCountInString(body.Reason) > 500 {
		refundError(c, status.Error(codes.InvalidArgument, "decision"))
		return
	}
	v, err := h.client.DecideBookingRefund(ticketIdentityContext(c), &bookingv1.DecideBookingRefundRequest{EventId: c.Param("id"), RequestId: c.Param("requestId"), Decision: body.Decision, Reason: body.Reason, IdempotencyKey: key})
	refundReply(c, v, err)
}
func (h *RefundHandler) List(c *gin.Context) {
	size := 50
	if raw := c.Query("page_size"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			refundError(c, status.Error(codes.InvalidArgument, "page size"))
			return
		}
		size = n
	}
	if len(c.Query("cursor")) > 256 {
		refundError(c, status.Error(codes.InvalidArgument, "cursor"))
		return
	}
	v, err := h.client.ListEventBookingRefunds(ticketIdentityContext(c), &bookingv1.ListEventBookingRefundsRequest{EventId: c.Param("id"), Cursor: c.Query("cursor"), PageSize: int32(size)})
	if err != nil {
		refundError(c, err)
		return
	}
	if v == nil {
		refundError(c, status.Error(codes.Unavailable, "empty"))
		return
	}
	rows := make([]gin.H, 0, len(v.Refunds))
	for _, row := range v.Refunds {
		if row != nil {
			rows = append(rows, refundJSON(row))
		}
	}
	c.JSON(200, gin.H{"refunds": rows, "next_cursor": v.NextCursor, "approval_enabled": v.ApprovalEnabled, "approval_block_reason": v.ApprovalBlockReason})
}
func refundApproverJSON(v *bookingv1.EventRefundApprover) gin.H {
	return gin.H{"event_id": v.EventId, "staff_user_id": v.StaffUserId, "active": v.Active, "changed_at": timestampString(v.ChangedAt)}
}
func (h *RefundHandler) SetApprover(active bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		v, err := h.client.SetEventRefundApprover(ticketIdentityContext(c), &bookingv1.SetEventRefundApproverRequest{EventId: c.Param("id"), StaffUserId: c.Param("staffId"), Active: active})
		if err != nil {
			refundError(c, err)
			return
		}
		if v == nil {
			refundError(c, status.Error(codes.Unavailable, "empty"))
			return
		}
		c.JSON(200, refundApproverJSON(v))
	}
}
func (h *RefundHandler) ListApprovers(c *gin.Context) {
	v, err := h.client.ListEventRefundApprovers(ticketIdentityContext(c), &bookingv1.ListEventRefundApproversRequest{EventId: c.Param("id")})
	if err != nil {
		refundError(c, err)
		return
	}
	if v == nil {
		refundError(c, status.Error(codes.Unavailable, "empty"))
		return
	}
	rows := make([]gin.H, 0, len(v.Assignments))
	for _, row := range v.Assignments {
		if row != nil {
			rows = append(rows, refundApproverJSON(row))
		}
	}
	c.JSON(200, gin.H{"assignments": rows})
}
