package handler

import (
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type BookingHandler struct {
	bookingClient    bookingv1.BookingServiceClient
	RequireRequestID bool
}

func NewBookingHandler(bookingClient bookingv1.BookingServiceClient) *BookingHandler {
	return &BookingHandler{bookingClient: bookingClient}
}

func (h *BookingHandler) CreateBooking(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, _ := c.Get("user_id")
	headers := c.Request.Header.Values("Idempotency-Key")
	key := ""
	if len(headers) > 1 || (len(headers) == 1 && !bookingKeyPattern.MatchString(headers[0])) || (h.RequireRequestID && len(headers) == 0) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_booking_request", "error": "invalid idempotency key"})
		return
	}
	if len(headers) == 1 {
		key = headers[0]
	}

	var req struct {
		EventID string `json:"event_id" binding:"required"`
		Items   []struct {
			TicketTierID string   `json:"ticket_tier_id" binding:"required"`
			Quantity     int32    `json:"quantity" binding:"required"`
			SeatIDs      []string `json:"seat_ids"`
		} `json:"items" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	mode := c.GetHeader("X-Booking-Mode")
	if mode == "" {
		mode = "pessimistic"
	}

	var items []*bookingv1.BookingItemRequest
	for _, item := range req.Items {
		items = append(items, &bookingv1.BookingItemRequest{
			TicketTierId: item.TicketTierID,
			Quantity:     item.Quantity,
			SeatIds:      item.SeatIDs,
		})
	}

	resp, err := h.bookingClient.CreateBooking(c.Request.Context(), &bookingv1.CreateBookingRequest{
		UserId:      userID.(string),
		EventId:     req.EventID,
		Items:       items,
		BookingMode: mode,
		RequestId:   key,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"code": "booking_request_conflict", "error": "request key has a different booking payload"})
			return
		}
		if status.Code(err) == codes.InvalidArgument {
			c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_booking_request", "error": "invalid booking request"})
			return
		}
		if status.Code(err) == codes.FailedPrecondition {
			if status.Convert(err).Message() == "event_not_on_sale" {
				c.JSON(http.StatusConflict, gin.H{"code": "event_not_on_sale", "error": "event is not accepting new bookings"})
				return
			}
			c.JSON(http.StatusConflict, gin.H{"code": "accounting_not_ready", "error": "ticket accounting is not ready for sale"})
			return
		}
		if status.Code(err) == codes.Unavailable {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "booking_authority_unavailable", "error": "booking authority unavailable; retry later"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "booking failed " + err.Error()})
		return
	}

	if resp == nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "booking response unavailable"})
		return
	}
	response := toBookingJSON(resp)
	switch resp.CreationState {
	case "starting":
		response["paymentIntentClientSecret"] = ""
		c.Header("Retry-After", "1")
		c.JSON(http.StatusAccepted, response)
	case "terminal":
		response["paymentIntentClientSecret"] = ""
		c.JSON(http.StatusOK, response)
	default:
		c.JSON(http.StatusCreated, response)
	}
}

var bookingKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func (h *BookingHandler) ListBookings(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, _ := c.Get("user_id")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	resp, err := h.bookingClient.ListUserBookings(c.Request.Context(), &bookingv1.ListUserBookingsRequest{
		UserId:   userID.(string),
		Page:     int32(page),
		PageSize: int32(pageSize),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list bookings"})
		return
	}

	bookings := make([]gin.H, 0)
	for _, b := range resp.Bookings {
		bookings = append(bookings, toBookingJSON(b))
	}

	c.JSON(http.StatusOK, gin.H{
		"bookings": bookings,
		"total":    resp.Total,
	})
}

func (h *BookingHandler) GetBooking(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	// Object-level authorization: the authenticated user may only read their
	// own booking. A missing identity fails closed, and a non-owner receives
	// the same 404 as a nonexistent booking so the response does not disclose
	// existence. The internal booking RPC has no caller identity, so this
	// gateway boundary is the only user-facing enforcement point.
	userID, present := c.Get("user_id")
	ownerID, valid := userID.(string)
	if !present || !valid || ownerID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}

	resp, err := h.bookingClient.GetBooking(c.Request.Context(), &bookingv1.GetBookingRequest{
		BookingId: c.Param("id"),
	})
	if err != nil || resp.GetUserId() != ownerID {
		c.JSON(http.StatusNotFound, gin.H{"error": "booking not found"})
		return
	}

	c.JSON(http.StatusOK, toBookingJSON(resp))
}

func (h *BookingHandler) CancelBooking(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, _ := c.Get("user_id")

	resp, err := h.bookingClient.CancelBooking(c.Request.Context(), &bookingv1.CancelBookingRequest{
		BookingId: c.Param("id"),
		UserId:    userID.(string),
	})
	if err != nil {
		bookingActionError(c, err)
		return
	}

	c.JSON(http.StatusOK, toBookingJSON(resp))
}

func (h *BookingHandler) ResumeBooking(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	owner := c.GetString("user_id")
	if owner == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	ctx := metadata.AppendToOutgoingContext(c.Request.Context(), "authorization", c.GetHeader("Authorization"))
	response, err := h.bookingClient.ResumeBooking(ctx, &bookingv1.CancelBookingRequest{BookingId: c.Param("id"), UserId: owner})
	if err != nil {
		bookingActionError(c, err)
		return
	}
	if response == nil || response.UserId != owner {
		c.JSON(http.StatusBadGateway, gin.H{"error": "checkout unavailable"})
		return
	}
	code := http.StatusOK
	if response.CheckoutState == "processing" {
		code = http.StatusAccepted
		c.Header("Retry-After", "1")
	}
	c.JSON(code, toBookingJSON(response))
}

func bookingActionError(c *gin.Context, err error) {
	code := http.StatusServiceUnavailable
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.PermissionDenied:
		code = http.StatusForbidden
	case codes.Unauthenticated:
		code = http.StatusUnauthorized
	case codes.FailedPrecondition, codes.Aborted:
		code = http.StatusConflict
	}
	c.JSON(code, gin.H{"code": "booking_action_unavailable", "error": "order action unavailable; refresh order status"})
}

func toBookingJSON(b *bookingv1.BookingDetail) gin.H {
	var deadline any
	if b.ReservationDeadline != nil && b.ReservationDeadline.IsValid() {
		deadline = b.ReservationDeadline.AsTime().Format(time.RFC3339)
	}
	items := make([]gin.H, 0)
	for _, item := range b.Items {
		seatIDs := item.SeatIds
		if seatIDs == nil {
			seatIDs = []string{}
		}
		items = append(items, gin.H{
			"id":               item.Id,
			"ticket_tier_id":   item.TicketTierId,
			"tier_name":        item.TierName,
			"quantity":         item.Quantity,
			"unit_price_cents": item.UnitPriceCents,
			"seat_ids":         seatIDs,
		})
	}
	return gin.H{
		"id":                        b.Id,
		"user_id":                   b.UserId,
		"event_id":                  b.EventId,
		"status":                    b.Status,
		"total_amount_cents":        b.TotalAmountCents,
		"currency":                  b.Currency,
		"items":                     items,
		"created_at":                b.CreatedAt.AsTime().Format(time.RFC3339),
		"paymentIntentClientSecret": b.PaymentIntentClientSecret,
		"creation_state":            b.CreationState,
		"retry_after_seconds":       b.RetryAfterSeconds,
		"lifecycle_generation":      b.LifecycleGeneration,
		"checkout_state":            b.CheckoutState,
		"reservation_deadline":      deadline,
		"can_cancel":                b.CanCancel,
	}
}
