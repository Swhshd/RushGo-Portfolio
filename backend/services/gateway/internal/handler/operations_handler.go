package handler

import (
	"context"
	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"strconv"
	"time"
)

type OperationsHandler struct {
	booking            bookingv1.BookingServiceClient
	secure             bool
	notification       notificationv1.NotificationAdminServiceClient
	secureNotification bool
}

func NewOperationsHandlerWithNotification(booking bookingv1.BookingServiceClient, secure bool, notification notificationv1.NotificationAdminServiceClient, secureNotification bool) *OperationsHandler {
	return &OperationsHandler{booking: booking, secure: secure, notification: notification, secureNotification: secureNotification}
}

func NewOperationsHandler(booking bookingv1.BookingServiceClient, secure bool) *OperationsHandler {
	return &OperationsHandler{booking: booking, secure: secure}
}
func operationsError(c *gin.Context, err error) {
	code := 503
	label := "source_unavailable"
	switch status.Code(err) {
	case codes.Unauthenticated:
		code = 401
		label = "unauthenticated"
	case codes.PermissionDenied:
		code = 403
		label = "forbidden"
	case codes.InvalidArgument:
		code = 400
		label = "invalid_query"
	}
	c.JSON(code, gin.H{"code": label, "error": label})
}
func operationsBounds(c *gin.Context) (int32, int32, bool) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, err2 := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || err2 != nil || limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		c.JSON(400, gin.H{"code": "invalid_query"})
		return 0, 0, false
	}
	return int32(limit), int32(offset), true
}
func (h *OperationsHandler) List(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.booking == nil || !h.secure {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	limit, offset, ok := operationsBounds(c)
	if !ok {
		return
	}
	now := time.Now().UTC()
	from, err := time.Parse(time.RFC3339, c.DefaultQuery("from", now.Add(-7*24*time.Hour).Format(time.RFC3339)))
	to, err2 := time.Parse(time.RFC3339, c.DefaultQuery("to", now.Format(time.RFC3339)))
	if err != nil || err2 != nil || !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		c.JSON(400, gin.H{"code": "invalid_query"})
		return
	}
	ctx, cancel := context.WithTimeout(ticketIdentityContext(c), 10*time.Second)
	defer cancel()
	query := &bookingv1.OperationsQuery{From: timestamppb.New(from), To: timestamppb.New(to), Limit: limit, Offset: offset}
	type observed struct {
		result *bookingv1.OperationsBookings
		err    error
	}
	notifications := make(chan observed, 1)
	go func() {
		if h.notification == nil || !h.secureNotification {
			notifications <- observed{err: status.Error(codes.Unavailable, "notification source unavailable")}
			return
		}
		r, e := h.notification.ListDeliveryExceptions(ctx, query)
		notifications <- observed{result: r, err: e}
	}()
	result, err := h.booking.ListOperationsBookings(ctx, query)
	if err != nil {
		operationsError(c, err)
		return
	}
	if result == nil {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	notificationResult := <-notifications
	if status.Code(notificationResult.err) == codes.Unauthenticated || status.Code(notificationResult.err) == codes.PermissionDenied {
		operationsError(c, notificationResult.err)
		return
	}
	notificationState := "unavailable"
	notificationItems := []gin.H{}
	var notificationObserved any
	notificationMore := false
	if notificationResult.err == nil && notificationResult.result != nil {
		notificationState = "available"
		notificationItems = operationItems(notificationResult.result)
		notificationMore = notificationResult.result.HasMore
		notificationObserved = notificationResult.result.ObservedAt.AsTime().Format(time.RFC3339)
	}
	c.JSON(http.StatusOK, gin.H{"items": operationItems(result), "has_more": result.HasMore, "offset": offset, "limit": limit, "from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339), "observed_at": result.ObservedAt.AsTime().Format(time.RFC3339), "scope": result.Scope, "source": "booking + payment independently observed", "notification_path": "/admin/outbox-events?source=notification", "notification_source_state": notificationState, "notification_items": notificationItems, "notification_has_more": notificationMore, "notification_observed_at": notificationObserved})
}

func operationItems(result *bookingv1.OperationsBookings) []gin.H {
	items := []gin.H{}
	for _, v := range result.Items {
		if v == nil {
			continue
		}
		issues := []gin.H{}
		for _, i := range v.Issues {
			if i == nil {
				continue
			}
			issues = append(issues, gin.H{"kind": i.Kind, "source": i.Source, "state": i.State, "observed_at": i.ObservedAt.AsTime().Format(time.RFC3339), "next_action": i.NextAction})
		}
		items = append(items, gin.H{"booking_id": v.BookingId, "event_id": v.EventId, "booking_status": v.BookingStatus, "created_at": v.CreatedAt.AsTime().Format(time.RFC3339), "ticket_count": v.TicketCount, "refund_status": v.RefundStatus, "payment_source_state": v.PaymentSourceState, "issues": issues, "detail_path": "/admin/orders/" + v.BookingId})
	}
	return items
}
func (h *OperationsHandler) Work(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.booking == nil || !h.secure {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	limit, offset, ok := operationsBounds(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(ticketIdentityContext(c), 10*time.Second)
	defer cancel()
	result, err := h.booking.ListStaffWork(ctx, &bookingv1.StaffWorkQuery{Limit: limit, Offset: offset})
	if err != nil {
		operationsError(c, err)
		return
	}
	if result == nil {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	items := []gin.H{}
	for _, v := range result.Items {
		var starts any
		if v.StartsAt != nil {
			starts = v.StartsAt.AsTime().Format(time.RFC3339)
		}
		items = append(items, gin.H{"event_id": v.EventId, "access": v.Access, "pending_refunds": v.PendingRefunds, "title": v.Title, "starts_at": starts, "event_state": v.EventState, "event_source_state": v.EventSourceState})
	}
	c.JSON(200, gin.H{"items": items, "has_more": result.HasMore, "observed_at": result.ObservedAt.AsTime().Format(time.RFC3339)})
}
