package handler

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	notificationInboxDefaultPageSize = 20
	notificationInboxMaxPageSize     = 100
	notificationInboxMaxPage         = 1000
)

type NotificationInboxHandler struct {
	client notificationv1.NotificationAdminServiceClient
	secure bool
}

func NewNotificationInboxHandler(client notificationv1.NotificationAdminServiceClient, secure bool) *NotificationInboxHandler {
	return &NotificationInboxHandler{client: client, secure: secure}
}

func (h *NotificationInboxHandler) List(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.client == nil || !h.secure {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
		return
	}
	page, pageSize, unreadOnly, ok := parseNotificationInboxQuery(c.Request.URL.Query())
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_query"})
		return
	}
	ctx, cancel := notificationRequestContext(c)
	defer cancel()
	response, err := h.client.ListMyNotifications(ctx, &notificationv1.ListMyNotificationsRequest{
		Page: int32(page), PageSize: int32(pageSize), UnreadOnly: unreadOnly,
	})
	if err != nil {
		writeNotificationInboxGRPCError(c, err)
		return
	}
	if response == nil || int(response.GetPage()) != page || int(response.GetPageSize()) != pageSize || response.GetTotal() < 0 || response.GetUnreadCount() < 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
		return
	}
	items := make([]gin.H, 0, len(response.GetNotifications()))
	for _, notification := range response.GetNotifications() {
		if notification == nil || !validInboxProjection(notification) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
			return
		}
		var readAt any
		if notification.GetReadAt() != nil {
			readAt = notification.GetReadAt().AsTime().UTC().Format(time.RFC3339Nano)
		}
		items = append(items, gin.H{
			"id": notification.GetId(), "type": notification.GetType(), "locale": notification.GetLocale(),
			"title": notification.GetTitle(), "body": notification.GetBody(),
			"created_at": notification.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano), "read_at": readAt,
			"booking_id": notification.GetBookingId(), "detail_path": notification.GetDetailPath(), "source": notification.GetSource(),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"notifications": items, "total": response.GetTotal(), "unread_count": response.GetUnreadCount(),
		"page": response.GetPage(), "page_size": response.GetPageSize(),
	})
}

func (h *NotificationInboxHandler) MarkRead(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.client == nil || !h.secure {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_notification_id"})
		return
	}
	ctx, cancel := notificationRequestContext(c)
	defer cancel()
	response, err := h.client.MarkMyNotificationRead(ctx, &notificationv1.MarkMyNotificationReadRequest{NotificationId: id.String()})
	if err != nil {
		writeNotificationInboxGRPCError(c, err)
		return
	}
	if response == nil || response.GetId() != id.String() || response.GetReadAt() == nil || !response.GetReadAt().IsValid() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": response.GetId(), "read_at": response.GetReadAt().AsTime().UTC().Format(time.RFC3339Nano)})
}

func parseNotificationInboxQuery(values url.Values) (int, int, bool, bool) {
	allowed := map[string]bool{"page": true, "page_size": true, "unread_only": true}
	for key, entries := range values {
		if !allowed[key] || len(entries) != 1 {
			return 0, 0, false, false
		}
	}
	page, err := parseInboxInteger(values, "page", 1)
	if err != nil || page < 1 || page > notificationInboxMaxPage {
		return 0, 0, false, false
	}
	pageSize, err := parseInboxInteger(values, "page_size", notificationInboxDefaultPageSize)
	if err != nil || pageSize < 1 || pageSize > notificationInboxMaxPageSize {
		return 0, 0, false, false
	}
	unreadOnly := false
	if raw, present := values["unread_only"]; present {
		parsed, err := strconv.ParseBool(raw[0])
		if err != nil {
			return 0, 0, false, false
		}
		unreadOnly = parsed
	}
	return page, pageSize, unreadOnly, true
}

func parseInboxInteger(values url.Values, key string, fallback int) (int, error) {
	if !values.Has(key) {
		return fallback, nil
	}
	return strconv.Atoi(values.Get(key))
}

func validInboxProjection(item *notificationv1.NotificationInboxItem) bool {
	if _, err := uuid.Parse(item.GetId()); err != nil || item.GetId() == uuid.Nil.String() {
		return false
	}
	bookingID := item.GetBookingId()
	if bookingID != "" && bookingID != uuid.Nil.String() {
		if _, err := uuid.Parse(bookingID); err != nil {
			return false
		}
	}
	if item.GetType() == "" || item.GetLocale() != "en" && item.GetLocale() != "zh" || item.GetTitle() == "" || item.GetBody() == "" || item.GetSource() != "notification.business_intent" {
		return false
	}
	wantPath := "/account"
	switch item.GetType() {
	case "event_onsale", "event_restock":
		eventID := item.GetEventId()
		parsedEventID, err := uuid.Parse(eventID)
		if err != nil || parsedEventID == uuid.Nil || parsedEventID.String() != eventID {
			return false
		}
		if bookingID != "" && bookingID != uuid.Nil.String() {
			return false
		}
		wantPath = "/events/" + parsedEventID.String()
	default:
		if bookingID != "" && bookingID != uuid.Nil.String() {
			wantPath = "/my-tickets/orders/" + bookingID
		}
	}
	if item.GetDetailPath() != wantPath || item.GetCreatedAt() == nil || !item.GetCreatedAt().IsValid() {
		return false
	}
	if item.GetReadAt() != nil && !item.GetReadAt().IsValid() {
		return false
	}
	return true
}

func notificationRequestContext(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ticketIdentityContext(c), 8*time.Second)
}

func writeNotificationInboxGRPCError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.Unauthenticated:
		c.JSON(http.StatusUnauthorized, gin.H{"code": "unauthenticated"})
	case codes.PermissionDenied:
		c.JSON(http.StatusForbidden, gin.H{"code": "forbidden"})
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_query"})
	case codes.NotFound:
		c.JSON(http.StatusNotFound, gin.H{"code": "notification_not_found"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "notification_unavailable"})
	}
}
