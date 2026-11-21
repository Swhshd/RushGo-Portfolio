package handler

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	adminOutboxDefaultLimit = 20
	adminOutboxMaxLimit     = 100
	adminOutboxMaxRange     = 7 * 24 * time.Hour
	adminOutboxCursorMaxLen = 4096
)

var adminOutboxQueryKeys = map[string]struct{}{
	"source": {}, "status": {}, "event_type": {}, "from": {}, "to": {}, "limit": {}, "cursor": {},
}

var adminOutboxSupportedTypes = []string{"BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"}

var notificationAdminOutboxSupportedTypes = []string{"booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email", "tickets_issued", "refund_requested", "refund_approved", "refund_rejected", "refund_succeeded", "manual_attention", "Other"}

var notificationAdminOutboxFilterableTypes = map[string]struct{}{
	"booking_confirmation": {}, "booking_failure": {}, "booking_cancellation": {}, "welcome_email": {},
	"tickets_issued": {}, "refund_requested": {}, "refund_approved": {}, "refund_rejected": {}, "refund_succeeded": {}, "manual_attention": {},
}

type adminOutboxRPCClient interface {
	ListAdminOutboxEvents(context.Context, *bookingv1.ListAdminOutboxEventsRequest, ...grpc.CallOption) (*bookingv1.ListAdminOutboxEventsResponse, error)
}

type AdminOutboxHandler struct {
	bookingClient      bookingv1.BookingServiceClient
	notificationClient notificationv1.NotificationAdminServiceClient
	secureBooking      bool
	secureNotification bool
}

func NewAdminOutboxHandler(bookingClient bookingv1.BookingServiceClient, secure bool) *AdminOutboxHandler {
	return &AdminOutboxHandler{bookingClient: bookingClient, secureBooking: secure}
}

func NewAdminOutboxHandlerWithNotification(bookingClient bookingv1.BookingServiceClient, secureBooking bool, notificationClient notificationv1.NotificationAdminServiceClient, secureNotification bool) *AdminOutboxHandler {
	return &AdminOutboxHandler{
		bookingClient: bookingClient, secureBooking: secureBooking,
		notificationClient: notificationClient, secureNotification: secureNotification,
	}
}

type adminOutboxHTTPRecord struct {
	EventID         string `json:"event_id"`
	SourceService   string `json:"source_service"`
	EventType       string `json:"event_type"`
	CreatedAt       string `json:"created_at"`
	RecordedStatus  string `json:"recorded_status"`
	ConsumerOutcome string `json:"consumer_outcome"`
}

type adminOutboxHTTPResponse struct {
	SourceService       string                  `json:"source_service"`
	Records             []adminOutboxHTTPRecord `json:"records"`
	NextCursor          string                  `json:"next_cursor"`
	HasMore             bool                    `json:"has_more"`
	ObservedAt          string                  `json:"observed_at"`
	Window              adminOutboxHTTPWindow   `json:"window"`
	SupportedEventTypes []string                `json:"supported_event_types"`
	SourceRecordLimit   int                     `json:"source_record_limit"`
	ConsumerOutcome     string                  `json:"consumer_outcome"`
}

type adminOutboxHTTPWindow struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (h *AdminOutboxHandler) ListEvents(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil {
		writeAdminOutboxError(c, http.StatusServiceUnavailable, "source_unavailable", "outbox source unavailable")
		return
	}
	query, err := parseAdminOutboxHTTPQuery(c.Request.URL.Query(), time.Now().UTC())
	if err != nil {
		writeAdminOutboxError(c, http.StatusBadRequest, "invalid_query", "invalid outbox query")
		return
	}
	var client adminOutboxRPCClient
	switch query.GetSource() {
	case "booking":
		if h.secureBooking {
			client = h.bookingClient
		}
	case "notification":
		if h.secureNotification {
			client = h.notificationClient
		}
	}
	if client == nil {
		writeAdminOutboxError(c, http.StatusServiceUnavailable, "source_unavailable", "outbox source unavailable")
		return
	}
	authorization := c.Request.Header.Values("Authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") || strings.TrimPrefix(authorization[0], "Bearer ") == "" {
		writeAdminOutboxError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	ctx := metadata.AppendToOutgoingContext(c.Request.Context(), "authorization", authorization[0])
	response, err := client.ListAdminOutboxEvents(ctx, query)
	if err != nil {
		writeAdminOutboxGRPCError(c, err)
		return
	}
	httpResponse, ok := projectAdminOutboxHTTPResponse(response, query.GetSource())
	if !ok {
		writeAdminOutboxError(c, http.StatusServiceUnavailable, "source_unavailable", "outbox source unavailable")
		return
	}
	c.JSON(http.StatusOK, httpResponse)
}

func parseAdminOutboxHTTPQuery(values url.Values, now time.Time) (*bookingv1.ListAdminOutboxEventsRequest, error) {
	for key, entries := range values {
		if _, ok := adminOutboxQueryKeys[key]; !ok || len(entries) != 1 {
			return nil, errInvalidAdminOutboxHTTPQuery
		}
	}
	source, ok := singleAdminOutboxQuery(values, "source")
	if !ok || source != "booking" && source != "notification" {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	statusValue, statusPresent := singleAdminOutboxQuery(values, "status")
	if !statusPresent && values.Has("status") {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	if !statusPresent {
		statusValue = "all"
	}
	if source == "booking" {
		if statusValue != "all" && statusValue != "pending" && statusValue != "published" {
			return nil, errInvalidAdminOutboxHTTPQuery
		}
	} else if statusValue != "all" && statusValue != "pending" && statusValue != "delivering" && statusValue != "sent" && statusValue != "retryable" && statusValue != "unknown" && statusValue != "attention" {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	eventType, ok := singleAdminOutboxQuery(values, "event_type")
	if !ok && values.Has("event_type") {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	if eventType != "" {
		if source == "booking" && !isAdminOutboxEventType(eventType) {
			return nil, errInvalidAdminOutboxHTTPQuery
		}
		if source == "notification" {
			if _, ok := notificationAdminOutboxFilterableTypes[eventType]; !ok {
				return nil, errInvalidAdminOutboxHTTPQuery
			}
		}
	}
	limit := adminOutboxDefaultLimit
	if rawLimit, present := singleAdminOutboxQuery(values, "limit"); present {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > adminOutboxMaxLimit || strconv.Itoa(parsed) != rawLimit {
			return nil, errInvalidAdminOutboxHTTPQuery
		}
		limit = parsed
	} else if values.Has("limit") {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	cursor, present := singleAdminOutboxQuery(values, "cursor")
	if !present && values.Has("cursor") || len(cursor) > adminOutboxCursorMaxLen {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
		if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != cursor || len(decoded) > adminOutboxCursorMaxLen {
			return nil, errInvalidAdminOutboxHTTPQuery
		}
	}

	fromValue, fromPresent := singleAdminOutboxQuery(values, "from")
	toValue, toPresent := singleAdminOutboxQuery(values, "to")
	if !fromPresent && values.Has("from") || !toPresent && values.Has("to") {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	if !fromPresent && !toPresent {
		to := now.UTC()
		from := to.Add(-24 * time.Hour)
		fromValue = from.Format(time.RFC3339Nano)
		toValue = to.Format(time.RFC3339Nano)
	} else if !fromPresent || !toPresent {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	from, err := time.Parse(time.RFC3339Nano, fromValue)
	if err != nil {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	to, err := time.Parse(time.RFC3339Nano, toValue)
	if err != nil || !to.After(from) || to.Sub(from) > adminOutboxMaxRange {
		return nil, errInvalidAdminOutboxHTTPQuery
	}
	return &bookingv1.ListAdminOutboxEventsRequest{
		Source: source, Status: statusValue, EventType: eventType,
		From: fromValue, To: toValue, Limit: int32(limit), Cursor: cursor,
	}, nil
}

var errInvalidAdminOutboxHTTPQuery = &adminOutboxQueryError{}

type adminOutboxQueryError struct{}

func (*adminOutboxQueryError) Error() string { return "invalid outbox query" }

func singleAdminOutboxQuery(values url.Values, key string) (string, bool) {
	entries, ok := values[key]
	if !ok || len(entries) != 1 || entries[0] == "" {
		return "", false
	}
	return entries[0], true
}

func isAdminOutboxEventType(value string) bool {
	for _, eventType := range adminOutboxSupportedTypes {
		if value == eventType {
			return true
		}
	}
	return false
}

func projectAdminOutboxHTTPResponse(response *bookingv1.ListAdminOutboxEventsResponse, source string) (*adminOutboxHTTPResponse, bool) {
	if response == nil || response.GetObservedAt() == nil || !response.GetObservedAt().IsValid() || response.GetFrom() == nil || !response.GetFrom().IsValid() || response.GetTo() == nil || !response.GetTo().IsValid() {
		return nil, false
	}
	if response.GetHasMore() && response.GetNextCursor() == "" || !response.GetHasMore() && response.GetNextCursor() != "" {
		return nil, false
	}
	if len(response.GetNextCursor()) > adminOutboxCursorMaxLen {
		return nil, false
	}
	if response.GetNextCursor() != "" {
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(response.GetNextCursor())
		if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != response.GetNextCursor() || len(decoded) > adminOutboxCursorMaxLen {
			return nil, false
		}
	}
	supportedEventTypes := adminOutboxSupportedTypes
	if source == "notification" {
		supportedEventTypes = notificationAdminOutboxSupportedTypes
	} else if source != "booking" {
		return nil, false
	}
	result := &adminOutboxHTTPResponse{
		SourceService:       source,
		Records:             make([]adminOutboxHTTPRecord, 0, len(response.GetRecords())),
		NextCursor:          response.GetNextCursor(),
		HasMore:             response.GetHasMore(),
		ObservedAt:          response.GetObservedAt().AsTime().UTC().Format(time.RFC3339Nano),
		Window:              adminOutboxHTTPWindow{From: response.GetFrom().AsTime().UTC().Format(time.RFC3339Nano), To: response.GetTo().AsTime().UTC().Format(time.RFC3339Nano)},
		SupportedEventTypes: append([]string(nil), supportedEventTypes...),
		SourceRecordLimit:   5000,
		ConsumerOutcome:     "not_verified",
	}
	for _, record := range response.GetRecords() {
		if record == nil || record.GetSourceService() != source || record.GetCreatedAt() == nil || !record.GetCreatedAt().IsValid() {
			return nil, false
		}
		eventID, err := uuid.Parse(record.GetEventId())
		if err != nil || eventID == uuid.Nil || eventID.String() != record.GetEventId() {
			return nil, false
		}
		projectedStatus := record.GetRecordedStatus()
		if source == "booking" {
			if projectedStatus != "pending" && projectedStatus != "published" {
				return nil, false
			}
		} else if !isNotificationAdminOutboxStatus(projectedStatus) {
			return nil, false
		}
		result.Records = append(result.Records, adminOutboxHTTPRecord{
			EventID: eventID.String(), SourceService: source, EventType: safeAdminOutboxEventTypeForSource(record.GetEventType(), source),
			CreatedAt: record.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano), RecordedStatus: projectedStatus, ConsumerOutcome: "not_verified",
		})
	}
	return result, true
}

func safeAdminOutboxEventType(value string) string {
	if value == "BookingConfirmed" || value == "BookingCancelled" || value == "BookingFailed" {
		return value
	}
	return "Other"
}

func safeAdminOutboxEventTypeForSource(value, source string) string {
	if source == "notification" {
		if _, ok := notificationAdminOutboxFilterableTypes[value]; ok {
			return value
		}
		return "Other"
	}
	return safeAdminOutboxEventType(value)
}

func isNotificationAdminOutboxStatus(value string) bool {
	switch value {
	case "pending", "delivering", "sent", "retryable", "unknown", "attention":
		return true
	default:
		return false
	}
}

func writeAdminOutboxGRPCError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		writeAdminOutboxError(c, http.StatusBadRequest, "invalid_query", "invalid outbox query")
	case codes.Unauthenticated:
		writeAdminOutboxError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
	case codes.PermissionDenied:
		writeAdminOutboxError(c, http.StatusForbidden, "forbidden", "admin access required")
	case codes.ResourceExhausted:
		writeAdminOutboxError(c, http.StatusServiceUnavailable, "capacity_exceeded", "outbox source capacity exceeded")
	default:
		writeAdminOutboxError(c, http.StatusServiceUnavailable, "source_unavailable", "outbox source unavailable")
	}
}

func writeAdminOutboxError(c *gin.Context, code int, safeCode, message string) {
	c.Header("Cache-Control", "no-store")
	c.JSON(code, gin.H{"code": safeCode, "error": message})
}
