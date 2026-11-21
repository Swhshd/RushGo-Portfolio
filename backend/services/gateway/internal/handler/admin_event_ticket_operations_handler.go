package handler

import (
	"context"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	adminEventTicketOperationsRequestTimeout = 10 * time.Second
	adminEventTicketOperationsSourceLimit    = 5000
	adminEventTicketOperationsRecentLimit    = 5
	adminEventTitleMaxRunes                  = 240
	adminEventVenueMaxRunes                  = 300
)

type AdminEventTicketOperationsHandler struct {
	eventClient   eventv1.EventServiceClient
	bookingClient bookingv1.BookingServiceClient
}

func NewAdminEventTicketOperationsHandler(eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient) *AdminEventTicketOperationsHandler {
	return &AdminEventTicketOperationsHandler{eventClient: eventClient, bookingClient: bookingClient}
}

type adminEventTicketOperationsHTTPResponse struct {
	DeliveryExceptions       []adminDeliveryException                  `json:"delivery_exceptions"`
	EventID                  string                                    `json:"event_id"`
	EventTitle               string                                    `json:"event_title"`
	EventVenue               string                                    `json:"event_venue"`
	EventDate                string                                    `json:"event_date"`
	ObservedAt               string                                    `json:"observed_at"`
	SourceService            string                                    `json:"source_service"`
	SourceStatus             string                                    `json:"source_status"`
	Coverage                 string                                    `json:"coverage"`
	TotalRecordedCount       int32                                     `json:"total_recorded_count"`
	IssuedCount              int32                                     `json:"issued_count"`
	CheckedInCount           int32                                     `json:"checked_in_count"`
	RevokedCount             int32                                     `json:"revoked_count"`
	SuccessfulCheckinCount   int32                                     `json:"successful_checkin_count"`
	LatestCheckinAt          *string                                   `json:"latest_checkin_at"`
	RecentCheckins           []adminEventTicketOperationsRecentCheckin `json:"recent_checkins"`
	SourceRecordLimit        int32                                     `json:"source_record_limit"`
	LiveAdmissionEligibility string                                    `json:"live_admission_eligibility"`
}

type adminDeliveryException struct {
	BookingID              string `json:"booking_id"`
	State                  string `json:"state"`
	BookingStatus          string `json:"booking_status"`
	ExpectedTicketCount    int64  `json:"expected_ticket_count"`
	RecordedTicketCount    int64  `json:"recorded_ticket_count"`
	IssuanceReceiptOutcome string `json:"issuance_receipt_outcome"`
	IssuanceReasonCode     string `json:"issuance_reason_code"`
	BookingCreatedAt       string `json:"booking_created_at"`
}

type adminEventTicketOperationsRecentCheckin struct {
	CheckedInAt string `json:"checked_in_at"`
}

func (h *AdminEventTicketOperationsHandler) GetTicketOperations(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil {
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
		return
	}
	eventID, err := uuid.Parse(c.Param("id"))
	if err != nil || eventID == uuid.Nil {
		writeAdminEventTicketOperationsError(c, http.StatusBadRequest, "invalid_event_id", "invalid event identifier")
		return
	}
	if h.eventClient == nil || h.bookingClient == nil {
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), adminEventTicketOperationsRequestTimeout)
	defer cancel()

	event, err := h.eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: eventID.String()})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			writeAdminEventTicketOperationsError(c, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
		return
	}
	metadata, ok := projectAdminEventTicketOperationsMetadata(event, eventID)
	if !ok {
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
		return
	}
	authorization := c.Request.Header.Values("Authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") || strings.TrimPrefix(authorization[0], "Bearer ") == "" || strings.ContainsAny(strings.TrimPrefix(authorization[0], "Bearer "), " \t\r\n") {
		writeAdminEventTicketOperationsError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	bookingCtx := metadataOutgoingContext(ctx, authorization[0])
	projection, err := h.bookingClient.GetAdminEventTicketOperations(bookingCtx, &bookingv1.GetAdminEventTicketOperationsRequest{EventId: eventID.String()})
	if err != nil {
		writeAdminEventTicketOperationsGRPCError(c, err)
		return
	}
	response, ok := projectAdminEventTicketOperationsResponse(projection, metadata)
	if !ok {
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
		return
	}
	c.JSON(http.StatusOK, response)
}

type adminEventTicketOperationsEventMetadata struct {
	eventID string
	title   string
	venue   string
	eventAt time.Time
}

func projectAdminEventTicketOperationsMetadata(event *eventv1.EventDetail, requestedID uuid.UUID) (adminEventTicketOperationsEventMetadata, bool) {
	if event == nil || requestedID == uuid.Nil || event.GetId() != requestedID.String() || event.GetDate() == nil || event.GetDate().CheckValid() != nil {
		return adminEventTicketOperationsEventMetadata{}, false
	}
	title, ok := safeAdminEventMetadataLabel(event.GetTitle(), adminEventTitleMaxRunes, true)
	if !ok {
		return adminEventTicketOperationsEventMetadata{}, false
	}
	venue, ok := safeAdminEventMetadataLabel(event.GetVenue(), adminEventVenueMaxRunes, false)
	if !ok {
		return adminEventTicketOperationsEventMetadata{}, false
	}
	return adminEventTicketOperationsEventMetadata{eventID: requestedID.String(), title: title, venue: venue, eventAt: event.GetDate().AsTime().UTC()}, true
}

func safeAdminEventMetadataLabel(value string, maxRunes int, required bool) (string, bool) {
	if !utf8.ValidString(value) {
		return "", false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", false
		}
	}
	value = strings.TrimSpace(value)
	if required && value == "" || utf8.RuneCountInString(value) > maxRunes {
		return "", false
	}
	return value, true
}

func projectAdminEventTicketOperationsResponse(source *bookingv1.GetAdminEventTicketOperationsResponse, metadata adminEventTicketOperationsEventMetadata) (adminEventTicketOperationsHTTPResponse, bool) {
	if source == nil || source.GetEventId() != metadata.eventID || source.GetObservedAt() == nil || source.GetObservedAt().CheckValid() != nil || source.GetSourceRecordLimit() != adminEventTicketOperationsSourceLimit {
		return adminEventTicketOperationsHTTPResponse{}, false
	}
	observedAt := source.GetObservedAt().AsTime().UTC()
	counts := []int32{source.GetTotalRecordedCount(), source.GetIssuedCount(), source.GetCheckedInCount(), source.GetRevokedCount(), source.GetSuccessfulCheckinCount()}
	for _, count := range counts {
		if count < 0 || count > adminEventTicketOperationsSourceLimit {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
	}
	totalFromStates := int64(source.GetIssuedCount()) + int64(source.GetCheckedInCount()) + int64(source.GetRevokedCount())
	if totalFromStates != int64(source.GetTotalRecordedCount()) || totalFromStates > adminEventTicketOperationsSourceLimit {
		return adminEventTicketOperationsHTTPResponse{}, false
	}
	successfulCheckins := int64(source.GetSuccessfulCheckinCount())
	checkedInStates := int64(source.GetCheckedInCount())
	revokedStates := int64(source.GetRevokedCount())
	if successfulCheckins < checkedInStates || successfulCheckins > checkedInStates+revokedStates || successfulCheckins > int64(source.GetTotalRecordedCount()) {
		return adminEventTicketOperationsHTTPResponse{}, false
	}
	recent := source.GetRecentCheckins()
	expectedRecentCount := int(successfulCheckins)
	if expectedRecentCount > adminEventTicketOperationsRecentLimit {
		expectedRecentCount = adminEventTicketOperationsRecentLimit
	}
	if len(recent) != expectedRecentCount {
		return adminEventTicketOperationsHTTPResponse{}, false
	}
	var latest *string
	if successfulCheckins == 0 {
		if source.GetLatestCheckinAt() != nil || len(recent) != 0 {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
	} else {
		latestTimestamp := source.GetLatestCheckinAt()
		if latestTimestamp == nil || latestTimestamp.CheckValid() != nil || len(recent) == 0 || recent[0] == nil || recent[0].CheckValid() != nil {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		latestTime := latestTimestamp.AsTime().UTC()
		if !latestTime.Equal(recent[0].AsTime().UTC()) {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		latestValue := latestTime.Format(time.RFC3339Nano)
		latest = &latestValue
	}
	recentCheckins := make([]adminEventTicketOperationsRecentCheckin, 0, len(recent))
	for index, timestamp := range recent {
		if timestamp == nil || timestamp.CheckValid() != nil {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		at := timestamp.AsTime().UTC()
		if at.After(observedAt) || index > 0 && at.After(recent[index-1].AsTime().UTC()) {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		recentCheckins = append(recentCheckins, adminEventTicketOperationsRecentCheckin{CheckedInAt: at.Format(time.RFC3339Nano)})
	}
	exceptions := make([]adminDeliveryException, 0, len(source.GetDeliveryExceptions()))
	if len(source.GetDeliveryExceptions()) > adminEventTicketOperationsSourceLimit {
		return adminEventTicketOperationsHTTPResponse{}, false
	}
	seen := make(map[uuid.UUID]bool)
	for _, row := range source.GetDeliveryExceptions() {
		if row == nil {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		id, err := uuid.Parse(row.BookingId)
		if err != nil || id == uuid.Nil || seen[id] || row.State != "CONFIRMED_TICKET_DELIVERY_PENDING" || row.BookingStatus != "CONFIRMED" || row.ExpectedTicketCount < 0 || row.RecordedTicketCount < 0 || row.ExpectedTicketCount > 9007199254740991 || row.RecordedTicketCount > 9007199254740991 || row.BookingCreatedAt == nil || row.BookingCreatedAt.CheckValid() != nil || row.BookingCreatedAt.AsTime().After(observedAt) {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		switch row.IssuanceReceiptOutcome {
		case "NOT_RECORDED", "issued", "denied", "CONFLICT":
		default:
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		if len(row.IssuanceReasonCode) > 80 {
			return adminEventTicketOperationsHTTPResponse{}, false
		}
		for _, ch := range row.IssuanceReasonCode {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_') {
				return adminEventTicketOperationsHTTPResponse{}, false
			}
		}
		seen[id] = true
		exceptions = append(exceptions, adminDeliveryException{BookingID: id.String(), State: row.State, BookingStatus: row.BookingStatus, ExpectedTicketCount: row.ExpectedTicketCount, RecordedTicketCount: row.RecordedTicketCount, IssuanceReceiptOutcome: row.IssuanceReceiptOutcome, IssuanceReasonCode: row.IssuanceReasonCode, BookingCreatedAt: row.BookingCreatedAt.AsTime().UTC().Format(time.RFC3339Nano)})
	}
	return adminEventTicketOperationsHTTPResponse{
		DeliveryExceptions:       exceptions,
		EventID:                  metadata.eventID,
		EventTitle:               metadata.title,
		EventVenue:               metadata.venue,
		EventDate:                metadata.eventAt.Format(time.RFC3339Nano),
		ObservedAt:               observedAt.Format(time.RFC3339Nano),
		SourceService:            "booking",
		SourceStatus:             "available",
		Coverage:                 "recorded_ticket_snapshot",
		TotalRecordedCount:       source.GetTotalRecordedCount(),
		IssuedCount:              source.GetIssuedCount(),
		CheckedInCount:           source.GetCheckedInCount(),
		RevokedCount:             source.GetRevokedCount(),
		SuccessfulCheckinCount:   source.GetSuccessfulCheckinCount(),
		LatestCheckinAt:          latest,
		RecentCheckins:           recentCheckins,
		SourceRecordLimit:        source.GetSourceRecordLimit(),
		LiveAdmissionEligibility: "not_evaluated",
	}, true
}

func metadataOutgoingContext(ctx context.Context, authorization string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
}

func writeAdminEventTicketOperationsError(c *gin.Context, statusCode int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.AbortWithStatusJSON(statusCode, gin.H{"code": code, "error": message})
}

func writeAdminEventTicketOperationsGRPCError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.Unauthenticated:
		writeAdminEventTicketOperationsError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
	case codes.PermissionDenied:
		writeAdminEventTicketOperationsError(c, http.StatusForbidden, "forbidden", "admin access required")
	case codes.InvalidArgument:
		writeAdminEventTicketOperationsError(c, http.StatusBadRequest, "invalid_event_id", "invalid event identifier")
	case codes.NotFound:
		writeAdminEventTicketOperationsError(c, http.StatusNotFound, "not_found", "event not found")
	case codes.ResourceExhausted:
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "capacity_exceeded", "ticket operations source capacity exceeded")
	default:
		writeAdminEventTicketOperationsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket operations source unavailable")
	}
}
