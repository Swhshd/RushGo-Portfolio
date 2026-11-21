package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/adminticketlist"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adminEventTicketListRequestTimeout = 10 * time.Second

type AdminEventTicketsHandler struct {
	eventClient   eventv1.EventServiceClient
	bookingClient bookingv1.BookingServiceClient
}

func NewAdminEventTicketsHandler(eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient) *AdminEventTicketsHandler {
	return &AdminEventTicketsHandler{eventClient: eventClient, bookingClient: bookingClient}
}

type adminEventTicketsHTTPResponse struct {
	EventID           string                       `json:"event_id"`
	ObservedAt        string                       `json:"observed_at"`
	SourceService     string                       `json:"source_service"`
	SourceStatus      string                       `json:"source_status"`
	Coverage          string                       `json:"coverage"`
	SourceRecordLimit int32                        `json:"source_record_limit"`
	StatusFilter      string                       `json:"status_filter"`
	PageSize          int32                        `json:"page_size"`
	HasMore           bool                         `json:"has_more"`
	NextCursor        *string                      `json:"next_cursor"`
	Tickets           []adminEventTicketHTTPRecord `json:"tickets"`
}

type adminEventTicketHTTPRecord struct {
	TicketID    string  `json:"ticket_id"`
	Status      string  `json:"status"`
	IssuedAt    string  `json:"issued_at"`
	CheckedInAt *string `json:"checked_in_at"`
	RevokedAt   *string `json:"revoked_at"`
}

type adminEventTicketListQuery struct {
	statusFilter string
	pageSize     int32
	cursor       string
	after        *adminticketlist.Cursor
}

func (h *AdminEventTicketsHandler) ListTickets(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil {
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
		return
	}
	eventID, err := uuid.Parse(c.Param("id"))
	if err != nil || eventID == uuid.Nil {
		writeAdminEventTicketsError(c, http.StatusBadRequest, "invalid_event_id", "invalid event identifier")
		return
	}
	query, ok := parseAdminEventTicketListQuery(c.Request, eventID)
	if !ok {
		writeAdminEventTicketsError(c, http.StatusBadRequest, "invalid_ticket_query", "invalid ticket list query")
		return
	}
	if h.eventClient == nil || h.bookingClient == nil {
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), adminEventTicketListRequestTimeout)
	defer cancel()
	event, err := h.eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: eventID.String()})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			writeAdminEventTicketsError(c, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
		return
	}
	if event == nil || event.GetId() != eventID.String() {
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
		return
	}
	authorization := c.Request.Header.Values("Authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") || strings.TrimPrefix(authorization[0], "Bearer ") == "" || strings.ContainsAny(strings.TrimPrefix(authorization[0], "Bearer "), " \t\r\n") {
		writeAdminEventTicketsError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	bookingCtx := metadata.AppendToOutgoingContext(ctx, "authorization", authorization[0])
	projection, err := h.bookingClient.ListAdminEventTickets(bookingCtx, &bookingv1.ListAdminEventTicketsRequest{
		EventId: eventID.String(), StatusFilter: query.statusFilter, PageSize: query.pageSize, Cursor: query.cursor,
	})
	if err != nil {
		writeAdminEventTicketsGRPCError(c, err)
		return
	}
	response, ok := projectAdminEventTicketsResponse(projection, eventID, query)
	if !ok {
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
		return
	}
	c.JSON(http.StatusOK, response)
}

func parseAdminEventTicketListQuery(request *http.Request, eventID uuid.UUID) (adminEventTicketListQuery, bool) {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return adminEventTicketListQuery{}, false
	}
	for key := range values {
		if key != "status" && key != "page_size" && key != "cursor" {
			return adminEventTicketListQuery{}, false
		}
	}
	filter := adminticketlist.StatusAll
	if raw, exists := values["status"]; exists {
		if len(raw) != 1 || raw[0] == "" {
			return adminEventTicketListQuery{}, false
		}
		filter, err = adminticketlist.NormalizeStatusFilter(raw[0])
		if err != nil {
			return adminEventTicketListQuery{}, false
		}
	}
	pageSize := adminticketlist.DefaultPageSize
	if raw, exists := values["page_size"]; exists {
		if len(raw) != 1 || raw[0] == "" {
			return adminEventTicketListQuery{}, false
		}
		for _, char := range raw[0] {
			if char < '0' || char > '9' {
				return adminEventTicketListQuery{}, false
			}
		}
		parsed, err := strconv.ParseInt(raw[0], 10, 32)
		if err != nil || parsed < 1 || parsed > int64(adminticketlist.MaxPageSize) {
			return adminEventTicketListQuery{}, false
		}
		pageSize = int32(parsed)
	}
	query := adminEventTicketListQuery{statusFilter: filter, pageSize: pageSize}
	if raw, exists := values["cursor"]; exists {
		if len(raw) != 1 || raw[0] == "" {
			return adminEventTicketListQuery{}, false
		}
		after, err := adminticketlist.DecodeCursor(raw[0], eventID, filter)
		if err != nil {
			return adminEventTicketListQuery{}, false
		}
		query.cursor = raw[0]
		query.after = &after
	}
	return query, true
}

func projectAdminEventTicketsResponse(source *bookingv1.ListAdminEventTicketsResponse, eventID uuid.UUID, query adminEventTicketListQuery) (adminEventTicketsHTTPResponse, bool) {
	if source == nil || eventID == uuid.Nil || source.GetEventId() != eventID.String() || source.GetObservedAt() == nil || source.GetObservedAt().CheckValid() != nil || source.GetSourceRecordLimit() != adminticketlist.SourceRecordCap || source.GetStatusFilter() != query.statusFilter || source.GetPageSize() != query.pageSize {
		return adminEventTicketsHTTPResponse{}, false
	}
	observedAt := source.GetObservedAt().AsTime().UTC()
	if !validAdminEventTicketListTime(observedAt) {
		return adminEventTicketsHTTPResponse{}, false
	}
	tickets := source.GetTickets()
	if len(tickets) > int(query.pageSize) || source.GetHasMore() && len(tickets) != int(query.pageSize) || !source.GetHasMore() && source.GetNextCursor() != "" || source.GetHasMore() && source.GetNextCursor() == "" {
		return adminEventTicketsHTTPResponse{}, false
	}
	projected := make([]adminEventTicketHTTPRecord, 0, len(tickets))
	seen := make(map[uuid.UUID]struct{}, len(tickets))
	for index, ticket := range tickets {
		if ticket == nil || ticket.GetIssuedAt() == nil || ticket.GetIssuedAt().CheckValid() != nil {
			return adminEventTicketsHTTPResponse{}, false
		}
		ticketID, err := uuid.Parse(ticket.GetTicketId())
		if err != nil || ticketID == uuid.Nil {
			return adminEventTicketsHTTPResponse{}, false
		}
		if _, exists := seen[ticketID]; exists {
			return adminEventTicketsHTTPResponse{}, false
		}
		seen[ticketID] = struct{}{}
		statusValue := ticket.GetStatus()
		if query.statusFilter != adminticketlist.StatusAll && statusValue != query.statusFilter {
			return adminEventTicketsHTTPResponse{}, false
		}
		if statusValue != adminticketlist.StatusIssued && statusValue != adminticketlist.StatusCheckedIn && statusValue != adminticketlist.StatusRevoked {
			return adminEventTicketsHTTPResponse{}, false
		}
		issuedAt := ticket.GetIssuedAt().AsTime().UTC()
		if !validAdminEventTicketListTime(issuedAt) || issuedAt.After(observedAt) {
			return adminEventTicketsHTTPResponse{}, false
		}
		checkedInAt, checkedInOK := projectAdminEventTicketListTimestamp(ticket.GetCheckedInAt(), issuedAt, observedAt)
		revokedAt, revokedOK := projectAdminEventTicketListTimestamp(ticket.GetRevokedAt(), issuedAt, observedAt)
		if !checkedInOK || !revokedOK {
			return adminEventTicketsHTTPResponse{}, false
		}
		switch statusValue {
		case adminticketlist.StatusIssued:
			if checkedInAt != nil || revokedAt != nil {
				return adminEventTicketsHTTPResponse{}, false
			}
		case adminticketlist.StatusCheckedIn:
			if checkedInAt == nil || revokedAt != nil {
				return adminEventTicketsHTTPResponse{}, false
			}
		case adminticketlist.StatusRevoked:
			if revokedAt == nil {
				return adminEventTicketsHTTPResponse{}, false
			}
		}
		if index > 0 && !adminEventTicketHTTPRecordBefore(tickets[index-1], issuedAt, ticketID) {
			return adminEventTicketsHTTPResponse{}, false
		}
		if query.after != nil && !adminEventTicketHTTPRecordAfterCursor(issuedAt, ticketID, *query.after) {
			return adminEventTicketsHTTPResponse{}, false
		}
		row := adminEventTicketHTTPRecord{TicketID: ticketID.String(), Status: statusValue, IssuedAt: issuedAt.Format(time.RFC3339Nano), CheckedInAt: checkedInAt, RevokedAt: revokedAt}
		projected = append(projected, row)
	}
	var nextCursor *string
	if source.GetHasMore() {
		last := tickets[len(tickets)-1]
		lastID, _ := uuid.Parse(last.GetTicketId())
		lastIssuedAt := last.GetIssuedAt().AsTime().UTC()
		decoded, err := adminticketlist.DecodeCursor(source.GetNextCursor(), eventID, query.statusFilter)
		if err != nil || decoded.TicketID != lastID || !decoded.IssuedAt.Equal(lastIssuedAt) {
			return adminEventTicketsHTTPResponse{}, false
		}
		cursor := source.GetNextCursor()
		nextCursor = &cursor
	}
	return adminEventTicketsHTTPResponse{
		EventID: eventID.String(), ObservedAt: observedAt.Format(time.RFC3339Nano),
		SourceService: "booking", SourceStatus: "available", Coverage: "recorded_ticket_snapshot",
		SourceRecordLimit: source.GetSourceRecordLimit(), StatusFilter: query.statusFilter, PageSize: query.pageSize,
		HasMore: source.GetHasMore(), NextCursor: nextCursor, Tickets: projected,
	}, true
}

func projectAdminEventTicketListTimestamp(value *timestamppb.Timestamp, issuedAt, observedAt time.Time) (*string, bool) {
	if value == nil {
		return nil, true
	}
	if value.CheckValid() != nil {
		return nil, false
	}
	at := value.AsTime().UTC()
	if !validAdminEventTicketListTime(at) || at.Before(issuedAt) || at.After(observedAt) {
		return nil, false
	}
	formatted := at.Format(time.RFC3339Nano)
	return &formatted, true
}

func adminEventTicketHTTPRecordBefore(previous *bookingv1.AdminEventTicketRecord, currentIssuedAt time.Time, currentID uuid.UUID) bool {
	if previous == nil || previous.GetIssuedAt() == nil || previous.GetIssuedAt().CheckValid() != nil {
		return false
	}
	previousIssuedAt := previous.GetIssuedAt().AsTime()
	previousID, err := uuid.Parse(previous.GetTicketId())
	if err != nil || previousID == uuid.Nil {
		return false
	}
	if previousIssuedAt.Equal(currentIssuedAt) {
		return bytes.Compare(previousID[:], currentID[:]) > 0
	}
	return previousIssuedAt.After(currentIssuedAt)
}

func adminEventTicketHTTPRecordAfterCursor(issuedAt time.Time, ticketID uuid.UUID, cursor adminticketlist.Cursor) bool {
	if issuedAt.Equal(cursor.IssuedAt) {
		return bytes.Compare(ticketID[:], cursor.TicketID[:]) < 0
	}
	return issuedAt.Before(cursor.IssuedAt)
}

func validAdminEventTicketListTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func writeAdminEventTicketsError(c *gin.Context, statusCode int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.AbortWithStatusJSON(statusCode, gin.H{"code": code, "error": message})
}

func writeAdminEventTicketsGRPCError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.Unauthenticated:
		writeAdminEventTicketsError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
	case codes.PermissionDenied:
		writeAdminEventTicketsError(c, http.StatusForbidden, "forbidden", "admin access required")
	case codes.InvalidArgument:
		writeAdminEventTicketsError(c, http.StatusBadRequest, "invalid_ticket_query", "invalid ticket list query")
	case codes.NotFound:
		writeAdminEventTicketsError(c, http.StatusNotFound, "not_found", "event not found")
	case codes.ResourceExhausted:
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "capacity_exceeded", "ticket list source capacity exceeded")
	default:
		writeAdminEventTicketsError(c, http.StatusServiceUnavailable, "source_unavailable", "ticket list source unavailable")
	}
}
