package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const eventInterestPageSizeDefault int32 = 20

type EventInterestHandler struct {
	client eventv1.EventServiceClient
}

func NewEventInterestHandler(client eventv1.EventServiceClient) *EventInterestHandler {
	return &EventInterestHandler{client: client}
}

func (h *EventInterestHandler) ListFavorites(c *gin.Context) {
	h.list(c, "favorite")
}

func (h *EventInterestHandler) ListAlerts(c *gin.Context) {
	h.list(c, "")
}

func (h *EventInterestHandler) list(c *gin.Context, kind string) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	limit, offset, valid := interestPageQuery(c)
	if !valid {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_page", "error": "limit or offset is invalid"})
		return
	}
	response, err := h.client.ListMyEventInterests(ticketIdentityContext(c), &eventv1.ListMyEventInterestsRequest{
		UserId: userID, Kind: kind, Limit: limit, Offset: offset,
	})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	items := make([]gin.H, 0, len(response.GetItems()))
	for _, item := range response.GetItems() {
		if item == nil {
			continue
		}
		if kind == "favorite" {
			items = append(items, gin.H{"eventId": item.GetEventId(), "createdAt": timestampString(item.GetCreatedAt())})
			continue
		}
		items = append(items, eventSubscriptionJSON(item))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "limit": response.GetLimit(), "offset": response.GetOffset(), "hasMore": response.GetHasMore()})
}

func (h *EventInterestHandler) SetFavorite(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	eventID, ok := interestEventID(c)
	if !ok {
		return
	}
	response, err := h.client.UpsertMyEventInterest(ticketIdentityContext(c), &eventv1.UpsertMyEventInterestRequest{
		UserId: userID, EventId: eventID, Kind: "favorite",
	})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"eventId": response.GetEventId(), "favorite": response.GetStatus() == "active"})
}

func (h *EventInterestHandler) RemoveFavorite(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	eventID, ok := interestEventID(c)
	if !ok {
		return
	}
	response, err := h.client.WithdrawMyEventInterest(ticketIdentityContext(c), &eventv1.WithdrawMyEventInterestRequest{
		UserId: userID, EventId: eventID, Kind: "favorite",
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			c.JSON(http.StatusOK, gin.H{"eventId": eventID, "favorite": false})
			return
		}
		writeEventInterestError(c, err)
		return
	}
	if response == nil {
		writeEventInterestError(c, nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"eventId": response.GetEventId(), "favorite": false})
}

func (h *EventInterestHandler) GetInterests(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	eventID, ok := interestEventID(c)
	if !ok {
		return
	}
	response, err := h.client.GetMyEventInterests(ticketIdentityContext(c), &eventv1.GetMyEventInterestsRequest{UserId: userID, EventId: eventID})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	subscriptions := make([]gin.H, 0, len(response.GetSubscriptions()))
	for _, item := range response.GetSubscriptions() {
		if item != nil {
			subscriptions = append(subscriptions, eventSubscriptionJSON(item))
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"eventId": response.GetEventId(), "favorite": response.GetFavorite(), "subscriptions": subscriptions,
		"saleStatus": response.GetSaleStatus(), "onsaleAvailable": response.GetOnsaleAvailable(), "restockStatus": response.GetRestockStatus(),
	})
}

func (h *EventInterestHandler) SetAlert(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	eventID, ok := interestEventID(c)
	if !ok {
		return
	}
	kind := c.Param("kind")
	if !validEventAlertKind(kind) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_alert_kind", "error": "alert category is invalid"})
		return
	}
	var request struct {
		EmailOptIn     bool   `json:"emailOptIn"`
		ConsentVersion string `json:"consentVersion"`
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_alert_request", "error": "alert preferences are invalid"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_alert_request", "error": "alert preferences are invalid"})
		return
	}
	response, err := h.client.UpsertMyEventInterest(ticketIdentityContext(c), &eventv1.UpsertMyEventInterestRequest{
		UserId: userID, EventId: eventID, Kind: kind, EmailOptIn: request.EmailOptIn, EmailConsentVersion: request.ConsentVersion,
	})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	c.JSON(http.StatusOK, eventSubscriptionJSON(response))
}

func (h *EventInterestHandler) RemoveAlert(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	eventID, ok := interestEventID(c)
	if !ok {
		return
	}
	kind := c.Param("kind")
	if !validEventAlertKind(kind) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_alert_kind", "error": "alert category is invalid"})
		return
	}
	response, err := h.client.WithdrawMyEventInterest(ticketIdentityContext(c), &eventv1.WithdrawMyEventInterestRequest{UserId: userID, EventId: eventID, Kind: kind})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"eventId": response.GetEventId(), "kind": kind, "status": response.GetStatus()})
}

func (h *EventInterestHandler) DisableAlertEmails(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	kind := c.Param("kind")
	if !validEventAlertKind(kind) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_alert_kind", "error": "alert category is invalid"})
		return
	}
	response, err := h.client.DisableMyEventAlertEmails(ticketIdentityContext(c), &eventv1.DisableMyEventAlertEmailsRequest{UserId: userID, Kind: kind})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"kind": response.GetKind(), "updated": response.GetUpdated()})
}

func (h *EventInterestHandler) AdminEventEngagement(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	eventID, err := uuid.Parse(c.Param("id"))
	if err != nil || eventID == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_event_id", "error": "event ID is invalid"})
		return
	}
	response, err := h.client.ReadEventInterestCounts(ticketIdentityContext(c), &eventv1.ReadEventInterestCountsRequest{EventId: eventID.String()})
	if err != nil || response == nil {
		writeEventInterestError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"favorites": response.GetFavorites(), "onsale": response.GetOnsale(), "restock": response.GetRestock(), "observedAt": timestampString(response.GetObservedAt())})
}

func eventSubscriptionJSON(item *eventv1.EventInterest) gin.H {
	return gin.H{
		"eventId": item.GetEventId(), "kind": item.GetKind(), "emailOptIn": item.GetEmailOptIn(),
		"consentVersion": item.GetEmailConsentVersion(), "status": item.GetStatus(),
		"createdAt": timestampString(item.GetCreatedAt()), "updatedAt": timestampString(item.GetUpdatedAt()),
	}
}

func interestPageQuery(c *gin.Context) (int32, int32, bool) {
	limit := eventInterestPageSizeDefault
	offset := int32(0)
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed < 1 || parsed > 100 {
			return 0, 0, false
		}
		limit = int32(parsed)
	}
	if raw := c.Query("offset"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed < 0 || parsed > 100000 {
			return 0, 0, false
		}
		offset = int32(parsed)
	}
	return limit, offset, true
}

func interestEventID(c *gin.Context) (string, bool) {
	id, err := uuid.Parse(c.Param("eventID"))
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_event_id", "error": "event ID is invalid"})
		return "", false
	}
	return id.String(), true
}

func validEventAlertKind(kind string) bool { return kind == "onsale" || kind == "restock" }

func writeEventInterestError(c *gin.Context, err error) {
	if err == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "event_interest_unavailable", "error": "event preferences are temporarily unavailable"})
		return
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_event_interest", "error": "event preference request is invalid"})
	case codes.NotFound:
		c.JSON(http.StatusNotFound, gin.H{"code": "event_interest_not_found", "error": "event preference was not found"})
	case codes.AlreadyExists:
		c.JSON(http.StatusConflict, gin.H{"code": "already_on_sale", "error": "ticket sales are already open; this event cannot receive a first-sale alert"})
	case codes.FailedPrecondition:
		c.JSON(http.StatusConflict, gin.H{"code": "event_interest_unavailable", "error": "event preference is not available"})
	case codes.PermissionDenied:
		c.JSON(http.StatusForbidden, gin.H{"code": "event_interest_forbidden", "error": "event preference is not available"})
	case codes.Unauthenticated:
		c.JSON(http.StatusUnauthorized, gin.H{"code": "authentication_required", "error": "authentication required"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "event_interest_unavailable", "error": "event preferences are temporarily unavailable"})
	}
}
