package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ticketbox/gateway/internal/realtime"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
)

type EventHandler struct {
	eventClient eventv1.EventServiceClient
	realtimeHub *realtime.Hub
}

func NewEventHandler(eventClient eventv1.EventServiceClient, realtimeHub *realtime.Hub) *EventHandler {
	return &EventHandler{eventClient: eventClient, realtimeHub: realtimeHub}
}

func (h *EventHandler) StreamSeats(c *gin.Context) {
	eventID := c.Param("id")
	if _, err := uuid.Parse(eventID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event ID"})
		return
	}
	if h.eventClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event visibility unavailable"})
		return
	}
	if _, err := h.eventClient.GetEvent(c.Request.Context(), &eventv1.GetEventRequest{EventId: eventID}); err != nil {
		if status.Code(err) == codes.NotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event visibility unavailable"})
		return
	}
	client, err := h.realtimeHub.Subscribe(c.Request.Context(), eventID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "seat stream unavailable"})
		return
	}
	defer client.Close()
	select {
	case <-client.Ready():
	case <-client.Done():
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "seat stream unavailable"})
		return
	case <-c.Request.Context().Done():
		return
	}

	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	if _, err := c.Writer.Write(realtime.ReadyFrame(eventID)); err != nil {
		return
	}
	c.Writer.Flush()
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-client.Done():
			return
		case <-heartbeat.C:
			if _, err := c.Writer.Write([]byte(realtime.HeartbeatFrame)); err != nil {
				return
			}
			c.Writer.Flush()
		case payload, ok := <-client.Events():
			if !ok {
				return
			}
			var message struct {
				MessageID string `json:"message_id"`
			}
			if err := json.Unmarshal(payload, &message); err != nil || message.MessageID == "" {
				continue
			}
			publicPayload := stripRealtimeTraceContext(payload)
			if _, err := c.Writer.Write(realtime.SeatUpdateFrame(message.MessageID, publicPayload)); err != nil {
				return
			}
			c.Writer.Flush()
		}
	}
}

func stripRealtimeTraceContext(payload []byte) []byte {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return payload
	}
	delete(envelope, "traceparent")
	delete(envelope, "tracestate")
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return payload
	}
	return encoded
}

func (h *EventHandler) ListEvents(c *gin.Context) {
	if h.eventClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event catalog unavailable"})
		return
	}
	request, err := eventListRequestFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event list query"})
		return
	}
	resp, err := h.eventClient.ListEvents(c.Request.Context(), request)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event list query"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list events"})
		return
	}

	events := make([]gin.H, 0)
	for _, e := range resp.Events {
		events = append(events, toEventJSON(e))
	}

	c.JSON(http.StatusOK, gin.H{
		"events":    events,
		"total":     resp.Total,
		"page":      resp.Page,
		"page_size": resp.PageSize,
	})
}

func (h *EventHandler) ListAdminEvents(c *gin.Context) {
	if h.eventClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event catalog unavailable"})
		return
	}
	request, err := eventListRequestFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event list query"})
		return
	}
	resp, err := h.eventClient.ListAdminEvents(c.Request.Context(), request)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event list query"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to list events"})
		return
	}
	events := make([]gin.H, 0, len(resp.Events))
	for _, e := range resp.Events {
		events = append(events, toEventJSON(e))
	}
	c.JSON(http.StatusOK, gin.H{"events": events, "total": resp.Total, "page": resp.Page, "page_size": resp.PageSize})
}

func eventListRequestFromQuery(c *gin.Context) (*eventv1.ListEventsRequest, error) {
	query := c.Request.URL.Query()
	getOptionalSingle := func(name string) (string, bool, error) {
		values, ok := query[name]
		if !ok {
			return "", false, nil
		}
		if len(values) != 1 || values[0] == "" {
			return "", false, errors.New("query parameter must have exactly one value")
		}
		return values[0], true, nil
	}
	page, err := parseEventListPositiveInt(query, "page", 1, 1000)
	if err != nil {
		return nil, err
	}
	pageSize, err := parseEventListPositiveInt(query, "page_size", 20, 50)
	if err != nil {
		return nil, err
	}
	filter := &eventv1.ListEventsRequest{
		Category: c.Query("category"), Search: c.Query("search"), Page: int32(page), PageSize: int32(pageSize), Sort: "date_asc",
	}
	if location, ok, err := getOptionalSingle("location"); err != nil {
		return nil, err
	} else if ok {
		if len(location) > 500 {
			return nil, errors.New("location is too long")
		}
		filter.Location = location
	}
	if sortBy, ok, err := getOptionalSingle("sort"); err != nil {
		return nil, err
	} else if ok {
		switch sortBy {
		case "date_asc", "date_desc", "price_asc", "price_desc":
			filter.Sort = sortBy
		default:
			return nil, errors.New("unsupported event sort")
		}
	}
	dateFrom, hasDateFrom, err := getOptionalSingle("date_from")
	if err != nil {
		return nil, err
	}
	dateTo, hasDateTo, err := getOptionalSingle("date_to")
	if err != nil {
		return nil, err
	}
	var from, toExclusive time.Time
	if hasDateFrom {
		from, err = time.Parse("2006-01-02", dateFrom)
		if err != nil || from.Format("2006-01-02") != dateFrom {
			return nil, errors.New("date_from must be YYYY-MM-DD")
		}
		filter.DateFrom = timestamppb.New(from.UTC())
	}
	if hasDateTo {
		var inclusiveTo time.Time
		inclusiveTo, err = time.Parse("2006-01-02", dateTo)
		if err != nil || inclusiveTo.Format("2006-01-02") != dateTo {
			return nil, errors.New("date_to must be YYYY-MM-DD")
		}
		toExclusive = inclusiveTo.AddDate(0, 0, 1)
		if toExclusive.Year() > 9999 {
			return nil, errors.New("date_to is outside the supported range")
		}
		filter.DateToExclusive = timestamppb.New(toExclusive.UTC())
	}
	if hasDateFrom && hasDateTo && !from.Before(toExclusive) {
		return nil, errors.New("date_from must not be after date_to")
	}
	minPrice, hasMinPrice, err := getOptionalSingle("min_price_cents")
	if err != nil {
		return nil, err
	}
	maxPrice, hasMaxPrice, err := getOptionalSingle("max_price_cents")
	if err != nil {
		return nil, err
	}
	var minValue, maxValue int64
	if hasMinPrice {
		minValue, err = parseEventListPrice(minPrice)
		if err != nil {
			return nil, err
		}
		filter.MinPriceCents = &minValue
	}
	if hasMaxPrice {
		maxValue, err = parseEventListPrice(maxPrice)
		if err != nil {
			return nil, err
		}
		filter.MaxPriceCents = &maxValue
	}
	if hasMinPrice && hasMaxPrice && minValue > maxValue {
		return nil, errors.New("min_price_cents must not exceed max_price_cents")
	}
	return filter, nil
}

func parseEventListPositiveInt(query map[string][]string, name string, defaultValue, max int) (int, error) {
	values, ok := query[name]
	if !ok {
		return defaultValue, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, errors.New("query parameter must have exactly one value")
	}
	value, err := strconv.Atoi(values[0])
	if err != nil || value < 1 || value > max {
		return 0, errors.New("integer query parameter is outside the allowed range")
	}
	return value, nil
}

func parseEventListPrice(value string) (int64, error) {
	price, err := strconv.ParseInt(value, 10, 64)
	if err != nil || price < 0 || price > 2_147_483_647 {
		return 0, errors.New("price query parameter is outside the allowed range")
	}
	return price, nil
}

func (h *EventHandler) GetEvent(c *gin.Context) {
	resp, err := h.eventClient.GetEvent(c.Request.Context(), &eventv1.GetEventRequest{
		EventId: c.Param("id"),
	})
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
		return
	}

	c.JSON(http.StatusOK, toEventJSON(resp))
}

func (h *EventHandler) GetAdminEvent(c *gin.Context) {
	resp, err := h.eventClient.GetAdminEvent(c.Request.Context(), &eventv1.GetEventRequest{EventId: c.Param("id")})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to get event"})
		return
	}
	c.JSON(http.StatusOK, toEventJSON(resp))
}

func (h *EventHandler) GetSeatsOfEventByTierId(c *gin.Context) {
	eventID := c.Query("event_id")
	if eventID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "event_id is required"})
		return
	}

	tierID := c.Query("ticket_tier_id")

	resp, err := h.eventClient.GetSeatsOfEventByTierId(c.Request.Context(), &eventv1.GetSeatsRequest{
		EventId:      eventID,
		TicketTierId: tierID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to get seats"})
		return
	}

	seats := make([]gin.H, 0)
	for _, seat := range resp.Seats {
		// Parse position JSON string into object
		var position map[string]interface{}
		_ = json.Unmarshal([]byte(seat.Position), &position)

		// Public availability allowlist: internal ownership stays in Event/gRPC.
		seats = append(seats, gin.H{
			"id":             seat.Id,
			"event_id":       seat.EventId,
			"ticket_tier_id": seat.TicketTierId,
			"status":         seat.Status,
			"position":       position,
			"created_at":     seat.CreatedAt.AsTime().Format(time.RFC3339),
			"updated_at":     seat.UpdatedAt.AsTime().Format(time.RFC3339),
			"seat_revision":  strconv.FormatInt(seat.SeatRevision, 10),
		})
	}

	c.JSON(http.StatusOK, gin.H{"seats": seats})
}

func (h *EventHandler) CreateEvent(c *gin.Context) {
	var req struct {
		Title             string `json:"title" binding:"required"`
		Description       string `json:"description"`
		Category          string `json:"category" binding:"required"`
		Venue             string `json:"venue" binding:"required"`
		Location          string `json:"location" binding:"required"`
		Date              string `json:"date" binding:"required"`
		ImageURL          string `json:"image_url"`
		PublicationStatus string `json:"publication_status"`
		SalesStatus       string `json:"sales_status"`
		SalesStartAt      string `json:"sales_start_at"`
		SalesEndAt        string `json:"sales_end_at"`
		Tiers             []struct {
			Name          string `json:"name" binding:"required"`
			PriceCents    int64  `json:"price_cents" binding:"required"`
			TotalQuantity int32  `json:"total_quantity" binding:"required"`
		} `json:"tiers"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	date, err := time.Parse(time.RFC3339, req.Date)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid date format, use RFC3339"})
		return
	}
	salesStartAt, err := parseOptionalTimestamp(req.SalesStartAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sales_start_at; use RFC3339"})
		return
	}
	salesEndAt, err := parseOptionalTimestamp(req.SalesEndAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sales_end_at; use RFC3339"})
		return
	}

	var tiers []*eventv1.CreateTicketTierRequest
	for _, t := range req.Tiers {
		tiers = append(tiers, &eventv1.CreateTicketTierRequest{
			Name: t.Name, PriceCents: t.PriceCents, TotalQuantity: t.TotalQuantity,
		})
	}
	requestID := c.GetHeader("Idempotency-Key")
	actorID := ""
	if requestID != "" {
		parsedActorID, err := uuid.Parse(c.GetString("user_id"))
		if err != nil || parsedActorID == uuid.Nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authenticated event creator identity is required for idempotency"})
			return
		}
		actorID = parsedActorID.String()
	}

	resp, err := h.eventClient.CreateEvent(c.Request.Context(), &eventv1.CreateEventRequest{
		Title: req.Title, Description: req.Description, Category: req.Category,
		Venue: req.Venue, Location: req.Location, Date: timestamppb.New(date),
		ImageUrl: req.ImageURL, Tiers: tiers,
		PublicationStatus: req.PublicationStatus, SalesStatus: req.SalesStatus,
		SalesStartAt: salesStartAt, SalesEndAt: salesEndAt,
		ActorId: actorID, RequestId: requestID,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event data or ticket inventory"})
		case codes.AlreadyExists:
			c.JSON(http.StatusConflict, gin.H{"error": "idempotency key was already used with different event data", "code": "EVENT_CREATE_IDEMPOTENCY_CONFLICT"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to create event and ticket inventory"})
		}
		return
	}

	c.JSON(http.StatusCreated, toEventJSON(resp))
}

func (h *EventHandler) UpdateEventSales(c *gin.Context) {
	var req struct {
		PublicationStatus string `json:"publication_status" binding:"required"`
		SalesStatus       string `json:"sales_status" binding:"required"`
		SalesStartAt      string `json:"sales_start_at"`
		SalesEndAt        string `json:"sales_end_at"`
		ClearSalesWindow  bool   `json:"clear_sales_window"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	salesStartAt, err := parseOptionalTimestamp(req.SalesStartAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sales_start_at; use RFC3339"})
		return
	}
	salesEndAt, err := parseOptionalTimestamp(req.SalesEndAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sales_end_at; use RFC3339"})
		return
	}
	resp, err := h.eventClient.UpdateEventSales(c.Request.Context(), &eventv1.UpdateEventSalesRequest{
		EventId: c.Param("id"), PublicationStatus: req.PublicationStatus, SalesStatus: req.SalesStatus,
		SalesStartAt: salesStartAt, SalesEndAt: salesEndAt, ClearSalesWindow: req.ClearSalesWindow,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
		case codes.InvalidArgument:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event sales lifecycle or window", "code": "EVENT_SALE_WINDOW_INVALID"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "failed to update event sales"})
		}
		return
	}
	c.JSON(http.StatusOK, toEventJSON(resp))
}

func parseOptionalTimestamp(value string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(parsed), nil
}

func (h *EventHandler) UpdateEvent(c *gin.Context) {
	var req struct {
		ExpectedDateVersion int64  `json:"expected_date_version"`
		Title               string `json:"title"`
		Description         string `json:"description"`
		Category            string `json:"category"`
		Venue               string `json:"venue"`
		Location            string `json:"location"`
		Date                string `json:"date"`
		ImageURL            string `json:"image_url"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	updateReq := &eventv1.UpdateEventRequest{
		ExpectedDateVersion: req.ExpectedDateVersion, EventId: c.Param("id"), Title: req.Title, Description: req.Description,
		Category: req.Category, Venue: req.Venue, Location: req.Location, ImageUrl: req.ImageURL,
	}
	if req.Date != "" {
		date, err := time.Parse(time.RFC3339, req.Date)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid date format"})
			return
		}
		updateReq.Date = timestamppb.New(date)
	}

	resp, err := h.eventClient.UpdateEvent(c.Request.Context(), updateReq)
	if err != nil {
		if eventTimeConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update event"})
		return
	}

	c.JSON(http.StatusOK, toEventJSON(resp))
}

func (h *EventHandler) DeleteEvent(c *gin.Context) {
	_, err := h.eventClient.DeleteEvent(c.Request.Context(), &eventv1.DeleteEventRequest{
		EventId: c.Param("id"),
	})
	if err != nil {
		if eventTimeConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete event"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "event sales cancelled; event and ticket records were retained"})
}

func toEventJSON(e *eventv1.EventDetail) gin.H {
	tiers := make([]gin.H, 0)
	for _, t := range e.Tiers {
		tiers = append(tiers, gin.H{
			"id":                 t.Id,
			"event_id":           t.EventId,
			"name":               t.Name,
			"price_cents":        t.PriceCents,
			"total_quantity":     t.TotalQuantity,
			"available_quantity": t.AvailableQuantity,
			"version":            t.Version,
			"accounting_ready":   t.AccountingReady,
		})
	}
	return gin.H{
		"id":                 e.Id,
		"title":              e.Title,
		"description":        e.Description,
		"category":           e.Category,
		"venue":              e.Venue,
		"location":           e.Location,
		"date":               e.Date.AsTime().Format(time.RFC3339),
		"image_url":          e.ImageUrl,
		"status":             e.Status,
		"publication_status": e.PublicationStatus,
		"sales_status":       e.SalesStatus,
		"sales_start_at":     timestampStringOptional(e.SalesStartAt),
		"sales_end_at":       timestampStringOptional(e.SalesEndAt),
		"tiers":              tiers,
		"created_at":         e.CreatedAt.AsTime().Format(time.RFC3339),
		"date_version":       e.DateVersion,
	}
}

func timestampStringOptional(value *timestamppb.Timestamp) string {
	if value == nil || !value.IsValid() {
		return ""
	}
	return value.AsTime().Format(time.RFC3339)
}

func eventTimeConflict(c *gin.Context, err error) bool {
	switch status.Code(err) {
	case codes.Aborted:
		c.JSON(http.StatusConflict, gin.H{"error": "Event time changed; refresh before editing.", "code": "EVENT_DATE_CONFLICT"})
		return true
	case codes.FailedPrecondition:
		c.JSON(http.StatusConflict, gin.H{"error": "Event time is protected by a pending refund approval. Retry after it resolves.", "code": "EVENT_REFUND_TIME_PENDING"})
		return true
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid event time or version.", "code": "EVENT_DATE_INVALID"})
		return true
	}
	return false
}
