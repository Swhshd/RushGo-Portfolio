package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	eventInterestTestUserID  = "11111111-1111-4111-8111-111111111111"
	eventInterestOtherUserID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	eventInterestTestEventID = "22222222-2222-4222-8222-222222222222"
)

// Embed the generated client so tests only need to implement RPCs used by the
// handler contract under test.
type eventInterestClientStub struct {
	eventv1.EventServiceClient

	listRequests     []*eventv1.ListMyEventInterestsRequest
	listResponse     *eventv1.ListMyEventInterestsResponse
	listErr          error
	getRequests      []*eventv1.GetMyEventInterestsRequest
	getResponse      *eventv1.GetMyEventInterestsResponse
	getErr           error
	upsertRequests   []*eventv1.UpsertMyEventInterestRequest
	upsertResponse   *eventv1.EventInterest
	upsertErr        error
	withdrawRequests []*eventv1.WithdrawMyEventInterestRequest
	withdrawResponse *eventv1.EventInterest
	withdrawErr      error
	disableRequests  []*eventv1.DisableMyEventAlertEmailsRequest
	disableResponse  *eventv1.DisableMyEventAlertEmailsResponse
	disableErr       error
	countRequests    []*eventv1.ReadEventInterestCountsRequest
	countResponse    *eventv1.EventInterestCounts
	countErr         error
}

func (s *eventInterestClientStub) ListMyEventInterests(_ context.Context, req *eventv1.ListMyEventInterestsRequest, _ ...grpc.CallOption) (*eventv1.ListMyEventInterestsResponse, error) {
	s.listRequests = append(s.listRequests, req)
	return s.listResponse, s.listErr
}

func (s *eventInterestClientStub) GetMyEventInterests(_ context.Context, req *eventv1.GetMyEventInterestsRequest, _ ...grpc.CallOption) (*eventv1.GetMyEventInterestsResponse, error) {
	s.getRequests = append(s.getRequests, req)
	return s.getResponse, s.getErr
}

func (s *eventInterestClientStub) UpsertMyEventInterest(_ context.Context, req *eventv1.UpsertMyEventInterestRequest, _ ...grpc.CallOption) (*eventv1.EventInterest, error) {
	s.upsertRequests = append(s.upsertRequests, req)
	if s.upsertErr != nil {
		return nil, s.upsertErr
	}
	if s.upsertResponse != nil {
		return s.upsertResponse, nil
	}
	return &eventv1.EventInterest{
		EventId: req.GetEventId(), Kind: req.GetKind(), Status: "active",
		EmailOptIn: req.GetEmailOptIn(), EmailConsentVersion: req.GetEmailConsentVersion(),
	}, nil
}

func (s *eventInterestClientStub) WithdrawMyEventInterest(_ context.Context, req *eventv1.WithdrawMyEventInterestRequest, _ ...grpc.CallOption) (*eventv1.EventInterest, error) {
	s.withdrawRequests = append(s.withdrawRequests, req)
	if s.withdrawErr != nil {
		return nil, s.withdrawErr
	}
	if s.withdrawResponse != nil {
		return s.withdrawResponse, nil
	}
	return &eventv1.EventInterest{EventId: req.GetEventId(), Kind: req.GetKind(), Status: "withdrawn"}, nil
}

func (s *eventInterestClientStub) DisableMyEventAlertEmails(_ context.Context, req *eventv1.DisableMyEventAlertEmailsRequest, _ ...grpc.CallOption) (*eventv1.DisableMyEventAlertEmailsResponse, error) {
	s.disableRequests = append(s.disableRequests, req)
	if s.disableErr != nil {
		return nil, s.disableErr
	}
	if s.disableResponse != nil {
		return s.disableResponse, nil
	}
	return &eventv1.DisableMyEventAlertEmailsResponse{Kind: req.GetKind(), Updated: 3}, nil
}

func (s *eventInterestClientStub) ReadEventInterestCounts(_ context.Context, req *eventv1.ReadEventInterestCountsRequest, _ ...grpc.CallOption) (*eventv1.EventInterestCounts, error) {
	s.countRequests = append(s.countRequests, req)
	return s.countResponse, s.countErr
}

func eventInterestTestRouter(userID string, method, path string, route gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	if userID != "" {
		engine.Use(func(c *gin.Context) {
			c.Set("user_id", userID)
			c.Next()
		})
	}
	engine.Handle(method, path, route)
	return engine
}

func eventInterestPerform(engine http.Handler, method, target, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(method, target, strings.NewReader(body)))
	return response
}

func eventInterestDecodeJSON(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode response JSON: %v; body=%s", err, response.Body.String())
	}
	return value
}

func eventInterestAssertKeys(t *testing.T, value map[string]any, want ...string) {
	t.Helper()
	got := make([]string, 0, len(value))
	for key := range value {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("JSON keys=%v, want %v", got, want)
	}
}

func TestEventInterestHandlerFavoriteUpsertIsIdempotentAndUsesAuthenticatedOwner(t *testing.T) {
	client := &eventInterestClientStub{}
	handler := NewEventInterestHandler(client)
	engine := gin.New()
	engine.Use(func(c *gin.Context) { c.Set("user_id", eventInterestTestUserID); c.Next() })
	engine.PUT("/favorites/:eventID", handler.SetFavorite)
	engine.DELETE("/favorites/:eventID", handler.RemoveFavorite)

	uppercaseEventID := strings.ToUpper(eventInterestTestEventID)
	for i := 0; i < 2; i++ {
		response := eventInterestPerform(engine, http.MethodPut, "/favorites/"+uppercaseEventID+"?userId="+eventInterestOtherUserID, `{"userId":"`+eventInterestOtherUserID+`"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("favorite upsert status=%d body=%s", response.Code, response.Body.String())
		}
		value := eventInterestDecodeJSON(t, response)
		eventInterestAssertKeys(t, value, "eventId", "favorite")
		if value["eventId"] != eventInterestTestEventID || value["favorite"] != true {
			t.Fatalf("favorite response=%v", value)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("favorite Cache-Control=%q", response.Header().Get("Cache-Control"))
		}
	}
	if len(client.upsertRequests) != 2 {
		t.Fatalf("favorite upsert RPC count=%d, want 2", len(client.upsertRequests))
	}
	for _, request := range client.upsertRequests {
		if request.GetUserId() != eventInterestTestUserID || request.GetEventId() != eventInterestTestEventID || request.GetKind() != "favorite" {
			t.Fatalf("favorite RPC request did not use authenticated identity/canonical event: %+v", request)
		}
	}

	client.withdrawErr = status.Error(codes.NotFound, "favorite absent")
	response := eventInterestPerform(engine, http.MethodDelete, "/favorites/"+eventInterestTestEventID, "")
	if response.Code != http.StatusOK {
		t.Fatalf("repeated favorite removal status=%d body=%s", response.Code, response.Body.String())
	}
	value := eventInterestDecodeJSON(t, response)
	if value["favorite"] != false || value["eventId"] != eventInterestTestEventID {
		t.Fatalf("idempotent removal response=%v", value)
	}
	if len(client.withdrawRequests) != 1 || client.withdrawRequests[0].GetUserId() != eventInterestTestUserID || client.withdrawRequests[0].GetKind() != "favorite" {
		t.Fatalf("favorite withdrawal request=%+v", client.withdrawRequests)
	}
}

func TestEventInterestHandlerRejectsInvalidEventUUIDWithoutRPC(t *testing.T) {
	client := &eventInterestClientStub{}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter(eventInterestTestUserID, http.MethodPut, "/favorites/:eventID", handler.SetFavorite)

	for _, eventID := range []string{"not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		response := eventInterestPerform(engine, http.MethodPut, "/favorites/"+eventID, "")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_event_id"`) {
			t.Fatalf("invalid UUID %q response status=%d body=%s", eventID, response.Code, response.Body.String())
		}
	}
	if len(client.upsertRequests) != 0 {
		t.Fatalf("invalid event IDs reached RPC: %+v", client.upsertRequests)
	}
}

func TestEventInterestHandlerAlertValidationConsentAndAuthenticatedOwner(t *testing.T) {
	client := &eventInterestClientStub{}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter(eventInterestTestUserID, http.MethodPut, "/events/:eventID/alerts/:kind", handler.SetAlert)

	response := eventInterestPerform(engine, http.MethodPut, "/events/"+strings.ToUpper(eventInterestTestEventID)+"/alerts/restock?userId="+eventInterestOtherUserID, `{"emailOptIn":true,"consentVersion":"event-alerts-v1"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("valid alert response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("valid alert upsert count=%d, want 1", len(client.upsertRequests))
	}
	request := client.upsertRequests[0]
	if request.GetUserId() != eventInterestTestUserID || request.GetEventId() != eventInterestTestEventID || request.GetKind() != "restock" || !request.GetEmailOptIn() || request.GetEmailConsentVersion() != "event-alerts-v1" {
		t.Fatalf("alert RPC did not preserve authenticated owner/kind/consent: %+v", request)
	}
	value := eventInterestDecodeJSON(t, response)
	eventInterestAssertKeys(t, value, "eventId", "kind", "emailOptIn", "consentVersion", "status", "createdAt", "updatedAt")
	if value["emailOptIn"] != true || value["consentVersion"] != "event-alerts-v1" || value["status"] != "active" {
		t.Fatalf("alert response projection=%v", value)
	}

	invalid := []struct {
		name string
		path string
		body string
	}{
		{name: "unsupported kind", path: "/events/" + eventInterestTestEventID + "/alerts/sale", body: `{}`},
		{name: "malformed JSON", path: "/events/" + eventInterestTestEventID + "/alerts/onsale", body: `{"emailOptIn":`},
		{name: "unknown owner field", path: "/events/" + eventInterestTestEventID + "/alerts/onsale", body: `{"emailOptIn":false,"userId":"` + eventInterestOtherUserID + `"}`},
		{name: "trailing JSON value", path: "/events/" + eventInterestTestEventID + "/alerts/onsale", body: `{} {}`},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			response := eventInterestPerform(engine, http.MethodPut, test.path, test.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid alert request status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("invalid alert requests reached RPC: %+v", client.upsertRequests)
	}
}

func TestEventInterestHandlerListAlertsPaginationAndPerEventStateUseCamelCase(t *testing.T) {
	createdAt := timestamppb.New(time.Date(2026, time.November, 13, 4, 5, 6, 0, time.UTC))
	updatedAt := timestamppb.New(time.Date(2026, time.November, 13, 4, 6, 7, 0, time.UTC))
	client := &eventInterestClientStub{
		listResponse: &eventv1.ListMyEventInterestsResponse{
			Items: []*eventv1.EventInterest{{EventId: eventInterestTestEventID, Kind: "onsale", Status: "active", EmailOptIn: true, EmailConsentVersion: "event-alerts-v1", CreatedAt: createdAt, UpdatedAt: updatedAt}},
			Limit: 7, Offset: 14, HasMore: true,
		},
		getResponse: &eventv1.GetMyEventInterestsResponse{
			EventId: eventInterestTestEventID, Favorite: true, Subscriptions: []*eventv1.EventInterest{{EventId: eventInterestTestEventID, Kind: "restock", Status: "active", EmailOptIn: false, EmailConsentVersion: "event-alerts-v1", CreatedAt: createdAt, UpdatedAt: updatedAt}},
			SaleStatus: "scheduled", OnsaleAvailable: false, RestockStatus: "available",
		},
	}
	handler := NewEventInterestHandler(client)
	engine := gin.New()
	engine.Use(func(c *gin.Context) { c.Set("user_id", eventInterestTestUserID); c.Next() })
	engine.GET("/me/alerts", handler.ListAlerts)
	engine.GET("/me/events/:eventID/interests", handler.GetInterests)

	response := eventInterestPerform(engine, http.MethodGet, "/me/alerts?limit=7&offset=14&userId="+eventInterestOtherUserID, "")
	if response.Code != http.StatusOK {
		t.Fatalf("list alerts response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.listRequests) != 1 {
		t.Fatalf("list RPC count=%d, want 1", len(client.listRequests))
	}
	listRequest := client.listRequests[0]
	if listRequest.GetUserId() != eventInterestTestUserID || listRequest.GetKind() != "" || listRequest.GetLimit() != 7 || listRequest.GetOffset() != 14 {
		t.Fatalf("list request did not use auth owner/combined pagination: %+v", listRequest)
	}
	value := eventInterestDecodeJSON(t, response)
	eventInterestAssertKeys(t, value, "items", "limit", "offset", "hasMore")
	if value["limit"] != float64(7) || value["offset"] != float64(14) || value["hasMore"] != true {
		t.Fatalf("list pagination response=%v", value)
	}
	items, ok := value["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("list items=%#v", value["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("list item=%#v", items[0])
	}
	eventInterestAssertKeys(t, item, "eventId", "kind", "emailOptIn", "consentVersion", "status", "createdAt", "updatedAt")
	if item["eventId"] != eventInterestTestEventID || item["consentVersion"] != "event-alerts-v1" || item["emailOptIn"] != true {
		t.Fatalf("combined alert item=%v", item)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("alerts Cache-Control=%q", response.Header().Get("Cache-Control"))
	}

	response = eventInterestPerform(engine, http.MethodGet, "/me/events/"+eventInterestTestEventID+"/interests", "")
	if response.Code != http.StatusOK {
		t.Fatalf("per-event interests response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.getRequests) != 1 || client.getRequests[0].GetUserId() != eventInterestTestUserID || client.getRequests[0].GetEventId() != eventInterestTestEventID {
		t.Fatalf("per-event interests RPC request=%+v", client.getRequests)
	}
	value = eventInterestDecodeJSON(t, response)
	eventInterestAssertKeys(t, value, "eventId", "favorite", "subscriptions", "saleStatus", "onsaleAvailable", "restockStatus")
	if value["favorite"] != true || value["saleStatus"] != "scheduled" || value["onsaleAvailable"] != false || value["restockStatus"] != "available" {
		t.Fatalf("per-event state=%v", value)
	}
	subscriptions, ok := value["subscriptions"].([]any)
	if !ok || len(subscriptions) != 1 {
		t.Fatalf("per-event subscriptions=%#v", value["subscriptions"])
	}
	subscription, ok := subscriptions[0].(map[string]any)
	if !ok {
		t.Fatalf("per-event subscription=%#v", subscriptions[0])
	}
	eventInterestAssertKeys(t, subscription, "eventId", "kind", "emailOptIn", "consentVersion", "status", "createdAt", "updatedAt")
}

func TestEventInterestHandlerFavoriteListUsesFavoriteFilterAndProjection(t *testing.T) {
	client := &eventInterestClientStub{listResponse: &eventv1.ListMyEventInterestsResponse{
		Items: []*eventv1.EventInterest{{EventId: eventInterestTestEventID, Kind: "favorite", Status: "active"}}, Limit: 20,
	}}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter(eventInterestTestUserID, http.MethodGet, "/me/favorites", handler.ListFavorites)
	response := eventInterestPerform(engine, http.MethodGet, "/me/favorites", "")
	if response.Code != http.StatusOK || len(client.listRequests) != 1 {
		t.Fatalf("favorite list status=%d requests=%d body=%s", response.Code, len(client.listRequests), response.Body.String())
	}
	request := client.listRequests[0]
	if request.GetUserId() != eventInterestTestUserID || request.GetKind() != "favorite" {
		t.Fatalf("favorite list request=%+v", request)
	}
	value := eventInterestDecodeJSON(t, response)
	items, ok := value["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("favorite list items=%#v", value["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("favorite list item=%#v", items[0])
	}
	eventInterestAssertKeys(t, item, "eventId", "createdAt")
}

func TestEventInterestHandlerDisablesCategoryEmailsForAuthenticatedOwner(t *testing.T) {
	client := &eventInterestClientStub{}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter(eventInterestTestUserID, http.MethodDelete, "/me/event-alerts/:kind/email", handler.DisableAlertEmails)
	response := eventInterestPerform(engine, http.MethodDelete, "/me/event-alerts/restock/email?userId="+eventInterestOtherUserID, "")
	if response.Code != http.StatusOK {
		t.Fatalf("disable email response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.disableRequests) != 1 || client.disableRequests[0].GetUserId() != eventInterestTestUserID || client.disableRequests[0].GetKind() != "restock" {
		t.Fatalf("disable email RPC request=%+v", client.disableRequests)
	}
	value := eventInterestDecodeJSON(t, response)
	eventInterestAssertKeys(t, value, "kind", "updated")
	if value["kind"] != "restock" || value["updated"] != float64(3) {
		t.Fatalf("disable email response=%v", value)
	}
}

func TestEventInterestHandlerAdminCountsExposeOnlyBoundedAggregateProjection(t *testing.T) {
	observedAt := timestamppb.New(time.Date(2026, time.November, 13, 5, 6, 7, 0, time.UTC))
	client := &eventInterestClientStub{countResponse: &eventv1.EventInterestCounts{Favorites: 11, Onsale: 7, Restock: 4, ObservedAt: observedAt}}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter("", http.MethodGet, "/admin/events/:id/engagement", handler.AdminEventEngagement)
	response := eventInterestPerform(engine, http.MethodGet, "/admin/events/"+strings.ToUpper(eventInterestTestEventID)+"/engagement", "")
	if response.Code != http.StatusOK {
		t.Fatalf("admin counts response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.countRequests) != 1 || client.countRequests[0].GetEventId() != eventInterestTestEventID {
		t.Fatalf("admin count RPC request=%+v", client.countRequests)
	}
	value := eventInterestDecodeJSON(t, response)
	eventInterestAssertKeys(t, value, "favorites", "onsale", "restock", "observedAt")
	if value["favorites"] != float64(11) || value["onsale"] != float64(7) || value["restock"] != float64(4) || value["observedAt"] != observedAt.AsTime().Format(time.RFC3339Nano) {
		t.Fatalf("admin count projection=%v", value)
	}

	response = eventInterestPerform(engine, http.MethodGet, "/admin/events/not-a-uuid/engagement", "")
	if response.Code != http.StatusBadRequest || len(client.countRequests) != 1 {
		t.Fatalf("invalid admin event ID status=%d requests=%d body=%s", response.Code, len(client.countRequests), response.Body.String())
	}
}

func TestEventInterestHandlerSurfacesFirstSaleConflictWithoutSuccessPayload(t *testing.T) {
	client := &eventInterestClientStub{upsertErr: status.Error(codes.AlreadyExists, "event is already on sale")}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter(eventInterestTestUserID, http.MethodPut, "/events/:eventID/alerts/:kind", handler.SetAlert)
	response := eventInterestPerform(engine, http.MethodPut, "/events/"+eventInterestTestEventID+"/alerts/onsale", `{"emailOptIn":false,"consentVersion":""}`)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"code":"already_on_sale"`) {
		t.Fatalf("first-sale conflict status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"status":"active"`) || strings.Contains(response.Body.String(), `"eventId"`) {
		t.Fatalf("conflict returned a success-shaped payload: %s", response.Body.String())
	}
	if len(client.upsertRequests) != 1 || client.upsertRequests[0].GetUserId() != eventInterestTestUserID || client.upsertRequests[0].GetKind() != "onsale" {
		t.Fatalf("first-sale conflict request=%+v", client.upsertRequests)
	}
}

func TestEventInterestHandlerRequiresAuthenticatedOwner(t *testing.T) {
	client := &eventInterestClientStub{}
	handler := NewEventInterestHandler(client)
	engine := eventInterestTestRouter("", http.MethodPut, "/favorites/:eventID", handler.SetFavorite)
	response := eventInterestPerform(engine, http.MethodPut, "/favorites/"+eventInterestTestEventID, "")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated favorite status=%d body=%s", response.Code, response.Body.String())
	}
	if len(client.upsertRequests) != 0 {
		t.Fatalf("unauthenticated request reached RPC: %+v", client.upsertRequests)
	}
}
