package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
)

type eventListClientStub struct {
	eventv1.EventServiceClient
	listRequest      *eventv1.ListEventsRequest
	adminListRequest *eventv1.ListEventsRequest
	listCalls        int
	adminListCalls   int
	response         *eventv1.ListEventsResponse
	err              error
}

func (c *eventListClientStub) ListEvents(_ context.Context, req *eventv1.ListEventsRequest, _ ...grpc.CallOption) (*eventv1.ListEventsResponse, error) {
	c.listCalls++
	c.listRequest = req
	return c.response, c.err
}

func (c *eventListClientStub) ListAdminEvents(_ context.Context, req *eventv1.ListEventsRequest, _ ...grpc.CallOption) (*eventv1.ListEventsResponse, error) {
	c.adminListCalls++
	c.adminListRequest = req
	return c.response, c.err
}

func TestEventDiscoveryListParsesInclusiveDatesAndIntegerCents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &eventListClientStub{response: &eventv1.ListEventsResponse{Events: []*eventv1.EventDetail{}, Total: 7, Page: 3, PageSize: 50}}
	router := gin.New()
	router.GET("/api/events", NewEventHandler(client, nil).ListEvents)
	request := httptest.NewRequest(http.MethodGet, "/api/events?category=concert&search=Jazz&date_from=2026-12-01&date_to=2026-12-31&location=Seattle&min_price_cents=0&max_price_cents=72500&sort=price_asc&page=3&page_size=50&include_private=true", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || client.listCalls != 1 {
		t.Fatalf("list status=%d calls=%d body=%s", response.Code, client.listCalls, response.Body.String())
	}
	req := client.listRequest
	if req.GetCategory() != "concert" || req.GetSearch() != "Jazz" || req.GetLocation() != "Seattle" || req.GetSort() != "price_asc" || req.GetPage() != 3 || req.GetPageSize() != 50 {
		t.Fatalf("parsed event list request=%+v", req)
	}
	if req.DateFrom == nil || req.DateFrom.AsTime() != time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("date_from should be inclusive UTC midnight: %+v", req.DateFrom)
	}
	if req.DateToExclusive == nil || req.DateToExclusive.AsTime() != time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("inclusive date_to was not converted to exclusive next midnight: %+v", req.DateToExclusive)
	}
	if req.MinPriceCents == nil || *req.MinPriceCents != 0 || req.MaxPriceCents == nil || *req.MaxPriceCents != 72500 {
		t.Fatalf("integer-cent bounds lost optional presence: min=%v max=%v", req.MinPriceCents, req.MaxPriceCents)
	}
}

func TestEventDiscoveryListDefaultsAndAdminContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &eventListClientStub{response: &eventv1.ListEventsResponse{Events: []*eventv1.EventDetail{}, Total: 0, Page: 1, PageSize: 20}}
	handler := NewEventHandler(client, nil)
	publicRouter := gin.New()
	publicRouter.GET("/api/events", handler.ListEvents)
	publicResponse := httptest.NewRecorder()
	publicRouter.ServeHTTP(publicResponse, httptest.NewRequest(http.MethodGet, "/api/events?search=Live", nil))
	if publicResponse.Code != http.StatusOK || client.listRequest.GetPage() != 1 || client.listRequest.GetPageSize() != 20 || client.listRequest.GetSort() != "date_asc" {
		t.Fatalf("default public list request=%+v status=%d body=%s", client.listRequest, publicResponse.Code, publicResponse.Body.String())
	}

	adminRouter := gin.New()
	adminRouter.GET("/api/admin/events", handler.ListAdminEvents)
	adminResponse := httptest.NewRecorder()
	adminRouter.ServeHTTP(adminResponse, httptest.NewRequest(http.MethodGet, "/api/admin/events?location=Harbor&min_price_cents=500", nil))
	if adminResponse.Code != http.StatusOK || client.adminListCalls != 1 || client.adminListRequest.GetLocation() != "Harbor" || client.adminListRequest.MinPriceCents == nil || *client.adminListRequest.MinPriceCents != 500 {
		t.Fatalf("admin list request=%+v calls=%d status=%d body=%s", client.adminListRequest, client.adminListCalls, adminResponse.Code, adminResponse.Body.String())
	}
}

func TestEventDiscoveryRejectsInvalidRangesAndUnboundedPagingBeforeRPC(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{
		"/api/events?date_from=2026-02-30",
		"/api/events?date_from=2026-12-31&date_to=2026-12-30",
		"/api/events?min_price_cents=500&max_price_cents=499",
		"/api/events?max_price_cents=-1",
		"/api/events?page=1001",
		"/api/events?page_size=51",
		"/api/events?sort=price_desc%3Bdrop%20table",
		"/api/events?min_price_cents=1&min_price_cents=2",
	} {
		t.Run(path, func(t *testing.T) {
			client := &eventListClientStub{}
			router := gin.New()
			router.GET("/api/events", NewEventHandler(client, nil).ListEvents)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusBadRequest || client.listCalls != 0 {
				t.Fatalf("invalid query reached Event RPC: status=%d calls=%d body=%s", response.Code, client.listCalls, response.Body.String())
			}
		})
	}
}
