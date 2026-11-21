//go:build admin_observability_e2e

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ticketbox/gateway/internal/router"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fixtureHTTPEventReadClient struct {
	eventv1.EventServiceClient
	event     *eventv1.EventDetail
	listCalls int
	getCalls  int
}

func (c *fixtureHTTPEventReadClient) ListEvents(context.Context, *eventv1.ListEventsRequest, ...grpc.CallOption) (*eventv1.ListEventsResponse, error) {
	c.listCalls++
	return &eventv1.ListEventsResponse{Events: []*eventv1.EventDetail{c.event}, Total: 1, Page: 1, PageSize: 20}, nil
}

func (c *fixtureHTTPEventReadClient) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	c.getCalls++
	return c.event, nil
}

func TestFixtureHTTPAllowsOnlyCurrentOwnerPublicEventReads(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventClient := &fixtureHTTPEventReadClient{event: &eventv1.EventDetail{
		Id: "event-fixture", Title: "Fixture event", Date: timestamppb.New(time.Unix(10, 0)), CreatedAt: timestamppb.New(time.Unix(1, 0)),
	}}
	engine := router.SetupRouter(nil, eventClient, nil, nil, nil, nil, nil)

	currentOwner := allowFixtureHTTP(engine, "admin-event-operations")
	for _, endpoint := range []string{"/api/events", "/api/events/event-fixture"} {
		response := httptest.NewRecorder()
		currentOwner.ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("current-owner GET %s status=%d, want %d", endpoint, response.Code, http.StatusOK)
		}
	}
	if eventClient.listCalls != 1 || eventClient.getCalls != 1 {
		t.Fatalf("real Event router calls: list=%d get=%d, want one each", eventClient.listCalls, eventClient.getCalls)
	}

	for _, request := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/events/event-fixture/seats/stream"},
		{method: http.MethodGet, path: "/api/events/event-fixture/scanners"},
		{method: http.MethodPost, path: "/api/events"},
	} {
		response := httptest.NewRecorder()
		currentOwner.ServeHTTP(response, httptest.NewRequest(request.method, request.path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("current-owner %s %s status=%d, want 404", request.method, request.path, response.Code)
		}
	}
	if eventClient.listCalls != 1 || eventClient.getCalls != 1 {
		t.Fatalf("non-public routes reached Event service: list=%d get=%d", eventClient.listCalls, eventClient.getCalls)
	}

	previousOwner := allowFixtureHTTP(engine, "admin-observability-v2")
	for _, endpoint := range []string{"/api/events", "/api/events/event-fixture"} {
		response := httptest.NewRecorder()
		previousOwner.ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("previous-owner GET %s status=%d, want 404", endpoint, response.Code)
		}
	}
	if eventClient.listCalls != 1 || eventClient.getCalls != 1 {
		t.Fatalf("previous owner reached Event service: list=%d get=%d", eventClient.listCalls, eventClient.getCalls)
	}
}
