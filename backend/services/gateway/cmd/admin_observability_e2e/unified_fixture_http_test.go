//go:build admin_observability_e2e

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fixtureHTTPRecordedRequest struct {
	method string
	path   string
	query  string
}

type fixtureHTTPRouteSpy struct {
	requests []fixtureHTTPRecordedRequest
}

func (s *fixtureHTTPRouteSpy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests = append(s.requests, fixtureHTTPRecordedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"forwarded":true}`))
}

func TestUnifiedFixtureHTTPForwardsOnlySupportedReadRoutesAndAuth(t *testing.T) {
	spy := &fixtureHTTPRouteSpy{}
	handler := allowFixtureHTTP(spy, "admin-unified-preview")
	routes := []fixtureHTTPRecordedRequest{
		{method: http.MethodGet, path: "/api/admin/outbox-events", query: "source=booking"},
		{method: http.MethodGet, path: "/api/admin/outbox-events", query: "source=notification"},
		{method: http.MethodGet, path: "/api/events", query: "page=1&page_size=20"},
		{method: http.MethodGet, path: "/api/events/fixture-event"},
		{method: http.MethodGet, path: "/api/admin/events/fixture-event/ticket-operations"},
		{method: http.MethodGet, path: "/api/admin/events/fixture-event/tickets", query: "status=ISSUED&page_size=25"},
		{method: http.MethodPost, path: "/api/auth/login"},
		{method: http.MethodPost, path: "/api/auth/refresh"},
		{method: http.MethodPost, path: "/api/auth/logout"},
		{method: http.MethodGet, path: "/api/users/me"},
	}

	for _, route := range routes {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(route.method, route.path+querySuffix(route.query), nil)
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("%s %s status=%d, want %d", route.method, request.URL.String(), response.Code, http.StatusAccepted)
		}
		if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
			t.Fatalf("%s %s Content-Type=%q, want JSON", route.method, request.URL.String(), response.Header().Get("Content-Type"))
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s Cache-Control=%q, want no-store", route.method, request.URL.String(), response.Header().Get("Cache-Control"))
		}
		var body map[string]bool
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || !body["forwarded"] {
			t.Fatalf("%s %s body=%q, want forwarded JSON, err=%v", route.method, request.URL.String(), response.Body.String(), err)
		}
	}

	if len(spy.requests) != len(routes) {
		t.Fatalf("forwarded request count=%d, want %d", len(spy.requests), len(routes))
	}
	for i, want := range routes {
		if got := spy.requests[i]; got != want {
			t.Errorf("forwarded request[%d]=%+v, want %+v", i, got, want)
		}
	}
}

func TestUnifiedFixtureHTTPRejectsUnsupportedOwnerMethodsAndPathsAsJSON404(t *testing.T) {
	cases := []struct {
		name   string
		owner  string
		method string
		path   string
	}{
		{name: "unknown owner", owner: "admin-unified-preview-extra", method: http.MethodGet, path: "/api/admin/outbox-events?source=booking"},
		{name: "legacy owner public events", owner: "admin-observability-v2", method: http.MethodGet, path: "/api/events"},
		{name: "legacy owner ticket list", owner: "admin-observability-v2", method: http.MethodGet, path: "/api/admin/events/fixture-event/tickets"},
		{name: "outbox method", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/admin/outbox-events"},
		{name: "event catalog method", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/events"},
		{name: "ticket list method", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/admin/events/fixture-event/tickets"},
		{name: "registration action", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/auth/register"},
		{name: "registration code action", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/auth/registration-code"},
		{name: "profile mutation", owner: "admin-unified-preview", method: http.MethodPost, path: "/api/users/me"},
		{name: "ticket credential path", owner: "admin-unified-preview", method: http.MethodGet, path: "/api/admin/events/fixture-event/tickets/credential"},
		{name: "event stream path", owner: "admin-unified-preview", method: http.MethodGet, path: "/api/events/fixture-event/seats/stream"},
		{name: "scheduler method", owner: "admin-unified-preview", method: http.MethodPatch, path: "/api/scheduler/jobs"},
		{name: "unknown scheduler owner", owner: "admin-event-operations", method: http.MethodGet, path: "/api/scheduler/jobs"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &fixtureHTTPRouteSpy{}
			handler := allowFixtureHTTP(spy, tc.owner)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("status=%d, want %d; body=%q", response.Code, http.StatusNotFound, response.Body.String())
			}
			assertFixtureJSONNoStore(t, response)
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["code"] != "not_found" {
				t.Fatalf("body=%q, want JSON code not_found, err=%v", response.Body.String(), err)
			}
			if len(spy.requests) != 0 {
				t.Fatalf("unsupported request reached engine: %+v", spy.requests)
			}
		})
	}
}

func TestObservabilityFixtureOutboxForwardingRemainsCompatible(t *testing.T) {
	for _, source := range []string{"booking", "notification"} {
		t.Run(source, func(t *testing.T) {
			spy := &fixtureHTTPRouteSpy{}
			handler := allowFixtureHTTP(spy, "admin-observability-v2")
			response := httptest.NewRecorder()
			path := "/api/admin/outbox-events?source=" + source
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusAccepted {
				t.Fatalf("status=%d, want %d; body=%q", response.Code, http.StatusAccepted, response.Body.String())
			}
			assertFixtureJSONNoStore(t, response)
			if len(spy.requests) != 1 || spy.requests[0].path != "/api/admin/outbox-events" || spy.requests[0].query != "source="+source {
				t.Fatalf("forwarded requests=%+v, want one %s source query", spy.requests, source)
			}
		})
	}
}

func TestEventOperationsFixtureOutboxReturnsExplicitUnavailableJSON(t *testing.T) {
	for _, source := range []string{"booking", "notification"} {
		t.Run(source, func(t *testing.T) {
			spy := &fixtureHTTPRouteSpy{}
			handler := allowFixtureHTTP(spy, "admin-event-operations")
			response := httptest.NewRecorder()
			path := "/api/admin/outbox-events?source=" + source
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want %d; body=%q", response.Code, http.StatusServiceUnavailable, response.Body.String())
			}
			assertFixtureJSONNoStore(t, response)
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["code"] != "source_unavailable" {
				t.Fatalf("body=%q, want source_unavailable JSON, err=%v", response.Body.String(), err)
			}
			if len(spy.requests) != 0 {
				t.Fatalf("unsupported outbox request reached engine: %+v", spy.requests)
			}
		})
	}
}

func TestUnifiedFixtureSchedulerReadReportsNotRunningAsJSON503(t *testing.T) {
	spy := &fixtureHTTPRouteSpy{}
	handler := allowFixtureHTTP(spy, "admin-unified-preview")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/scheduler/jobs", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d; body=%q", response.Code, http.StatusServiceUnavailable, response.Body.String())
	}
	assertFixtureJSONNoStore(t, response)
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["code"] != "not_running" || body["error"] != "scheduler is not running in this fixture" {
		t.Fatalf("body=%q, want NOT_RUNNING JSON status, err=%v", response.Body.String(), err)
	}
	if len(spy.requests) != 0 {
		t.Fatalf("Scheduler request reached engine: %+v", spy.requests)
	}
}

func assertFixtureJSONNoStore(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type=%q, want JSON", response.Header().Get("Content-Type"))
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control=%q, want no-store", response.Header().Get("Cache-Control"))
	}
}

func querySuffix(query string) string {
	if query == "" {
		return ""
	}
	return "?" + query
}
