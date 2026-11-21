//go:build admin_observability_e2e

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFixtureTicketListAllowsOnlyCurrentOwnerExactReadPath(t *testing.T) {
	for _, c := range []struct {
		owner, method, path string
		want                bool
	}{
		{"admin-event-operations", "GET", "/api/admin/events/fixture/tickets", true},
		{"admin-unified-preview", "GET", "/api/admin/events/fixture/tickets", true},
		{"admin-observability-v2", "GET", "/api/admin/events/fixture/tickets", false},
		{"admin-event-operations", "POST", "/api/admin/events/fixture/tickets", false},
		{"admin-unified-preview", "POST", "/api/admin/events/fixture/tickets", false},
		{"admin-event-operations", "GET", "/api/admin/events/fixture/tickets/credential", false},
		{"admin-unified-preview", "GET", "/api/admin/events/fixture/tickets/credential", false},
		{"admin-event-operations", "GET", "/api/admin/events//tickets", false},
		{"admin-unified-preview", "GET", "/api/admin/events//tickets", false},
	} {
		called := false
		h := allowFixtureHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(204) }), c.owner)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(c.method, c.path, nil))
		if called != c.want {
			t.Fatalf("%s %s %s reached=%t", c.owner, c.method, c.path, called)
		}
	}
}
