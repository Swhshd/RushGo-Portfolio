package router

import (
	"github.com/gin-gonic/gin"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"testing"
)

func TestLegacyAdminWritesRequireCurrentRole(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name      string
		role      string
		err       error
		redisDown bool
		revoked   bool
		want      int
	}{
		{name: "demoted", role: "user", want: 403},
		{name: "revoked", role: "admin", revoked: true, want: 401},
		{name: "deleted", err: status.Error(codes.NotFound, "deleted"), want: 403},
		{name: "user unavailable", err: status.Error(codes.Unavailable, "offline"), want: 503},
		{name: "revocation source unavailable", role: "admin", redisDown: true, want: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			users := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role}, currentRoleErr: tc.err, currentRoleSet: true}
			cache := newAdminOutboxTestRedis(t)
			if tc.redisDown {
				cache = newAdminOutboxUnavailableTestRedis(t)
			}
			engine, err := SetupRouterWithAdminOutboxSecurity(users, nil, nil, nil, nil, cache, nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, route := range []struct{ method, path string }{
				{http.MethodPost, "/api/events"}, {http.MethodPut, "/api/events/60000000-0000-4000-8000-000000000001"}, {http.MethodDelete, "/api/events/60000000-0000-4000-8000-000000000001"},
				{http.MethodPut, "/api/events/60000000-0000-4000-8000-000000000001/scanners/70000000-0000-4000-8000-000000000002"},
				{http.MethodPost, "/api/scheduler/jobs"},
				{http.MethodGet, "/api/admin/events/60000000-0000-4000-8000-000000000001/engagement"},
				{http.MethodPatch, "/api/scheduler/jobs/60000000-0000-4000-8000-000000000001"},
			} {
				token := "old-admin-token"
				if tc.revoked {
					token = "revoked-admin-token"
				}
				response := adminOutboxRouterRequest(engine, route.method, route.path, token)
				if response.Code != tc.want {
					t.Fatalf("%s %s: %d want %d", route.method, route.path, response.Code, tc.want)
				}
			}
		})
	}
}

func TestLegacyAdminWriteAllowsVerifiedCurrentAdmin(t *testing.T) {
	users := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}, currentRole: &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}, currentRoleSet: true}
	engine, err := SetupRouterWithAdminOutboxSecurity(users, nil, nil, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodPost, "/api/events", "current-admin")
	// The current administrator reaches body validation. Invalid body causes no mutation.
	if response.Code != http.StatusBadRequest || users.currentRoleCalls != 1 {
		t.Fatalf("current admin denied: %d role calls %d", response.Code, users.currentRoleCalls)
	}
	response = adminOutboxRouterRequest(engine, http.MethodPost, "/api/scheduler/jobs", "current-admin")
	if response.Code != http.StatusBadRequest || users.currentRoleCalls != 2 {
		t.Fatalf("scheduler create did not require the current role before body validation: %d role calls %d", response.Code, users.currentRoleCalls)
	}
}
