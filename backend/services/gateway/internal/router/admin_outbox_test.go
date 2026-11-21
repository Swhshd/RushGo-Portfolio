package router

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adminOutboxRouterUserClient struct {
	userv1.UserServiceClient
	response           *userv1.ValidateTokenResponse
	err                error
	currentRole        *userv1.ValidateTokenResponse
	currentRoleErr     error
	currentRoleSet     bool
	calls              int
	currentRoleCalls   int
	currentRoleFlagged int
	sawDeadline        bool
	deadline           time.Time
}

func (c *adminOutboxRouterUserClient) ValidateToken(ctx context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	c.calls++
	c.deadline, c.sawDeadline = ctx.Deadline()
	if validateTokenRequestsCurrentRole(req) {
		c.currentRoleCalls++
		c.currentRoleFlagged++
		if c.currentRoleErr != nil {
			return nil, c.currentRoleErr
		}
		if c.currentRoleSet {
			return c.currentRole, nil
		}
		if c.currentRole != nil {
			return c.currentRole, nil
		}
	}
	return c.response, c.err
}

func validateTokenRequestsCurrentRole(req *userv1.ValidateTokenRequest) bool {
	if req == nil {
		return false
	}
	field := req.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("require_current_role"))
	return field != nil && req.ProtoReflect().Get(field).Bool()
}

type adminOutboxRouterBookingClient struct {
	bookingv1.BookingServiceClient
	response *bookingv1.ListAdminOutboxEventsResponse
	calls    int
}

func (c *adminOutboxRouterBookingClient) ListAdminOutboxEvents(context.Context, *bookingv1.ListAdminOutboxEventsRequest, ...grpc.CallOption) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	c.calls++
	return c.response, nil
}

func newAdminOutboxTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveAdminOutboxTestRedis(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newAdminOutboxUnavailableTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func serveAdminOutboxTestRedis(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		command, err := readAdminOutboxTestRedisCommand(reader)
		if err != nil || len(command) == 0 {
			return
		}
		response := "+OK\r\n"
		if strings.EqualFold(command[0], "HELLO") {
			response = "-ERR unknown command 'HELLO'\r\n"
		} else if strings.EqualFold(command[0], "EXISTS") {
			response = ":0\r\n"
			if len(command) > 1 && command[1] == "blacklist:revoked-admin-token" {
				response = ":1\r\n"
			}
		} else if strings.EqualFold(command[0], "PING") {
			response = "+PONG\r\n"
		}
		if _, err := io.WriteString(conn, response); err != nil {
			return
		}
	}
}

func readAdminOutboxTestRedisCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[0] != '*' {
		return nil, io.ErrUnexpectedEOF
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	command := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err = reader.ReadString('\n')
		if err != nil || len(line) < 3 || line[0] != '$' {
			return nil, io.ErrUnexpectedEOF
		}
		length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || length < 0 {
			return nil, io.ErrUnexpectedEOF
		}
		value := make([]byte, length+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		command = append(command, string(value[:length]))
	}
	return command, nil
}

func adminOutboxRouterRequest(engine http.Handler, method, path, token string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	engine.ServeHTTP(response, request)
	return response
}

func adminOutboxRouterFixtureResponse() *bookingv1.ListAdminOutboxEventsResponse {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	return &bookingv1.ListAdminOutboxEventsResponse{
		Records:    []*bookingv1.AdminOutboxEventRecord{{EventId: "60000000-0000-4000-8000-000000000001", SourceService: "booking", EventType: "Other", CreatedAt: timestamppb.New(now), RecordedStatus: "pending", ConsumerOutcome: "not_verified"}},
		ObservedAt: timestamppb.New(now), From: timestamppb.New(now.Add(-24 * time.Hour)), To: timestamppb.New(now),
		SupportedEventTypes: []string{"BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"}, SourceRecordLimit: 5000, ConsumerOutcome: "not_verified",
	}
}

func TestAdminOutboxSecurityGuardRunsBeforeAuthAndCoversUnsupportedMethods(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct{ method, path string }{
		{method: http.MethodGet, path: "/api/admin/outbox-events"},
		{method: http.MethodPost, path: "/api/admin/outbox-events"},
		{method: http.MethodGet, path: "/api/admin/outbox-events/unknown-child"},
	}
	for _, tc := range cases {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}}
			bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, false)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, tc.method, tc.path, "token-must-not-reach-user")
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("HTTP status = %d, want 503 before auth: %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			if userClient.calls != 0 || bookingClient.calls != 0 {
				t.Fatalf("insecure route reached downstream auth/data: user=%d booking=%d", userClient.calls, bookingClient.calls)
			}
		})
	}
}

func TestAdminOutboxRouteRequiresAdminAndIsReadOnlyGETOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name            string
		role            string
		token           string
		valid           bool
		wantStatus      int
		wantBooking     int
		wantUser        int
		wantCurrentRole int
	}{
		{name: "missing token", role: "admin", valid: true, wantStatus: http.StatusUnauthorized},
		{name: "expired or invalid token", role: "admin", token: "expired-token", wantStatus: http.StatusUnauthorized, wantUser: 1},
		{name: "ordinary user", role: "user", token: "valid-user", valid: true, wantStatus: http.StatusForbidden, wantUser: 1},
		{name: "scanner role", role: "scanner", token: "valid-scanner", valid: true, wantStatus: http.StatusForbidden, wantUser: 1},
		{name: "admin", role: "admin", token: "valid-admin", valid: true, wantStatus: http.StatusOK, wantBooking: 1, wantUser: 2, wantCurrentRole: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:    &userv1.ValidateTokenResponse{Valid: tc.valid, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role},
				currentRole: &userv1.ValidateTokenResponse{Valid: tc.valid, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role, CurrentRoleVerified: true},
			}
			bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, true)
			if err != nil {
				t.Fatal(err)
			}
			path := "/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00Z"
			response := adminOutboxRouterRequest(engine, http.MethodGet, path, tc.token)
			if response.Code != tc.wantStatus {
				t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			if bookingClient.calls != tc.wantBooking {
				t.Fatalf("Booking calls = %d, want %d", bookingClient.calls, tc.wantBooking)
			}
			if userClient.calls != tc.wantUser || userClient.currentRoleCalls != tc.wantCurrentRole {
				t.Fatalf("User ValidateToken/current-role calls = %d/%d, want %d/%d", userClient.calls, userClient.currentRoleCalls, tc.wantUser, tc.wantCurrentRole)
			}
		})
	}
}

func TestAdminOutboxRequiresMatchingLiveAdminRole(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const userID = "70000000-0000-4000-8000-000000000001"
	for _, tc := range []struct {
		name           string
		currentRole    *userv1.ValidateTokenResponse
		currentRoleErr error
		currentRoleSet bool
		wantStatus     int
		wantRoleCalls  int
		wantBooking    int
		wantCode       string
		wantError      string
	}{
		{name: "current admin without role verification attestation", currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"}, wantStatus: http.StatusServiceUnavailable, wantRoleCalls: 1, wantCode: "source_unavailable", wantError: "outbox source unavailable"},
		{name: "admin demoted since token issue", currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "user", CurrentRoleVerified: true}, wantStatus: http.StatusForbidden, wantRoleCalls: 1, wantCode: "forbidden", wantError: "admin access required"},
		{name: "current account missing", currentRoleErr: status.Error(codes.NotFound, "private current account detail"), wantStatus: http.StatusForbidden, wantRoleCalls: 1, wantCode: "forbidden", wantError: "admin access required"},
		{name: "current validation invalid", currentRole: &userv1.ValidateTokenResponse{Valid: false}, wantStatus: http.StatusUnauthorized, wantRoleCalls: 1, wantCode: "unauthenticated", wantError: "authentication required"},
		{name: "current identity mismatch", currentRole: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000002", Role: "admin", CurrentRoleVerified: true}, wantStatus: http.StatusForbidden, wantRoleCalls: 1, wantCode: "forbidden", wantError: "admin access required"},
		{name: "malformed current-role response", currentRoleSet: true, wantStatus: http.StatusServiceUnavailable, wantRoleCalls: 1, wantCode: "source_unavailable", wantError: "outbox source unavailable"},
		{name: "current-role service unavailable", currentRoleErr: status.Error(codes.Unavailable, "private current-role detail"), wantStatus: http.StatusServiceUnavailable, wantRoleCalls: 1, wantCode: "source_unavailable", wantError: "outbox source unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:    &userv1.ValidateTokenResponse{Valid: true, UserId: userID, Role: "admin"},
				currentRole: tc.currentRole, currentRoleErr: tc.currentRoleErr, currentRoleSet: tc.currentRoleSet,
			}
			bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, true)
			if err != nil {
				t.Fatal(err)
			}
			path := "/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00Z"
			response := adminOutboxRouterRequest(engine, http.MethodGet, path, "valid-admin")
			if response.Code != tc.wantStatus {
				t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("could not decode fixed safe error body: %v", err)
			}
			if body.Code != tc.wantCode || body.Error != tc.wantError {
				t.Fatalf("safe current-role error = %+v, want code=%q error=%q", body, tc.wantCode, tc.wantError)
			}
			if strings.Contains(response.Body.String(), "private") || strings.Contains(response.Body.String(), "current-role detail") {
				t.Fatalf("current-role error detail leaked: %s", response.Body.String())
			}
			if userClient.currentRoleCalls != tc.wantRoleCalls {
				t.Fatalf("User current-role ValidateToken calls = %d, want %d", userClient.currentRoleCalls, tc.wantRoleCalls)
			}
			if bookingClient.calls != tc.wantBooking {
				t.Fatalf("Booking calls = %d, want %d", bookingClient.calls, tc.wantBooking)
			}
		})
	}
}

func TestAdminOutboxRedisRevocationReadFailureFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	userClient := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}}
	bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxUnavailableTestRedis(t), nil, "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00Z"
	response := adminOutboxRouterRequest(engine, http.MethodGet, path, "valid-admin")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("Redis revocation read failure status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	if userClient.calls != 0 || userClient.currentRoleCalls != 0 || bookingClient.calls != 0 {
		t.Fatalf("Redis failure reached downstream dependencies: validate=%d current-role=%d booking=%d", userClient.calls, userClient.currentRoleCalls, bookingClient.calls)
	}
	if !strings.Contains(response.Body.String(), `"code":"source_unavailable"`) || !strings.Contains(response.Body.String(), `"error":"outbox source unavailable"`) {
		t.Fatalf("Redis failure response was not the fixed safe unavailable body: %s", response.Body.String())
	}
}

func TestLegacyRouterConstructorLeavesNewOutboxPathDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	userClient := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}}
	bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
	engine, err := SetupRouterWithBookingPolicy(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/outbox-events", "valid-admin")
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("legacy constructor response = status %d, cache %q, body %s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
	}
	if userClient.calls != 0 || bookingClient.calls != 0 {
		t.Fatalf("legacy constructor forwarded new-path traffic: user=%d booking=%d", userClient.calls, bookingClient.calls)
	}
}

func TestAdminOutboxTotalDeadlineStartsBeforeAuthAndIsScopedToNewPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name         string
		path         string
		role         string
		valid        bool
		wantStatus   int
		wantUser     int
		wantRoleCall int
		wantDeadline bool
	}{
		{name: "outbox auth receives bounded context", path: "/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00Z", role: "admin", valid: true, wantStatus: http.StatusOK, wantUser: 2, wantRoleCall: 1, wantDeadline: true},
		{name: "existing protected route has no new deadline", path: "/api/bookings", role: "user", valid: false, wantStatus: http.StatusUnauthorized, wantUser: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:    &userv1.ValidateTokenResponse{Valid: tc.valid, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role},
				currentRole: &userv1.ValidateTokenResponse{Valid: tc.valid, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role, CurrentRoleVerified: true},
			}
			bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, tc.path, "signed-token")
			if response.Code != tc.wantStatus {
				t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if userClient.calls != tc.wantUser || userClient.currentRoleCalls != tc.wantRoleCall {
				t.Fatalf("ValidateToken/current-role calls = %d/%d, want %d/%d", userClient.calls, userClient.currentRoleCalls, tc.wantUser, tc.wantRoleCall)
			}
			if userClient.sawDeadline != tc.wantDeadline {
				t.Fatalf("ValidateToken saw deadline = %v, want %v", userClient.sawDeadline, tc.wantDeadline)
			}
			if tc.wantDeadline {
				remaining := time.Until(userClient.deadline)
				if remaining <= 0 || remaining > adminOutboxRequestTimeout {
					t.Fatalf("Admin outbox auth deadline remaining = %s, want a future deadline bounded to %s", remaining, adminOutboxRequestTimeout)
				}
			}
		})
	}
}

func TestAdminOutboxUserValidationTransientErrorsAreUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "user service unavailable", err: status.Error(codes.Unavailable, "synthetic upstream transport failure")},
		{name: "user validation deadline exceeded", err: status.Error(codes.DeadlineExceeded, "synthetic validation timeout")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{err: tc.err}
			bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/outbox-events", "valid-admin-token")
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("transient User validation status = %d, want 503 without invalidating the browser session: %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("could not decode fixed safe error body: %v", err)
			}
			if body.Code != "source_unavailable" || body.Error != "outbox source unavailable" || body.Error == tc.err.Error() {
				t.Fatalf("transient User error leaked or returned wrong safe error: %+v", body)
			}
			if userClient.calls != 1 || bookingClient.calls != 0 {
				t.Fatalf("transient User error reached wrong dependencies: user=%d booking=%d", userClient.calls, bookingClient.calls)
			}
		})
	}
}

func TestLegacyProtectedRoutesPreserveSessionOnAuthDependencyFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	userClient := &adminOutboxRouterUserClient{err: status.Error(codes.Unavailable, "synthetic upstream transport failure")}
	bookingClient := &adminOutboxRouterBookingClient{response: adminOutboxRouterFixtureResponse()}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/users/me", "valid-admin-token")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("protected route status = %d, want 503 for unavailable identity source: %s", response.Code, response.Body.String())
	}
	if userClient.calls != 1 || bookingClient.calls != 0 {
		t.Fatalf("legacy route reached wrong dependencies: user=%d booking=%d", userClient.calls, bookingClient.calls)
	}
}

func TestAdminEventTicketOperationsAuthAndNoStore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := "/api/admin/events/60000000-0000-4000-8000-000000000001/ticket-operations"
	for _, tc := range []struct {
		name       string
		secure     bool
		wantStatus int
	}{
		{name: "insecure upstream wiring", secure: false, wantStatus: http.StatusServiceUnavailable},
		{name: "missing bearer token", secure: true, wantStatus: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"}}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, &adminOutboxRouterBookingClient{}, nil, nil, nil, nil, "", false, tc.secure)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, path, "")
			if response.Code != tc.wantStatus {
				t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			if userClient.calls != 0 {
				t.Fatalf("unauthenticated request reached User service %d times, want 0", userClient.calls)
			}
		})
	}
}
