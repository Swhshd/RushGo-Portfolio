package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adminOutboxBookingClientStub struct {
	bookingv1.BookingServiceClient
	response *bookingv1.ListAdminOutboxEventsResponse
	err      error
	calls    int
	request  *bookingv1.ListAdminOutboxEventsRequest
	token    string
}

type adminOutboxNotificationClientStub struct {
	notificationv1.NotificationAdminServiceClient
	response *bookingv1.ListAdminOutboxEventsResponse
	err      error
	calls    int
	request  *bookingv1.ListAdminOutboxEventsRequest
	token    string
}

func (s *adminOutboxNotificationClientStub) ListAdminOutboxEvents(ctx context.Context, req *bookingv1.ListAdminOutboxEventsRequest, _ ...grpc.CallOption) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	s.calls++
	s.request = req
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		values := md.Get("authorization")
		if len(values) == 1 {
			s.token = values[0]
		}
	}
	return s.response, s.err
}

func (s *adminOutboxBookingClientStub) ListAdminOutboxEvents(ctx context.Context, req *bookingv1.ListAdminOutboxEventsRequest, _ ...grpc.CallOption) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	s.calls++
	s.request = req
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		values := md.Get("authorization")
		if len(values) == 1 {
			s.token = values[0]
		}
	}
	return s.response, s.err
}

func performAdminOutboxHandlerRequest(h *AdminOutboxHandler, method, path, authorization string) *httptest.ResponseRecorder {
	t := httptest.NewRecorder()
	r := gin.New()
	r.Handle(method, "/api/admin/outbox-events", h.ListEvents)
	req := httptest.NewRequest(method, path, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	r.ServeHTTP(t, req)
	return t
}

func TestAdminOutboxHandlerForwardsOnlyValidatedQueryAndSafeProjection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	observed := time.Date(2026, 10, 31, 12, 0, 0, 123456789, time.UTC)
	client := &adminOutboxBookingClientStub{response: &bookingv1.ListAdminOutboxEventsResponse{
		Records: []*bookingv1.AdminOutboxEventRecord{{
			EventId: "50000000-0000-4000-8000-000000000001", SourceService: "booking", EventType: "BookingConfirmed",
			CreatedAt: timestamppb.New(observed.Add(-time.Minute)), RecordedStatus: "published", ConsumerOutcome: "not_verified",
		}},
		HasMore: false, ObservedAt: timestamppb.New(observed), From: timestamppb.New(observed.Add(-24 * time.Hour)), To: timestamppb.New(observed),
		SupportedEventTypes: []string{"BookingConfirmed", "BookingCancelled", "BookingFailed", "Other"},
		SourceRecordLimit:   5000, ConsumerOutcome: "not_verified",
	}}
	handler := NewAdminOutboxHandler(client, true)
	path := "/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00.123456789Z&limit=20"
	response := performAdminOutboxHandlerRequest(handler, http.MethodGet, path, "Bearer validated-admin-token")
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	if client.calls != 1 || client.request == nil || client.request.Source != "booking" || client.request.Status != "all" || client.request.Limit != 20 {
		t.Fatalf("Booking request = %+v, calls=%d", client.request, client.calls)
	}
	if client.request.GetFrom() != "2026-10-30T12:00:00Z" || client.request.GetTo() != "2026-10-31T12:00:00.123456789Z" {
		t.Fatalf("forwarded time bounds = %q..%q", client.request.GetFrom(), client.request.GetTo())
	}
	if client.token != "Bearer validated-admin-token" {
		t.Fatalf("forwarded Authorization = %q", client.token)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["source_service"] != "booking" || body["consumer_outcome"] != "not_verified" || body["source_record_limit"] != float64(5000) {
		t.Fatalf("response summary fields = %#v", body)
	}
	encoded := response.Body.String()
	for _, forbidden := range []string{"payload", "event_key", "error_summary", "attempt_count", "published_at", "validated-admin-token"} {
		if strings.Contains(encoded, forbidden) {
			t.Errorf("response leaked forbidden value/field %q: %s", forbidden, encoded)
		}
	}
}

func TestAdminOutboxHandlerDispatchesNotificationWithItsOwnStatusAndTypeContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	observed := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	booking := &adminOutboxBookingClientStub{}
	notification := &adminOutboxNotificationClientStub{response: &bookingv1.ListAdminOutboxEventsResponse{
		Records: []*bookingv1.AdminOutboxEventRecord{{
			EventId: "50000000-0000-4000-8000-000000000002", SourceService: "notification", EventType: "unrecognized", CreatedAt: timestamppb.New(observed.Add(-time.Minute)), RecordedStatus: "sent", ConsumerOutcome: "delivered",
		}},
		HasMore: false, ObservedAt: timestamppb.New(observed), From: timestamppb.New(observed.Add(-24 * time.Hour)), To: timestamppb.New(observed),
		SupportedEventTypes: []string{"booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email", "Other"},
		SourceRecordLimit:   5000, ConsumerOutcome: "not_verified",
	}}
	handler := NewAdminOutboxHandlerWithNotification(booking, true, notification, true)
	path := "/api/admin/outbox-events?source=notification&status=retryable&event_type=booking_cancellation&from=2026-10-30T12%3A00%3A00Z&to=2026-10-31T12%3A00%3A00Z&limit=15"
	response := performAdminOutboxHandlerRequest(handler, http.MethodGet, path, "Bearer current-admin-token")
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if booking.calls != 0 || notification.calls != 1 || notification.request == nil {
		t.Fatalf("owner dispatch booking=%d notification=%d request=%+v", booking.calls, notification.calls, notification.request)
	}
	if notification.request.Source != "notification" || notification.request.Status != "retryable" || notification.request.EventType != "booking_cancellation" || notification.request.Limit != 15 {
		t.Fatalf("Notification query = %+v", notification.request)
	}
	if notification.token != "Bearer current-admin-token" {
		t.Fatalf("forwarded Authorization = %q", notification.token)
	}
	var body struct {
		SourceService   string `json:"source_service"`
		ConsumerOutcome string `json:"consumer_outcome"`
		Records         []struct {
			SourceService   string `json:"source_service"`
			EventType       string `json:"event_type"`
			RecordedStatus  string `json:"recorded_status"`
			ConsumerOutcome string `json:"consumer_outcome"`
		} `json:"records"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.SourceService != "notification" || body.ConsumerOutcome != "not_verified" || len(body.Records) != 1 {
		t.Fatalf("Notification response projection = %+v", body)
	}
	if record := body.Records[0]; record.SourceService != "notification" || record.EventType != "Other" || record.RecordedStatus != "sent" || record.ConsumerOutcome != "not_verified" {
		t.Fatalf("safe Notification row projection = %+v", record)
	}
	for _, forbidden := range []string{"not-trusted", "unrecognized", "delivered", "current-admin-token"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("response trusted or leaked %q: %s", forbidden, response.Body.String())
		}
	}
}

func TestAdminOutboxHandlerRejectsUnknownAndOutOfBoundsQueryWithoutRPC(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &adminOutboxBookingClientStub{}
	handler := NewAdminOutboxHandler(client, true)
	validBounds := "&from=2026-10-30T00%3A00%3A00Z&to=2026-10-31T00%3A00%3A00Z"
	cases := []struct{ name, query string }{
		{name: "unknown parameter", query: "?source=booking&status=all&unexpected=x" + validBounds},
		{name: "unknown source", query: "?source=saga&status=all" + validBounds},
		{name: "notification status is not authoritative", query: "?source=notification&status=delivered" + validBounds},
		{name: "notification Other type is not filterable", query: "?source=notification&status=all&event_type=Other" + validBounds},
		{name: "notification unknown type is not filterable", query: "?source=notification&status=all&event_type=arbitrary" + validBounds},
		{name: "unknown status", query: "?source=booking&status=failed" + validBounds},
		{name: "unknown event type", query: "?source=booking&status=all&event_type=arbitrary" + validBounds},
		{name: "repeated source", query: "?source=booking&source=booking&status=all" + validBounds},
		{name: "limit over maximum", query: "?source=booking&status=all&limit=101" + validBounds},
		{name: "noncanonical limit", query: "?source=booking&status=all&limit=020" + validBounds},
		{name: "malformed cursor", query: "?source=booking&status=all&cursor=%25%25%25" + validBounds},
		{name: "time range over seven days", query: "?source=booking&status=all&from=2026-10-23T00%3A00%3A00Z&to=2026-10-31T00%3A00%3A00Z"},
		{name: "only one time bound", query: "?source=booking&status=all&from=2026-10-30T00%3A00%3A00Z"},
		{name: "empty time bound", query: "?source=booking&status=all&from=&to=2026-10-31T00%3A00%3A00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := performAdminOutboxHandlerRequest(handler, http.MethodGet, "/api/admin/outbox-events"+tc.query, "Bearer validated-admin-token")
			if response.Code != http.StatusBadRequest {
				t.Fatalf("HTTP status = %d, want 400: %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
		})
	}
	if client.calls != 0 {
		t.Fatalf("invalid queries reached Booking %d times", client.calls)
	}
}

func TestAdminOutboxHandlerRefusesInsecureWiringBeforeForwardingBearer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &adminOutboxBookingClientStub{}
	handler := NewAdminOutboxHandler(client, false)
	response := performAdminOutboxHandlerRequest(handler, http.MethodGet,
		"/api/admin/outbox-events?source=booking&status=all", "Bearer must-not-forward")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	if client.calls != 0 || client.token != "" {
		t.Fatalf("insecure route forwarded to Booking: calls=%d token=%q", client.calls, client.token)
	}
	if strings.Contains(response.Body.String(), "must-not-forward") {
		t.Fatalf("error response echoed Authorization: %s", response.Body.String())
	}
}

func TestAdminOutboxHandlerUsesFixedErrorsForOwnerFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name   string
		owner  error
		status int
		code   string
	}{
		{name: "invalid query", owner: status.Error(codes.InvalidArgument, "untrusted details"), status: http.StatusBadRequest, code: "invalid_query"},
		{name: "capacity exceeded", owner: status.Error(codes.ResourceExhausted, "raw database detail"), status: http.StatusServiceUnavailable, code: "capacity_exceeded"},
		{name: "unauthenticated", owner: status.Error(codes.Unauthenticated, "raw token detail"), status: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "forbidden", owner: status.Error(codes.PermissionDenied, "raw role detail"), status: http.StatusForbidden, code: "forbidden"},
		{name: "source unavailable", owner: status.Error(codes.Unavailable, "private database detail"), status: http.StatusServiceUnavailable, code: "source_unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &adminOutboxBookingClientStub{err: tc.owner}
			handler := NewAdminOutboxHandler(client, true)
			response := performAdminOutboxHandlerRequest(handler, http.MethodGet,
				"/api/admin/outbox-events?source=booking&status=all&from=2026-10-30T00%3A00%3A00Z&to=2026-10-31T00%3A00%3A00Z", "Bearer validated-admin-token")
			if response.Code != tc.status {
				t.Fatalf("HTTP status = %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != tc.code || body.Error == "" {
				t.Fatalf("safe error response = %+v, want code %q", body, tc.code)
			}
			for _, raw := range []string{"untrusted details", "raw database detail", "raw token detail", "raw role detail", "private database detail"} {
				if strings.Contains(response.Body.String(), raw) {
					t.Fatalf("response leaked owner error %q: %s", raw, response.Body.String())
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestAdminOutboxProjectionRejectsMismatchedOwnerSource(t *testing.T) {
	now := time.Now().UTC()
	for _, source := range []string{"booking", "notification"} {
		response := &bookingv1.ListAdminOutboxEventsResponse{
			ObservedAt: timestamppb.New(now), From: timestamppb.New(now.Add(-time.Hour)), To: timestamppb.New(now),
			Records: []*bookingv1.AdminOutboxEventRecord{{EventId: "50000000-0000-4000-8000-000000000001", SourceService: "wrong-owner", EventType: "Other", CreatedAt: timestamppb.New(now), RecordedStatus: "pending"}},
		}
		if _, ok := projectAdminOutboxHTTPResponse(response, source); ok {
			t.Errorf("%s response silently relabelled a different owner", source)
		}
	}
}
