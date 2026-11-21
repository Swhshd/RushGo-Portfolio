package router

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ticketbox/pkg/adminticketlist"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adminEventTicketsRouterBookingClient struct {
	bookingv1.BookingServiceClient
	response            *bookingv1.ListAdminEventTicketsResponse
	err                 error
	calls               int
	request             *bookingv1.ListAdminEventTicketsRequest
	authorizationValues []string
	order               *[]string
}

func (c *adminEventTicketsRouterBookingClient) ListAdminEventTickets(ctx context.Context, request *bookingv1.ListAdminEventTicketsRequest, _ ...grpc.CallOption) (*bookingv1.ListAdminEventTicketsResponse, error) {
	c.calls++
	c.request = request
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
		c.authorizationValues = outgoing.Get("authorization")
	}
	if c.order != nil {
		*c.order = append(*c.order, "booking")
	}
	return c.response, c.err
}

func TestAdminEventTicketsRouteValidatesQueryBeforeSourceCalls(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	boundCursor, err := adminticketlist.EncodeCursor(adminticketlist.Cursor{
		EventID: uuid.MustParse(eventID), StatusFilter: adminticketlist.StatusIssued,
		IssuedAt: time.Date(2026, 11, 1, 11, 0, 0, 0, time.UTC),
		TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{}
	bookingClient := &adminEventTicketsRouterBookingClient{}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/api/admin/events/" + eventID + "/tickets?status=ISSUED&status=ALL",
		"/api/admin/events/" + eventID + "/tickets?status=ISSUED&page_size=101",
		"/api/admin/events/" + eventID + "/tickets?status=issued",
		"/api/admin/events/" + eventID + "/tickets?page_size=0",
		"/api/admin/events/" + eventID + "/tickets?cursor=",
		"/api/admin/events/" + eventID + "/tickets?status=CHECKED_IN&cursor=" + boundCursor,
	} {
		response := adminOutboxRouterRequest(engine, http.MethodGet, path, "signed-admin-token")
		if response.Code != http.StatusBadRequest || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("invalid query %q = status %d cache %q body %s", path, response.Code, response.Header().Get("Cache-Control"), response.Body.String())
		}
	}
	if eventClient.calls != 0 || bookingClient.calls != 0 {
		t.Fatalf("invalid queries reached source services: Event=%d Booking=%d", eventClient.calls, bookingClient.calls)
	}
}

func TestAdminEventTicketsRouteRequiresCurrentAdminBeforeSourceReads(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	cases := []struct {
		name       string
		token      string
		role       string
		wantStatus int
	}{
		{name: "missing token", wantStatus: http.StatusUnauthorized},
		{name: "ordinary user", token: "signed-user-token", role: "user", wantStatus: http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role},
				currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: tc.role},
				currentRoleSet: true,
			}
			eventClient := &adminEventTicketOperationsRouterEventClient{}
			bookingClient := &adminEventTicketsRouterBookingClient{}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/tickets", tc.token)
			if response.Code != tc.wantStatus || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("authorization response = %d cache=%q body=%s, want %d/no-store", response.Code, response.Header().Get("Cache-Control"), response.Body.String(), tc.wantStatus)
			}
			if eventClient.calls != 0 || bookingClient.calls != 0 {
				t.Fatalf("unauthorized route reached source services: Event=%d Booking=%d", eventClient.calls, bookingClient.calls)
			}
		})
	}
}

func TestAdminEventTicketsRouteProjectsOnlySafeRowsAfterEventLookup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	issuedAt := observedAt.Add(-time.Hour)
	order := []string{}
	nextCursor, err := adminticketlist.EncodeCursor(adminticketlist.Cursor{
		EventID: uuid.MustParse(eventID), StatusFilter: adminticketlist.StatusIssued,
		IssuedAt: issuedAt, TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{
		response: &eventv1.EventDetail{Id: eventID, Title: "Event name must not enter this DTO", Venue: "Private Venue", Date: timestamppb.New(issuedAt)},
		order:    &order,
	}
	bookingClient := &adminEventTicketsRouterBookingClient{
		response: &bookingv1.ListAdminEventTicketsResponse{
			EventId: eventID, ObservedAt: timestamppb.New(observedAt), StatusFilter: "ISSUED", PageSize: 1,
			HasMore: true, NextCursor: nextCursor, SourceRecordLimit: 5000,
			Tickets: []*bookingv1.AdminEventTicketRecord{{
				TicketId: "61000000-0000-4000-8000-000000000001", Status: "ISSUED", IssuedAt: timestamppb.New(issuedAt),
			}},
		},
		order: &order,
	}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/tickets?status=ISSUED&page_size=1", "signed-admin-token")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("HTTP response = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
	}
	if strings.Join(order, ",") != "event,booking" {
		t.Fatalf("source call order = %v, want Event existence lookup then Booking", order)
	}
	if bookingClient.calls != 1 || bookingClient.request == nil || bookingClient.request.GetEventId() != eventID || bookingClient.request.GetStatusFilter() != "ISSUED" || bookingClient.request.GetPageSize() != 1 || bookingClient.request.GetCursor() != "" {
		t.Fatalf("Booking request = calls:%d request:%#v", bookingClient.calls, bookingClient.request)
	}
	if len(bookingClient.authorizationValues) != 1 || bookingClient.authorizationValues[0] != "Bearer signed-admin-token" {
		t.Fatalf("Booking authorization metadata = %#v", bookingClient.authorizationValues)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode ticket list response: %v", err)
	}
	for key, want := range map[string]any{
		"event_id": eventID, "observed_at": observedAt.Format(time.RFC3339Nano), "source_service": "booking",
		"source_status": "available", "coverage": "recorded_ticket_snapshot", "source_record_limit": float64(5000),
		"status_filter": "ISSUED", "page_size": float64(1), "has_more": true, "next_cursor": nextCursor,
	} {
		if body[key] != want {
			t.Errorf("body[%q] = %#v, want %#v", key, body[key], want)
		}
	}
	rows, ok := body["tickets"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("tickets = %#v", body["tickets"])
	}
	row, ok := rows[0].(map[string]any)
	if !ok || row["ticket_id"] != "61000000-0000-4000-8000-000000000001" || row["status"] != "ISSUED" || row["issued_at"] != issuedAt.Format(time.RFC3339Nano) || row["checked_in_at"] != nil || row["revoked_at"] != nil {
		t.Fatalf("safe ticket row = %#v", rows[0])
	}
	for _, forbidden := range []string{"event_title", "event_venue", "event_date", "booking_id", "seat_id", "user_id", "staff_user_id", "token", "digest", "payment_id", "credentials"} {
		if _, exists := body[forbidden]; exists {
			t.Errorf("forbidden root field %q appeared in response", forbidden)
		}
		if _, exists := row[forbidden]; exists {
			t.Errorf("forbidden ticket field %q appeared in response", forbidden)
		}
	}
}

func TestAdminEventTicketsRouteRejectsChildTimestampsBeforeIssuedAt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	ticketID := "61000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	issuedAt := observedAt.Add(-time.Hour)
	beforeIssuedAt := issuedAt.Add(-time.Second)
	afterIssuedAt := issuedAt.Add(time.Second)
	cases := []struct {
		name        string
		status      string
		checkedInAt *time.Time
		revokedAt   *time.Time
	}{
		{name: "check-in before issued", status: "CHECKED_IN", checkedInAt: &beforeIssuedAt},
		{name: "revocation before issued", status: "REVOKED", revokedAt: &beforeIssuedAt},
		{name: "check-in before issued on revoked ticket", status: "REVOKED", checkedInAt: &beforeIssuedAt, revokedAt: &afterIssuedAt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRoleSet: true,
			}
			eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID}}
			bookingClient := &adminEventTicketsRouterBookingClient{response: &bookingv1.ListAdminEventTicketsResponse{
				EventId: eventID, ObservedAt: timestamppb.New(observedAt), StatusFilter: adminticketlist.StatusAll,
				PageSize: 20, SourceRecordLimit: 5000,
				Tickets: []*bookingv1.AdminEventTicketRecord{{
					TicketId: ticketID, Status: tc.status, IssuedAt: timestamppb.New(issuedAt),
					CheckedInAt: timestampForAdminEventTicketTest(tc.checkedInAt),
					RevokedAt:   timestampForAdminEventTicketTest(tc.revokedAt),
				}},
			}}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/tickets", "signed-admin-token")
			if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" || !strings.Contains(response.Body.String(), `"code":"source_unavailable"`) {
				t.Fatalf("impossible timestamp response = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
			}
			if eventClient.calls != 1 || bookingClient.calls != 1 {
				t.Fatalf("source calls = Event:%d Booking:%d, want 1/1", eventClient.calls, bookingClient.calls)
			}
		})
	}
}

func timestampForAdminEventTicketTest(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func TestAdminEventTicketsUpstreamFailureNeverBecomesEmptySuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID, Date: timestamppb.Now()}}
	bookingClient := &adminEventTicketsRouterBookingClient{}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		code       codes.Code
		wantStatus int
	}{
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.ResourceExhausted, http.StatusServiceUnavailable},
		{codes.Internal, http.StatusServiceUnavailable},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			bookingClient.err = status.Error(tc.code, "private source details")
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/tickets", "signed-admin-token")
			if response.Code != tc.wantStatus || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("source failure = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
			}
			if strings.Contains(response.Body.String(), "private source details") || strings.Contains(response.Body.String(), `"tickets"`) {
				t.Fatalf("source failure leaked private details or success rows: %s", response.Body.String())
			}
		})
	}
}
