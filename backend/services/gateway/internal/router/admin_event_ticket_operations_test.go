package router

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adminEventTicketOperationsRouterEventClient struct {
	eventv1.EventServiceClient
	response *eventv1.EventDetail
	err      error
	calls    int
	order    *[]string
}

func (c *adminEventTicketOperationsRouterEventClient) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	c.calls++
	if c.order != nil {
		*c.order = append(*c.order, "event")
	}
	return c.response, c.err
}

type adminEventTicketOperationsRouterBookingClient struct {
	bookingv1.BookingServiceClient
	response            *bookingv1.GetAdminEventTicketOperationsResponse
	err                 error
	calls               int
	authorizationValues []string
	order               *[]string
}

func (c *adminEventTicketOperationsRouterBookingClient) GetAdminEventTicketOperations(ctx context.Context, _ *bookingv1.GetAdminEventTicketOperationsRequest, _ ...grpc.CallOption) (*bookingv1.GetAdminEventTicketOperationsResponse, error) {
	c.calls++
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
		c.authorizationValues = outgoing.Get("authorization")
	}
	if c.order != nil {
		*c.order = append(*c.order, "booking")
	}
	return c.response, c.err
}

func TestAdminEventTicketOperationsRejectsMismatchedUpstreamProjectionWithoutPartialTotals(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	latest := observedAt.Add(-time.Minute)
	previous := observedAt.Add(-time.Hour)
	valid := func() *bookingv1.GetAdminEventTicketOperationsResponse {
		return &bookingv1.GetAdminEventTicketOperationsResponse{
			EventId: eventID, ObservedAt: timestamppb.New(observedAt),
			TotalRecordedCount: 5, IssuedCount: 2, CheckedInCount: 1, RevokedCount: 2,
			SuccessfulCheckinCount: 2, LatestCheckinAt: timestamppb.New(latest),
			RecentCheckins:    []*timestamppb.Timestamp{timestamppb.New(latest), timestamppb.New(previous)},
			SourceRecordLimit: 5000,
		}
	}
	cases := []struct {
		name   string
		mutate func(*bookingv1.GetAdminEventTicketOperationsResponse)
	}{
		{name: "state count sum mismatch", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) { r.TotalRecordedCount = 4 }},
		{name: "negative state count", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) { r.IssuedCount = -1; r.TotalRecordedCount = 2 }},
		{name: "audit count below checked-in state count", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			r.SuccessfulCheckinCount = 0
			r.LatestCheckinAt = nil
			r.RecentCheckins = nil
		}},
		{name: "audit count exceeds states with check-in history", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			r.SuccessfulCheckinCount = 4
			r.RecentCheckins = []*timestamppb.Timestamp{timestamppb.New(latest), timestamppb.New(previous), timestamppb.New(observedAt.Add(-2 * time.Hour)), timestamppb.New(observedAt.Add(-3 * time.Hour))}
		}},
		{name: "latest differs from recent first", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			r.LatestCheckinAt = timestamppb.New(previous)
		}},
		{name: "recent length does not match audit count", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) { r.RecentCheckins = r.RecentCheckins[:1] }},
		{name: "recent timestamp is after observation", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			future := timestamppb.New(observedAt.Add(time.Second))
			r.LatestCheckinAt = future
			r.RecentCheckins[0] = future
		}},
		{name: "source count exceeds advertised limit", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			r.TotalRecordedCount = 5001
			r.IssuedCount = 5001
		}},
		{name: "invalid observation timestamp", mutate: func(r *bookingv1.GetAdminEventTicketOperationsResponse) {
			r.ObservedAt = &timestamppb.Timestamp{Seconds: 253402300800}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRoleSet: true,
			}
			eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID, Title: "Preview Night", Venue: "Main Hall", Date: timestamppb.New(observedAt.Add(24 * time.Hour))}}
			bookingClient := &adminEventTicketOperationsRouterBookingClient{response: valid()}
			tc.mutate(bookingClient.response)
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("mismatched upstream HTTP status = %d, want 503: %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode fixed error response: %v", err)
			}
			if body["code"] != "source_unavailable" || body["error"] != "ticket operations source unavailable" {
				t.Fatalf("mismatched upstream error body = %#v", body)
			}
			for _, forbidden := range []string{"total_recorded_count", "issued_count", "checked_in_count", "revoked_count", "successful_checkin_count", "recent_checkins"} {
				if _, leaked := body[forbidden]; leaked {
					t.Fatalf("partial upstream field %q leaked in %s", forbidden, response.Body.String())
				}
			}
		})
	}
}

func TestAdminEventTicketOperationsBadUUIDAndUnsupportedMethodAreNoStore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, nil, &adminEventTicketOperationsRouterBookingClient{}, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	badUUID := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/not-a-uuid/ticket-operations", "signed-admin-token")
	if badUUID.Code != http.StatusBadRequest || badUUID.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("bad UUID response = %d cache=%q body=%s", badUUID.Code, badUUID.Header().Get("Cache-Control"), badUUID.Body.String())
	}
	unsupported := adminOutboxRouterRequest(engine, http.MethodPost, "/api/admin/events/60000000-0000-4000-8000-000000000001/ticket-operations", "")
	if unsupported.Code != http.StatusNotFound || unsupported.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unsupported method response = %d cache=%q body=%s", unsupported.Code, unsupported.Header().Get("Cache-Control"), unsupported.Body.String())
	}
	if strings.Contains(badUUID.Body.String(), "signed-admin-token") || strings.Contains(unsupported.Body.String(), "signed-admin-token") {
		t.Fatalf("response contains credential material")
	}
}

func TestAdminEventTicketOperationsRouteProjectsSafeSnapshotAfterEventLookup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	latestCheckIn := observedAt.Add(-time.Minute)
	previousCheckIn := observedAt.Add(-time.Hour)
	order := []string{}
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{
		response: &eventv1.EventDetail{Id: eventID, Title: "<Preview Night>", Venue: "Main Hall", Date: timestamppb.New(observedAt.Add(24 * time.Hour))},
		order:    &order,
	}
	bookingClient := &adminEventTicketOperationsRouterBookingClient{
		response: &bookingv1.GetAdminEventTicketOperationsResponse{
			EventId: eventID, ObservedAt: timestamppb.New(observedAt),
			TotalRecordedCount: 5, IssuedCount: 2, CheckedInCount: 1, RevokedCount: 2,
			SuccessfulCheckinCount: 2, LatestCheckinAt: timestamppb.New(latestCheckIn),
			RecentCheckins:    []*timestamppb.Timestamp{timestamppb.New(latestCheckIn), timestamppb.New(previousCheckIn)},
			SourceRecordLimit: 5000,
		},
		order: &order,
	}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	var body struct {
		EventID                string `json:"event_id"`
		EventTitle             string `json:"event_title"`
		EventVenue             string `json:"event_venue"`
		EventDate              string `json:"event_date"`
		ObservedAt             string `json:"observed_at"`
		SourceService          string `json:"source_service"`
		SourceStatus           string `json:"source_status"`
		Coverage               string `json:"coverage"`
		TotalRecordedCount     int32  `json:"total_recorded_count"`
		IssuedCount            int32  `json:"issued_count"`
		CheckedInCount         int32  `json:"checked_in_count"`
		RevokedCount           int32  `json:"revoked_count"`
		SuccessfulCheckInCount int32  `json:"successful_checkin_count"`
		LatestCheckInAt        string `json:"latest_checkin_at"`
		RecentCheckIns         []struct {
			CheckedInAt string `json:"checked_in_at"`
		} `json:"recent_checkins"`
		SourceRecordLimit        int32  `json:"source_record_limit"`
		LiveAdmissionEligibility string `json:"live_admission_eligibility"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode event operations response: %v", err)
	}
	if body.EventID != eventID || body.EventTitle != "<Preview Night>" || body.EventVenue != "Main Hall" || body.EventDate != observedAt.Add(24*time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("event metadata = %#v", body)
	}
	if body.SourceService != "booking" || body.SourceStatus != "available" || body.Coverage != "recorded_ticket_snapshot" || body.LiveAdmissionEligibility != "not_evaluated" {
		t.Fatalf("source labels = %#v", body)
	}
	if body.TotalRecordedCount != 5 || body.IssuedCount != 2 || body.CheckedInCount != 1 || body.RevokedCount != 2 || body.SuccessfulCheckInCount != 2 {
		t.Fatalf("independent recorded and audit counts = %#v", body)
	}
	if body.ObservedAt != observedAt.Format(time.RFC3339Nano) || body.LatestCheckInAt != latestCheckIn.Format(time.RFC3339Nano) || len(body.RecentCheckIns) != 2 || body.RecentCheckIns[0].CheckedInAt != latestCheckIn.Format(time.RFC3339Nano) || body.RecentCheckIns[1].CheckedInAt != previousCheckIn.Format(time.RFC3339Nano) || body.SourceRecordLimit != 5000 {
		t.Fatalf("snapshot timestamps or coverage limit = %#v", body)
	}
	if eventClient.calls != 1 || bookingClient.calls != 1 || len(order) != 2 || order[0] != "event" || order[1] != "booking" {
		t.Fatalf("dependency order/calls = %v / Event=%d Booking=%d", order, eventClient.calls, bookingClient.calls)
	}
	if len(bookingClient.authorizationValues) != 1 || bookingClient.authorizationValues[0] != "Bearer signed-admin-token" {
		t.Fatalf("Booking authorization metadata = %#v", bookingClient.authorizationValues)
	}
}

func TestAdminEventTicketOperationsRouteRejectsMismatchedEventIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID, Title: "Preview Night", Venue: "Main Hall", Date: timestamppb.Now()}}
	bookingClient := &adminEventTicketOperationsRouterBookingClient{response: &bookingv1.GetAdminEventTicketOperationsResponse{
		EventId: "60000000-0000-4000-8000-000000000002", ObservedAt: timestamppb.Now(),
		TotalRecordedCount: 0, SourceRecordLimit: 5000,
	}}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("mismatched upstream event HTTP status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	if bookingClient.calls != 1 {
		t.Fatalf("Booking calls = %d, want 1 before its mismatched identity is rejected", bookingClient.calls)
	}
	var body struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode fixed error response: %v", err)
	}
	if body.Code != "source_unavailable" || body.Error != "ticket operations source unavailable" {
		t.Fatalf("mismatched identity error body = %#v", body)
	}
}

func TestAdminEventTicketOperationsMapsCapacityFailureToFixedNoStoreResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	userClient := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID, Title: "Preview Night", Venue: "Main Hall", Date: timestamppb.New(observedAt.Add(24 * time.Hour))}}
	bookingClient := &adminEventTicketOperationsRouterBookingClient{err: status.Error(codes.ResourceExhausted, "private cap details")}
	engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("capacity response = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode capacity response: %v", err)
	}
	if body["code"] != "capacity_exceeded" || body["error"] != "ticket operations source capacity exceeded" || strings.Contains(response.Body.String(), "private cap details") {
		t.Fatalf("capacity response exposed an unapproved payload: %#v", body)
	}
	for _, forbidden := range []string{"total_recorded_count", "issued_count", "checked_in_count", "revoked_count", "successful_checkin_count", "recent_checkins"} {
		if _, leaked := body[forbidden]; leaked {
			t.Fatalf("partial upstream field %q leaked in %s", forbidden, response.Body.String())
		}
	}
}

func TestAdminEventTicketOperationsRejectsUnsafeEventMetadataBeforeBookingRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	observedAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		title string
		venue string
	}{
		{name: "title exceeds 240 characters", title: strings.Repeat("x", 241), venue: "Main Hall"},
		{name: "venue exceeds 300 characters", title: "Preview Night", venue: strings.Repeat("x", 301)},
		{name: "title contains control character", title: "Preview\nNight", venue: "Main Hall"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRoleSet: true,
			}
			eventClient := &adminEventTicketOperationsRouterEventClient{response: &eventv1.EventDetail{Id: eventID, Title: tc.title, Venue: tc.venue, Date: timestamppb.New(observedAt.Add(24 * time.Hour))}}
			bookingClient := &adminEventTicketOperationsRouterBookingClient{response: &bookingv1.GetAdminEventTicketOperationsResponse{EventId: eventID, ObservedAt: timestamppb.New(observedAt), SourceRecordLimit: 5000}}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
			if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("unsafe metadata response = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
			}
			if bookingClient.calls != 0 {
				t.Fatalf("Booking reads = %d, want 0 for unsafe Event metadata", bookingClient.calls)
			}
		})
	}
}

func TestAdminEventTicketOperationsMapsEventLookupFailuresToFixedErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "60000000-0000-4000-8000-000000000001"
	for _, tc := range []struct {
		name       string
		upstream   error
		wantStatus int
		wantCode   string
		wantError  string
	}{
		{name: "event missing", upstream: status.Error(codes.NotFound, "private event lookup detail"), wantStatus: http.StatusNotFound, wantCode: "not_found", wantError: "event not found"},
		{name: "event service unavailable", upstream: status.Error(codes.Unavailable, "private event dependency detail"), wantStatus: http.StatusServiceUnavailable, wantCode: "source_unavailable", wantError: "ticket operations source unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userClient := &adminOutboxRouterUserClient{
				response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
				currentRoleSet: true,
			}
			eventClient := &adminEventTicketOperationsRouterEventClient{err: tc.upstream}
			bookingClient := &adminEventTicketOperationsRouterBookingClient{}
			engine, err := SetupRouterWithAdminOutboxSecurity(userClient, eventClient, bookingClient, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
			if err != nil {
				t.Fatal(err)
			}
			response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/events/"+eventID+"/ticket-operations", "signed-admin-token")
			if response.Code != tc.wantStatus || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Event lookup response = %d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
			}
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode fixed Event lookup error: %v", err)
			}
			if body.Code != tc.wantCode || body.Error != tc.wantError || strings.Contains(response.Body.String(), tc.upstream.Error()) {
				t.Fatalf("Event lookup response exposed upstream detail: %#v", body)
			}
			if bookingClient.calls != 0 {
				t.Fatalf("Booking reads = %d, want 0 when Event lookup fails", bookingClient.calls)
			}
		})
	}
}
