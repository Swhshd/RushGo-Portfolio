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
	"github.com/google/uuid"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type notificationInboxClientStub struct {
	notificationv1.NotificationAdminServiceClient
	listRequest *notificationv1.ListMyNotificationsRequest
	listItems   []*notificationv1.NotificationInboxItem
	readRequest *notificationv1.MarkMyNotificationReadRequest
	listErr     error
	readErr     error
	listCalls   int
	readCalls   int
	authorize   string
}

func (s *notificationInboxClientStub) ListMyNotifications(ctx context.Context, req *notificationv1.ListMyNotificationsRequest, _ ...grpc.CallOption) (*notificationv1.ListMyNotificationsResponse, error) {
	s.listCalls++
	s.listRequest = req
	md, _ := metadata.FromOutgoingContext(ctx)
	values := md.Get("authorization")
	if len(values) == 1 {
		s.authorize = values[0]
	}
	if s.listErr != nil {
		return nil, s.listErr
	}
	items := s.listItems
	if items == nil {
		items = []*notificationv1.NotificationInboxItem{inboxTestItem(
			"87df179f-43f8-49b2-89c3-74e4b61f6e3b", "tickets_issued", "zh", "d25512e4-c46a-4eac-864f-1fd1da30d763", "",
			"/my-tickets/orders/d25512e4-c46a-4eac-864f-1fd1da30d763",
		)}
	}
	return &notificationv1.ListMyNotificationsResponse{
		Notifications: items, Total: int64(len(items)), UnreadCount: int64(len(items)), Page: req.Page, PageSize: req.PageSize}, nil
}

func inboxTestItem(id, typ, locale, bookingID, eventID, detailPath string) *notificationv1.NotificationInboxItem {
	return &notificationv1.NotificationInboxItem{
		Id: id, Type: typ, Locale: locale, Title: "Notification title", Body: "Notification body",
		BookingId: bookingID, EventId: eventID, DetailPath: detailPath, Source: "notification.business_intent",
		CreatedAt: timestamppb.New(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)),
	}
}

func (s *notificationInboxClientStub) MarkMyNotificationRead(ctx context.Context, req *notificationv1.MarkMyNotificationReadRequest, _ ...grpc.CallOption) (*notificationv1.MarkMyNotificationReadResponse, error) {
	s.readCalls++
	s.readRequest = req
	md, _ := metadata.FromOutgoingContext(ctx)
	values := md.Get("authorization")
	if len(values) == 1 {
		s.authorize = values[0]
	}
	if s.readErr != nil {
		return nil, s.readErr
	}
	return &notificationv1.MarkMyNotificationReadResponse{Id: req.NotificationId, ReadAt: timestamppb.New(time.Date(2026, 11, 12, 10, 1, 0, 0, time.UTC))}, nil
}

func TestNotificationInboxListForwardsBearerAndReturnsSafeOwnedProjection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &notificationInboxClientStub{}
	router := gin.New()
	router.GET("/api/users/me/notifications", NewNotificationInboxHandler(client, true).List)
	req := httptest.NewRequest(http.MethodGet, "/api/users/me/notifications?page=2&page_size=10&unread_only=true", nil)
	req.Header.Set("Authorization", "Bearer synthetic-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusOK || client.listCalls != 1 || client.authorize != "Bearer synthetic-token" {
		t.Fatalf("list status=%d calls=%d authorization=%q body=%s", response.Code, client.listCalls, client.authorize, response.Body.String())
	}
	if client.listRequest.GetPage() != 2 || client.listRequest.GetPageSize() != 10 || !client.listRequest.GetUnreadOnly() {
		t.Fatalf("list RPC request = %+v", client.listRequest)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	encoded := response.Body.String()
	if !strings.Contains(encoded, `"detail_path":"/my-tickets/orders/d25512e4-c46a-4eac-864f-1fd1da30d763"`) ||
		!strings.Contains(encoded, `"source":"notification.business_intent"`) || strings.Contains(encoded, "recipient") || strings.Contains(encoded, "payload") {
		t.Fatalf("unsafe or incomplete inbox projection: %s", encoded)
	}
}

func TestNotificationInboxListProjectsEventRemindersWithLegacyItems(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bookingID := "d25512e4-c46a-4eac-864f-1fd1da30d763"
	eventOnSaleID := "9bca21a6-2f47-43b9-a9c4-fc0af9111258"
	eventRestockID := "34be1f8f-cba4-4f29-b9c0-796f44067188"
	client := &notificationInboxClientStub{listItems: []*notificationv1.NotificationInboxItem{
		inboxTestItem("87df179f-43f8-49b2-89c3-74e4b61f6e3b", "tickets_issued", "zh", bookingID, "", "/my-tickets/orders/"+bookingID),
		inboxTestItem("b55ec1ad-8b6e-4f06-96bf-c38497f50b35", "event_onsale", "en", "", eventOnSaleID, "/events/"+eventOnSaleID),
		inboxTestItem("7e628f8c-bcc6-487b-a6dc-d1c90de51981", "event_restock", "zh", "", eventRestockID, "/events/"+eventRestockID),
		inboxTestItem("50ff23eb-63db-41d1-b595-6c0d8dab6849", "refund_requested", "en", "", "", "/account"),
	}}
	router := gin.New()
	router.GET("/api/users/me/notifications", NewNotificationInboxHandler(client, true).List)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/users/me/notifications", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("mixed inbox status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Notifications []struct {
			Type       string `json:"type"`
			BookingID  string `json:"booking_id"`
			DetailPath string `json:"detail_path"`
		} `json:"notifications"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Notifications) != 4 {
		t.Fatalf("mixed inbox returned %d items, want 4: %s", len(body.Notifications), response.Body.String())
	}
	if body.Notifications[1].Type != "event_onsale" || body.Notifications[1].BookingID != "" || body.Notifications[1].DetailPath != "/events/"+eventOnSaleID {
		t.Fatalf("event_onsale projection = %+v", body.Notifications[1])
	}
	if body.Notifications[2].Type != "event_restock" || body.Notifications[2].BookingID != "" || body.Notifications[2].DetailPath != "/events/"+eventRestockID {
		t.Fatalf("event_restock projection = %+v", body.Notifications[2])
	}
	if body.Notifications[3].Type != "refund_requested" || body.Notifications[3].DetailPath != "/account" {
		t.Fatalf("account projection = %+v", body.Notifications[3])
	}
}

func TestNotificationInboxRejectsInvalidEventAndLegacyProjections(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := "9bca21a6-2f47-43b9-a9c4-fc0af9111258"
	bookingID := "d25512e4-c46a-4eac-864f-1fd1da30d763"
	validEvent := inboxTestItem("b55ec1ad-8b6e-4f06-96bf-c38497f50b35", "event_onsale", "en", "", eventID, "/events/"+eventID)
	validBooking := inboxTestItem("87df179f-43f8-49b2-89c3-74e4b61f6e3b", "tickets_issued", "zh", bookingID, "", "/my-tickets/orders/"+bookingID)
	validAccount := inboxTestItem("50ff23eb-63db-41d1-b595-6c0d8dab6849", "refund_requested", "en", "", "", "/account")
	tests := []struct {
		name string
		item *notificationv1.NotificationInboxItem
	}{
		{name: "event path mismatches event id", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", eventID, "/events/34be1f8f-cba4-4f29-b9c0-796f44067188")},
		{name: "zero event id", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", uuid.Nil.String(), "/events/"+uuid.Nil.String())},
		{name: "malformed event id", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", "not-a-uuid", "/events/not-a-uuid")},
		{name: "event id is not canonical", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", "9BCA21A6-2F47-43B9-A9C4-FC0AF9111258", "/events/9BCA21A6-2F47-43B9-A9C4-FC0AF9111258")},
		{name: "event has booking id", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", bookingID, eventID, "/events/"+eventID)},
		{name: "event external path", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", eventID, "https://example.com/events/"+eventID)},
		{name: "event path query", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", eventID, "/events/"+eventID+"?next=/account")},
		{name: "event extra path segment", item: inboxTestItem(validEvent.GetId(), validEvent.GetType(), "en", "", eventID, "/events/"+eventID+"/orders/"+bookingID)},
		{name: "legacy booking wrong path", item: inboxTestItem(validBooking.GetId(), validBooking.GetType(), "zh", bookingID, "", "/events/"+eventID)},
		{name: "legacy booking malformed uuid", item: inboxTestItem(validBooking.GetId(), validBooking.GetType(), "zh", "not-a-uuid", "", "/my-tickets/orders/not-a-uuid")},
		{name: "legacy account external path", item: inboxTestItem(validAccount.GetId(), validAccount.GetType(), "en", "", "", "https://example.com/account")},
		{name: "malformed notification uuid", item: inboxTestItem("not-a-uuid", validEvent.GetType(), "en", "", eventID, "/events/"+eventID)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &notificationInboxClientStub{listItems: []*notificationv1.NotificationInboxItem{test.item}}
			router := gin.New()
			router.GET("/api/users/me/notifications", NewNotificationInboxHandler(client, true).List)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/users/me/notifications", nil))
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"notification_unavailable"`) {
				t.Fatalf("invalid projection status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestNotificationInboxRejectsUnboundedOrUnknownQueryBeforeRPC(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, target := range []string{
		"/api/users/me/notifications?page=1001", "/api/users/me/notifications?page_size=101",
		"/api/users/me/notifications?user_id=" + uuid.NewString(), "/api/users/me/notifications?unread_only=maybe",
		"/api/users/me/notifications?page=1&page=2",
	} {
		client := &notificationInboxClientStub{}
		router := gin.New()
		router.GET("/api/users/me/notifications", NewNotificationInboxHandler(client, true).List)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusBadRequest || client.listCalls != 0 {
			t.Fatalf("query %s status=%d calls=%d", target, response.Code, client.listCalls)
		}
	}
}

func TestNotificationInboxMarkReadUsesPathIDAndMapsOwnerMiss(t *testing.T) {
	gin.SetMode(gin.TestMode)
	id := uuid.New()
	client := &notificationInboxClientStub{}
	router := gin.New()
	router.PATCH("/api/users/me/notifications/:id/read", NewNotificationInboxHandler(client, true).MarkRead)
	req := httptest.NewRequest(http.MethodPatch, "/api/users/me/notifications/"+id.String()+"/read", nil)
	req.Header.Set("Authorization", "Bearer synthetic-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusOK || client.readCalls != 1 || client.readRequest.GetNotificationId() != id.String() || client.authorize != "Bearer synthetic-token" {
		t.Fatalf("read status=%d calls=%d request=%+v authorization=%q", response.Code, client.readCalls, client.readRequest, client.authorize)
	}

	client.readErr = status.Error(codes.NotFound, "missing")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/api/users/me/notifications/"+id.String()+"/read", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("owner miss status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestNotificationInboxFailsClosedWithoutProtectedTransport(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &notificationInboxClientStub{}
	router := gin.New()
	router.GET("/inbox", NewNotificationInboxHandler(client, false).List)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/inbox", nil))
	if response.Code != http.StatusServiceUnavailable || client.listCalls != 0 {
		t.Fatalf("insecure inbox status=%d calls=%d", response.Code, client.listCalls)
	}
}
