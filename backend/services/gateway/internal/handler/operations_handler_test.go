package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
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

type operationsBookingClient struct {
	bookingv1.BookingServiceClient
	calls int
}

func (f *operationsBookingClient) ListOperationsBookings(ctx context.Context, q *bookingv1.OperationsQuery, _ ...grpc.CallOption) (*bookingv1.OperationsBookings, error) {
	f.calls++
	return &bookingv1.OperationsBookings{ObservedAt: timestamppb.Now()}, nil
}

type operationsNotificationClient struct {
	notificationv1.NotificationAdminServiceClient
	err error
}

func (f operationsNotificationClient) ListDeliveryExceptions(ctx context.Context, q *bookingv1.OperationsQuery, _ ...grpc.CallOption) (*bookingv1.OperationsBookings, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer synthetic" {
		return nil, status.Error(codes.Unauthenticated, "identity missing")
	}
	if f.err != nil {
		return nil, f.err
	}
	return &bookingv1.OperationsBookings{ObservedAt: timestamppb.Now(), HasMore: true, Items: []*bookingv1.OperationsBooking{{BookingId: "test-booking", CreatedAt: timestamppb.New(time.Now()), Issues: []*bookingv1.OperationIssue{{Kind: "notification_unknown", Source: "notification.delivery_intents", State: "UNKNOWN", ObservedAt: timestamppb.Now()}}}}}, nil
}
func TestOperationsNotificationSourceAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		upstream error
		code     int
		source   string
	}{
		{"available", "", nil, 200, "available"},
		{"unavailable", "", status.Error(codes.Unavailable, "offline"), 200, "unavailable"},
		{"role-revoked", "", status.Error(codes.PermissionDenied, "revoked"), 403, ""},
		{"unbounded", "?limit=101", nil, 400, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			bookings := &operationsBookingClient{}
			h := NewOperationsHandlerWithNotification(bookings, true, operationsNotificationClient{err: tc.upstream}, true)
			r := gin.New()
			r.GET("/operations", h.List)
			req := httptest.NewRequest("GET", "/operations"+tc.query, nil)
			req.Header.Set("Authorization", "Bearer synthetic")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.code {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if tc.code == 400 && bookings.calls != 0 {
				t.Fatal("invalid bounds reached source")
			}
			if tc.code == 200 {
				var body map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body["notification_source_state"] != tc.source {
					t.Fatal("source status lost")
				}
				if tc.source == "available" && (len(body["notification_items"].([]any)) != 1 || body["notification_has_more"] != true) {
					t.Fatal("exceptions or pagination lost")
				}
				if tc.source == "unavailable" && body["notification_observed_at"] != nil {
					t.Fatal("unavailable source fabricated observation")
				}
			}
		})
	}
}
