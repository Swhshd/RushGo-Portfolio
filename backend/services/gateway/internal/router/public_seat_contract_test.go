package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
)

type publicSeatEventClient struct {
	eventv1.EventServiceClient
	seats []*eventv1.Seat
}

func (c *publicSeatEventClient) GetSeatsOfEventByTierId(context.Context, *eventv1.GetSeatsRequest, ...grpc.CallOption) (*eventv1.GetSeatsResponse, error) {
	return &eventv1.GetSeatsResponse{Seats: c.seats}, nil
}

// Catch accidental ownership exposure at the real public route and serializer,
// even when the internal Event response contains non-empty booking/order IDs.
func TestPublicSeatSnapshotOmitsOwnershipIdentifiers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, status := range []string{"available", "reserved", "booked"} {
		t.Run(status, func(t *testing.T) {
			internal := &eventv1.Seat{
				Id: "seat-fixture", EventId: "event-fixture", TicketTierId: "tier-fixture",
				Status: status, BookingId: "internal-booking-fixture", OrderId: "internal-order-fixture",
				Position:     `{"sectionId":"Floor-center","row":"A","seat":7,"x":120,"y":240}`,
				CreatedAt:    timestamppb.New(time.Date(2026, 10, 8, 1, 0, 0, 0, time.UTC)),
				UpdatedAt:    timestamppb.New(time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)),
				SeatRevision: 9007199254740993,
			}
			client := &publicSeatEventClient{seats: []*eventv1.Seat{internal}}
			router := SetupRouter(nil, client, nil, nil, nil, nil, nil)
			response := httptest.NewRecorder()
			// No authorization header: the existing snapshot policy is public.
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/seats?event_id=event-fixture&ticket_tier_id=tier-fixture", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("HTTP status = %d, want 200", response.Code)
			}
			var body struct {
				Seats []map[string]json.RawMessage `json:"seats"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Seats) != 1 {
				t.Fatalf("seat count = %d, want 1", len(body.Seats))
			}
			seat := body.Seats[0]
			for _, forbidden := range []string{"booking_id", "order_id", "reserved_by_booking_id", "reservation_owner", "user_id", "saga_id", "payment_id"} {
				if _, present := seat[forbidden]; present {
					t.Errorf("public seat JSON key %q must be absent", forbidden)
				}
			}
			keys := make([]string, 0, len(seat))
			for key := range seat {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			wantKeys := []string{"created_at", "event_id", "id", "position", "seat_revision", "status", "ticket_tier_id", "updated_at"}
			if !reflect.DeepEqual(keys, wantKeys) {
				t.Errorf("public seat keys = %v, want %v", keys, wantKeys)
			}
			for key, want := range map[string]string{
				"id": "seat-fixture", "event_id": "event-fixture", "ticket_tier_id": "tier-fixture",
				"status": status, "seat_revision": "9007199254740993",
				"created_at": "2026-10-08T01:00:00Z", "updated_at": "2026-10-08T02:00:00Z",
			} {
				var got string
				if err := json.Unmarshal(seat[key], &got); err != nil || got != want {
					t.Errorf("public field %q = %q, want %q (decode error: %v)", key, got, want, err)
				}
			}
			var position map[string]interface{}
			if err := json.Unmarshal(seat["position"], &position); err != nil {
				t.Fatal(err)
			}
			wantPosition := map[string]interface{}{"sectionId": "Floor-center", "row": "A", "seat": float64(7), "x": float64(120), "y": float64(240)}
			if !reflect.DeepEqual(position, wantPosition) {
				t.Error("public layout position changed")
			}
			if internal.BookingId != "internal-booking-fixture" || internal.OrderId != "internal-order-fixture" {
				t.Error("public response mapping mutated internal ownership data")
			}
		})
	}
}
