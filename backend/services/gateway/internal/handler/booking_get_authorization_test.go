package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubBookingDetailClient struct {
	bookingv1.BookingServiceClient
	detail *bookingv1.BookingDetail
	err    error
}

func (s *stubBookingDetailClient) GetBooking(context.Context, *bookingv1.GetBookingRequest, ...grpc.CallOption) (*bookingv1.BookingDetail, error) {
	return s.detail, s.err
}

func getBookingResponse(
	t *testing.T,
	handler *BookingHandler,
	userID string,
	role string,
) *httptest.ResponseRecorder {
	t.Helper()
	return getBookingResponseWithUserValue(t, handler, userID, role, userID != "")
}

func getBookingResponseWithUserValue(
	t *testing.T,
	handler *BookingHandler,
	userValue any,
	role string,
	setUser bool,
) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/bookings/booking-1", nil)
	ctx.Params = gin.Params{{Key: "id", Value: "booking-1"}}
	if setUser {
		ctx.Set("user_id", userValue)
	}
	if role != "" {
		ctx.Set("role", role)
	}
	handler.GetBooking(ctx)
	return recorder
}

func bookingOwnedBy(userID string) *bookingv1.BookingDetail {
	return &bookingv1.BookingDetail{
		Id:               "booking-1",
		UserId:           userID,
		EventId:          "event-1",
		Status:           "CONFIRMED",
		TotalAmountCents: 9999,
	}
}

func TestGetBookingAllowsOwner(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponse(t, handler, "user-a", "")

	if response.Code != http.StatusOK {
		t.Fatalf("owner read status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("booking read is cacheable: %q", response.Header().Get("Cache-Control"))
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["user_id"] != "user-a" || body["id"] != "booking-1" {
		t.Fatalf("unexpected owner payload: %v", body)
	}
}

func TestGetBookingSerializesTicketlessItemSeatsAsEmptyArray(t *testing.T) {
	detail := bookingOwnedBy("user-a")
	detail.Items = []*bookingv1.BookingItem{{
		Id: "item-1", TicketTierId: "tier-1", Quantity: 1, UnitPriceCents: 100,
		// An empty protobuf repeated field is nil in Go, but its JSON contract
		// is an empty array so readers can safely use array operations.
		SeatIds: nil,
	}}
	handler := NewBookingHandler(&stubBookingDetailClient{detail: detail})

	response := getBookingResponse(t, handler, "user-a", "")
	if response.Code != http.StatusOK {
		t.Fatalf("ticketless owner read status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("ticketless booking items = %#v, want one-item array", body["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("ticketless booking item = %#v, want object", items[0])
	}
	seatIDs, ok := item["seat_ids"].([]any)
	if !ok || len(seatIDs) != 0 {
		t.Fatalf("ticketless item seat_ids = %#v, want empty JSON array", item["seat_ids"])
	}
}

func TestGetBookingRejectsOtherUser(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponse(t, handler, "user-b", "")

	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-user read status = %d, want %d; body=%s", response.Code, http.StatusNotFound, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingRejectsAdminReadingAnotherUser(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponse(t, handler, "admin-b", "admin")

	if response.Code != http.StatusNotFound {
		t.Fatalf("admin cross-user read status = %d, want %d; body=%s", response.Code, http.StatusNotFound, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingRejectsMissingAuthentication(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponse(t, handler, "", "")

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated read status = %d, want %d; body=%s", response.Code, http.StatusUnauthorized, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingRejectsMissingOwner(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy(""),
	})

	response := getBookingResponse(t, handler, "user-a", "")

	if response.Code != http.StatusNotFound {
		t.Fatalf("ownerless booking status = %d, want %d; body=%s", response.Code, http.StatusNotFound, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingRejectsEmptyIdentity(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponseWithUserValue(t, handler, "", "", true)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("empty identity status = %d, want %d; body=%s", response.Code, http.StatusUnauthorized, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingRejectsNonStringIdentity(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		detail: bookingOwnedBy("user-a"),
	})

	response := getBookingResponseWithUserValue(t, handler, 42, "", true)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("non-string identity status = %d, want %d; body=%s", response.Code, http.StatusUnauthorized, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
}

func TestGetBookingHidesBackendFailure(t *testing.T) {
	handler := NewBookingHandler(&stubBookingDetailClient{
		err: status.Error(codes.Internal, "database unavailable"),
	})

	response := getBookingResponse(t, handler, "user-a", "")

	if response.Code != http.StatusNotFound {
		t.Fatalf("backend failure status = %d, want %d; body=%s", response.Code, http.StatusNotFound, response.Body.String())
	}
	assertNoBookingLeak(t, response.Body.String())
	if strings.Contains(response.Body.String(), "database unavailable") {
		t.Fatalf("backend error leaked: %s", response.Body.String())
	}
}

func assertNoBookingLeak(t *testing.T, body string) {
	t.Helper()
	for _, secret := range []string{"user-a", "event-1", "9999", "CONFIRMED"} {
		if strings.Contains(body, secret) {
			t.Fatalf("response leaked %q: %s", secret, body)
		}
	}
}
