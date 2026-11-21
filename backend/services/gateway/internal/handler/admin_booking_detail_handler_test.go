package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func adminBookingDetailFixture() (*bookingv1.AdminBookingDetail, uuid.UUID) {
	bookingID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	tierID, itemID, seatID, ticketID, requestID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC().Add(-time.Minute)
	return &bookingv1.AdminBookingDetail{
		Booking: &bookingv1.BookingDetail{
			Id: bookingID.String(), UserId: userID.String(), EventId: eventID.String(), Status: "CONFIRMED",
			TotalAmountCents: 1000, Currency: "usd", CreatedAt: timestamppb.New(now),
			Items: []*bookingv1.BookingItem{{Id: itemID.String(), TicketTierId: tierID.String(), TierName: "Floor", Quantity: 1, UnitPriceCents: 1000, SeatIds: []string{seatID.String()}}},
		},
		LifecycleGeneration: 1, LifecyclePhase: "TERMINAL", Currency: "usd", BookingMode: "pessimistic",
		Tickets:        []*bookingv1.AdminBookingDetailTicket{{TicketId: ticketID.String(), Status: "ISSUED", SeatId: seatID.String(), TicketTierId: tierID.String(), IssuedAt: timestamppb.New(now)}},
		ReceiptPresent: true,
		Receipt:        &bookingv1.AdminBookingDetailReceipt{Outcome: "issued", ReasonCode: "eligible", TicketCount: 1, ReceiptCount: 1, ObservedAt: timestamppb.New(now), CreatedAt: timestamppb.New(now)},
		RefundPresent:  true,
		Refund: &bookingv1.BookingRefund{
			RequestId: requestID.String(), BookingId: bookingID.String(), EventId: eventID.String(), Status: "REQUESTED",
			Reason: "Changed plans", AmountCents: 1000, Currency: "usd", RequestedAt: timestamppb.New(now),
			PaymentRefundState: "unknown", TicketRevocationState: "NOT_STARTED", CanWithdraw: true,
		},
		ObservedAt: timestamppb.New(now.Add(2 * time.Minute)),
	}, bookingID
}

func TestProjectAdminBookingDetailKeepsSafeExactFields(t *testing.T) {
	source, bookingID := adminBookingDetailFixture()
	response, ok := projectAdminBookingDetail(source, bookingID)
	if !ok {
		t.Fatal("valid detail was rejected")
	}
	if response.BookingID != bookingID.String() || response.Currency != "USD" || response.BookingStatus != "CONFIRMED" ||
		len(response.Items) != 1 || len(response.Tickets) != 1 || response.Receipt == nil || response.Refund == nil {
		t.Fatalf("unexpected projection: %+v", response)
	}
	if response.Refund.Status != "REQUESTED" || response.Refund.PaymentRefundState != "unknown" ||
		response.Refund.RequestedAt == nil || response.Refund.CanWithdraw != true {
		t.Fatalf("unexpected refund projection: %+v", response.Refund)
	}
	if response.SourceService != "booking" || response.Coverage != "booking_owned_exact" {
		t.Fatalf("unexpected source labels: %+v", response)
	}
}

func TestProjectAdminBookingDetailRejectsIdentityAndUnknownStates(t *testing.T) {
	source, bookingID := adminBookingDetailFixture()
	if _, ok := projectAdminBookingDetail(source, uuid.New()); ok {
		t.Fatal("mismatched booking identity was accepted")
	}
	source.Booking.Currency = "eur"
	source.Currency = "eur"
	if _, ok := projectAdminBookingDetail(source, bookingID); ok {
		t.Fatal("non-USD detail was accepted")
	}
	source, bookingID = adminBookingDetailFixture()
	source.Refund.Currency = "USD"
	source.Refund.Status = "SOMETHING_ELSE"
	if _, ok := projectAdminBookingDetail(source, bookingID); !ok {
		t.Fatal("projection should preserve an unexpected refund state for review rather than hide the booking")
	}
	source, bookingID = adminBookingDetailFixture()
	source.Receipt.Outcome = "unknown"
	if _, ok := projectAdminBookingDetail(source, bookingID); ok {
		t.Fatal("unknown receipt outcome was accepted")
	}
}

type adminBookingDetailClient struct {
	bookingv1.BookingServiceClient
	detail *bookingv1.AdminBookingDetail
	err    error
}

func (c adminBookingDetailClient) GetAdminBookingDetail(context.Context, *bookingv1.GetAdminBookingDetailRequest, ...grpc.CallOption) (*bookingv1.AdminBookingDetail, error) {
	return c.detail, c.err
}

func TestAdminBookingDetailHandlerRejectsInvalidAndMissingExactIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewAdminBookingDetailHandler(adminBookingDetailClient{err: status.Error(codes.NotFound, "missing")})
	router := gin.New()
	router.GET("/admin/bookings/:id", handler.Get)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/bookings/not-a-uuid", nil)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status = %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/bookings/"+uuid.NewString(), nil)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/admin/bookings/"+uuid.NewString(), nil)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing booking status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminBookingDetailHandlerProjectsSafeJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	source, bookingID := adminBookingDetailFixture()
	handler := NewAdminBookingDetailHandler(adminBookingDetailClient{detail: source})
	router := gin.New()
	router.GET("/admin/bookings/:id", handler.Get)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/bookings/"+bookingID.String(), nil)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, unexpected := range []string{"client_secret", "paymentIntentClientSecret", "payment_intent_client_secret"} {
		if strings.Contains(body, unexpected) {
			t.Fatalf("sensitive field %q leaked into admin detail", unexpected)
		}
	}
	if !strings.Contains(body, `"booking_id":"`+bookingID.String()+`"`) {
		t.Fatalf("booking identity missing: %s", body)
	}
}
