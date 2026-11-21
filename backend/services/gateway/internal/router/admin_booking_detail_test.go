package router

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adminBookingDetailRouterBookingClient struct {
	bookingv1.BookingServiceClient
	authorization []string
	calls         int
}

func (c *adminBookingDetailRouterBookingClient) GetAdminBookingDetail(ctx context.Context, req *bookingv1.GetAdminBookingDetailRequest, _ ...grpc.CallOption) (*bookingv1.AdminBookingDetail, error) {
	c.calls++
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
		c.authorization = outgoing.Get("authorization")
	}
	now := time.Now().UTC().Add(-time.Minute)
	return &bookingv1.AdminBookingDetail{
		Booking: &bookingv1.BookingDetail{
			Id: req.GetBookingId(), UserId: "70000000-0000-4000-8000-000000000001",
			EventId: "60000000-0000-4000-8000-000000000001", Status: "CONFIRMED",
			TotalAmountCents: 1000, Currency: "usd", CreatedAt: timestamppb.New(now),
		},
		LifecycleGeneration: 1, LifecyclePhase: "TERMINAL", Currency: "usd", BookingMode: "pessimistic",
		ObservedAt: timestamppb.New(now.Add(time.Minute)),
	}, nil
}

func TestAdminBookingDetailRouteRequiresLiveAdminAndForwardsIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bookingID := "50000000-0000-4000-8000-000000000001"
	admin := &adminOutboxRouterUserClient{
		response:       &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRole:    &userv1.ValidateTokenResponse{Valid: true, CurrentRoleVerified: true, UserId: "70000000-0000-4000-8000-000000000001", Role: "admin"},
		currentRoleSet: true,
	}
	booking := &adminBookingDetailRouterBookingClient{}
	engine, err := SetupRouterWithAdminOutboxSecurity(admin, nil, booking, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/admin/bookings/" + bookingID

	unauthorized := adminOutboxRouterRequest(engine, http.MethodGet, path, "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d", unauthorized.Code)
	}
	allowed := adminOutboxRouterRequest(engine, http.MethodGet, path, "signed-admin-token")
	if allowed.Code != http.StatusOK || booking.calls != 1 {
		t.Fatalf("admin status = %d calls=%d body=%s", allowed.Code, booking.calls, allowed.Body.String())
	}
	if len(booking.authorization) != 1 || booking.authorization[0] != "Bearer signed-admin-token" {
		t.Fatalf("forwarded authorization = %v", booking.authorization)
	}

	customer := &adminOutboxRouterUserClient{
		response: &userv1.ValidateTokenResponse{Valid: true, UserId: "70000000-0000-4000-8000-000000000002", Role: "customer"},
	}
	engine, err = SetupRouterWithAdminOutboxSecurity(customer, nil, &adminBookingDetailRouterBookingClient{}, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := adminOutboxRouterRequest(engine, http.MethodGet, path, "signed-customer-token")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("customer status = %d", forbidden.Code)
	}
}

func TestAdminBookingDetailRouteFailsClosedWhenAdminSourceUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine, err := SetupRouterWithAdminOutboxSecurity(nil, nil, &adminBookingDetailRouterBookingClient{}, nil, nil, newAdminOutboxTestRedis(t), nil, "", true, false)
	if err != nil {
		t.Fatal(err)
	}
	response := adminOutboxRouterRequest(engine, http.MethodGet, "/api/admin/bookings/50000000-0000-4000-8000-000000000001", "signed-admin-token")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsecured source status = %d", response.Code)
	}
}
