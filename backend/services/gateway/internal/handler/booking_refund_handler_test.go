package handler_test

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/ticketbox/gateway/internal/handler"
	"github.com/ticketbox/gateway/internal/router"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type refundClient struct {
	bookingv1.BookingServiceClient
	calls int
	req   *bookingv1.RequestBookingRefundRequest
	auth  []string
	err   error
}

func (s *refundClient) RequestBookingRefund(ctx context.Context, r *bookingv1.RequestBookingRefundRequest, _ ...grpc.CallOption) (*bookingv1.BookingRefund, error) {
	s.calls++
	s.req = r
	m, _ := metadata.FromOutgoingContext(ctx)
	s.auth = m.Get("authorization")
	if s.err != nil {
		return nil, s.err
	}
	return &bookingv1.BookingRefund{RequestId: "request", BookingId: r.BookingId, Status: "REQUESTED", Currency: "USD", AmountCents: 100, CanWithdraw: true}, nil
}
func TestRefundRequestPreservesIdentityKeyAndSafeProjection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &refundClient{}
	r := gin.New()
	r.POST("/bookings/:id/refund", handler.NewRefundHandler(client).Request)
	req := httptest.NewRequest("POST", "/bookings/order/refund", strings.NewReader(`{"reason":"unused","buyer_id":"forged","amount_cents":1}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer authenticated")
	req.Header.Set("Idempotency-Key", "stable_key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 || client.req.BookingId != "order" || client.req.IdempotencyKey != "stable_key" || len(client.auth) != 1 || client.auth[0] != "Bearer authenticated" {
		t.Fatalf("unexpected request mapping: %d %+v", w.Code, client.req)
	}
	for _, secret := range []string{"client_secret", "provider_account", "payment_intent_id", "buyer_id"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("projection leaked %s", secret)
		}
	}
}
func TestRefundRequiresSingleKeyBeforeCallingAuthority(t *testing.T) {
	for _, keys := range [][]string{nil, {"a", "b"}, {"invalid key"}} {
		t.Run(strings.Join(keys, "-"), func(t *testing.T) {
			client := &refundClient{}
			r := gin.New()
			r.POST("/bookings/:id/refund", handler.NewRefundHandler(client).Request)
			req := httptest.NewRequest("POST", "/bookings/order/refund", strings.NewReader(`{"reason":"unused"}`))
			req.Header.Set("Content-Type", "application/json")
			for _, key := range keys {
				req.Header.Add("Idempotency-Key", key)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != 400 || client.calls != 0 {
				t.Fatalf("invalid key reached authority")
			}
		})
	}
}
func TestRefundErrorsDoNotLeakAuthorityDetails(t *testing.T) {
	client := &refundClient{err: status.Error(codes.PermissionDenied, "database-secret")}
	r := gin.New()
	r.POST("/bookings/:id/refund", handler.NewRefundHandler(client).Request)
	req := httptest.NewRequest("POST", "/bookings/order/refund", strings.NewReader(`{"reason":"unused"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 403 || strings.Contains(w.Body.String(), "database-secret") {
		t.Fatalf("unsafe error %s", w.Body.String())
	}
}
func TestRefundRoutesFailClosedWithoutSecureAuthorities(t *testing.T) {
	r := router.SetupRouter(nil, nil, nil, nil, nil, nil, nil)
	for _, route := range []struct{ method, path string }{{"GET", "/api/bookings/order/refund"}, {"POST", "/api/events/event/refund-requests/request/decision"}, {"PUT", "/api/admin/events/event/refund-approvers/staff"}} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(route.method, route.path, nil))
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("route unsafe %s: %d", route.path, w.Code)
		}
	}
}
