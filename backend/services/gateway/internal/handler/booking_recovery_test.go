package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type bookingRecoveryClient struct {
	bookingv1.BookingServiceClient
	request  *bookingv1.CreateBookingRequest
	response *bookingv1.BookingDetail
	err      error
}

func (c *bookingRecoveryClient) CreateBooking(_ context.Context, req *bookingv1.CreateBookingRequest, _ ...grpc.CallOption) (*bookingv1.BookingDetail, error) {
	c.request = req
	return c.response, c.err
}

func TestBookingIdempotencyHeaderAndRecoveryResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, scenario := range []struct {
		name, state string
		headers     []string
		err         error
		want        int
		code        string
	}{
		{"ready", "ready", []string{"original_key-1"}, nil, 201, ""},
		{"starting", "starting", []string{"original_key-1"}, nil, 202, ""},
		{"terminal", "terminal", []string{"original_key-1"}, nil, 200, ""},
		{"mismatch", "", []string{"original_key-1"}, status.Error(codes.AlreadyExists, "booking_request_conflict"), 409, "booking_request_conflict"},
		{"accounting_not_ready", "", []string{"original_key-1"}, status.Error(codes.FailedPrecondition, "accounting_not_ready"), 409, "accounting_not_ready"},
		{"malformed", "", []string{"invalid key"}, nil, 400, "invalid_booking_request"},
		{"repeated", "", []string{"one", "two"}, nil, 400, "invalid_booking_request"},
		{"comma", "", []string{"one,two"}, nil, 400, "invalid_booking_request"},
		{"legacy_optional", "ready", nil, nil, 201, ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			client := &bookingRecoveryClient{response: &bookingv1.BookingDetail{Id: "original-booking", Status: "PENDING", CreationState: scenario.state, PaymentIntentClientSecret: "original_secret", CreatedAt: timestamppb.New(time.Now())}, err: scenario.err}
			h := NewBookingHandler(client)
			r := gin.New()
			r.POST("/", func(c *gin.Context) { c.Set("user_id", "user-original"); h.CreateBooking(c) })
			req := httptest.NewRequest("POST", "/", strings.NewReader(`{"user_id":"untrusted-body-field","event_id":"event","items":[{"ticket_tier_id":"tier","quantity":1,"seat_ids":["seat"]}]}`))
			req.Header.Set("Content-Type", "application/json")
			for _, header := range scenario.headers {
				req.Header.Add("Idempotency-Key", header)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != scenario.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, scenario.want, w.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if scenario.code != "" && body["code"] != scenario.code {
				t.Fatal("stable error code absent", body)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("booking response is cacheable: %q", w.Header().Get("Cache-Control"))
			}
			if w.Code == 202 && w.Header().Get("Retry-After") != "1" {
				t.Fatal("starting has no retry guidance")
			}
			if w.Code == 200 && body["paymentIntentClientSecret"] != "" {
				t.Fatal("terminal returned payable secret")
			}
			if len(scenario.headers) == 1 && scenario.want < 400 && client.request.RequestId != scenario.headers[0] {
				t.Fatal("Gateway changed idempotency key")
			}
			if scenario.want == 400 && client.request != nil {
				t.Fatal("malformed header reached backend")
			}
			if client.request != nil && client.request.UserId != "user-original" {
				t.Fatal("Booking request lost the authenticated Gateway identity")
			}
		})
	}
}

func TestBookingRequiredKeyRejectsMissingBeforeBackend(t *testing.T) {
	client := &bookingRecoveryClient{}
	h := NewBookingHandler(client)
	h.RequireRequestID = true
	r := gin.New()
	r.POST("/", func(c *gin.Context) { c.Set("user_id", "user"); h.CreateBooking(c) })
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"event_id":"event","items":[{"ticket_tier_id":"tier","quantity":1,"seat_ids":["seat"]}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 || client.request != nil {
		t.Fatal("required key bypassed", w.Code)
	}
}
