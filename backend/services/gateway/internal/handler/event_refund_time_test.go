package handler_test

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/ticketbox/gateway/internal/handler"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/http/httptest"
	"strings"
	"testing"
)

type timeEventClient struct {
	eventv1.EventServiceClient
	request *eventv1.UpdateEventRequest
	err     error
}

func (c *timeEventClient) UpdateEvent(_ context.Context, r *eventv1.UpdateEventRequest, _ ...grpc.CallOption) (*eventv1.EventDetail, error) {
	c.request = r
	if c.err != nil {
		return nil, c.err
	}
	return &eventv1.EventDetail{Date: timestamppb.Now(), CreatedAt: timestamppb.Now(), DateVersion: 7}, nil
}
func TestRefundTimeEventVersionAndConflictProjection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		body   string
		err    error
		code   int
		reason string
	}{
		{`{"date":"2030-01-01T00:00:00Z","expected_date_version":6}`, nil, 200, `"date_version":7`},
		{`{"title":"text only"}`, nil, 200, `"date_version":7`},
		{`{"date":"bad"}`, nil, 400, "invalid date"},
		{`{}`, status.Error(codes.Aborted, "private details"), 409, "EVENT_DATE_CONFLICT"},
		{`{}`, status.Error(codes.FailedPrecondition, "private details"), 409, "EVENT_REFUND_TIME_PENDING"},
	} {
		client := &timeEventClient{err: tc.err}
		r := gin.New()
		r.PUT("/events/:id", handler.NewEventHandler(client, nil).UpdateEvent)
		req := httptest.NewRequest("PUT", "/events/test", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.reason) || strings.Contains(w.Body.String(), "private details") {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		if strings.Contains(tc.body, "expected_date_version") && client.request.ExpectedDateVersion != 6 {
			t.Fatal("version lost")
		}
		if strings.Contains(tc.body, "text only") && client.request.Date != nil {
			t.Fatal("metadata unexpectedly changed date")
		}
	}
}
