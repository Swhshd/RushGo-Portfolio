package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
)

type eventImageClient struct {
	eventv1.EventServiceClient
	event *eventv1.EventDetail
}

func (c *eventImageClient) ListEvents(context.Context, *eventv1.ListEventsRequest, ...grpc.CallOption) (*eventv1.ListEventsResponse, error) {
	return &eventv1.ListEventsResponse{Events: []*eventv1.EventDetail{c.event}, Total: 1, Page: 1, PageSize: 20}, nil
}

func (c *eventImageClient) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	return c.event, nil
}

func TestEventImageURLStringContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, imageURL := range []string{"", "https://example.test/event.jpg"} {
		name := "populated_image"
		if imageURL == "" {
			name = "empty_image"
		}
		t.Run(name, func(t *testing.T) {
			client := &eventImageClient{event: &eventv1.EventDetail{
				Id: "event-fixture", Title: "Event fixture", Description: "", Category: "concerts",
				Venue: "fixture venue", Location: "fixture location", ImageUrl: imageURL, Status: "active",
				Date: timestamppb.New(time.Unix(10, 0)), CreatedAt: timestamppb.New(time.Unix(1, 0)),
			}}
			router := SetupRouter(nil, client, nil, nil, nil, nil, nil)
			for _, endpoint := range []string{"/api/events", "/api/events/event-fixture"} {
				t.Run(endpoint, func(t *testing.T) {
					response := httptest.NewRecorder()
					router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
					if response.Code != http.StatusOK {
						t.Fatalf("HTTP status=%d, want 200", response.Code)
					}
					var event map[string]json.RawMessage
					if endpoint == "/api/events" {
						var body struct {
							Events []map[string]json.RawMessage `json:"events"`
						}
						if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
							t.Fatal(err)
						}
						if len(body.Events) != 1 {
							t.Fatalf("event count=%d, want 1", len(body.Events))
						}
						event = body.Events[0]
					} else if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil {
						t.Fatal(err)
					}
					// Comparing encoded values distinguishes a string from null/omission.
					wantImage, _ := json.Marshal(imageURL)
					if string(event["image_url"]) != string(wantImage) {
						t.Error("image_url must remain a present JSON string with the original value")
					}
					if string(event["description"]) != `""` || string(event["id"]) != `"event-fixture"` || string(event["title"]) != `"Event fixture"` {
						t.Error("event identity/title/empty-description string contract changed")
					}
				})
			}
		})
	}
}
