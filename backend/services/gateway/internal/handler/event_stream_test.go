package handler

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ticketbox/gateway/internal/realtime"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
)

type streamVisibilityEventClient struct {
	eventv1.EventServiceClient
	err error
}

func (c streamVisibilityEventClient) GetEvent(_ context.Context, req *eventv1.GetEventRequest, _ ...grpc.CallOption) (*eventv1.EventDetail, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &eventv1.EventDetail{Id: req.EventId, PublicationStatus: "published"}, nil
}

type streamTestSession struct {
	initial chan interface{}
	frames  chan interface{}
	closed  chan struct{}
	once    sync.Once
}

func newStreamTestSession() *streamTestSession {
	return &streamTestSession{initial: make(chan interface{}, 1), frames: make(chan interface{}, 4), closed: make(chan struct{})}
}

func (s *streamTestSession) Receive(ctx context.Context) (interface{}, error) {
	select {
	case msg := <-s.initial:
		return msg, nil
	case <-s.closed:
		return nil, errors.New("closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *streamTestSession) ChannelWithSubscriptions() <-chan interface{} { return s.frames }
func (s *streamTestSession) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func TestSSEReadyThenImmediateUpdateAndCancelCleanup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := uuid.NewString()
	session := newStreamTestSession()
	hub := realtime.NewHubWithSessionFactory(func(context.Context, string) (realtime.SubscriptionSession, error) {
		session.initial <- realtime.SubscriptionConfirmation{Channel: realtime.SeatChannel(eventID)}
		return session, nil
	})
	defer hub.Close()
	router := gin.New()
	router.GET("/api/events/:id/seats/stream", NewEventHandler(streamVisibilityEventClient{}, hub).StreamSeats)
	server := httptest.NewServer(router)
	defer server.Close()

	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, server.URL+"/api/events/"+eventID+"/seats/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content type=%q", got)
	}
	// Proxy compression buffers small SSE frames unless the stream forbids
	// transformations. Browser clients must receive ready before taking a snapshot.
	if got := response.Header.Get("Cache-Control"); got != "no-cache, no-transform" {
		t.Fatalf("cache control=%q", got)
	}
	reader := bufio.NewReader(response.Body)
	readyFrame := readSSEFrame(t, reader)
	if !strings.Contains(readyFrame, "event: ready") || !strings.Contains(readyFrame, eventID) {
		t.Fatalf("ready frame=%q", readyFrame)
	}

	// Publish immediately after ready: ready is the subscription barrier.
	session.frames <- realtime.PubSubMessage{Channel: realtime.SeatChannel(eventID), Payload: `{"event_id":"` + eventID + `","message_id":"message-1","seat_updates":[]}`}
	updateFrame := readSSEFrame(t, reader)
	if !strings.Contains(updateFrame, "event: seat-update") || !strings.Contains(updateFrame, "id: message-1") {
		t.Fatalf("seat update frame=%q", updateFrame)
	}
	if got := hub.LocalSubscriberCount(eventID); got != 1 {
		t.Fatalf("subscriber count before cancel=%d", got)
	}
	cancel()
	_ = response.Body.Close()
	waitForSubscriberCount(t, hub, eventID, 0)
}

func TestSSESubscriptionLossClosesHttpStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := uuid.NewString()
	session := newStreamTestSession()
	hub := realtime.NewHubWithSessionFactory(func(context.Context, string) (realtime.SubscriptionSession, error) {
		session.initial <- realtime.SubscriptionConfirmation{Channel: realtime.SeatChannel(eventID)}
		return session, nil
	})
	defer hub.Close()
	router := gin.New()
	router.GET("/api/events/:id/seats/stream", NewEventHandler(streamVisibilityEventClient{}, hub).StreamSeats)
	server := httptest.NewServer(router)
	defer server.Close()
	response, err := http.Get(server.URL + "/api/events/" + eventID + "/seats/stream")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	if frame := readSSEFrame(t, reader); !strings.Contains(frame, "event: ready") {
		t.Fatalf("ready frame=%q", frame)
	}
	streamClosed := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(io.Discard, response.Body)
		streamClosed <- copyErr
	}()
	session.frames <- realtime.SubscriptionConfirmation{Channel: realtime.SeatChannel(eventID)}
	select {
	case <-streamClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the invalidated stream to close")
	}
	_ = response.Body.Close()
}

func readSSEFrame(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	frameCh := make(chan struct {
		frame string
		err   error
	}, 1)
	go func() {
		var frame strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				frameCh <- struct {
					frame string
					err   error
				}{frame.String(), err}
				return
			}
			frame.WriteString(line)
			if line == "\r\n" || line == "\n" {
				frameCh <- struct {
					frame string
					err   error
				}{frame.String(), nil}
				return
			}
		}
	}()
	select {
	case result := <-frameCh:
		if result.err != nil && !errors.Is(result.err, io.EOF) {
			t.Fatal(result.err)
		}
		return result.frame
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SSE frame")
		return ""
	}
}

func waitForSubscriberCount(t *testing.T, hub *realtime.Hub, eventID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.LocalSubscriberCount(eventID) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscriber count=%d, want %d", hub.LocalSubscriberCount(eventID), want)
}

func TestStreamSeatsRejectsPrivateEventBeforeSubscribing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	eventID := uuid.NewString()
	router := gin.New()
	router.GET("/api/events/:id/seats/stream", NewEventHandler(streamVisibilityEventClient{
		err: status.Error(codes.NotFound, "event not found"),
	}, nil).StreamSeats)
	request := httptest.NewRequest(http.MethodGet, "/api/events/"+eventID+"/seats/stream", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("private event stream status = %d, want 404", response.Code)
	}
	if strings.Contains(response.Body.String(), "private") {
		t.Fatalf("private event response leaked event data: %q", response.Body.String())
	}
}
