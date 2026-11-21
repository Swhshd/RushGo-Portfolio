package realtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type fakeSubscriptionSession struct {
	started  chan struct{}
	commands chan interface{}
	close    chan struct{}
	once     sync.Once
}

func newFakeSubscriptionSession() *fakeSubscriptionSession {
	return &fakeSubscriptionSession{
		started:  make(chan struct{}),
		commands: make(chan interface{}, 32),
		close:    make(chan struct{}),
	}
}

func (s *fakeSubscriptionSession) Receive(ctx context.Context) (interface{}, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case command := <-s.commands:
		return command, nil
	case <-s.close:
		return nil, errors.New("session closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *fakeSubscriptionSession) ChannelWithSubscriptions() <-chan interface{} {
	return s.commands
}

func (s *fakeSubscriptionSession) Close() error {
	select {
	case <-s.close:
	default:
		close(s.close)
	}
	return nil
}

func TestHubRegistersBeforeReadyAndDeliversImmediatePublish(t *testing.T) {
	sessions := make(chan *fakeSubscriptionSession, 1)
	hub := NewHubWithSessionFactory(func(context.Context, string) (SubscriptionSession, error) {
		session := newFakeSubscriptionSession()
		sessions <- session
		return session, nil
	})
	t.Cleanup(func() { hub.Close() })

	client, err := hub.Subscribe(context.Background(), "event-a")
	if err != nil {
		t.Fatal(err)
	}
	session := <-sessions
	select {
	case <-session.started:
	case <-time.After(time.Second):
		t.Fatal("subscription worker did not start")
	}
	if client.ReadyIsClosed() {
		t.Fatal("client became ready before Redis confirmation")
	}
	if got := hub.LocalSubscriberCount("event-a"); got != 1 {
		t.Fatalf("local subscribers before Redis confirmation=%d, want 1", got)
	}

	session.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	select {
	case <-client.Ready():
	case <-time.After(time.Second):
		t.Fatal("ready was not emitted after subscription confirmation")
	}
	client.PublishForTest([]byte(`{"event_id":"event-a"}`))
	select {
	case payload := <-client.Events():
		if string(payload) != `{"event_id":"event-a"}` {
			t.Fatalf("payload=%s", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("message immediately after ready was lost")
	}
	client.Close()
}

func TestHubFansOutAndIsolatesEvents(t *testing.T) {
	sessions := make(chan *fakeSubscriptionSession, 4)
	hub := NewHubWithSessionFactory(func(context.Context, string) (SubscriptionSession, error) {
		session := newFakeSubscriptionSession()
		sessions <- session
		return session, nil
	})
	t.Cleanup(func() { hub.Close() })

	first, err := hub.Subscribe(context.Background(), "event-a")
	if err != nil {
		t.Fatal(err)
	}
	sessionA := <-sessions
	sessionA.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	waitReady(t, first)
	second, err := hub.Subscribe(context.Background(), "event-a")
	if err != nil {
		t.Fatal(err)
	}
	waitReady(t, second)
	other, err := hub.Subscribe(context.Background(), "event-b")
	if err != nil {
		t.Fatal(err)
	}
	sessionB := <-sessions
	sessionB.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-b"}
	waitReady(t, other)

	hub.Broadcast("event-a", []byte("a"))
	for name, client := range map[string]*Client{"first": first, "second": second} {
		select {
		case payload := <-client.Events():
			if string(payload) != "a" {
				t.Fatalf("%s received %q", name, payload)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not receive event-a", name)
		}
	}
	select {
	case payload := <-other.Events():
		t.Fatalf("event-b received event-a payload %q", payload)
	case <-time.After(30 * time.Millisecond):
	}
	first.Close()
	second.Close()
	other.Close()
}

func TestHubResubscriptionInvalidatesOldGeneration(t *testing.T) {
	sessions := make(chan *fakeSubscriptionSession, 4)
	hub := NewHubWithSessionFactory(func(context.Context, string) (SubscriptionSession, error) {
		session := newFakeSubscriptionSession()
		sessions <- session
		return session, nil
	})
	t.Cleanup(func() { hub.Close() })

	old, err := hub.Subscribe(context.Background(), "event-a")
	if err != nil {
		t.Fatal(err)
	}
	oldSession := <-sessions
	oldSession.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	waitReady(t, old)
	oldGeneration := old.Generation()
	oldSession.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	select {
	case <-old.Done():
	case <-time.After(time.Second):
		t.Fatal("old stream remained active after Pub/Sub resubscription")
	}

	newClient, err := hub.Subscribe(context.Background(), "event-a")
	if err != nil {
		t.Fatal(err)
	}
	newSession := <-sessions
	newSession.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	waitReady(t, newClient)
	if newClient.Generation() <= oldGeneration {
		t.Fatalf("generation=%d, old=%d", newClient.Generation(), oldGeneration)
	}
	newClient.Close()
}

func TestSlowClientDoesNotBlockHealthySubscriber(t *testing.T) {
	sessions := make(chan *fakeSubscriptionSession, 2)
	hub := NewHubWithOptions(func(context.Context, string) (SubscriptionSession, error) {
		session := newFakeSubscriptionSession()
		sessions <- session
		return session, nil
	}, HubOptions{ClientBuffer: 1})
	t.Cleanup(func() { hub.Close() })

	slow, _ := hub.Subscribe(context.Background(), "event-a")
	session := <-sessions
	session.commands <- SubscriptionConfirmation{Channel: "rushgo:seat-updates:event-a"}
	waitReady(t, slow)
	healthy, _ := hub.Subscribe(context.Background(), "event-a")
	waitReady(t, healthy)
	hub.Broadcast("event-a", []byte("one"))
	hub.Broadcast("event-a", []byte("two"))
	select {
	case <-slow.Done():
	case <-time.After(time.Second):
		t.Fatal("slow client was not removed")
	}
	select {
	case got := <-healthy.Events():
		if string(got) != "one" {
			t.Fatalf("first healthy payload=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy client was blocked")
	}
	healthy.Close()
}

func TestTwoIndependentHubsReceiveSamePublish(t *testing.T) {
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("REDIS_TEST_URL is not set; multi-hub Redis integration test skipped")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse Redis test URL: %v", err)
	}
	publisher := redis.NewClient(options)
	defer publisher.Close()
	if err := publisher.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis test service unavailable: %v", err)
	}
	eventID := "m4-multihub-" + uuid.NewString()
	channel := SeatChannel(eventID)
	hubA, clientA := NewHub(publisher), (*Client)(nil)
	hubB, clientB := NewHub(publisher), (*Client)(nil)
	t.Cleanup(func() { hubA.Close(); hubB.Close() })
	clientA, err = hubA.Subscribe(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	clientB, err = hubB.Subscribe(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	waitReady(t, clientA)
	waitReady(t, clientB)
	if err := publisher.Publish(context.Background(), channel, `{"event_id":"`+eventID+`","message_id":"m1"}`).Err(); err != nil {
		t.Fatalf("publish to Redis: %v", err)
	}
	for name, client := range map[string]*Client{"gateway-a": clientA, "gateway-b": clientB} {
		select {
		case got := <-client.Events():
			if !strings.Contains(string(got), `"message_id":"m1"`) {
				t.Fatalf("%s payload=%s", name, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s missed publish", name)
		}
	}
}

func waitReady(t *testing.T, client *Client) {
	t.Helper()
	select {
	case <-client.Ready():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready")
	}
}

func TestSeatChannel(t *testing.T) {
	if got := SeatChannel("event-x"); got != "rushgo:seat-updates:event-x" {
		t.Fatalf("channel=%q", got)
	}
}
