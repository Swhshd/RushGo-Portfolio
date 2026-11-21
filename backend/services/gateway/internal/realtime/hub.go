package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const defaultClientBuffer = 32

type SubscriptionConfirmation struct {
	Channel string
}

type PubSubMessage struct {
	Channel string
	Payload string
}

type SubscriptionSession interface {
	Receive(context.Context) (interface{}, error)
	ChannelWithSubscriptions() <-chan interface{}
	Close() error
}

type HubOptions struct {
	ClientBuffer int
	Metrics      *observability.Metrics
	Propagator   propagation.TextMapPropagator
}

type SessionFactory func(context.Context, string) (SubscriptionSession, error)

type Hub struct {
	mu          sync.Mutex
	workers     map[string]*eventWorker
	factory     SessionFactory
	options     HubOptions
	closed      bool
	nextID      atomic.Uint64
	invalidated map[string]bool
}

type eventWorker struct {
	eventID    string
	generation uint64
	hub        *Hub
	ctx        context.Context
	cancel     context.CancelFunc
	session    SubscriptionSession
	clients    map[uint64]*Client
	ready      chan struct{}
	readyOnce  sync.Once
	done       chan struct{}
	startOnce  sync.Once
}

type Client struct {
	id         uint64
	eventID    string
	generation uint64
	worker     *eventWorker
	mu         sync.RWMutex
	ready      chan struct{}
	readyOnce  sync.Once
	events     chan []byte
	done       chan struct{}
	closeOnce  sync.Once
}

func NewHub(client *redis.Client) *Hub {
	return NewHubWithOptions(func(ctx context.Context, channel string) (SubscriptionSession, error) {
		pubsub := client.Subscribe(ctx, channel)
		return &redisSubscriptionSession{pubsub: pubsub}, nil
	}, HubOptions{})
}

func NewHubWithTelemetry(client *redis.Client, metrics *observability.Metrics, propagator propagation.TextMapPropagator) *Hub {
	return NewHubWithOptions(func(ctx context.Context, channel string) (SubscriptionSession, error) {
		pubsub := client.Subscribe(ctx, channel)
		return &redisSubscriptionSession{pubsub: pubsub}, nil
	}, HubOptions{Metrics: metrics, Propagator: propagator})
}

func NewHubWithSessionFactory(factory SessionFactory) *Hub {
	return NewHubWithOptions(factory, HubOptions{})
}

func NewHubWithOptions(factory SessionFactory, options HubOptions) *Hub {
	if options.ClientBuffer < 1 {
		options.ClientBuffer = defaultClientBuffer
	}
	if options.Propagator == nil {
		options.Propagator = otel.GetTextMapPropagator()
	}
	return &Hub{workers: make(map[string]*eventWorker), factory: factory, options: options, invalidated: make(map[string]bool)}
}

func SeatChannel(eventID string) string {
	return "rushgo:seat-updates:" + eventID
}

func (h *Hub) Subscribe(ctx context.Context, eventID string) (*Client, error) {
	if h == nil || h.factory == nil {
		return nil, errors.New("realtime hub is not configured")
	}
	if eventID == "" {
		return nil, errors.New("event id is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	clientID := h.nextID.Add(1)
	var worker *eventWorker
	var start bool
	var recovery bool
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errors.New("realtime hub is closed")
	}
	worker = h.workers[eventID]
	if worker == nil {
		workerCtx, cancel := context.WithCancel(context.Background())
		worker = &eventWorker{
			eventID: eventID, generation: clientID, hub: h, ctx: workerCtx,
			cancel: cancel, clients: make(map[uint64]*Client), ready: make(chan struct{}),
			done: make(chan struct{}),
		}
		h.workers[eventID] = worker
		start = true
		recovery = h.invalidated[eventID]
		delete(h.invalidated, eventID)
	}
	client := &Client{
		id: clientID, eventID: eventID, generation: worker.generation, worker: worker,
		ready: make(chan struct{}), events: make(chan []byte, h.options.ClientBuffer), done: make(chan struct{}),
	}
	worker.clients[clientID] = client
	if h.options.Metrics != nil {
		h.options.Metrics.SSEActiveConnections.Add(ctx, 1)
		h.options.Metrics.SSEConnections.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "opened")))
		if recovery {
			h.options.Metrics.SubscriptionGenerationEvent.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "recovery")))
		}
	}
	if start {
		worker.session = nil
	}
	if !start {
		select {
		case <-worker.ready:
			client.readyOnce.Do(func() { close(client.ready) })
		default:
		}
	}
	h.mu.Unlock()
	if start {
		worker.startOnce.Do(func() { go h.runWorker(worker) })
	}
	return client, nil
}

func (h *Hub) runWorker(worker *eventWorker) {
	lost := false
	if h.options.Metrics != nil {
		h.options.Metrics.SubscriptionGenerationEvent.Add(context.Background(), 1, metric.WithAttributes(attribute.String("event", "started")))
	}
	defer close(worker.done)
	defer func() {
		h.mu.Lock()
		if h.workers[worker.eventID] == worker {
			delete(h.workers, worker.eventID)
		}
		if lost {
			h.invalidated[worker.eventID] = true
		}
		for _, client := range worker.clients {
			client.closeFromWorker()
		}
		h.mu.Unlock()
		worker.cancel()
		if worker.session != nil {
			_ = worker.session.Close()
		}
	}()

	session, err := h.factory(worker.ctx, SeatChannel(worker.eventID))
	if err != nil {
		return
	}
	worker.session = session
	confirmation, err := session.Receive(worker.ctx)
	if err != nil {
		return
	}
	if _, ok := confirmation.(*redis.Subscription); !ok {
		if _, ok := confirmation.(SubscriptionConfirmation); !ok {
			return
		}
	}

	// Every local client is in the worker map before Subscribe/Receive starts.
	// Hold the lock while opening ready and copying the initial client set so no
	// publish can race between Redis confirmation and local fanout activation.
	h.mu.Lock()
	if h.workers[worker.eventID] != worker || h.closed {
		h.mu.Unlock()
		return
	}
	worker.readyOnce.Do(func() { close(worker.ready) })
	for _, client := range worker.clients {
		client.readyOnce.Do(func() { close(client.ready) })
	}
	h.mu.Unlock()

	messageCh := session.ChannelWithSubscriptions()
	for {
		select {
		case <-worker.ctx.Done():
			return
		case message, ok := <-messageCh:
			if !ok {
				return
			}
			switch value := message.(type) {
			case *redis.Subscription:
				lost = true
				if h.options.Metrics != nil {
					h.options.Metrics.SubscriptionGenerationEvent.Add(context.Background(), 1, metric.WithAttributes(attribute.String("event", "loss")))
				}
				return
			case SubscriptionConfirmation:
				lost = true
				if h.options.Metrics != nil {
					h.options.Metrics.SubscriptionGenerationEvent.Add(context.Background(), 1, metric.WithAttributes(attribute.String("event", "loss")))
				}
				return
			case *redis.Message:
				h.broadcastWorker(worker, []byte(value.Payload))
			case PubSubMessage:
				h.broadcastWorker(worker, []byte(value.Payload))
			default:
				// Unknown Pub/Sub frames are ignored; transport errors close the channel.
			}
		}
	}
}

func (h *Hub) broadcastWorker(worker *eventWorker, payload []byte) {
	var envelope struct {
		EventID     string `json:"event_id"`
		Traceparent string `json:"traceparent"`
		Tracestate  string `json:"tracestate"`
	}
	if json.Unmarshal(payload, &envelope) != nil || envelope.EventID != worker.eventID {
		return
	}
	ctx := context.Background()
	if envelope.Traceparent != "" && h.options.Propagator != nil {
		carrier := propagation.MapCarrier{"traceparent": envelope.Traceparent}
		if envelope.Tracestate != "" {
			carrier["tracestate"] = envelope.Tracestate
		}
		ctx = h.options.Propagator.Extract(ctx, carrier)
	}
	ctx, receiveSpan := otel.Tracer("github.com/ticketbox/gateway/realtime").Start(ctx, "seat_realtime.receive", trace.WithAttributes(attribute.String("event_id", worker.eventID)))
	defer receiveSpan.End()
	if h.options.Metrics != nil {
		h.options.Metrics.RealtimeReceived.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "received")))
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.workers[worker.eventID] != worker {
		return
	}
	h.broadcastLocked(worker, payload, ctx)
}

func (h *Hub) Broadcast(eventID string, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if worker := h.workers[eventID]; worker != nil {
		h.broadcastLocked(worker, payload, context.Background())
	}
}

func (h *Hub) broadcastLocked(worker *eventWorker, payload []byte, ctx context.Context) {
	started := time.Now()
	defer func() {
		if h.options.Metrics != nil {
			h.options.Metrics.SSEFanoutDuration.Record(ctx, time.Since(started).Seconds())
		}
	}()
	for id, client := range worker.clients {
		select {
		case client.events <- append([]byte(nil), payload...):
			if h.options.Metrics != nil {
				h.options.Metrics.SSEFanoutDeliveries.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "delivered")))
			}
		default:
			delete(worker.clients, id)
			if h.options.Metrics != nil {
				h.options.Metrics.SSEFanoutDeliveries.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "slow_client_disconnect")))
			}
			client.closeFromWorker()
		}
	}
}

func (h *Hub) removeClient(client *Client) {
	h.mu.Lock()
	worker := client.worker
	if worker == nil || worker.clients[client.id] != client {
		h.mu.Unlock()
		return
	}
	delete(worker.clients, client.id)
	if len(worker.clients) == 0 {
		worker.cancel()
	}
	h.mu.Unlock()
	client.closeFromWorker()
}

func (h *Hub) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	workers := make([]*eventWorker, 0, len(h.workers))
	for _, worker := range h.workers {
		workers = append(workers, worker)
		worker.cancel()
	}
	h.mu.Unlock()
	for _, worker := range workers {
		select {
		case <-worker.done:
		case <-time.After(2 * time.Second):
			_ = fmt.Errorf("timed out waiting for event realtime worker %s", worker.eventID)
		}
	}
}

func (h *Hub) LocalSubscriberCount(eventID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if worker := h.workers[eventID]; worker != nil {
		return len(worker.clients)
	}
	return 0
}

func (h *Hub) clientForTest(eventID string) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	worker := h.workers[eventID]
	if worker == nil {
		return nil
	}
	for _, client := range worker.clients {
		return client
	}
	return nil
}

func (c *Client) Ready() <-chan struct{} { return c.ready }
func (c *Client) Events() <-chan []byte  { return c.events }
func (c *Client) Done() <-chan struct{}  { return c.done }
func (c *Client) Generation() uint64     { return c.generation }
func (c *Client) ReadyIsClosed() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}
func (c *Client) PublishForTest(payload []byte) {
	c.worker.hub.broadcastWorker(c.worker, payload)
}
func (c *Client) Close() {
	if c == nil {
		return
	}
	c.worker.hub.removeClient(c)
}
func (c *Client) closeFromWorker() {
	c.closeOnce.Do(func() {
		if c.worker.hub.options.Metrics != nil {
			c.worker.hub.options.Metrics.SSEActiveConnections.Add(context.Background(), -1)
			c.worker.hub.options.Metrics.SSEConnections.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", "closed")))
		}
		close(c.done)
		close(c.events)
	})
}

type redisSubscriptionSession struct {
	pubsub *redis.PubSub
}

func (s *redisSubscriptionSession) Receive(ctx context.Context) (interface{}, error) {
	return s.pubsub.Receive(ctx)
}

func (s *redisSubscriptionSession) ChannelWithSubscriptions() <-chan interface{} {
	return s.pubsub.ChannelWithSubscriptions(redis.WithChannelSize(128), redis.WithChannelHealthCheckInterval(3*time.Second))
}

func (s *redisSubscriptionSession) Close() error { return s.pubsub.Close() }
