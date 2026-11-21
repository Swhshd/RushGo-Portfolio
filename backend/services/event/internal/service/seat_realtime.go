package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/ticketbox/event/internal/domain"
)

const seatRealtimeChannelPrefix = "rushgo:seat-updates:"

type SeatUpdate struct {
	SeatID       string `json:"seat_id"`
	TicketTierID string `json:"ticket_tier_id"`
	Status       string `json:"status"`
	SeatRevision string `json:"seat_revision"`
	UpdatedAt    string `json:"updated_at"`
}

type SeatUpdateBatch struct {
	EventID     string       `json:"event_id"`
	MessageID   string       `json:"message_id"`
	PublishedAt string       `json:"published_at"`
	SeatUpdates []SeatUpdate `json:"seat_updates"`
	Traceparent string       `json:"traceparent,omitempty"`
	Tracestate  string       `json:"tracestate,omitempty"`
}

type SeatRealtimePublisher interface {
	Publish(ctx context.Context, batch SeatUpdateBatch) error
}

type noOpSeatRealtimePublisher struct{}

func (noOpSeatRealtimePublisher) Publish(context.Context, SeatUpdateBatch) error { return nil }

type RedisSeatRealtimePublisher struct {
	client *redis.Client
}

func NewRedisSeatRealtimePublisher(client *redis.Client) *RedisSeatRealtimePublisher {
	return &RedisSeatRealtimePublisher{client: client}
}

func (p *RedisSeatRealtimePublisher) Publish(ctx context.Context, batch SeatUpdateBatch) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("seat realtime redis publisher is not configured")
	}
	if len(batch.SeatUpdates) == 0 {
		return nil
	}
	carrier := otel.GetTextMapPropagator()
	traceHeaders := propagation.MapCarrier{}
	carrier.Inject(ctx, traceHeaders)
	batch.Traceparent = traceHeaders.Get("traceparent")
	batch.Tracestate = traceHeaders.Get("tracestate")
	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("encode seat realtime batch: %w", err)
	}
	channel := seatRealtimeChannelPrefix + batch.EventID
	if err := p.client.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("publish %s: %w", channel, err)
	}
	return nil
}

func buildSeatUpdateBatch(eventID uuid.UUID, seats []*domain.Seat, publishedAt time.Time) SeatUpdateBatch {
	batch := SeatUpdateBatch{
		EventID:     eventID.String(),
		MessageID:   uuid.NewString(),
		PublishedAt: publishedAt.UTC().Format(time.RFC3339Nano),
		SeatUpdates: make([]SeatUpdate, 0, len(seats)),
	}
	for _, seat := range seats {
		if seat == nil || seat.EventID != eventID {
			continue
		}
		batch.SeatUpdates = append(batch.SeatUpdates, SeatUpdate{
			SeatID:       seat.ID.String(),
			TicketTierID: seat.TicketTierID.String(),
			Status:       string(seat.Status),
			SeatRevision: strconv.FormatInt(seat.SeatRevision, 10),
			UpdatedAt:    seat.UpdatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return batch
}

func buildSeatUpdateBatchesByEvent(seats []*domain.Seat, publishedAt time.Time) map[uuid.UUID]SeatUpdateBatch {
	grouped := make(map[uuid.UUID][]*domain.Seat)
	for _, seat := range seats {
		if seat == nil {
			continue
		}
		grouped[seat.EventID] = append(grouped[seat.EventID], seat)
	}
	batches := make(map[uuid.UUID]SeatUpdateBatch, len(grouped))
	for eventID, eventSeats := range grouped {
		batches[eventID] = buildSeatUpdateBatch(eventID, eventSeats, publishedAt)
	}
	return batches
}
