package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type Event struct {
	ID        string          `json:"id,omitempty"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type Producer struct {
	writers   map[string]*kafkago.Writer
	logger    *zap.Logger
	configErr error
}

var (
	producerMetricsOnce sync.Once
	producerPublishes   metric.Int64Counter
)

func initProducerMetrics() {
	producerMetricsOnce.Do(func() {
		producerPublishes, _ = otel.Meter("github.com/ticketbox/pkg/kafka").Int64Counter("rushgo.kafka.publish", metric.WithUnit("{message}"))
	})
}

func NewProducer(brokers []string, topics []string, logger *zap.Logger) *Producer {
	return newProducer(brokers, topics, kafkago.RequireOne, logger)
}

// NewProducerWithAllAcks waits for every in-sync replica to acknowledge writes.
func NewProducerWithAllAcks(brokers []string, topics []string, logger *zap.Logger) *Producer {
	return newProducer(brokers, topics, kafkago.RequireAll, logger)
}

func newProducer(brokers []string, topics []string, requiredAcks kafkago.RequiredAcks, logger *zap.Logger) *Producer {
	producer := &Producer{writers: make(map[string]*kafkago.Writer), logger: logger}
	tlsConfig, tlsErr := config.KafkaTLSFromEnv()
	if tlsErr != nil {
		producer.configErr = tlsErr
		return producer
	}
	reliability, err := reliabilityhook.FromEnv()
	if err != nil {
		producer.configErr = errors.New("invalid reliability hook configuration")
		return producer
	}
	for _, topic := range topics {
		candidateTopic, err := candidateKafkaTopic(
			topic, os.Getenv("RELIABILITY_KAFKA_TOPIC_SUFFIX"), reliability.Enabled())
		if err != nil {
			producer.configErr = errors.New("invalid candidate Kafka topic configuration")
			return producer
		}
		producer.writers[topic] = &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        candidateTopic,
			Balancer:     &kafkago.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: requiredAcks,
		}
		if tlsConfig != nil {
			producer.writers[topic].Transport = &kafkago.Transport{TLS: tlsConfig}
		}
	}
	return producer
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, event Event) error {
	if p.configErr != nil {
		return errors.New("candidate Kafka producer configuration is invalid")
	}
	initProducerMetrics()
	ctx, span := otel.Tracer("github.com/ticketbox/pkg/kafka").Start(ctx, "kafka.publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic))
	writer, ok := p.writers[topic]
	if !ok {
		producerPublishes.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic), attribute.String("result", "failed")))
		span.SetStatus(codes.Error, "unknown topic")
		p.logger.Error("Unknown topic", zap.String("topic", topic))
		return fmt.Errorf("unknown topic: %s", topic)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(key),
		Value: data,
	}
	msg.Headers = injectTraceContext(ctx, msg.Headers, otel.GetTextMapPropagator())

	if err := writer.WriteMessages(ctx, msg); err != nil {
		producerPublishes.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic), attribute.String("result", "failed")))
		span.SetStatus(codes.Error, "publish failed")
		p.logger.Error("Failed to publish", zap.String("topic", topic), zap.Error(err))
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	producerPublishes.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic), attribute.String("result", "success")))

	p.logger.Debug("Published event", zap.String("topic", topic), zap.String("type", event.Type))
	return nil
}

type kafkaHeaderCarrier struct {
	headers []kafkago.Header
}

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, header := range c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, value string) {
	for i := range c.headers {
		if c.headers[i].Key == key {
			c.headers[i].Value = []byte(value)
			return
		}
	}
	c.headers = append(c.headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for _, header := range c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func kafkaHeaders(headers []kafkago.Header) propagation.TextMapCarrier {
	return &kafkaHeaderCarrier{headers: headers}
}

func injectTraceContext(ctx context.Context, headers []kafkago.Header, propagator propagation.TextMapPropagator) []kafkago.Header {
	carrier := &kafkaHeaderCarrier{headers: append([]kafkago.Header(nil), headers...)}
	propagator.Inject(ctx, carrier)
	return carrier.headers
}

func extractTraceContext(ctx context.Context, headers []kafkago.Header, propagator propagation.TextMapPropagator) context.Context {
	return propagator.Extract(ctx, kafkaHeaders(headers))
}

func (p *Producer) Close() error {
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			p.logger.Error("Failed to close writer", zap.Error(err))
		}
		if transport, ok := w.Transport.(*kafkago.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	return nil
}
