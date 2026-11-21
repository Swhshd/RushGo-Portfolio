package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

type MessageHandler func(ctx context.Context, event Event) error

var ErrPermanent = errors.New("permanent kafka handler rejection")

type Consumer struct {
	tlsConfig      *tls.Config
	brokers        []string
	topic, groupID string
	logger         *zap.Logger
	handler        MessageHandler
	reliability    *reliabilityhook.Controller
	reliabilityErr error
	propagator     propagation.TextMapPropagator
	commit         func(*kafkago.Generation, map[string]map[int]int64) error
	observe        func(int32, int, int64, string)
	observation    *ConsumerObservation
}

var (
	consumerMetricsOnce sync.Once
	consumerProcesses   metric.Int64Counter
	consumerStalled     metric.Int64Gauge
)

func initConsumerMetrics() {
	consumerMetricsOnce.Do(func() {
		consumerProcesses, _ = otel.Meter("github.com/ticketbox/pkg/kafka").Int64Counter("rushgo.kafka.process", metric.WithUnit("{message}"))
		consumerStalled, _ = otel.Meter("github.com/ticketbox/pkg/kafka").Int64Gauge("rushgo.kafka.partition.stalled")
	})
}

func decodeEvent(data []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return Event{}, err
	}
	if event.Type == "" || len(event.Data) == 0 || string(event.Data) == "null" {
		return Event{}, ErrPermanent
	}
	return event, nil
}

func NewConsumer(brokers []string, topic string, groupID string, handler MessageHandler, logger *zap.Logger) *Consumer {
	return NewConsumerWithObservation(brokers, topic, groupID, handler, logger, nil)
}

// NewConsumerWithObservation is the production consumer with an optional
// local-only observation hook. Passing nil is identical to NewConsumer.
func NewConsumerWithObservation(brokers []string, topic string, groupID string, handler MessageHandler, logger *zap.Logger, observation *ConsumerObservation) *Consumer {
	reliability, reliabilityErr := reliabilityhook.FromEnv()
	tlsConfig, tlsErr := config.KafkaTLSFromEnv()
	if tlsErr != nil {
		reliabilityErr = tlsErr
	}
	if reliabilityErr == nil {
		topic, reliabilityErr = candidateKafkaTopic(
			topic, os.Getenv("RELIABILITY_KAFKA_TOPIC_SUFFIX"), reliability.Enabled())
	}
	return &Consumer{brokers: append([]string(nil), brokers...), topic: topic, groupID: groupID,
		tlsConfig: tlsConfig,
		logger:    logger, handler: handler, reliability: reliability,
		reliabilityErr: reliabilityErr, propagator: otel.GetTextMapPropagator(),
		observation: observation,
		commit: func(g *kafkago.Generation, o map[string]map[int]int64) error {
			return g.CommitOffsets(o)
		}}
}

func (c *Consumer) Start(ctx context.Context) error {
	initConsumerMetrics()
	if c.handler == nil || c.logger == nil {
		return fmt.Errorf("missing Kafka consumer dependency")
	}
	if c.reliabilityErr != nil {
		return fmt.Errorf("invalid reliability hook configuration")
	}
	for ctx.Err() == nil {
		if err := c.runGroup(ctx); err != nil && ctx.Err() == nil {
			c.logger.Warn("Kafka group rejoining", zap.String("topic", c.topic), zap.String("group", c.groupID))
		}
		if !consumerWait(ctx, 100*time.Millisecond) {
			break
		}
	}
	return nil
}

func consumerWait(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (c *Consumer) runGroup(ctx context.Context) error {
	sockets := &connectionScope{}
	stopSockets := context.AfterFunc(ctx, sockets.close)
	defer stopSockets()
	defer sockets.close()
	var observedDial func(context.Context, string, string) (net.Conn, error)
	if c.observation != nil {
		observedDial = c.observation.DialFunc
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		return sockets.dialObserved(ctx, network, address, observedDial)
	}
	group, err := kafkago.NewConsumerGroup(kafkago.ConsumerGroupConfig{ID: c.groupID, Brokers: c.brokers, Dialer: &kafkago.Dialer{DialFunc: dial, Timeout: time.Second, TLS: c.tlsConfig}, Topics: []string{c.topic}, StartOffset: kafkago.FirstOffset, Timeout: time.Second, RebalanceTimeout: 5 * time.Second, HeartbeatInterval: time.Second, JoinGroupBackoff: 100 * time.Millisecond})
	if err != nil {
		return err
	}
	retire := make(chan struct{}, 1)
	closerDone := make(chan struct{})
	// Close outside Generation.Start routines: Close waits for those routines.
	go func() {
		defer close(closerDone)
		select {
		case <-ctx.Done():
		case <-retire:
		}
		sockets.close()
		_ = group.Close()
	}()
	defer func() {
		select {
		case retire <- struct{}{}:
		default:
		}
		<-closerDone
	}()
	for {
		generation, err := group.Next(ctx)
		if err != nil {
			return err
		}
		commitGate := make(chan struct{}, 1)
		for topic, assignments := range generation.Assignments {
			for _, assignment := range assignments {
				generation.Start(func(genCtx context.Context) {
					workCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					stop := context.AfterFunc(genCtx, cancel)
					defer stop()
					c.partitionLoop(workCtx, generation, topic, assignment, commitGate, retire, sockets.close)
				})
			}
		}
	}
}

func (c *Consumer) record(ctx context.Context, g *kafkago.Generation, msg kafkago.Message, result string) {
	consumerProcesses.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", c.topic), attribute.String("result", result)))
	if c.observe != nil {
		c.observe(g.ID, msg.Partition, msg.Offset, result)
	}
}

func (c *Consumer) partitionLoop(ctx context.Context, g *kafkago.Generation, topic string, a kafkago.PartitionAssignment, gate chan struct{}, retire chan struct{}, abort func()) {
	stalledAttrs := metric.WithAttributes(attribute.String("topic", topic), attribute.String("group", c.groupID), attribute.Int("partition", a.ID))
	if consumerStalled != nil {
		consumerStalled.Record(ctx, 0, stalledAttrs)
		defer consumerStalled.Record(context.Background(), 0, stalledAttrs)
	}
	sockets := &connectionScope{}
	stopSockets := context.AfterFunc(ctx, sockets.close)
	defer stopSockets()
	defer sockets.close()
	var observedDial func(context.Context, string, string) (net.Conn, error)
	if c.observation != nil {
		observedDial = c.observation.DialFunc
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		return sockets.dialObserved(ctx, network, address, observedDial)
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: c.brokers, Topic: topic, Partition: a.ID, Dialer: &kafkago.Dialer{DialFunc: dial, Timeout: time.Second, TLS: c.tlsConfig}, MinBytes: 1, MaxBytes: 10e6, QueueCapacity: 1, MaxWait: time.Second, ReadBatchTimeout: time.Second})
	defer func() { sockets.close(); _ = r.Close() }()
	if err := r.SetOffset(a.Offset); err != nil {
		return
	}
	for ctx.Err() == nil {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return
		}
		messageCtx := extractTraceContext(ctx, msg.Headers, c.propagator)
		messageCtx, span := otel.Tracer("github.com/ticketbox/pkg/kafka").Start(messageCtx, "kafka.process", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic)))
		if c.observe != nil {
			c.observe(g.ID, msg.Partition, msg.Offset, "received")
		}
		event, decodeErr := decodeEvent(msg.Value)
		result := "success"
		if decodeErr != nil {
			err = ErrPermanent
			result = "decode_failed"
		} else {
			retryBoundaryPending := false
			for attempt := 0; attempt < 5; attempt++ {
				if ctx.Err() != nil {
					span.End()
					return
				}
				injectedFailure, hookErr := c.awaitT23KafkaBoundary(
					messageCtx, g.ID, msg, event)
				if hookErr != nil {
					if ctx.Err() != nil {
						if gateErr := c.reportKafkaRevocationProofGap(g, msg, event); gateErr != nil {
							c.logger.Warn("Kafka rebalance recovery is blocked without broker revocation proof",
								zap.String("human_gate", "BROKER_REVOCATION_PROOF_MISSING"),
								zap.String("topic", c.topic), zap.String("group", c.groupID),
								zap.Int32("generation", g.ID), zap.Int("partition", msg.Partition),
								zap.Int64("offset", msg.Offset), zap.Error(gateErr))
						}
						span.End()
						return
					}
					err = hookErr
				} else if injectedFailure {
					err = errT23KafkaInjectedTransient
					retryBoundaryPending = true
					c.record(ctx, g, msg, "reliability_injected_failure")
				} else {
					if retryBoundaryPending {
						_, hookErr = c.awaitT23KafkaRetryBoundary(
							messageCtx, g.ID, msg, event)
						if hookErr != nil {
							if ctx.Err() != nil {
								span.End()
								return
							}
							err = hookErr
						} else {
							retryBoundaryPending = false
							err = c.handler(messageCtx, event)
						}
					} else {
						_, hookErr = c.awaitT23KafkaRebalanceBoundary(
							messageCtx, g.ID, msg, event)
						if hookErr != nil {
							if ctx.Err() != nil {
								span.End()
								return
							}
							err = hookErr
						} else {
							err = c.handler(messageCtx, event)
						}
					}
				}
				if err == nil || errors.Is(err, ErrPermanent) {
					break
				}
				if attempt < 4 && !consumerWait(ctx, min(2*time.Second, 100*time.Millisecond*time.Duration(1<<attempt))) {
					span.End()
					return
				}
			}
			if err != nil {
				result = "failed"
			}
		}
		if err != nil {
			span.SetStatus(codes.Error, "partition stalled")
			span.End()
			c.record(ctx, g, msg, result)
			c.logger.Warn("Kafka partition stalled", zap.String("topic", topic), zap.String("group", c.groupID), zap.Int32("generation", g.ID), zap.Int("partition", a.ID), zap.Int64("offset", msg.Offset))
			if consumerStalled != nil {
				consumerStalled.Record(ctx, 1, stalledAttrs)
			}
			<-ctx.Done()
			return
		}
		span.End()
		if ctx.Err() != nil {
			return
		}
		if err = beforeOffsetCommit(ctx, event, c.topic, msg.Partition, msg.Offset); err != nil {
			c.record(ctx, g, msg, "handler_committed_offset_deferred")
			c.logger.Warn("Kafka offset intentionally left uncommitted after handler completion", zap.String("topic", topic), zap.String("group", c.groupID), zap.Int32("generation", g.ID), zap.Int("partition", a.ID), zap.Int64("offset", msg.Offset))
			return
		}
		if err = c.commitNext(ctx, g, topic, a.ID, msg.Offset+1, gate, abort); err != nil {
			c.record(ctx, g, msg, "commit_unknown")
			c.logger.Warn("Kafka offset commit unresolved", zap.String("topic", topic), zap.String("group", c.groupID), zap.Int32("generation", g.ID), zap.Int("partition", a.ID), zap.Int64("offset", msg.Offset))
			select {
			case retire <- struct{}{}:
			default:
			}
			return
		}
		c.record(ctx, g, msg, "success")
	}
}

var errT23KafkaInjectedTransient = errors.New("T23 injected Kafka handler failure")

func (c *Consumer) awaitT23KafkaBoundary(ctx context.Context, generation int32,
	message kafkago.Message, event Event) (bool, error) {
	if c.reliability == nil || !c.reliability.Enabled() {
		return false, nil
	}
	decision, matched, err := c.reliability.AwaitKafkaRecord(
		ctx, c.groupID, message.Topic, generation, message.Partition,
		message.Offset, event.ID, event.Type)
	if err != nil {
		return false, err
	}
	if matched && decision == reliabilityhook.DecisionFailOnce {
		if err := c.reliability.RecordKafkaInjectedFailure(
			reliabilityhook.KafkaInjectedFailure{
				WindowID: "kafka.partition.A_failure_offset_frontier",
				GroupID:  c.groupID, Generation: generation, Topic: message.Topic,
				Partition: message.Partition, Offset: message.Offset,
				EventID: event.ID, EventType: event.Type,
			}); err != nil {
			return false, errors.New("Kafka injected failure evidence could not be recorded")
		}
		return true, nil
	}
	return false, nil
}

func (c *Consumer) awaitT23KafkaRetryBoundary(ctx context.Context, generation int32,
	message kafkago.Message, event Event) (bool, error) {
	if c.reliability == nil || !c.reliability.Enabled() {
		return false, nil
	}
	decision, matched, err := c.reliability.AwaitKafkaRecordAtCheckpoint(
		ctx, c.groupID, message.Topic, generation, message.Partition,
		message.Offset, event.ID, event.Type, "kafka_retry_before_handler")
	if err != nil {
		return false, err
	}
	if matched && decision != reliabilityhook.DecisionContinue {
		return false, errors.New("Kafka retry boundary received an invalid decision")
	}
	return matched, nil
}

func (c *Consumer) awaitT23KafkaRebalanceBoundary(ctx context.Context, generation int32,
	message kafkago.Message, event Event) (bool, error) {
	if c.reliability == nil || !c.reliability.Enabled() {
		return false, nil
	}
	decision, matched, err := c.reliability.AwaitKafkaRecordAtCheckpoint(
		ctx, c.groupID, message.Topic, generation, message.Partition,
		message.Offset, event.ID, event.Type,
		"kafka_rebalance_new_generation_before_handler")
	if err != nil {
		return matched, err
	}
	if matched && decision != reliabilityhook.DecisionContinue {
		return true, errors.New("Kafka rebalance boundary received an invalid decision")
	}
	return matched, nil
}

func (c *Consumer) reportKafkaRevocationProofGap(g *kafkago.Generation,
	message kafkago.Message, event Event) error {
	if c.reliability == nil || !c.reliability.KafkaRebalanceBoundaryClaimed(
		c.groupID, message.Topic, g.ID, message.Partition,
		message.Offset, event.ID, event.Type) {
		return nil
	}
	return reliabilityhook.ErrKafkaBrokerRevocationProofMissing
}

func (c *Consumer) commitNext(ctx context.Context, g *kafkago.Generation, topic string, partition int, next int64, gate chan struct{}, abort func()) error {
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case gate <- struct{}{}:
	case <-budget.Done():
		return budget.Err()
	}
	defer func() { <-gate }()
	// Generation.CommitOffsets has no context. Closing the owned coordinator
	// sockets interrupts the call even when heartbeat code resets its deadline.
	stop := context.AfterFunc(budget, abort)
	defer stop()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if budget.Err() != nil {
			return budget.Err()
		}
		if c.observation != nil && c.observation.BeforeCommit != nil {
			if err = c.observation.BeforeCommit(budget, g.ID, topic, partition, next); err != nil {
				return err
			}
		}
		err = c.commit(g, map[string]map[int]int64{topic: {partition: next}})
		if c.observation != nil && c.observation.AfterCommit != nil {
			c.observation.AfterCommit(g.ID, topic, partition, next, err)
		}
		if budget.Err() != nil {
			return budget.Err()
		}
		if err == nil {
			return nil
		}
		if !consumerWait(budget, 100*time.Millisecond*time.Duration(1<<attempt)) {
			return budget.Err()
		}
	}
	return err
}
