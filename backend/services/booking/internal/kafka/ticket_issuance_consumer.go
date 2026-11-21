package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/grpcsecurity"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type ticketIssuanceRequestedV1 struct {
	Version          int       `json:"version"`
	TicketIssuanceID uuid.UUID `json:"ticket_issuance_id"`
	SagaID           uuid.UUID `json:"saga_id"`
	BookingID        uuid.UUID `json:"booking_id"`
	PaymentID        uuid.UUID `json:"payment_id"`
	EventID          uuid.UUID `json:"event_id"`
	Generation       int64     `json:"generation"`
}

type TicketIssuanceProcessor interface {
	Process(context.Context, domain.TicketIssuanceEvent) error
}

func NewTicketIssuanceConsumer(brokers []string, groupID string, processor TicketIssuanceProcessor, logger *zap.Logger) *pkgkafka.Consumer {
	namespace, valid := grpcsecurity.TicketE2EKafkaIdentity()
	topic, group := ticketIssuanceKafkaIdentity(topics.TopicTicketIssuanceEvents, groupID, namespace, valid)
	return pkgkafka.NewConsumer(brokers, topic, group, TicketIssuanceHandler(processor, logger), logger)
}

func ticketIssuanceKafkaIdentity(topic, group, namespace string, valid bool) (string, string) {
	if !valid || namespace == "" {
		return topic, group
	}
	return topic + "." + namespace, group + "-" + namespace
}

func TicketIssuanceHandler(processor TicketIssuanceProcessor, loggers ...*zap.Logger) pkgkafka.MessageHandler {
	return func(ctx context.Context, event pkgkafka.Event) error {
		if event.Type != topics.TypeTicketIssuanceRequestedV1 || processor == nil {
			return pkgkafka.ErrPermanent
		}
		var payload ticketIssuanceRequestedV1
		if len(event.Data) == 0 || json.Unmarshal(event.Data, &payload) != nil || payload.Version != 1 || payload.TicketIssuanceID == uuid.Nil {
			return pkgkafka.ErrPermanent
		}
		outerID, err := uuid.Parse(event.ID)
		if err != nil || outerID != payload.TicketIssuanceID {
			return pkgkafka.ErrPermanent
		}
		err = processor.Process(ctx, domain.TicketIssuanceEvent{ID: payload.TicketIssuanceID, SagaID: payload.SagaID, BookingID: payload.BookingID,
			PaymentID: payload.PaymentID, EventID: payload.EventID, Generation: payload.Generation})
		if err == nil && len(loggers) > 0 && loggers[0] != nil {
			loggers[0].Info("Ticket issuance processing completed", zap.String("issuance_id", payload.TicketIssuanceID.String()), zap.String("booking_id", payload.BookingID.String()), zap.String("source_commit", issuanceRevision))
		}
		if err != nil && len(loggers) > 0 && loggers[0] != nil {
			logIssuanceFailure(loggers[0], payload, err)
		}
		if errors.Is(err, domain.ErrInvalidTicketIssuanceEvent) || errors.Is(err, repository.ErrTicketBookingMissing) || errors.Is(err, repository.ErrTicketIdentityConflict) {
			return pkgkafka.ErrPermanent
		}
		return err
	}
}
