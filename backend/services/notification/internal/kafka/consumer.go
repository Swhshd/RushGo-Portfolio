package kafka

import (
	"context"
	"errors"

	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/eventreminder"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/topics"
	"go.uber.org/zap"
)

type NotificationConsumer struct {
	bookingConsumer *pkgkafka.Consumer
	userConsumer    *pkgkafka.Consumer
	eventConsumer   *pkgkafka.Consumer
	service         *service.NotificationService
	logger          *zap.Logger
}

func NewNotificationConsumer(brokers []string, svc *service.NotificationService, logger *zap.Logger) *NotificationConsumer {
	nc := &NotificationConsumer{service: svc, logger: logger}

	nc.bookingConsumer = pkgkafka.NewConsumer(
		brokers, topics.TopicBookingEvents, "notification-booking-group",
		nc.handleBookingEvent, logger,
	)
	nc.userConsumer = pkgkafka.NewConsumer(
		brokers, topics.TopicUserEvents, "notification-user-group",
		nc.handleUserEvent, logger,
	)
	nc.eventConsumer = pkgkafka.NewConsumer(
		brokers, topics.TopicEventEvents, "notification-event-reminder-group",
		nc.handleEventEvent, logger,
	)

	return nc
}

func (c *NotificationConsumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 3)
	go func() { errCh <- c.bookingConsumer.Start(ctx) }()
	go func() { errCh <- c.userConsumer.Start(ctx) }()
	go func() { errCh <- c.eventConsumer.Start(ctx) }()
	err := <-errCh
	cancel()
	<-errCh
	<-errCh
	return err
}

func (c *NotificationConsumer) handleEventEvent(ctx context.Context, event pkgkafka.Event) error {
	if event.Type != eventreminder.EnvelopeType || len(event.Data) == 0 {
		return pkgkafka.ErrPermanent
	}
	reminder, err := eventreminder.Parse(event.Data)
	if err != nil || event.ID == "" || event.ID != reminder.NotificationID.String() {
		return errors.Join(pkgkafka.ErrPermanent, eventreminder.ErrInvalidEnvelope)
	}
	return c.service.SendEventReminderV1(ctx, event.Data)
}

func (c *NotificationConsumer) handleBookingEvent(ctx context.Context, event pkgkafka.Event) error {
	if err := pkgkafka.RequireEntityPayload(event.Data, "booking_id"); err != nil {
		return err
	}
	switch event.Type {
	case topics.TypeBookingConfirmed:
		return c.service.SendBookingConfirmation(ctx, event.Data)
	case topics.TypeBookingFailed:
		return c.service.SendBookingFailure(ctx, event.Data)
	case topics.TypeBookingCancelled:
		return c.service.SendBookingCancellation(ctx, event.Data)
	case "BusinessNotificationV1":
		business, _, err := domain.ParseBusinessNotificationV1(event.Data)
		if err != nil {
			return errors.Join(pkgkafka.ErrPermanent, err)
		}
		if event.ID == "" || event.ID != business.NotificationID.String() {
			return errors.Join(pkgkafka.ErrPermanent, domain.ErrInvalidBusinessNotification)
		}
		return c.service.SendBusinessNotificationV1(ctx, event.Data)
	default:
		return pkgkafka.ErrPermanent
	}
}

func (c *NotificationConsumer) handleUserEvent(ctx context.Context, event pkgkafka.Event) error {
	if err := pkgkafka.RequireEntityPayload(event.Data, "user_id"); err != nil {
		return err
	}
	switch event.Type {
	case topics.TypeUserRegistered:
		return c.service.SendWelcomeEmail(ctx, event.Data)
	default:
		return pkgkafka.ErrPermanent
	}
}
