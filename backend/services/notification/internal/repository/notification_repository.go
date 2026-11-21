package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
)

type NotificationRepository interface {
	AcceptIntent(context.Context, domain.SemanticKey, *domain.Notification, [32]byte) (*domain.Notification, bool, error)
	Create(ctx context.Context, notification *domain.Notification) error
	ProcessOnce(ctx context.Context, notification *domain.Notification, send func(context.Context, *domain.Notification) error) (bool, error)
	MarkSent(ctx context.Context, id uuid.UUID) error
}

type DeliveryRepository interface {
	AcceptIntent(context.Context, domain.SemanticKey, *domain.Notification, [32]byte) (*domain.Notification, bool, error)
	ClaimDeliveries(context.Context, int, time.Duration) ([]domain.DeliveryClaim, error)
	FinishDelivery(context.Context, domain.DeliveryClaim, domain.DeliveryResult) (bool, error)
}

type EventReminderDeliveryGate interface {
	EventReminderEmailEligible(context.Context, *domain.Notification) (bool, error)
}

var ErrSemanticConflict = errors.New("notification immutable payload conflict")
var ErrLegacyAttention = errors.New("legacy notification requires evidence classification")
var ErrInboxNotFound = errors.New("notification inbox item not found")
var ErrInvalidInboxQuery = errors.New("invalid notification inbox query")

type InboxPage struct {
	Items       []domain.BusinessInboxRecord
	Total       int64
	UnreadCount int64
}

type DeliveryException struct {
	BookingID uuid.UUID
	EventID   uuid.UUID
	Type      string
	State     string
	CreatedAt time.Time
}

type BusinessNotificationRepository interface {
	AcceptBusinessNotification(context.Context, *domain.BusinessNotificationV1, [32]byte, domain.InboxTemplate, string, bool) (bool, error)
	ListInbox(context.Context, uuid.UUID, int32, int32, bool) (InboxPage, error)
	MarkInboxRead(context.Context, uuid.UUID, uuid.UUID) (time.Time, error)
}

type EventReminderRepository interface {
	AcceptEventReminder(context.Context, *eventreminder.Envelope, [32]byte, domain.InboxTemplate, string, bool) (bool, error)
}
