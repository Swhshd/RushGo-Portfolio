package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
)

const (
	InboxDefaultPageSize       int32 = 20
	InboxMaxPageSize           int32 = 100
	InboxMaxPage               int32 = 1000
	DeliveryExceptionMaxLimit  int32 = 100
	DeliveryExceptionMaxOffset int32 = 10000
	DeliveryExceptionMaxRange        = 31 * 24 * time.Hour
)

var (
	ErrInboxUnavailable              = errors.New("Notification inbox unavailable")
	ErrDeliveryExceptionsUnavailable = errors.New("Notification delivery exceptions unavailable")
	ErrInvalidDeliveryExceptionQuery = errors.New("invalid Notification delivery exception query")
)

type DeliveryException = repository.DeliveryException

type DeliveryExceptionReader interface {
	ReadDeliveryExceptions(context.Context, time.Time, time.Time, int32, int32) ([]DeliveryException, bool, time.Time, error)
}

type NotificationInboxService struct {
	reader     repository.BusinessNotificationRepository
	exceptions DeliveryExceptionReader
}

func NewNotificationInboxService(repo repository.BusinessNotificationRepository) *NotificationInboxService {
	service := &NotificationInboxService{reader: repo}
	if reader, ok := any(repo).(DeliveryExceptionReader); ok {
		service.exceptions = reader
	}
	return service
}

func (s *NotificationInboxService) List(ctx context.Context, userID uuid.UUID, page, pageSize int32, unreadOnly bool) (repository.InboxPage, error) {
	if s == nil || s.reader == nil {
		return repository.InboxPage{}, ErrInboxUnavailable
	}
	if page == 0 {
		page = 1
	}
	if pageSize == 0 {
		pageSize = InboxDefaultPageSize
	}
	if page < 1 || page > InboxMaxPage || pageSize < 1 || pageSize > InboxMaxPageSize {
		return repository.InboxPage{}, repository.ErrInvalidInboxQuery
	}
	return s.reader.ListInbox(ctx, userID, page, pageSize, unreadOnly)
}

func (s *NotificationInboxService) MarkRead(ctx context.Context, userID, notificationID uuid.UUID) (time.Time, error) {
	if s == nil || s.reader == nil {
		return time.Time{}, ErrInboxUnavailable
	}
	return s.reader.MarkInboxRead(ctx, userID, notificationID)
}

func (s *NotificationInboxService) ListDeliveryExceptions(ctx context.Context, from, to time.Time, limit, offset int32) ([]DeliveryException, bool, time.Time, error) {
	if s == nil || s.exceptions == nil {
		return nil, false, time.Time{}, ErrDeliveryExceptionsUnavailable
	}
	now := time.Now().UTC()
	from, to = from.UTC(), to.UTC()
	if from.IsZero() || to.IsZero() || !from.Before(to) || to.After(now.Add(5*time.Minute)) || to.Sub(from) > DeliveryExceptionMaxRange || limit < 1 || limit > DeliveryExceptionMaxLimit || offset < 0 || offset > DeliveryExceptionMaxOffset {
		return nil, false, time.Time{}, ErrInvalidDeliveryExceptionQuery
	}
	items, more, observedAt, err := s.exceptions.ReadDeliveryExceptions(ctx, from, to, limit, offset)
	if err != nil {
		return nil, false, time.Time{}, ErrDeliveryExceptionsUnavailable
	}
	if observedAt.IsZero() {
		return nil, false, time.Time{}, ErrDeliveryExceptionsUnavailable
	}
	return items, more, observedAt, nil
}

func InboxDetailPath(bookingID uuid.UUID) string {
	if bookingID == uuid.Nil {
		return "/account"
	}
	return "/my-tickets/orders/" + bookingID.String()
}

// InboxRecordDetailPath only builds links from server-validated UUID fields.
// Event reminders have no booking and must never receive a zero-UUID order URL.
func InboxRecordDetailPath(item domain.BusinessInboxRecord) string {
	switch item.Type {
	case "event_onsale", "event_restock":
		if item.EventID == uuid.Nil {
			return "/events"
		}
		return "/events/" + item.EventID.String()
	default:
		return InboxDetailPath(item.BookingID)
	}
}

const InboxSource = "notification.business_intent"
