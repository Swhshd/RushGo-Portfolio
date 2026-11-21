package domain

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/eventreminder"
)

func ParseEventReminder(raw []byte) (*eventreminder.Envelope, [32]byte, error) {
	var zero [32]byte
	envelope, err := eventreminder.Parse(raw)
	if err != nil {
		return nil, zero, ErrInvalidEventReminder
	}
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return nil, zero, ErrInvalidEventReminder
	}
	return envelope, sha256.Sum256(canonical), nil
}

func RenderEventReminder(envelope *eventreminder.Envelope, requestedLocale string) (InboxTemplate, error) {
	if envelope == nil || envelope.Action != eventreminder.ActionTrigger {
		return InboxTemplate{}, ErrInvalidEventReminder
	}
	locale := NormalizeLocale(requestedLocale)
	date := envelope.EventDate.UTC().Format(time.RFC1123)
	if locale == "zh" {
		switch envelope.Type {
		case eventreminder.TypeOnSale:
			return InboxTemplate{Locale: locale, Subject: "您关注的活动现已开售", Title: "门票现已开售", Body: fmt.Sprintf("%s 现已开放购票（活动时间：%s）。", envelope.EventTitle, date)}, nil
		case eventreminder.TypeRestock:
			return InboxTemplate{Locale: locale, Subject: "您关注的活动已有余票", Title: "门票库存已恢复", Body: fmt.Sprintf("%s 有可用门票了（活动时间：%s）。", envelope.EventTitle, date)}, nil
		default:
			return InboxTemplate{}, ErrInvalidEventReminder
		}
	}
	switch envelope.Type {
	case eventreminder.TypeOnSale:
		return InboxTemplate{Locale: locale, Subject: "Tickets are now on sale", Title: "Tickets are on sale", Body: fmt.Sprintf("Tickets for %s are now available (event time: %s).", envelope.EventTitle, date)}, nil
	case eventreminder.TypeRestock:
		return InboxTemplate{Locale: locale, Subject: "Tickets are available again", Title: "Tickets are available again", Body: fmt.Sprintf("Tickets for %s are available again (event time: %s).", envelope.EventTitle, date)}, nil
	default:
		return InboxTemplate{}, ErrInvalidEventReminder
	}
}

func NewEventReminderInboxRecord(envelope *eventreminder.Envelope, rendered InboxTemplate, now time.Time) (BusinessInboxRecord, error) {
	if envelope == nil || envelope.Action != eventreminder.ActionTrigger || envelope.NotificationID == uuid.Nil || envelope.SubscriptionID == uuid.Nil ||
		envelope.UserID == uuid.Nil || envelope.EventID == uuid.Nil || envelope.SubscriptionVersion < 1 || rendered.Locale != "en" && rendered.Locale != "zh" || rendered.Title == "" || rendered.Body == "" {
		return BusinessInboxRecord{}, ErrInvalidEventReminder
	}
	return BusinessInboxRecord{ID: envelope.NotificationID, EventID: envelope.EventID, UserID: envelope.UserID, Type: envelope.Type,
		Locale: rendered.Locale, Title: rendered.Title, Body: rendered.Body, CreatedAt: now, SubscriptionID: envelope.SubscriptionID,
		SubscriptionVersion: envelope.SubscriptionVersion}, nil
}

var ErrInvalidEventReminder = errors.New("invalid event reminder")
