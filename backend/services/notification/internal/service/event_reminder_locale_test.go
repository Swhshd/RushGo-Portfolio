package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
	"go.uber.org/zap"
)

type reminderLocaleRepository struct {
	*duplicateNotificationRepo
	rendered  domain.InboxTemplate
	recipient string
	verified  bool
}

func (r *reminderLocaleRepository) AcceptEventReminder(_ context.Context, _ *eventreminder.Envelope, _ [32]byte, rendered domain.InboxTemplate, recipient string, verified bool) (bool, error) {
	r.rendered, r.recipient, r.verified = rendered, recipient, verified
	return true, nil
}

type reminderLocaleLookup struct{ calls int }

func (l *reminderLocaleLookup) LookupNotificationRecipient(_ context.Context, id uuid.UUID) (NotificationRecipient, error) {
	l.calls++
	return NotificationRecipient{UserID: id, Locale: "zh-CN", Email: "not-consented@rushgo.invalid", EmailVerified: true}, nil
}

func TestInAppOnlyEventReminderUsesRecipientLocaleWithoutEmailConsent(t *testing.T) {
	repo := &reminderLocaleRepository{duplicateNotificationRepo: newDuplicateNotificationRepo()}
	lookup := &reminderLocaleLookup{}
	svc := NewNotificationService(repo, nil, zap.NewNop())
	svc.SetRecipientLookup(lookup)
	event := eventreminder.Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 1,
		ActiveSinceVersion: 1, Action: eventreminder.ActionTrigger, Type: eventreminder.TypeOnSale,
		UserID: uuid.New(), EventID: uuid.New(), EventTitle: "隔离测试活动", EventDate: time.Now().Add(time.Hour),
		EmailOptIn: false,
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.SendEventReminderV1(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if lookup.calls != 1 || repo.rendered.Locale != "zh" || repo.rendered.Title == "" {
		t.Fatalf("in-app reminder did not resolve Chinese preference: calls=%d rendered=%+v", lookup.calls, repo.rendered)
	}
	if repo.recipient != "" || repo.verified {
		t.Fatal("in-app-only reminder acquired email delivery eligibility")
	}
}
