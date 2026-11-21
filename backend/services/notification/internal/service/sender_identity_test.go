package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogSenderEmitsNotificationIdentityOnlyWhenEnabled(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	sender := NewLogSender(zap.New(core))
	notificationID := uuid.New()
	notification := &domain.Notification{
		ID: notificationID, Type: "booking_cancellation", Channel: "email",
		Recipient: "private-recipient@example.com", Payload: map[string]interface{}{"private": "private-payload"},
	}
	if err := sender.Send(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
	assertNotificationLogFields(t, logs.All()[0].ContextMap(), false, notificationID.String())

	sender.SetIncludeNotificationIdentity(true)
	if err := sender.Send(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
	assertNotificationLogFields(t, logs.All()[1].ContextMap(), true, notificationID.String())
}

func assertNotificationLogFields(t *testing.T, fields map[string]interface{}, includeIdentity bool, notificationID string) {
	t.Helper()
	if fields["type"] != "booking_cancellation" || fields["channel"] != "email" {
		t.Fatalf("sender log lost the stable adapter fields: %v", fields)
	}
	if fields["adapter"] != "log" {
		t.Fatalf("sender log adapter = %v, want log", fields["adapter"])
	}
	if got, exists := fields["notification_id"]; exists != includeIdentity || includeIdentity && got != notificationID {
		t.Fatalf("notification identity emission mismatch: %v", fields)
	}
	for _, forbidden := range []string{"recipient", "payload", "private-recipient@example.com", "private-payload"} {
		if _, exists := fields[forbidden]; exists {
			t.Fatalf("private Notification data escaped into sender logs: %v", fields)
		}
	}
}
