package service

import (
	"context"
	"encoding/json"
	"io"
	"mime/quotedprintable"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
)

func TestSMTPNotificationSenderDeliversEventReminderToLocalTLSCapture(t *testing.T) {
	capture := runSMTPTestServer(t, smtpTestAccept)
	sender := smtpFixtureSender(t, capture)
	consentID := uuid.New()
	eventID := uuid.New()
	envelope := &eventreminder.Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 3,
		Action: eventreminder.ActionTrigger, Type: eventreminder.TypeOnSale, UserID: uuid.New(), EventID: eventID,
		EventTitle: "Synthetic reminder event", EventDate: time.Now().UTC().Add(24 * time.Hour),
		EmailOptIn: true, EmailConsentVersion: eventreminder.ConsentVersion, EmailConsentID: &consentID,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	notification := &domain.Notification{
		ID: envelope.NotificationID, Type: envelope.Type, Recipient: "synthetic@rushgo.invalid", Locale: "en", Payload: payload,
	}
	if err := sender.Send(context.Background(), notification); err != nil {
		t.Fatalf("send event reminder through local SMTP TLS capture: %v", err)
	}
	select {
	case message := <-capture.message:
		parts := strings.SplitN(message, "\r\n\r\n", 2)
		if len(parts) != 2 {
			t.Fatalf("captured event reminder has no MIME body: %s", message)
		}
		body, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(parts[1])))
		if err != nil {
			t.Fatalf("decode captured event reminder body: %v", err)
		}
		if !strings.Contains(message, "Subject: Tickets are now on sale") ||
			!strings.Contains(string(body), "rushgo.example.test/events/"+eventID.String()) ||
			strings.Contains(string(body), "/my-tickets/orders/") {
			t.Fatalf("captured event reminder has an unsafe or incorrect link/template: %s", message)
		}
	case <-time.After(time.Second):
		t.Fatal("local TLS SMTP catcher did not receive event reminder")
	}
}
