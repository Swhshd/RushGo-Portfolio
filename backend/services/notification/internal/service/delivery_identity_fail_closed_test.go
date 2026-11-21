package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/reliabilityhook"
)

func TestEnabledNotificationHookRejectsMissingDeliveryIdentity(t *testing.T) {
	hooks, err := reliabilityhook.New(reliabilityhook.Config{
		Enabled: true, Token: "private-notification-identity-token", Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := &DeliveryWorker{hooks: hooks}
	valid := domain.DeliveryClaim{
		Notification: &domain.Notification{ID: uuid.New(), Type: "booking_confirmation", Status: "PENDING",
			Payload: map[string]interface{}{"booking_id": uuid.NewString()}},
		Token: uuid.New(), Fence: 2, Attempts: 1, LeaseUntil: time.Now().Add(time.Minute).UTC(),
	}
	cases := []struct {
		name   string
		mutate func(*domain.DeliveryClaim)
	}{
		{name: "missing notification", mutate: func(c *domain.DeliveryClaim) { c.Notification = nil }},
		{name: "zero notification id", mutate: func(c *domain.DeliveryClaim) { c.Notification.ID = uuid.Nil }},
		{name: "missing entity identity", mutate: func(c *domain.DeliveryClaim) { c.Notification.Payload = nil }},
		{name: "zero claim token", mutate: func(c *domain.DeliveryClaim) { c.Token = uuid.Nil }},
		{name: "zero claim fence", mutate: func(c *domain.DeliveryClaim) { c.Fence = 0 }},
		{name: "zero attempt", mutate: func(c *domain.DeliveryClaim) { c.Attempts = 0 }},
		{name: "zero lease", mutate: func(c *domain.DeliveryClaim) { c.LeaseUntil = time.Time{} }},
		{name: "expired active pre-effect lease", mutate: func(c *domain.DeliveryClaim) { c.LeaseUntil = time.Now().Add(-time.Minute).UTC() }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			claim := valid
			claim.Notification = cloneNotificationForIdentityTest(valid.Notification)
			test.mutate(&claim)
			if _, _, err := worker.awaitDelivery(context.Background(), "notification.intent_before_sender",
				"notification_before_sender", claim, ""); err == nil {
				t.Fatal("enabled Notification hook silently accepted an incomplete delivery identity")
			}
		})
	}
}

func TestDisabledNotificationHookLeavesMissingIdentityInert(t *testing.T) {
	disabled, err := reliabilityhook.New(reliabilityhook.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for name, worker := range map[string]*DeliveryWorker{
		"nil":      &DeliveryWorker{},
		"disabled": &DeliveryWorker{hooks: disabled},
	} {
		t.Run(name, func(t *testing.T) {
			_, matched, err := worker.awaitDelivery(context.Background(), "notification.intent_before_sender",
				"notification_before_sender", domain.DeliveryClaim{}, "")
			if err != nil || matched {
				t.Fatalf("disabled Notification hook changed default behavior: matched=%v err=%v", matched, err)
			}
		})
	}
}

func cloneNotificationForIdentityTest(n *domain.Notification) *domain.Notification {
	copy := *n
	copy.Payload = map[string]interface{}{}
	for key, value := range n.Payload {
		copy.Payload[key] = value
	}
	return &copy
}
