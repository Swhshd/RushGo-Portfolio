package main

import (
	"testing"

	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
)

func TestNotificationSenderUsesSMTPWithoutEnabledRecoveryHooks(t *testing.T) {
	sender, adapter, err := notificationSender(testNotificationConfig(), zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("create production sender: %v", err)
	}
	if adapter != "smtp" {
		t.Fatalf("production adapter = %q, want smtp", adapter)
	}
	if _, ok := sender.(*service.SMTPNotificationSender); !ok {
		t.Fatalf("sender type = %T, want SMTPNotificationSender", sender)
	}
}

func TestNotificationSenderUsesLogSenderOnlyWithEnabledRecoveryHooks(t *testing.T) {
	hook, err := reliabilityhook.New(reliabilityhook.Config{
		Enabled: true, Token: "notification-recovery-test-token",
		Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create enabled recovery hook: %v", err)
	}
	sender, adapter, err := notificationSender(testNotificationConfig(), zap.NewNop(), hook)
	if err != nil {
		t.Fatalf("create recovery sender: %v", err)
	}
	if adapter != "log" {
		t.Fatalf("recovery adapter = %q, want log", adapter)
	}
	if _, ok := sender.(*service.LogSender); !ok {
		t.Fatalf("sender type = %T, want LogSender", sender)
	}
}

func testNotificationConfig() *config.Config {
	return &config.Config{
		SMTPHost: "smtp.fixture.test", SMTPPort: 465, SMTPTLSMode: "implicit",
		SMTPUsername: "fixture-user", SMTPPassword: "fixture-password",
		SMTPFrom: "sender@example.test", AuthFrontendOrigin: "https://rushgo.example.test",
	}
}
