package service

import (
	"bytes"
	"context"
	"github.com/ticketbox/user/internal/domain"
	"go.opentelemetry.io/otel/baggage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/metadata"
	"strings"
	"testing"
)

type privacyMail struct{ t *testing.T }

func (m privacyMail) Send(ctx context.Context, _ AuthMail) DeliveryResult {
	if baggage.FromContext(ctx).Len() != 0 {
		m.t.Error("mail inherited caller baggage")
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md) != 0 {
		m.t.Error("mail inherited caller metadata")
	}
	return DeliveryResult{domain.Unknown, "synthetic-provider-canary"}
}
func TestPrivacyMailAndServiceLogs(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	s := NewAuthService(nil, &registrationRepo{}, nil, 7, zap.New(core), AuthDependencies{MailSender: privacyMail{t}, RNG: bytes.NewReader(make([]byte, 32))})
	member, _ := baggage.NewMember("secret", "synthetic-secret-canary")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("secret", "synthetic-secret-canary"))
	_, _ = s.RequestRegistrationCode(ctx, "canary@example.com")
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "canary") || len(entry.Context) > 0 {
			t.Fatal("auth diagnostic includes sensitive data")
		}
	}
}
