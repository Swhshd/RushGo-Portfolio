package middleware

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/baggage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"strings"
	"testing"
)

func TestPrivacyAuthInterceptorNoErrorTextOrBaggage(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	member, _ := baggage.NewMember("secret", "synthetic-canary")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("baggage", "secret=synthetic-canary"))
	_, _ = UnaryLoggingInterceptor(zap.New(core))(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/user.v1.UserService/ResetPassword"}, func(ctx context.Context, _ interface{}) (interface{}, error) {
		if baggage.FromContext(ctx).Len() != 0 {
			t.Error("business baggage retained")
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		if len(md) != 0 {
			t.Error("caller metadata retained")
		}
		return nil, errors.New("synthetic-canary")
	})
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "synthetic-canary") || strings.Contains(fmt.Sprint(entry.ContextMap()), "synthetic-canary") {
			t.Fatal("auth error text logged")
		}
	}
}

func TestPrivacyTokenValidationInterceptorNoErrorTextOrBaggage(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	member, _ := baggage.NewMember("secret", "synthetic-canary")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer synthetic-canary"))
	_, _ = UnaryLoggingInterceptor(zap.New(core))(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/user.v1.UserService/ValidateToken"}, func(ctx context.Context, _ interface{}) (interface{}, error) {
		if baggage.FromContext(ctx).Len() != 0 {
			t.Error("business baggage retained for token validation")
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		if len(md) != 0 {
			t.Error("caller metadata retained for token validation")
		}
		return nil, errors.New("synthetic-canary")
	})
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "synthetic-canary") || strings.Contains(fmt.Sprint(entry.ContextMap()), "synthetic-canary") {
			t.Fatal("token validation error text logged")
		}
	}
}

func TestTicketRPCLoggingDoesNotRecordForwardedBearer(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer synthetic-canary"))
	_, _ = UnaryLoggingInterceptor(zap.New(core))(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/booking.v1.BookingService/CheckInTicket"}, func(context.Context, interface{}) (interface{}, error) {
		return nil, errors.New("ticket principal invalid")
	})
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "synthetic-canary") || strings.Contains(fmt.Sprint(entry.ContextMap()), "synthetic-canary") {
			t.Fatal("forwarded bearer appeared in Ticket RPC logs")
		}
	}
}
