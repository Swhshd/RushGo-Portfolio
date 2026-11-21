package handler

import (
	"github.com/ticketbox/gateway/internal/authlimiter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

func TestPrivacyHTTPProviderCanaryAbsent(t *testing.T) {
	client := &authClient{err: status.Error(codes.Internal, "synthetic-secret-canary")}
	h := NewAuthHandler(client, nil)
	h.limiter = deniedLimiter{}
	w := requestAuth(h, "forgot", `{"email":"canary@example.com"}`)
	if strings.Contains(w.Body.String(), "canary") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("provider/identity leaked in HTTP")
	}
	h.limiter = deniedLimiter{authlimiter.ErrUnavailable}
	w = requestAuth(h, "send", `{"email":"canary@example.com"}`)
	if strings.Contains(w.Body.String(), "canary") {
		t.Fatal("limiter identity leaked")
	}
}
