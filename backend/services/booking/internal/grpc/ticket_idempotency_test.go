package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rotationKeyStore struct {
	repository.TicketRepository
	keys []string
}

func (s *rotationKeyStore) LookupTicketCredentialRotation(_ context.Context, _, _ uuid.UUID, key string, _ []byte) (domain.TicketCredentialRotationResult, bool, error) {
	s.keys = append(s.keys, key)
	return domain.TicketCredentialRotationResult{Outcome: domain.TicketCredentialRotationAlreadyApplied, CredentialVersion: 1, ExpiresAt: time.Now().Add(time.Minute)}, true, nil
}

func TestRotationGRPCLegacyCallerGetsFreshFallbackKey(t *testing.T) {
	store := &rotationKeyStore{}
	server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
	req := &bookingv1.RotateTicketCredentialRequest{UserId: uuid.NewString(), TicketId: uuid.NewString()}
	userID, _ := uuid.Parse(req.UserId)
	server.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: userID, Role: "user"}})
	for range 2 {
		if _, err := server.RotateTicketCredential(context.Background(), req); err != nil {
			t.Fatalf("legacy keyless request rejected: %v", err)
		}
	}
	if len(store.keys) != 2 || store.keys[0] == store.keys[1] {
		t.Fatalf("legacy requests must get independent one-shot keys: %v", store.keys)
	}
	for _, key := range store.keys {
		if _, err := uuid.Parse(key); err != nil {
			t.Fatal("fallback key is not a UUID")
		}
	}
	if req.IdempotencyKey != "" {
		t.Fatal("server mutated caller request")
	}
}

func TestRotationGRPCPreservesExplicitKeyAndRejectsMalformedKey(t *testing.T) {
	store := &rotationKeyStore{}
	server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
	req := &bookingv1.RotateTicketCredentialRequest{UserId: uuid.NewString(), TicketId: uuid.NewString(), IdempotencyKey: "stable-key"}
	userID, _ := uuid.Parse(req.UserId)
	server.SetTicketPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: userID, Role: "user"}})
	if _, err := server.RotateTicketCredential(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(store.keys) != 1 || store.keys[0] != "stable-key" {
		t.Fatal("explicit key changed")
	}
	req.IdempotencyKey = "invalid key"
	if _, err := server.RotateTicketCredential(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed key error=%v", err)
	}
	if len(store.keys) != 1 {
		t.Fatal("malformed key reached repository")
	}
}
