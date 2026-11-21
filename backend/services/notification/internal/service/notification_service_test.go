package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ticketbox/notification/internal/domain"
)

type duplicateNotificationRepo struct {
	records map[uuid.UUID]*domain.Notification
}

func newDuplicateNotificationRepo() *duplicateNotificationRepo {
	return &duplicateNotificationRepo{records: make(map[uuid.UUID]*domain.Notification)}
}

func (r *duplicateNotificationRepo) Create(_ context.Context, n *domain.Notification) error {
	r.records[n.ID] = n
	return nil
}

func (r *duplicateNotificationRepo) AcceptIntent(_ context.Context, _ domain.SemanticKey, n *domain.Notification, _ [32]byte) (*domain.Notification, bool, error) {
	if existing, ok := r.records[n.ID]; ok {
		return existing, false, nil
	}
	r.records[n.ID] = n
	return n, true, nil
}

func (r *duplicateNotificationRepo) ProcessOnce(ctx context.Context, n *domain.Notification, send func(context.Context, *domain.Notification) error) (bool, error) {
	if existing, exists := r.records[n.ID]; exists && existing.Status == "SENT" {
		return false, nil
	}
	if _, exists := r.records[n.ID]; !exists {
		r.records[n.ID] = n
	}
	if err := send(ctx, r.records[n.ID]); err != nil {
		return false, err
	}
	r.records[n.ID].Status = "SENT"
	return true, nil
}

func (r *duplicateNotificationRepo) CreateIfAbsent(_ context.Context, n *domain.Notification) (bool, error) {
	if _, exists := r.records[n.ID]; exists {
		return false, nil
	}
	r.records[n.ID] = n
	return true, nil
}

func (r *duplicateNotificationRepo) MarkSent(_ context.Context, id uuid.UUID) error {
	r.records[id].Status = "SENT"
	return nil
}

type countingNotificationSender struct {
	sent int
}

func (s *countingNotificationSender) Send(context.Context, *domain.Notification) error {
	s.sent++
	return nil
}

func TestSendBookingConfirmationSuppressesDuplicateEvent(t *testing.T) {
	repo := newDuplicateNotificationRepo()
	sender := &countingNotificationSender{}
	svc := NewNotificationService(repo, sender, zap.NewNop())
	payload, err := json.Marshal(map[string]string{
		"event_id":   uuid.New().String(),
		"booking_id": uuid.New().String(),
		"email":      "customer@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.SendBookingConfirmation(context.Background(), payload); err != nil {
		t.Fatalf("first confirmation: %v", err)
	}
	if err := svc.SendBookingConfirmation(context.Background(), payload); err != nil {
		t.Fatalf("duplicate confirmation: %v", err)
	}
	if got := len(repo.records); got != 1 {
		t.Fatalf("expected one durable notification record, got %d", got)
	}
	if sender.sent != 0 {
		t.Fatalf("acceptance must not send, got %d side effects", sender.sent)
	}
}
