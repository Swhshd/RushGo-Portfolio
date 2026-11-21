package service

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
)

func TestBookingConfirmationDedupPersistsAcrossServiceCallsIntegration(t *testing.T) {
	p := semanticNotificationPool(t)

	repo := repository.NewPostgresNotificationRepository(p)
	var sends atomic.Int32
	sender := NotificationSenderFunc(func(context.Context, *domain.Notification) error {
		sends.Add(1)
		return nil
	})
	svc := NewNotificationService(repo, sender, zap.NewNop())
	bookingID := uuid.New()
	payload, _ := json.Marshal(map[string]string{"booking_id": bookingID.String(), "email": "integration@example.com"})

	if err := svc.SendBookingConfirmation(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if err := svc.SendBookingConfirmation(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	defer p.Exec(context.Background(), `DELETE FROM notifications WHERE id = $1`, uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking_confirmation:"+bookingID.String())))

	var count int
	if err := p.QueryRow(context.Background(), `SELECT COUNT(*) FROM notifications WHERE id = $1`, uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking_confirmation:"+bookingID.String()))).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sends.Load() != 0 {
		t.Fatalf("notification rows=%d sends=%d, want one durable intent and no inline send", count, sends.Load())
	}
}

type NotificationSenderFunc func(context.Context, *domain.Notification) error

func (f NotificationSenderFunc) Send(ctx context.Context, n *domain.Notification) error {
	return f(ctx, n)
}
