package service

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"go.uber.org/zap"
)

type singleClaimDeliveryRepository struct {
	repository.DeliveryRepository
	claim      domain.DeliveryClaim
	claimCalls atomic.Int32
	result     domain.DeliveryResult
}

func (r *singleClaimDeliveryRepository) ClaimDeliveries(context.Context, int, time.Duration) ([]domain.DeliveryClaim, error) {
	if r.claimCalls.Add(1) == 1 {
		return []domain.DeliveryClaim{r.claim}, nil
	}
	return nil, nil
}

func (r *singleClaimDeliveryRepository) FinishDelivery(_ context.Context, _ domain.DeliveryClaim, result domain.DeliveryResult) (bool, error) {
	r.result = result
	return true, nil
}

func TestSMTPAcceptanceWithLostFinalResponseIsNotRetriedByDeliveryWorker(t *testing.T) {
	capture := runSMTPTestServer(t, smtpTestDropAfterData)
	sender := smtpFixtureSender(t, capture)
	notification := businessEmailFixture(uuid.New(), "en")
	notification.Status = "PENDING"
	claim := domain.DeliveryClaim{
		Notification: notification, Token: uuid.New(), Fence: 1, Attempts: 1,
		LeaseUntil: time.Now().Add(time.Minute).UTC(),
	}
	repo := &singleClaimDeliveryRepository{claim: claim}
	worker := NewDeliveryWorker(repo, sender, zap.NewNop())
	if count, err := worker.RunOnce(context.Background(), 1); err != nil || count != 1 {
		t.Fatalf("persist ambiguous SMTP outcome: count=%d err=%v", count, err)
	}
	if repo.result.State != "ATTENTION" || repo.result.ErrorCategory != "sender_outcome_unknown" || !repo.result.NextAttemptAt.IsZero() {
		t.Fatalf("lost final SMTP response was not terminalized: %+v", repo.result)
	}
	select {
	case message := <-capture.message:
		if !strings.Contains(message, "Message-ID: <"+notification.ID.String()+"@example.test>") {
			t.Fatal("accepted SMTP message lost its stable Message-ID")
		}
	case <-time.After(time.Second):
		t.Fatal("SMTP fixture did not capture the accepted message before dropping its final response")
	}
	if count, err := worker.RunOnce(context.Background(), 1); err != nil || count != 0 {
		t.Fatalf("worker claimed terminal ambiguous delivery again: count=%d err=%v", count, err)
	}
	if repo.claimCalls.Load() != 2 {
		t.Fatalf("unexpected claim sweeps after terminal outcome: %d", repo.claimCalls.Load())
	}
	select {
	case duplicate := <-capture.message:
		t.Fatalf("worker resent ambiguous accepted SMTP message: %s", duplicate)
	default:
	}
}
