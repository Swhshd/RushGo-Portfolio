package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type staleFenceRejectingDeliveryRepository struct {
	repository.DeliveryRepository
	claim       domain.DeliveryClaim
	finishCalls int
}

func (r *staleFenceRejectingDeliveryRepository) ClaimDeliveries(context.Context, int, time.Duration) ([]domain.DeliveryClaim, error) {
	return []domain.DeliveryClaim{r.claim}, nil
}

func (r *staleFenceRejectingDeliveryRepository) FinishDelivery(_ context.Context, claim domain.DeliveryClaim, result domain.DeliveryResult) (bool, error) {
	r.finishCalls++
	if claim.Token != r.claim.Token || claim.Fence != r.claim.Fence || result.State != "SENT" {
		return false, errors.New("unexpected stale delivery marker request")
	}
	return false, nil
}

func TestStaleFenceBarrierAllowsExpiredClaimToReachCASAndLogsRedactedMarker(t *testing.T) {
	const controlToken = "private-notification-stale-fence-token"
	dir := t.TempDir()
	notificationID := uuid.New()
	claim := domain.DeliveryClaim{
		Notification: &domain.Notification{ID: notificationID, Type: "booking_cancellation", Status: "RETRYABLE",
			Recipient: "private-canary@example.com", Payload: map[string]interface{}{
				"booking_id": uuid.NewString(), "private_note": "never-log-payload",
			}},
		Token: uuid.New(), Fence: 9, Attempts: 2, LeaseUntil: time.Now().Add(-time.Minute).UTC(),
	}
	writeControlJSON(t, filepath.Join(dir, "command.json"), map[string]string{
		"window_id": "notification.stale_fence_multiple_workers", "operation_id": notificationID.String(),
		"checkpoint": "notification_stale_before_sender", "action": "pause",
		"token": controlToken, "nonce": "nonce-stale-fence-expired-claim",
	})
	hooks, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: controlToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	repo := &staleFenceRejectingDeliveryRepository{claim: claim}
	observedCore, observedLogs := observer.New(zap.WarnLevel)
	logger := zap.New(observedCore)
	var sends atomic.Int32
	worker := NewDeliveryWorker(repo, NotificationSenderFunc(func(context.Context, *domain.Notification) error {
		sends.Add(1)
		return nil
	}), logger)
	worker.SetReliabilityHooks(hooks)
	type outcome struct {
		count int
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		count, runErr := worker.RunOnce(context.Background(), 1)
		done <- outcome{count: count, err: runErr}
	}()
	barrierPath := waitForNotificationBarrier(t, dir)
	var barrier struct {
		Facts map[string]string `json:"facts"`
	}
	readControlJSON(t, barrierPath, &barrier)
	if barrier.Facts["delivery_fence"] != "9" || barrier.Facts["delivery_lease_until"] == "" || sends.Load() != 0 {
		t.Fatalf("stale claim barrier lost its fenced identity or crossed sender early: %+v sends=%d", barrier.Facts, sends.Load())
	}
	key := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(barrierPath), "barrier-"), ".json")
	writeControlJSON(t, filepath.Join(dir, "decision-"+key+".json"), map[string]string{
		"token": controlToken, "nonce": "nonce-stale-fence-expired-claim", "decision": "continue",
	})
	select {
	case result := <-done:
		if result.count != 1 || result.err == nil || !strings.Contains(result.err.Error(), "notification delivery marker unresolved") {
			t.Fatalf("stale worker did not report its fenced marker rejection: count=%d err=%v", result.count, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stale worker did not finish its sender and marker path")
	}
	if sends.Load() != 1 || repo.finishCalls != 1 {
		t.Fatalf("stale worker did not reach the expected sender->CAS path: sends=%d markers=%d", sends.Load(), repo.finishCalls)
	}
	entries := observedLogs.All()
	if len(entries) != 1 || entries[0].Message != "notification delivery marker unresolved" {
		t.Fatalf("expected one generic unresolved-marker log, got %+v", entries)
	}
	fields := entries[0].ContextMap()
	if len(fields) != 3 || fields["state"] != "UNKNOWN" || fields["notification_id"] != notificationID.String() || fields["delivery_fence"] != int64(9) {
		t.Fatalf("unresolved marker identity log is incomplete or overbroad: %+v", fields)
	}
	for _, forbidden := range []string{"recipient", "payload", "error", "sender_error"} {
		if _, found := fields[forbidden]; found {
			t.Fatalf("unresolved marker log exposed forbidden field %q", forbidden)
		}
	}
	if strings.Contains(fmt.Sprint(fields), "private-canary@example.com") || strings.Contains(fmt.Sprint(fields), "never-log-payload") {
		t.Fatal("unresolved marker log included recipient or payload content")
	}
}
