package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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
)

type barrierDeliveryRepository struct {
	repository.DeliveryRepository
	claim       domain.DeliveryClaim
	expected    string
	result      domain.DeliveryResult
	finishCalls atomic.Int32
}

func (r *barrierDeliveryRepository) ClaimDeliveries(context.Context, int, time.Duration) ([]domain.DeliveryClaim, error) {
	return []domain.DeliveryClaim{r.claim}, nil
}

func (r *barrierDeliveryRepository) FinishDelivery(_ context.Context, claim domain.DeliveryClaim, result domain.DeliveryResult) (bool, error) {
	expected := r.expected
	if expected == "" {
		expected = "SENT"
	}
	if claim.Token != r.claim.Token || claim.Fence != r.claim.Fence || result.State != expected {
		return false, errors.New("unexpected delivery marker")
	}
	r.result = result
	r.finishCalls.Add(1)
	return true, nil
}

func TestNotificationUnknownSeedPersistsAttentionAfterObservedSenderCall(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	dir := t.TempDir()
	notificationID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	entityID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	claim := domain.DeliveryClaim{
		Notification: &domain.Notification{ID: notificationID, Type: "booking_cancellation",
			Payload: map[string]any{"booking_id": entityID.String()}, Status: "PENDING"},
		Token: uuid.MustParse("33333333-3333-4333-8333-333333333333"), Fence: 1, Attempts: 1,
		LeaseUntil: time.Now().Add(time.Minute).UTC(),
	}
	writeControlJSON(t, filepath.Join(dir, "command.json"), map[string]string{
		"window_id":    "notification.unknown_retry_attention",
		"operation_id": notificationID.String(),
		"checkpoint":   "notification_unknown_seed_before_sender",
		"action":       "pause", "token": token, "nonce": "nonce-notification-unknown-seed",
	})
	hooks, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	repo := &barrierDeliveryRepository{claim: claim, expected: "ATTENTION"}
	var senderCalls atomic.Int32
	worker := NewDeliveryWorker(repo, NotificationSenderFunc(func(context.Context, *domain.Notification) error {
		senderCalls.Add(1)
		return nil
	}), zap.NewNop())
	worker.SetReliabilityHooks(hooks)
	done := make(chan error, 1)
	go func() {
		_, runErr := worker.RunOnce(context.Background(), 1)
		done <- runErr
	}()
	barrierPath := waitForNotificationBarrier(t, dir)
	var barrier map[string]any
	readControlJSON(t, barrierPath, &barrier)
	key := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(barrierPath), "barrier-"), ".json")
	writeControlJSON(t, filepath.Join(dir, "decision-"+key+".json"), map[string]string{
		"token": token, "nonce": "nonce-notification-unknown-seed", "decision": "fail_once",
	})
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not persist its one-shot ATTENTION result")
	}
	if senderCalls.Load() != 1 || repo.finishCalls.Load() != 1 ||
		repo.result.State != "ATTENTION" || repo.result.ErrorCategory != "sender_outcome_unknown" ||
		!repo.result.NextAttemptAt.IsZero() {
		t.Fatalf("T23 uncertain outcome did not become terminal ATTENTION after the sender call: sends=%d result=%+v",
			senderCalls.Load(), repo.result)
	}
}

func TestDeliveryWorkerPausesBeforeSenderOnExactSemanticIntent(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	dir := t.TempDir()
	notificationID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	entityID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	deliveryToken := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	claim := domain.DeliveryClaim{
		Notification: &domain.Notification{ID: notificationID, Type: "booking_cancellation",
			Recipient: "private-canary@example.com", Payload: map[string]any{"booking_id": entityID.String()},
			Status: "PENDING"},
		Token: deliveryToken, Fence: 4, Attempts: 2,
		LeaseUntil: time.Now().Add(time.Minute).UTC(),
	}
	command := map[string]string{
		"window_id": "notification.intent_before_sender", "operation_id": notificationID.String(),
		"checkpoint": "notification_before_sender", "action": "pause", "token": token,
		"nonce": "nonce-notification-worker",
	}
	writeControlJSON(t, filepath.Join(dir, "command.json"), command)
	hooks, err := reliabilityhook.New(reliabilityhook.Config{Enabled: true, Token: token, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	repo := &barrierDeliveryRepository{claim: claim}
	var senderCalls atomic.Int32
	worker := NewDeliveryWorker(repo, NotificationSenderFunc(func(context.Context, *domain.Notification) error {
		senderCalls.Add(1)
		return nil
	}), zap.NewNop())
	worker.SetReliabilityHooks(hooks)
	done := make(chan error, 1)
	go func() {
		count, runErr := worker.RunOnce(context.Background(), 1)
		if runErr == nil && count != 1 {
			runErr = errors.New("worker did not process its exact claim")
		}
		done <- runErr
	}()

	barrierPath := waitForNotificationBarrier(t, dir)
	var barrier map[string]any
	readControlJSON(t, barrierPath, &barrier)
	facts, ok := barrier["facts"].(map[string]any)
	if !ok || facts["notification_id"] != notificationID.String() ||
		facts["delivery_fence"] != "4" || senderCalls.Load() != 0 {
		t.Fatalf("worker crossed or misreported the semantic intent barrier: facts=%v sends=%d", facts, senderCalls.Load())
	}
	if _, exists := facts["recipient"]; exists {
		t.Fatal("Notification recipient escaped into the T23 barrier")
	}
	if _, exists := facts["payload"]; exists {
		t.Fatal("Notification payload escaped into the T23 barrier")
	}
	key := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(barrierPath), "barrier-"), ".json")
	writeControlJSON(t, filepath.Join(dir, "decision-"+key+".json"), map[string]string{
		"token": token, "nonce": "nonce-notification-worker", "decision": "continue",
	})
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not resume after the exact barrier decision")
	}
	if senderCalls.Load() != 1 || repo.finishCalls.Load() != 1 {
		t.Fatalf("worker did not execute one sender effect and marker: sends=%d markers=%d", senderCalls.Load(), repo.finishCalls.Load())
	}
}

func TestDeliveryWorkerKeepsUnknownSendOutcomeSeparateFromDefinitePreSendFailure(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		state    string
		category string
	}{
		{name: "pre-send rejection is retryable", err: ErrDefinitelyNotAccepted, state: "RETRYABLE", category: "sender_not_accepted"},
		{name: "post-submit transport loss needs attention", err: ErrDeliveryOutcomeUnknown, state: "ATTENTION", category: "sender_outcome_unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := domain.DeliveryClaim{
				Notification: &domain.Notification{ID: uuid.New(), Type: "booking_cancellation", Status: "PENDING",
					Payload: map[string]any{"booking_id": uuid.NewString()}},
				Token: uuid.New(), Fence: 1, Attempts: 1, LeaseUntil: time.Now().Add(time.Minute),
			}
			repo := &barrierDeliveryRepository{claim: claim, expected: test.state}
			worker := NewDeliveryWorker(repo, NotificationSenderFunc(func(context.Context, *domain.Notification) error { return test.err }), zap.NewNop())
			if _, err := worker.RunOnce(context.Background(), 1); err != nil {
				t.Fatalf("run delivery: %v", err)
			}
			if repo.result.State != test.state || repo.result.ErrorCategory != test.category {
				t.Fatalf("delivery result = %+v, want %s/%s", repo.result, test.state, test.category)
			}
			if test.state == "RETRYABLE" && repo.result.NextAttemptAt.IsZero() ||
				test.state == "ATTENTION" && !repo.result.NextAttemptAt.IsZero() {
				t.Fatalf("delivery scheduling does not match terminality: %+v", repo.result)
			}
		})
	}
}

func TestDeliveryWorkerNeverSendsLegacyUnknownClaim(t *testing.T) {
	claim := domain.DeliveryClaim{
		Notification: &domain.Notification{ID: uuid.New(), Type: "booking_confirmation", Status: "UNKNOWN",
			Payload: map[string]any{"booking_id": uuid.NewString()}},
		Token: uuid.New(), Fence: 2, Attempts: 2, LeaseUntil: time.Now().Add(time.Minute),
	}
	repo := &barrierDeliveryRepository{claim: claim, expected: "ATTENTION"}
	var senderCalls atomic.Int32
	worker := NewDeliveryWorker(repo, NotificationSenderFunc(func(context.Context, *domain.Notification) error {
		senderCalls.Add(1)
		return nil
	}), zap.NewNop())
	if _, err := worker.RunOnce(context.Background(), 1); err != nil {
		t.Fatalf("resolve legacy UNKNOWN claim: %v", err)
	}
	if senderCalls.Load() != 0 || repo.result.State != "ATTENTION" || repo.result.ErrorCategory != "sender_outcome_unknown" {
		t.Fatalf("legacy UNKNOWN was sent or not terminalized: sends=%d result=%+v", senderCalls.Load(), repo.result)
	}
}

func waitForNotificationBarrier(t *testing.T, dir string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		paths, err := filepath.Glob(filepath.Join(dir, "barrier-*.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) == 1 {
			return paths[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not publish the expected Notification barrier")
	return ""
}

func writeControlJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".control-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		t.Fatal(err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		t.Fatal(err)
	}
}

func readControlJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}
