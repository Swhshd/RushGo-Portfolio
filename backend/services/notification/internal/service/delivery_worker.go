package service

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type DeliveryWorker struct {
	repo    repository.DeliveryRepository
	sender  NotificationSender
	logger  *zap.Logger
	slots   chan struct{}
	metrics *observability.Metrics
	hooks   *reliabilityhook.Controller
}

// SetReliabilityHooks installs private, opt-in barriers for isolated Recovery
// components. A disabled controller leaves ordinary delivery behavior intact.
func (w *DeliveryWorker) SetReliabilityHooks(hooks *reliabilityhook.Controller) {
	w.hooks = hooks
}

func NewDeliveryWorker(repo repository.DeliveryRepository, sender NotificationSender, logger *zap.Logger, metrics ...*observability.Metrics) *DeliveryWorker {
	w := &DeliveryWorker{repo: repo, sender: sender, logger: logger, slots: make(chan struct{}, 4)}
	if len(metrics) > 0 {
		w.metrics = metrics[0]
	}
	return w
}

func (w *DeliveryWorker) RunOnce(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, errors.New("invalid delivery batch")
	}
	defer w.recordBacklog(ctx)
	capacity := 0
	for capacity < limit {
		select {
		case w.slots <- struct{}{}:
			capacity++
		default:
			goto acquired
		}
	}
acquired:
	if capacity == 0 {
		return 0, nil
	}
	defer func() {
		for range capacity {
			<-w.slots
		}
	}()
	claims, err := w.repo.ClaimDeliveries(ctx, capacity, 30*time.Second)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	results := make(chan error, len(claims))
	for _, claim := range claims {
		wg.Add(1)
		go func(c domain.DeliveryClaim) { defer wg.Done(); results <- w.processClaim(ctx, c) }(claim)
	}
	wg.Wait()
	close(results)
	var resultErr error
	for err := range results {
		resultErr = errors.Join(resultErr, err)
	}
	return len(claims), resultErr
}

func (w *DeliveryWorker) recordBacklog(ctx context.Context) {
	if w.metrics == nil {
		return
	}
	reader, ok := w.repo.(interface {
		DeliveryStats(context.Context) ([]repository.DeliveryStat, error)
	})
	if !ok {
		return
	}
	stats, err := reader.DeliveryStats(ctx)
	if err != nil {
		return
	}
	byState := map[string]repository.DeliveryStat{}
	for _, s := range stats {
		byState[s.State] = s
	}
	for _, state := range []string{"PENDING", "DELIVERING", "RETRYABLE", "UNKNOWN", "ATTENTION"} {
		s := byState[state]
		attrs := metric.WithAttributes(attribute.String("state", state))
		w.metrics.NotificationDeliveryPending.Record(ctx, s.Count, attrs)
		w.metrics.NotificationDeliveryOldestAge.Record(ctx, s.OldestAge, attrs)
	}
}

func (w *DeliveryWorker) processClaim(ctx context.Context, c domain.DeliveryClaim) error {
	if c.Notification == nil {
		return errors.New("notification delivery claim is invalid")
	}
	if c.Notification.Status == "UNKNOWN" {
		// Compatibility fence for a claim produced by an older repository or
		// worker. An ambiguous external effect is never safe to repeat.
		return w.finishClaim(ctx, c, domain.DeliveryResult{State: "ATTENTION", ErrorCategory: "sender_outcome_unknown"})
	}
	if c.Notification.Status != "PENDING" && c.Notification.Status != "RETRYABLE" {
		return errors.New("notification delivery claim is not sendable")
	}
	if c.Notification.Type == "event_onsale" || c.Notification.Type == "event_restock" {
		gate, ok := w.repo.(repository.EventReminderDeliveryGate)
		if !ok {
			return errors.New("event reminder consent gate unavailable")
		}
		eligible, err := gate.EventReminderEmailEligible(ctx, c.Notification)
		if err != nil {
			nextAttempt := time.Now().UTC().Add(5 * time.Second)
			return w.finishClaim(ctx, c, domain.DeliveryResult{State: "RETRYABLE", ErrorCategory: "consent_state_unavailable", NextAttemptAt: nextAttempt})
		}
		if !eligible {
			return w.finishClaim(ctx, c, domain.DeliveryResult{State: "CANCELLED", ErrorCategory: "consent_withdrawn"})
		}
	}
	unknownOutcome := false
	if c.Notification.Status == "PENDING" {
		if _, _, err := w.awaitDelivery(ctx, "notification.intent_before_sender",
			"notification_before_sender", c, ""); err != nil {
			return err
		}
		decision, matched, err := w.awaitDelivery(ctx, "notification.unknown_retry_attention",
			"notification_unknown_seed_before_sender", c, "")
		if err != nil {
			return err
		}
		unknownOutcome = matched && decision == reliabilityhook.DecisionFailOnce
	}
	if c.Notification.Status == "PENDING" || c.Notification.Status == "RETRYABLE" {
		if _, _, err := w.awaitDelivery(ctx, "notification.stale_fence_multiple_workers",
			"notification_stale_before_sender", c, ""); err != nil {
			return err
		}
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := w.sender.Send(sendCtx, c.Notification)
	sendErr := sendCtx.Err()
	cancel()
	if err == nil && sendErr == nil {
		if _, _, hookErr := w.awaitDelivery(ctx, "notification.sender_effect_before_marker",
			"notification_sender_effect_before_marker", c, "sent"); hookErr != nil {
			return hookErr
		}
	}
	if unknownOutcome && err == nil && sendErr == nil {
		// The one-shot isolated fault models a lost adapter response after the
		// sender effect. The outcome is terminal because SMTP cannot deduplicate.
		err = context.DeadlineExceeded
	}
	result := domain.DeliveryResult{State: "SENT"}
	if err != nil || sendErr != nil {
		result.State = "ATTENTION"
		result.ErrorCategory = "sender_outcome_unknown"
		if errors.Is(err, ErrDefinitelyNotAccepted) {
			result.State = "RETRYABLE"
			result.ErrorCategory = "sender_not_accepted"
			if c.Attempts >= 8 {
				result.State = "ATTENTION"
			}
			if result.State == "RETRYABLE" {
				backoff := 500 * time.Millisecond
				for i := int64(1); i < c.Attempts && backoff < 60*time.Second; i++ {
					backoff *= 2
				}
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
				// Bounded jitter avoids every delivery retrying at the same instant.
				backoff += time.Duration(rand.Int64N(int64(backoff/5) + 1))
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
				result.NextAttemptAt = time.Now().Add(backoff)
			}
		}
	}
	if result.State == "SENT" {
		result.ProviderID = c.Notification.ProviderID
	}
	return w.finishClaim(ctx, c, result)
}

func (w *DeliveryWorker) finishClaim(ctx context.Context, c domain.DeliveryClaim, result domain.DeliveryResult) error {
	finishCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 6*time.Second)
	defer stop()
	ok, finishErr := w.repo.FinishDelivery(finishCtx, c, result)
	if finishErr != nil {
		ok, finishErr = w.repo.FinishDelivery(finishCtx, c, result)
	}
	state := result.State
	if finishErr != nil || !ok {
		state = "UNKNOWN"
	}
	if w.metrics != nil {
		w.metrics.NotificationProcessed.Add(finishCtx, 1, metric.WithAttributes(attribute.String("result", state)))
	}
	if finishErr != nil || !ok {
		if w.logger != nil {
			fields := []zap.Field{zap.String("state", "UNKNOWN")}
			if w.hooks != nil && w.hooks.Enabled() && c.Notification != nil {
				fields = append(fields,
					zap.String("notification_id", c.Notification.ID.String()),
					zap.Int64("delivery_fence", c.Fence))
			}
			w.logger.Warn("notification delivery marker unresolved", fields...)
		}
		return errors.New("notification delivery marker unresolved")
	}
	return nil
}

func (w *DeliveryWorker) awaitDelivery(ctx context.Context, windowID, checkpoint string,
	c domain.DeliveryClaim, senderResult string) (reliabilityhook.Decision, bool, error) {
	if w == nil || w.hooks == nil || !w.hooks.Enabled() {
		return reliabilityhook.DecisionContinue, false, nil
	}
	if c.Notification == nil || c.Notification.ID == uuid.Nil || c.Token == uuid.Nil ||
		c.Fence < 1 || c.Attempts < 1 || c.LeaseUntil.IsZero() ||
		c.Notification.Type == "" || c.Notification.Status == "" {
		return reliabilityhook.DecisionContinue, false, errors.New("enabled Notification hook has an incomplete delivery identity")
	}
	requiresActiveClaim := checkpoint == "notification_before_sender" ||
		checkpoint == "notification_unknown_seed_before_sender"
	if requiresActiveClaim && !c.LeaseUntil.After(time.Now()) {
		return reliabilityhook.DecisionContinue, false, errors.New("enabled Notification hook received an expired pre-effect claim")
	}
	entityID := notificationEntityID(c.Notification)
	if _, err := uuid.Parse(entityID); err != nil {
		return reliabilityhook.DecisionContinue, false, errors.New("enabled Notification hook has no valid semantic entity identity")
	}
	facts := map[string]string{
		"notification_id":      c.Notification.ID.String(),
		"delivery_id":          c.Notification.ID.String(),
		"notification_type":    c.Notification.Type,
		"entity_id":            entityID,
		"prior_status":         c.Notification.Status,
		"delivery_token":       c.Token.String(),
		"delivery_fence":       strconv.FormatInt(c.Fence, 10),
		"delivery_attempts":    strconv.FormatInt(c.Attempts, 10),
		"delivery_lease_until": c.LeaseUntil.UTC().Format(time.RFC3339Nano),
	}
	if senderResult != "" {
		facts["sender_result"] = senderResult
	}
	decision, matched, err := w.hooks.AwaitNotificationDelivery(ctx, windowID,
		c.Notification.ID.String(), checkpoint, facts)
	if err != nil {
		return decision, matched, err
	}
	if requiresActiveClaim && !c.LeaseUntil.After(time.Now()) {
		return reliabilityhook.DecisionContinue, matched, errors.New("Notification claim lease expired while awaiting its pre-effect barrier")
	}
	return decision, matched, nil
}

func notificationEntityID(n *domain.Notification) string {
	for _, key := range []string{"booking_id", "user_id"} {
		if value, ok := n.Payload[key].(string); ok {
			return value
		}
	}
	return ""
}

func (w *DeliveryWorker) Start(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := w.RunOnce(ctx, 16); err != nil && w.logger != nil {
			w.logger.Warn("notification delivery sweep failed", zap.String("state", "UNKNOWN"))
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
