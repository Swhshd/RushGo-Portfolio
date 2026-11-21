package service_outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

type OutboundPublisher interface {
	Publish(context.Context, string, string, pkgkafka.Event) error
}
type Relay struct {
	repo      repository.OutboundWorkRepository
	publisher OutboundPublisher
	logger    *zap.Logger
	slots     chan struct{}
	hook      *reliabilityhook.Controller
}

func NewRelay(repo repository.OutboundWorkRepository, publisher OutboundPublisher, logger *zap.Logger) *Relay {
	return &Relay{repo: repo, publisher: publisher, logger: logger, slots: make(chan struct{}, 4)}
}

// ConfigureReliabilityHook enables private barriers around a durable Scheduler
// claim and its publish-before-marker gap. A disabled controller is inert.
func (r *Relay) ConfigureReliabilityHook(hook *reliabilityhook.Controller) error {
	if r == nil || hook == nil {
		return errors.New("missing Scheduler reliability hook")
	}
	r.hook = hook
	return nil
}

func (r *Relay) awaitBoundary(ctx context.Context, windowID, checkpoint string,
	c repository.OutboundClaim) error {
	if r.hook == nil || !r.hook.Enabled() {
		return nil
	}
	var config struct {
		Id      uuid.UUID
		Version int
	}
	if json.Unmarshal(c.Event.Payload, &config) != nil || config.Id == uuid.Nil || config.Version < 2 ||
		c.Event.Id != uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf(
			"ticketbox:scheduler:%s:%d", config.Id, config.Version))) ||
		c.Token == uuid.Nil || c.Fence < 1 || c.LeaseUntil.IsZero() || c.Event.Topic == "" ||
		c.Event.EventType != "SchedulerConfigChanged" {
		return errors.New("Scheduler relay claim identity is invalid")
	}
	decision, matched, err := r.hook.AwaitSchedulerBoundary(ctx, windowID,
		c.Event.Id.String(), checkpoint, map[string]string{
			"scheduler_config_id": config.Id.String(),
			"outbound_event_id":   c.Event.Id.String(),
			"version":             fmt.Sprint(config.Version),
			"claim_token":         c.Token.String(),
			"claim_fence":         fmt.Sprint(c.Fence),
			"lease_until":         c.LeaseUntil.UTC().Format(time.RFC3339Nano),
			"topic":               c.Event.Topic,
			"event_type":          c.Event.EventType,
		})
	if err != nil {
		return err
	}
	if matched && decision != reliabilityhook.DecisionContinue {
		return errors.New("Scheduler reliability boundary did not continue")
	}
	return nil
}

func (r *Relay) RunOnce(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, errors.New("invalid Scheduler relay batch")
	}
	capacity := 0
	for capacity < limit {
		select {
		case r.slots <- struct{}{}:
			capacity++
		default:
			goto ready
		}
	}
ready:
	if capacity == 0 {
		return 0, nil
	}
	defer func() {
		for range capacity {
			<-r.slots
		}
	}()
	claims, err := r.repo.ClaimOutbound(ctx, capacity, 30*time.Second)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(claims))
	for _, c := range claims {
		wg.Add(1)
		go func(c repository.OutboundClaim) {
			defer wg.Done()
			for _, windowID := range []string{
				"scheduler.outbound_claim_before_effect",
				"scheduler.multiple_instances_restart",
			} {
				if err := r.awaitBoundary(ctx, windowID,
					"scheduler_claimed_before_publish", c); err != nil {
					errs <- errors.New("Scheduler outbound claim boundary failed")
					return
				}
			}
			pubctx, stop := context.WithTimeout(ctx, 5*time.Second)
			err := r.publisher.Publish(pubctx, c.Event.Topic, c.Event.Id.String(), pkgkafka.Event{ID: c.Event.Id.String(), Type: c.Event.EventType, Timestamp: c.CreatedAt, Data: c.Event.Payload})
			stop()
			success := err == nil
			if success {
				if boundaryErr := r.awaitBoundary(ctx, "scheduler.effect_before_marker",
					"scheduler_effect_before_marker", c); boundaryErr != nil {
					errs <- errors.New("Scheduler effect-before-marker boundary failed")
					return
				}
			}
			category := ""
			if !success {
				category = "publish_unknown"
			}
			backoff := 500 * time.Millisecond
			for i := 1; i < c.Attempts && backoff < 60*time.Second; i++ {
				backoff *= 2
			}
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			next := time.Now().Add(backoff)
			resultctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 6*time.Second)
			defer cancel()
			ok, finishErr := r.repo.FinishOutbound(resultctx, c, success, category, next)
			if finishErr != nil {
				ok, finishErr = r.repo.FinishOutbound(resultctx, c, success, category, next)
			}
			if finishErr != nil || !ok {
				errs <- errors.New("Scheduler outbound marker unresolved")
			} else if err != nil {
				errs <- errors.New("Scheduler publication unknown; retry retained")
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return len(claims), result
}
func (r *Relay) Start(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := r.RunOnce(ctx, 16); err != nil && r.logger != nil {
			r.logger.Warn("Scheduler relay retry retained")
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
