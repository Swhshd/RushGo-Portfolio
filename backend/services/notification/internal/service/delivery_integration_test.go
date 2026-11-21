package service

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/pkg/observability"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func deliveryIntent(t *testing.T, p *pgxpool.Pool) uuid.UUID {
	t.Helper()
	entity := uuid.New()
	raw := []byte(`{"booking_id":"` + entity.String() + `","email":"private-canary@example.com","reason":"private-payload-canary"}`)
	if err := NewNotificationService(repository.NewPostgresNotificationRepository(p), nil, zap.NewNop()).SendBookingFailure(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	if err := p.QueryRow(context.Background(), `SELECT id FROM notifications WHERE entity_id=$1`, entity).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// Inject at the repository boundary while retaining real PostgreSQL commits and claims.
type deliveryFault struct {
	repository.DeliveryRepository
	failMarker bool
	loseACK    bool
	claim      domain.DeliveryClaim
}

type startupDeliveryFault struct {
	repository.DeliveryRepository
	calls atomic.Int32
}

func (r *startupDeliveryFault) ClaimDeliveries(ctx context.Context, limit int, lease time.Duration) ([]domain.DeliveryClaim, error) {
	if r.calls.Add(1) == 1 {
		return nil, errors.New("transient startup database error")
	}
	return r.DeliveryRepository.ClaimDeliveries(ctx, limit, lease)
}

func TestDeliveryStartupRetryAndBoundedSlotsIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	for range 5 {
		deliveryIntent(t, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	entered := make(chan struct{}, 5)
	release := make(chan struct{})
	defer close(release)
	sender := NotificationSenderFunc(func(ctx context.Context, n *domain.Notification) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 10*time.Second {
			return errors.New("missing bounded sender timeout")
		}
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	fault := &startupDeliveryFault{DeliveryRepository: r}
	w := NewDeliveryWorker(fault, sender, zap.NewNop())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	for range 4 {
		select {
		case <-entered:
		case <-done:
			t.Fatal("startup failure permanently stopped worker")
		case <-ctx.Done():
			t.Fatal("parallel sender slots never filled")
		}
	}
	if n, err := w.RunOnce(ctx, 16); err != nil || n != 0 {
		t.Fatal("overlapping sweep claimed beyond capacity", n, err)
	}
	var pending int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE status='PENDING'`).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("queued work leased without execution capacity", pending, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("worker did not finish shutdown")
	}
	var attention int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE status='ATTENTION' AND delivery_due_at IS NULL AND last_error='sender_outcome_unknown'`).Scan(&attention); err != nil || attention != 4 {
		t.Fatal("canceled sends did not retain terminal uncertain outcomes", attention, err)
	}
}

func TestDeliverySendDeadlinePersistsAttentionIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	id := deliveryIntent(t, p)
	started := time.Now()
	sender := NotificationSenderFunc(func(ctx context.Context, _ *domain.Notification) error { <-ctx.Done(); return ctx.Err() })
	if n, err := NewDeliveryWorker(r, sender, zap.NewNop()).RunOnce(context.Background(), 1); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if time.Since(started) > 13*time.Second {
		t.Fatal("sender deadline was not bounded")
	}
	var attention bool
	if err := p.QueryRow(context.Background(), `SELECT status='ATTENTION' AND delivery_due_at IS NULL AND last_error='sender_outcome_unknown' FROM notifications WHERE id=$1`, id).Scan(&attention); err != nil || !attention {
		t.Fatal("send timeout was not retained as terminal attention", attention, err)
	}
}

func (r *deliveryFault) FinishDelivery(ctx context.Context, c domain.DeliveryClaim, v domain.DeliveryResult) (bool, error) {
	r.claim = c
	if r.failMarker {
		return false, errors.New("injected marker unavailable")
	}
	ok, err := r.DeliveryRepository.FinishDelivery(ctx, c, v)
	if err == nil && ok && r.loseACK {
		r.loseACK = false
		return false, errors.New("injected applied marker acknowledgement loss")
	}
	return ok, err
}

func TestMarkerGapAfterSenderEffectPersistsAttentionWithoutResendIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	id := deliveryIntent(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var mu sync.Mutex
	var effects []uuid.UUID
	sender := NotificationSenderFunc(func(ctx context.Context, n *domain.Notification) error {
		mu.Lock()
		defer mu.Unlock()
		effects = append(effects, n.ID)
		return nil
	})
	fault := &deliveryFault{DeliveryRepository: r, failMarker: true}
	if _, err := NewDeliveryWorker(fault, sender, zap.NewNop()).RunOnce(ctx, 1); err == nil {
		t.Fatal("worker hid unresolved delivery marker")
	}
	if _, err := p.Exec(ctx, `UPDATE notifications SET delivery_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	restarted := NewDeliveryWorker(r, sender, zap.NewNop())
	if n, err := restarted.RunOnce(ctx, 1); err != nil || n != 0 {
		t.Fatal("expired sender lease must become terminal without resend", n, err)
	}
	var attention bool
	if err := p.QueryRow(ctx, `SELECT status='ATTENTION' AND delivery_due_at IS NULL AND last_error='delivery_lease_expired' FROM notifications WHERE id=$1`, id).Scan(&attention); err != nil || !attention {
		t.Fatal("expired claim did not retain terminal attention", attention, err)
	}
	if n, err := restarted.RunOnce(ctx, 1); err != nil || n != 0 {
		t.Fatal("terminal uncertain delivery was claimed again", n, err)
	}
	if len(effects) != 1 || effects[0] != id {
		t.Fatal("ambiguous SMTP acceptance was resent", effects)
	}
	if ok, err := r.FinishDelivery(ctx, fault.claim, domain.DeliveryResult{State: "UNKNOWN", ErrorCategory: "late", NextAttemptAt: time.Now()}); err == nil || ok {
		t.Fatal("repository accepted a new retryable UNKNOWN state", ok, err)
	}
	if ok, err := r.FinishDelivery(ctx, fault.claim, domain.DeliveryResult{State: "SENT"}); err != nil || ok {
		t.Fatal("stale worker overwrote terminal attention", ok, err)
	}
	var count int
	var status string
	if err := p.QueryRow(ctx, `SELECT count(*),min(status) FROM notifications`).Scan(&count, &status); err != nil || count != 1 || status != "ATTENTION" {
		t.Fatal("semantic identity or terminal ambiguity lost", count, status, err)
	}
}

func TestDeliveryClaimsAndCommitUncertaintyIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	id := deliveryIntent(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var sends atomic.Int32
	sender := NotificationSenderFunc(func(ctx context.Context, n *domain.Notification) error {
		// NOWAIT fails if claim transaction or any row lock survived into Send.
		tx, err := p.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())
		var status string
		var token uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT status,delivery_token FROM notifications WHERE id=$1 FOR UPDATE NOWAIT`, n.ID).Scan(&status, &token); err != nil {
			return err
		}
		if status != "DELIVERING" || token == uuid.Nil {
			return errors.New("sender invoked before committed claim")
		}
		sends.Add(1)
		return nil
	})
	fault := &deliveryFault{DeliveryRepository: r, loseACK: true}
	if n, err := NewDeliveryWorker(fault, sender, zap.NewNop()).RunOnce(ctx, 1); err != nil || n != 1 {
		t.Fatal("applied marker ACK loss not resolved", n, err)
	}
	if ok, err := r.FinishDelivery(ctx, fault.claim, domain.DeliveryResult{State: "SENT"}); err != nil || !ok {
		t.Fatal("successful marker cannot be reread after ACK loss", ok, err)
	}
	if n, err := NewDeliveryWorker(r, sender, zap.NewNop()).RunOnce(ctx, 1); err != nil || n != 0 {
		t.Fatal("SENT was rediscovered", n, err)
	}
	var status string
	var delivery uuid.UUID
	if err := p.QueryRow(ctx, `SELECT status,delivery_id FROM notifications WHERE id=$1`, id).Scan(&status, &delivery); err != nil || status != "SENT" || delivery != id || sends.Load() != 1 {
		t.Fatal("marker replay caused duplicate effect", status, delivery, sends.Load(), err)
	}
}

func TestDeliveryExhaustionBecomesQueryableAttentionIntegration(t *testing.T) {
	for _, kind := range []string{"definite_rejection", "unknown_expiry"} {
		t.Run(kind, func(t *testing.T) {
			p := semanticNotificationPool(t)
			r := repository.NewPostgresNotificationRepository(p)
			id := deliveryIntent(t, p)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var sends atomic.Int32
			w := NewDeliveryWorker(r, NotificationSenderFunc(func(context.Context, *domain.Notification) error {
				sends.Add(1)
				if kind == "definite_rejection" {
					return ErrDefinitelyNotAccepted
				}
				return ErrDeliveryOutcomeUnknown
			}), zap.NewNop())
			maxAttempts := 8
			if kind == "unknown_expiry" {
				maxAttempts = 1
			}
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				if kind == "definite_rejection" {
					if n, err := w.RunOnce(ctx, 1); err != nil || n != 1 {
						t.Fatal("send attempt", attempt, n, err)
					}
				} else {
					claims, err := r.ClaimDeliveries(ctx, 1, time.Second)
					if err != nil || len(claims) != 1 {
						t.Fatal("unknown claim", attempt, claims, err)
					}
					if _, err := p.Exec(ctx, `UPDATE notifications SET delivery_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
						t.Fatal(err)
					}
					if claims, err := r.ClaimDeliveries(ctx, 1, time.Second); err != nil || len(claims) != 0 {
						t.Fatal("expiry sweep", claims, err)
					}
				}
				var status string
				var attempts int
				var category string
				if err := p.QueryRow(ctx, `SELECT status,delivery_attempts,COALESCE(last_error,'') FROM notifications WHERE id=$1`, id).Scan(&status, &attempts, &category); err != nil {
					t.Fatal(err)
				}
				if attempts != attempt {
					t.Fatal("attempt count", attempts, attempt)
				}
				if kind == "definite_rejection" && attempt < 8 && status != "RETRYABLE" {
					t.Fatalf("definite pre-send rejection did not remain retryable: %d %s", attempt, status)
				}
				if status != "ATTENTION" && (kind == "unknown_expiry" || attempt == 8) {
					t.Fatal("eight outcomes failed to retain ATTENTION", status)
				}
				if strings.Contains(category, "@") {
					t.Fatal("persisted raw sender error", category)
				}
				if kind == "definite_rejection" && attempt < 8 {
					if _, err := p.Exec(ctx, `UPDATE notifications SET delivery_due_at=clock_timestamp() WHERE id=$1`, id); err != nil {
						t.Fatal(err)
					}
				}
			}
			if n, err := w.RunOnce(ctx, 1); err != nil || n != 0 {
				t.Fatal("exhausted delivery was sent again", n, err)
			}
			if kind == "definite_rejection" && sends.Load() != 8 {
				t.Fatal("sender attempts", sends.Load())
			}
		})
	}
}

func TestDeliveryExpiredOwnerCannotFinishAfterLockWaitIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	id := deliveryIntent(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	claims, err := r.ClaimDeliveries(ctx, 1, time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `UPDATE notifications SET delivery_lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ok, err := r.FinishDelivery(ctx, claims[0], domain.DeliveryResult{State: "SENT"})
		if ok {
			err = errors.New("expired owner finished after row lock wait")
		}
		done <- err
	}()
	for {
		var waiting bool
		if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatal("finish returned before observed lock wait", err)
		case <-ctx.Done():
			t.Fatal("lock wait not observed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLogSenderOmitsRecipientAndPayload(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	sender := NewLogSender(zap.New(core))
	n := &domain.Notification{ID: uuid.New(), Type: "booking_failure", Channel: "email", Recipient: "private-canary@example.com", Payload: map[string]any{"reason": "private-payload-canary"}}
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	for _, entry := range logs.All() {
		for _, f := range entry.Context {
			if strings.Contains(f.String, "private-") {
				t.Fatal("sensitive sender output", f.Key)
			}
		}
	}
}

func TestDeliveryMigrationRoundTripIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	deliveryIntent(t, p)
	deliveryIntent(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	claims, err := r.ClaimDeliveries(ctx, 2, time.Second)
	if err != nil || len(claims) != 2 {
		t.Fatal(claims, err)
	}
	for i, c := range claims {
		state := "SENT"
		if i == 1 {
			state = "ATTENTION"
		}
		if ok, err := r.FinishDelivery(ctx, c, domain.DeliveryResult{State: state}); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	for _, args := range [][]string{{"down", "2"}, {"up"}} {
		cmd := append([]string{"-path", "../../migrations", "-database", p.Config().ConnString()}, args...)
		out, err := exec.CommandContext(ctx, "migrate", cmd...).CombinedOutput()
		if err != nil {
			t.Fatalf("populated migration roundtrip: %s %v", out, err)
		}
	}
	var terminals int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE status IN ('SENT','ATTENTION')`).Scan(&terminals); err != nil || terminals != 2 {
		t.Fatal("downgrade requeued terminal work", terminals, err)
	}
}

func TestDeliveryMetricsReportAttentionAgeWithoutIdentityIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	id := deliveryIntent(t, p)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `UPDATE notifications SET status='ATTENTION',delivery_attempts=8,delivery_due_at=NULL,created_at=clock_timestamp()-interval '1 minute' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(ctx)
	metrics, err := observability.NewMetrics(provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDeliveryWorker(r, NewLogSender(zap.NewNop()), zap.NewNop(), metrics).RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	var attention, age bool
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "rushgo.notification.delivery.pending" {
				g := m.Data.(metricdata.Gauge[int64])
				for _, dp := range g.DataPoints {
					if v, ok := dp.Attributes.Value("state"); ok && v.AsString() == "ATTENTION" && dp.Value == 1 {
						attention = true
					}
					if dp.Attributes.Len() != 1 {
						t.Fatal("delivery metric has identity labels")
					}
				}
			}
			if m.Name == "rushgo.notification.delivery.oldest_age" {
				g := m.Data.(metricdata.Gauge[float64])
				for _, dp := range g.DataPoints {
					if dp.Value >= 60 {
						age = true
					}
				}
			}
		}
	}
	if !attention || !age {
		t.Fatal("durable attention/age metrics missing", attention, age)
	}
}
