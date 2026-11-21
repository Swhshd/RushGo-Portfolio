package admission

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type CapturedAdmission struct {
	Key, Value      string
	EventID, SeatID uuid.UUID
}
type ReconciliationStore interface {
	ScanAdmissions(context.Context, uint64, int64) ([]CapturedAdmission, uint64, error)
	CompareDeleteAdmission(context.Context, CapturedAdmission) (bool, error)
}
type SeatAuthorityReader interface {
	GetSeatAuthorities(context.Context, []uuid.UUID) ([]domain.SeatAuthority, error)
}
type AdmissionRepairBarrier interface {
	AwaitAdmissionRepair(context.Context, string, string, string, string, string) (reliabilityhook.Decision, bool, error)
	RecordAdmissionRepairOutcome(string, string, string, string, string, bool, error) error
}
type Reconciler struct {
	store         ReconciliationStore
	authority     SeatAuthorityReader
	cursor        uint64
	mu            sync.Mutex
	batch         int
	interval      time.Duration
	pending       []CapturedAdmission
	lastCycle     time.Time
	metrics       *observability.Metrics
	logger        *zap.Logger
	repairBarrier AdmissionRepairBarrier
	sweeperID     string
}

func NewReconciler(store ReconciliationStore, authority SeatAuthorityReader, metrics ...*observability.Metrics) *Reconciler {
	r := &Reconciler{store: store, authority: authority, batch: 256, interval: time.Second, lastCycle: time.Now(), sweeperID: "sweeper-1"}
	if len(metrics) > 0 {
		r.metrics = metrics[0]
	}
	return r
}

func (r *Reconciler) SetAdmissionRepairBarrier(barrier AdmissionRepairBarrier, sweeperID string) error {
	if barrier == nil || (sweeperID != "sweeper-1" && sweeperID != "sweeper-2") {
		return errors.New("invalid admission repair barrier identity")
	}
	r.repairBarrier = barrier
	r.sweeperID = sweeperID
	return nil
}

func NewReconcilerWithLogger(store ReconciliationStore, authority SeatAuthorityReader, logger *zap.Logger, metrics ...*observability.Metrics) *Reconciler {
	r := NewReconciler(store, authority, metrics...)
	r.logger = logger
	return r
}
func (r *Reconciler) RunOnce(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	defer func() {
		if r.metrics != nil {
			r.metrics.AdmissionRepairDuration.Record(ctx, time.Since(started).Seconds())
			r.metrics.AdmissionRepairBacklog.Record(ctx, int64(len(r.pending)))
			r.metrics.AdmissionRepairCycleAge.Record(ctx, time.Since(r.lastCycle).Seconds())
		}
	}()
	if r.store == nil || r.authority == nil {
		return 0, errors.New("admission reconciler dependencies are required")
	}
	captures := r.pending
	r.pending = nil
	if len(captures) == 0 {
		var err error
		var next uint64
		captures, next, err = r.store.ScanAdmissions(ctx, r.cursor, int64(r.batch))
		if err != nil {
			r.record(ctx, "scan_error", 1)
			return 0, err
		}
		r.cursor = next
		if next == 0 {
			r.lastCycle = time.Now()
		}
		r.record(ctx, "captured", int64(len(captures)))
	}
	if len(captures) == 0 {
		return 0, nil
	}
	ids := make([]uuid.UUID, 0, len(captures))
	seen := map[uuid.UUID]struct{}{}
	for _, c := range captures {
		if c.SeatID != uuid.Nil {
			if _, ok := seen[c.SeatID]; !ok {
				seen[c.SeatID] = struct{}{}
				ids = append(ids, c.SeatID)
			}
		}
	}
	authorities, err := r.authority.GetSeatAuthorities(ctx, ids)
	r.record(ctx, "pg_batch", 1)
	if err != nil {
		r.pending = captures
		r.record(ctx, "pg_unknown", 1)
		return 0, err
	}
	bySeat := map[uuid.UUID]domain.SeatAuthority{}
	for _, a := range authorities {
		bySeat[a.SeatID] = a
	}
	deleted := 0
	var failures []error
	for index, c := range captures {
		if ctx.Err() != nil {
			r.pending = append(r.pending, captures[index:]...)
			r.record(ctx, "budget_exhausted", 1)
			break
		}
		a, ok := bySeat[c.SeatID]
		if !ok {
			r.record(ctx, "authority_missing", 1)
			continue
		}
		if staleCapturedOwner(c, a) {
			barrierMatched := false
			if r.repairBarrier != nil {
				decision, matched, barrierErr := r.repairBarrier.AwaitAdmissionRepair(
					ctx, c.Value, c.EventID.String(), c.SeatID.String(), c.Key, r.sweeperID)
				if barrierErr != nil || matched && decision != reliabilityhook.DecisionContinue {
					r.pending = append(r.pending, captures[index:]...)
					if barrierErr != nil {
						failures = append(failures, barrierErr)
					}
					break
				}
				barrierMatched = matched
			}
			ok, err := r.store.CompareDeleteAdmission(ctx, c)
			if barrierMatched {
				if resultErr := r.repairBarrier.RecordAdmissionRepairOutcome(
					c.Value, c.EventID.String(), c.SeatID.String(), c.Key,
					r.sweeperID, ok, err); resultErr != nil {
					failures = append(failures, resultErr)
				}
			}
			if err != nil {
				r.pending = append(r.pending, c)
				r.record(ctx, "delete_unknown", 1)
				failures = append(failures, err)
			} else if ok {
				deleted++
				r.record(ctx, "deleted", 1)
			} else {
				r.record(ctx, "compare_noop", 1)
			}
		} else {
			r.record(ctx, "preserved", 1)
		}
	}
	return deleted, errors.Join(failures...)
}

func (r *Reconciler) record(ctx context.Context, state string, n int64) {
	if r.metrics != nil {
		r.metrics.AdmissionRepair.Add(ctx, n, metric.WithAttributes(attribute.String("result", state)))
	}
}
func staleCapturedOwner(c CapturedAdmission, a domain.SeatAuthority) bool {
	if c.EventID == uuid.Nil || c.SeatID == uuid.Nil || a.EventID != c.EventID || !a.Coherent {
		return false
	}
	if a.BookingID == nil && a.ReservationDeadline == nil && a.Generation == nil && a.LifecycleState == nil {
		return a.Status == domain.SeatStatusAvailable
	}
	if a.BookingID != nil && a.BookingID.String() != c.Value {
		return true
	}
	if a.Status == domain.SeatStatusBooked || a.Status == domain.SeatStatusReserved {
		return false
	}
	return false
}
func (r *Reconciler) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil && r.logger != nil {
			r.logger.Warn("admission repair sweep failed; retry retained", zap.String("result", "unknown"))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

var compareDeleteAdmissionScript = redis.NewScript(`local current=redis.call('GET',KEYS[1]);if current==ARGV[1] then return redis.call('DEL',KEYS[1]) end;return 0`)

func (r *RedisAdmission) ScanAdmissions(ctx context.Context, cursor uint64, count int64) ([]CapturedAdmission, uint64, error) {
	if r == nil || r.client == nil {
		return nil, cursor, fmt.Errorf("redis client is nil: %w", ErrInfrastructure)
	}
	if count < 1 || count > 512 {
		return nil, cursor, ErrInvalidRequest
	}
	r.scanMu.Lock()
	defer r.scanMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if len(r.pendingKeys) == 0 {
		keys, next, err := r.client.Scan(ctx, cursor, keyPrefix+":*", count).Result()
		if err != nil {
			return nil, cursor, fmt.Errorf("scan seat admissions: %w: %v", ErrInfrastructure, err)
		}
		r.pendingKeys = keys
		r.pendingCursor = next
	}
	if len(r.pendingKeys) == 0 {
		return nil, r.pendingCursor, nil
	}
	n := int(count)
	if n > len(r.pendingKeys) {
		n = len(r.pendingKeys)
	}
	keys := r.pendingKeys[:n]
	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, cursor, fmt.Errorf("read seat admissions: %w: %v", ErrInfrastructure, err)
	}
	r.pendingKeys = r.pendingKeys[n:]
	next := r.pendingCursor
	if len(r.pendingKeys) > 0 {
		next = cursor
	}
	result := make([]CapturedAdmission, 0, len(keys))
	for i, key := range keys {
		value, ok := values[i].(string)
		if !ok || value == "" {
			continue
		}
		parts := strings.Split(key, ":")
		if len(parts) != 4 {
			continue
		}
		event, err1 := uuid.Parse(parts[2])
		seat, err2 := uuid.Parse(parts[3])
		if err1 != nil || err2 != nil || event.String() != parts[2] || seat.String() != parts[3] {
			continue
		}
		owner, err := uuid.Parse(value)
		if err != nil || owner == uuid.Nil || owner.String() != value {
			continue
		}
		result = append(result, CapturedAdmission{Key: key, Value: value, EventID: event, SeatID: seat})
	}
	return result, next, nil
}
func (r *RedisAdmission) CompareDeleteAdmission(ctx context.Context, c CapturedAdmission) (bool, error) {
	if r == nil || r.client == nil {
		return false, fmt.Errorf("redis client is nil: %w", ErrInfrastructure)
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	n, err := compareDeleteAdmissionScript.Run(ctx, r.client, []string{c.Key}, c.Value).Int()
	if err != nil {
		return false, fmt.Errorf("compare delete admission: %w: %v", ErrInfrastructure, err)
	}
	return n == 1, nil
}
