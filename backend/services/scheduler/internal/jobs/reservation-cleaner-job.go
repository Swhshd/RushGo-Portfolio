package cronjob

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	pkgredis "github.com/ticketbox/pkg/redis"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type bookingExpiryClient interface {
	ListExpiryCandidates(context.Context, *bookingv1.ListExpiryCandidatesRequest, ...grpc.CallOption) (*bookingv1.ListExpiryCandidatesResponse, error)
	GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error)
	DecideLifecycle(context.Context, *bookingv1.LifecycleDecisionRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error)
}
type sagaSettlementClient interface {
	SettleOrderSaga(context.Context, *sagav1.SettleOrderSagaRequest, ...grpc.CallOption) (*sagav1.OrderSagaResponse, error)
}
type ReservationCleanerJob struct {
	bookingClient bookingExpiryClient
	sagaClient    sagaSettlementClient
	redisLock     *pkgredis.RedisClient
	logger        *zap.Logger
	metrics       *observability.Metrics
	mu            sync.Mutex
	cursor        string
}

// Legacy dependency arguments remain source compatible. Booking's terminal
// dispatch owns durable settlement; no independent Payment or Event mutation.
func NewReservationCleanerJob(_ eventv1.EventServiceClient, b bookingExpiryClient, _ paymentv1.PaymentServiceClient, s sagaSettlementClient, lock *pkgredis.RedisClient, logger *zap.Logger, metrics ...*observability.Metrics) *ReservationCleanerJob {
	r := &ReservationCleanerJob{bookingClient: b, sagaClient: s, redisLock: lock, logger: logger}
	if len(metrics) > 0 {
		r.metrics = metrics[0]
	}
	return r
}
func (r *ReservationCleanerJob) Name() string { return "reservation-cleaner-job" }
func (r *ReservationCleanerJob) Run(ctx context.Context) (result error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	started := time.Now()
	ctx, span := otel.Tracer("github.com/ticketbox/scheduler").Start(ctx, "scheduler.reservation_cleanup")
	defer span.End()
	defer func() {
		if r.metrics != nil {
			state := "success"
			if result != nil {
				state = "failed"
			}
			r.metrics.SchedulerRuns.Add(ctx, 1, metric.WithAttributes(attribute.String("result", state)))
			r.metrics.SchedulerDuration.Record(ctx, time.Since(started).Seconds())
		}
	}()
	if r.redisLock != nil {
		lockCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		lock, err := r.redisLock.AcquireLock(lockCtx, r.Name(), 30*time.Second)
		stop()
		if errors.Is(err, pkgredis.ErrLockAlreadyAcquire) {
			return nil
		}
		if lock != nil {
			defer func() {
				release, stop := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
				defer stop()
				_ = lock.ReleaseLock(release)
			}()
		}
	}
	var failures []error
	after := r.cursor
	for page := 0; page < 4; page++ {
		rpcctx, stop := context.WithTimeout(ctx, 5*time.Second)
		candidates, err := r.bookingClient.ListExpiryCandidates(rpcctx, &bookingv1.ListExpiryCandidatesRequest{Limit: 16, AfterId: after})
		stop()
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if candidates == nil {
			return errors.New("nil authoritative expiry candidates")
		}
		for _, candidate := range candidates.Bookings {
			if candidate != nil && candidate.Booking != nil {
				r.cursor = candidate.Booking.Id
			}
			if err := r.expire(ctx, candidate); err != nil {
				failures = append(failures, err)
			}
		}
		if len(candidates.Bookings) < 16 {
			r.cursor = ""
			break
		}
		last := candidates.Bookings[len(candidates.Bookings)-1]
		if last == nil || last.Booking == nil {
			return errors.New("invalid expiry pagination")
		}
		after = last.Booking.Id
	}
	return errors.Join(failures...)
}
func (r *ReservationCleanerJob) expire(ctx context.Context, candidate *bookingv1.LifecycleDisposition) error {
	if candidate == nil || candidate.Booking == nil || candidate.Generation != 1 || candidate.Phase != "OPEN" {
		return errors.New("invalid expiry candidate")
	}
	id, err := uuid.Parse(candidate.Booking.Id)
	if err != nil || id == uuid.Nil {
		return errors.New("invalid expiry booking identity")
	}
	decision := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+id.String()+":g1:expire"))
	rpcctx, stop := context.WithTimeout(ctx, 5*time.Second)
	disposition, err := r.bookingClient.DecideLifecycle(rpcctx, &bookingv1.LifecycleDecisionRequest{BookingId: id.String(), Generation: 1, DecisionId: decision.String(), Kind: "expire", Reason: "reservation_deadline_elapsed", ExpectedVersion: candidate.Version})
	stop()
	if err != nil {
		// A lost response or CAS race must be resolved by Booking authority.
		rpcctx, stop = context.WithTimeout(ctx, 5*time.Second)
		disposition, err = r.bookingClient.GetLifecycleDisposition(rpcctx, &bookingv1.LifecycleRequest{BookingId: id.String(), Generation: 1})
		stop()
		if err != nil {
			return err
		}
	}
	if disposition == nil || disposition.Booking == nil || disposition.Booking.Id != id.String() {
		return errors.New("invalid Booking expiry response")
	}
	if disposition.Phase != "TERMINAL" || disposition.Booking.Status == "CONFIRMED" {
		return nil
	}
	if disposition.DecisionId == "" {
		return errors.New("terminal Booking lacks durable decision")
	}
	if r.metrics != nil && disposition.Booking.Status == "EXPIRED" {
		r.metrics.SchedulerExpiredBookings.Add(ctx, 1)
	}
	if r.sagaClient == nil {
		return nil
	} // durable Booking dispatcher remains responsible
	rpcctx, stop = context.WithTimeout(ctx, 5*time.Second)
	_, err = r.sagaClient.SettleOrderSaga(rpcctx, &sagav1.SettleOrderSagaRequest{BookingId: id.String(), Generation: 1, DecisionId: disposition.DecisionId, Reason: "scheduler_terminal_wake"})
	stop()
	if err != nil {
		return fmt.Errorf("durable settlement wake retained: %w", err)
	}
	return nil
}
