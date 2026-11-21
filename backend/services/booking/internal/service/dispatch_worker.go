package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DispatchConfig struct {
	ScanInterval, LeaseTTL, RemoteTimeout time.Duration
	BatchSize, Concurrency, MaxAttempts   int
}

func DefaultDispatchConfig() DispatchConfig {
	return DispatchConfig{time.Second, 30 * time.Second, 5 * time.Second, 16, 4, 10}
}
func DispatchConfigFromEnv() (DispatchConfig, error) {
	c := DefaultDispatchConfig()
	for name, dst := range map[string]*time.Duration{"RELIABILITY_SCAN_INTERVAL": &c.ScanInterval, "RELIABILITY_LEASE_TTL": &c.LeaseTTL, "RELIABILITY_REMOTE_TIMEOUT": &c.RemoteTimeout} {
		if raw := os.Getenv(name); raw != "" {
			v, err := time.ParseDuration(raw)
			if err != nil {
				return c, err
			}
			*dst = v
		}
	}
	for name, dst := range map[string]*int{"RELIABILITY_BATCH_SIZE": &c.BatchSize, "RELIABILITY_CONCURRENCY": &c.Concurrency, "RELIABILITY_MAX_ATTEMPTS": &c.MaxAttempts} {
		if raw := os.Getenv(name); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil {
				return c, err
			}
			*dst = v
		}
	}
	return c, c.Validate()
}
func (c DispatchConfig) Validate() error {
	if c.ScanInterval <= 0 || c.LeaseTTL < time.Second || c.LeaseTTL > time.Minute || c.RemoteTimeout <= 0 || c.RemoteTimeout+3*time.Second >= c.LeaseTTL || c.BatchSize < 1 || c.BatchSize > 16 || c.Concurrency < 1 || c.Concurrency > 4 || c.MaxAttempts < 1 {
		return fmt.Errorf("invalid Booking dispatch configuration")
	}
	return nil
}

type dispatchLeaseRepository interface {
	RenewDispatch(context.Context, domain.DispatchClaim, time.Duration) (bool, error)
	ClaimBookingDispatch(context.Context, uuid.UUID, time.Duration) (*domain.DispatchClaim, error)
}
type DispatchWorker struct {
	repo    repository.LifecycleRepository
	service *BookingService
	logger  *zap.Logger
	config  DispatchConfig
	mu      sync.Mutex
	ticker  func(time.Duration) (<-chan time.Time, func())
	jitter  func(time.Duration) time.Duration
}

// ConfigureDispatch is called by the owning process before requests/workers run.
func (s *BookingService) ConfigureDispatch(c DispatchConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	s.dispatchConfig = c
	s.dispatchSlots = make(chan struct{}, c.Concurrency)
	return nil
}
func (s *BookingService) acquireDispatchSlot() bool {
	select {
	case s.dispatchSlots <- struct{}{}:
		return true
	default:
		return false
	}
}
func (s *BookingService) releaseDispatchSlot() { <-s.dispatchSlots }

func (w *DispatchWorker) awaitReliabilityDispatchBoundary(ctx context.Context, c domain.DispatchClaim) error {
	if w == nil || w.service == nil || w.service.reliabilityHook == nil || !w.service.reliabilityHook.Enabled() {
		return nil
	}
	if c.Kind != "start" || c.Snapshot.Booking == nil || !c.Snapshot.Booking.Managed {
		return nil
	}
	if c.Snapshot.Booking.ID == uuid.Nil || !domain.ValidBookingRequestID(c.Snapshot.Booking.RequestID) ||
		c.Token == uuid.Nil || c.Fence < 1 || c.LeaseUntil.IsZero() || !c.LeaseUntil.After(time.Now()) {
		return fmt.Errorf("enabled Booking dispatch hook has an incomplete managed claim identity")
	}
	facts := map[string]string{
		"booking_id":     c.Snapshot.Booking.ID.String(),
		"request_id":     c.Snapshot.Booking.RequestID,
		"dispatch_state": "claimed",
		"dispatch_fence": strconv.FormatInt(c.Fence, 10),
	}
	const checkpoint = "booking_dispatch_claimed_before_saga_start"
	windowIDs := []string{
		"booking.commit_before_saga_dispatch",
		"booking.saga_ack_lost",
	}
	for step := 0; step < 5; step++ {
		for _, phase := range []string{"before_effect", "effect_before_result", "result_before_next"} {
			windowIDs = append(windowIDs, fmt.Sprintf("saga.forward.S%d.%s", step, phase))
			if step < 4 {
				windowIDs = append(windowIDs, fmt.Sprintf("saga.compensation.S%d.%s", step, phase))
			}
		}
	}
	for _, windowID := range windowIDs {
		_, _, err := w.service.reliabilityHook.Await(ctx, reliabilityhook.Boundary{
			WindowID: windowID, OperationID: c.Snapshot.Booking.RequestID,
			Checkpoint: checkpoint, Action: reliabilityhook.ActionPause, Facts: facts,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func NewDispatchWorker(r repository.LifecycleRepository, s *BookingService, logger *zap.Logger, configs ...DispatchConfig) (*DispatchWorker, error) {
	if r == nil || s == nil || logger == nil {
		return nil, fmt.Errorf("missing dispatch dependency")
	}
	c := s.dispatchConfig
	if len(configs) > 1 {
		return nil, fmt.Errorf("invalid dispatch config count")
	}
	if len(configs) == 1 {
		c = configs[0]
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c != s.dispatchConfig {
		return nil, fmt.Errorf("dispatch configuration differs from shared service budget")
	}
	if _, ok := r.(dispatchLeaseRepository); !ok {
		return nil, fmt.Errorf("missing dispatch lease support")
	}
	return &DispatchWorker{repo: r, service: s, logger: logger, config: c, ticker: func(interval time.Duration) (<-chan time.Time, func()) {
		t := time.NewTicker(interval)
		return t.C, t.Stop
	}, jitter: func(delay time.Duration) time.Duration {
		return min(time.Minute, time.Duration(float64(delay)*(0.8+rand.Float64()*0.4)))
	}}, nil
}
func (s *BookingService) dispatchRPC(ctx context.Context, c domain.DispatchClaim) (*sagav1.OrderSagaResponse, error) {
	v := c.Snapshot
	if v.Booking == nil || v.Generation != 1 || v.ReservationDeadline == nil {
		return nil, repository.ErrInvalidBookingRequest
	}
	if c.Kind == "settle" {
		if v.Phase != "TERMINAL" || v.DecisionID == nil || v.Booking.Status == domain.StatusConfirmed {
			return nil, repository.ErrLifecycleConflict
		}
		return s.sagaClient.SettleOrderSaga(ctx, &sagav1.SettleOrderSagaRequest{BookingId: v.Booking.ID.String(), Generation: 1, DecisionId: v.DecisionID.String(), Reason: "booking_terminal_dispatch"})
	}
	var seats []string
	for _, item := range v.Booking.Items {
		for _, id := range item.SeatIDs {
			seats = append(seats, id.String())
		}
	}
	return s.sagaClient.StartOrderSaga(ctx, &sagav1.StartOrderSagaRequest{BookingId: v.Booking.ID.String(), UserId: v.Booking.UserID.String(), EventId: v.Booking.EventID.String(), SeatIds: seats, TotalCents: int32(v.Booking.TotalAmountCents), LifecycleGeneration: 1, BookingVersion: v.Booking.Version, ReservationDeadline: timestamppb.New(*v.ReservationDeadline), BookingMode: v.BookingMode, Currency: v.Currency})
}
func (w *DispatchWorker) execute(ctx context.Context, c domain.DispatchClaim) (*sagav1.OrderSagaResponse, error) {
	leaseRepo := w.repo.(dispatchLeaseRepository)
	owned, err := leaseRepo.RenewDispatch(ctx, c, w.config.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if !owned {
		return nil, repository.ErrLifecycleConflict
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		ticks, stop := w.ticker(w.config.LeaseTTL / 3)
		defer stop()
		for {
			select {
			case <-workCtx.Done():
				done <- nil
				return
			case <-ticks:
				owned, err := leaseRepo.RenewDispatch(workCtx, c, w.config.LeaseTTL)
				if err != nil || !owned {
					if workCtx.Err() != nil {
						done <- nil
						return
					}
					cancel()
					if err == nil {
						err = repository.ErrLifecycleConflict
					}
					done <- err
					return
				}
			}
		}
	}()
	if err := w.awaitReliabilityDispatchBoundary(workCtx, c); err != nil {
		cancel()
		return nil, errors.Join(err, <-done)
	}
	remote, remoteCancel := context.WithTimeout(workCtx, w.config.RemoteTimeout)
	response, callErr := w.service.dispatchRPC(remote, c)
	remoteCancel()
	var sagaID uuid.UUID
	if callErr == nil {
		if response == nil {
			callErr = fmt.Errorf("missing Saga response")
		} else {
			sagaID, callErr = uuid.Parse(response.SagaId)
			if callErr == nil && sagaID == uuid.Nil {
				callErr = fmt.Errorf("missing Saga identity")
			}
		}
	}
	category := ""
	if callErr != nil {
		category = "saga_response_unknown"
	}
	// Scheduling uses the count owned in the durable snapshot, never fresh IDs.
	attempts := c.Snapshot.DispatchAttempts
	delay := min(time.Minute, 500*time.Millisecond*time.Duration(1<<min(max(attempts-1, 0), 7)))
	delay = w.jitter(delay)
	if callErr != nil && attempts >= w.config.MaxAttempts {
		category = "retry_age_attention"
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 3*time.Second)
	owned, finishErr := w.repo.FinishDispatch(finishCtx, c, sagaID, callErr == nil, category, time.Now().Add(delay))
	finishCancel()
	cancel()
	renewErr := <-done
	if !owned && finishErr == nil {
		finishErr = repository.ErrLifecycleConflict
	}
	return response, errors.Join(callErr, finishErr, renewErr)
}
func (w *DispatchWorker) RunOnce(ctx context.Context, limit int) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if limit < 1 || limit > 16 {
		return 0, repository.ErrInvalidBookingRequest
	}
	handled := 0
	var first error
	for handled < limit && ctx.Err() == nil {
		free := 0
		for free < min(w.config.Concurrency, limit-handled) && w.service.acquireDispatchSlot() {
			free++
		}
		if free == 0 {
			break
		}
		claims, err := w.repo.ClaimDispatch(ctx, free, w.config.LeaseTTL)
		for i := len(claims); i < free; i++ {
			w.service.releaseDispatchSlot()
		}
		if err != nil {
			for range claims {
				w.service.releaseDispatchSlot()
			}
			return handled, errors.Join(first, err)
		}
		if len(claims) == 0 {
			break
		}
		results := make(chan error, len(claims))
		for _, c := range claims {
			go func(c domain.DispatchClaim) {
				defer w.service.releaseDispatchSlot()
				_, err := w.execute(ctx, c)
				results <- err
			}(c)
		}
		for range claims {
			err := <-results
			if err != nil && !errors.Is(err, repository.ErrLifecycleConflict) && first == nil {
				first = err
			}
			handled++
		}
	}
	return handled, first
}
func (w *DispatchWorker) Start(ctx context.Context) error {
	ticks, stop := w.ticker(w.config.ScanInterval)
	defer stop()
	for {
		if _, err := w.RunOnce(ctx, w.config.BatchSize); err != nil && ctx.Err() == nil {
			w.logger.Warn("Booking dispatch retained retry responsibility")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
		}
	}
}

func (s *BookingService) recoverBookingResponse(ctx context.Context, v *domain.LifecycleSnapshot) (*CreateBookingResponse, error) {
	response := &CreateBookingResponse{Booking: v.Booking, CreationState: "starting", RetryAfterSeconds: 1}
	if v.Phase == "TERMINAL" {
		response.CreationState = "terminal"
		response.RetryAfterSeconds = 0
		return response, nil
	}
	var secret string
	if !s.acquireDispatchSlot() {
		return response, nil
	}
	defer s.releaseDispatchSlot()
	if v.DispatchState == "accepted" {
		remote, cancel := context.WithTimeout(ctx, s.dispatchConfig.RemoteTimeout)
		result, err := s.dispatchRPC(remote, domain.DispatchClaim{Kind: "start", Snapshot: *v})
		cancel()
		if err == nil && result != nil {
			if id, err := uuid.Parse(result.SagaId); err != nil || id == uuid.Nil {
				return response, nil
			}
			secret = result.PaymentIntentClientSecret
		}
	} else if r, ok := s.bookingRepo.(dispatchLeaseRepository); ok {
		c, err := r.ClaimBookingDispatch(ctx, v.Booking.ID, s.dispatchConfig.LeaseTTL)
		if err != nil {
			return nil, err
		}
		if c != nil {
			w, err := NewDispatchWorker(s.bookingRepo, s, s.logger)
			if err != nil {
				return nil, err
			}
			result, executionErr := w.execute(ctx, *c)
			if executionErr == nil && result != nil {
				secret = result.PaymentIntentClientSecret
			}
		}
	}
	current, err := s.bookingRepo.GetLifecycle(ctx, domain.LifecycleRef{BookingID: v.Booking.ID, Generation: 1})
	if err != nil {
		return nil, err
	}
	response.Booking = current.Booking
	if current.Phase == "TERMINAL" {
		response.CreationState = "terminal"
		response.RetryAfterSeconds = 0
		return response, nil
	}
	if secret != "" && current.ReservationDeadline != nil && current.ReservationDeadline.After(time.Now()) {
		response.PaymentClientSecret = secret
		response.CreationState = "ready"
		response.RetryAfterSeconds = 0
	}
	return response, nil
}
