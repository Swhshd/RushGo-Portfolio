package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *SagaService) replayRepository() (repository.ReplayRepository, error) {
	r, ok := s.repo.(repository.ReplayRepository)
	if !ok {
		return nil, repository.ErrRecoveryAttention
	}
	return r, nil
}

// ExecuteClaim performs bounded original-identity effects. Every network call is
// between committed local intent/result transactions; an expired owner stops.
func (s *SagaService) ExecuteClaim(ctx context.Context, c domain.SagaClaim) error {
	return s.executeClaim(ctx, c, 5*time.Second, 10)
}
func (s *SagaService) executeClaim(ctx context.Context, c domain.SagaClaim, remoteTimeout time.Duration, maxAttempts int) error {
	return s.executeClaimWithJitter(ctx, c, remoteTimeout, maxAttempts, defaultRetryJitter)
}
func (s *SagaService) executeClaimWithJitter(ctx context.Context, c domain.SagaClaim, remoteTimeout time.Duration, maxAttempts int, jitter func(time.Duration) time.Duration) error {
	r, err := s.replayRepository()
	if err != nil {
		return err
	}
	recordFailure := func(original error, category string, attention bool) error {
		return s.persistRecoveryFailure(c, original, category, attention, maxAttempts, jitter)
	}
	recordRepositoryFailure := func(failure error) error {
		if errors.Is(failure, repository.ErrRecoveryConflict) {
			return failure
		}
		if errors.Is(failure, repository.ErrPaidUnissuedBindingMissing) {
			return recordFailure(failure, "paid_unissued_binding_missing", true)
		}
		if errors.Is(failure, repository.ErrRecoveryAttention) {
			return recordFailure(failure, "invalid_persisted_graph", true)
		}
		return recordFailure(failure, "infrastructure_unknown", false)
	}
	for budget := 0; budget < 12; budget++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		v, err := s.repo.GetSagaById(readCtx, c.SagaID)
		readCancel()
		if err != nil {
			return recordFailure(err, "infrastructure_unknown", false)
		}
		c.Saga = v
		if v.Status == domain.SAGA_COMPLETED || v.Status == domain.SAGA_ROLLED_BACK || v.Status == domain.SAGA_WAIT_FOR_PAYMENT {
			return nil
		}
		h, err := s.reBuildSagaHandler(ctx, v, nil)
		if err != nil {
			return recordFailure(err, "invalid_persisted_graph", true)
		}
		index := v.CurrentStepIndex
		if v.Direction != c.Direction {
			return repository.ErrRecoveryConflict
		}
		if index < 0 || index >= len(v.Steps) {
			return recordFailure(repository.ErrRecoveryAttention, "invalid_persisted_graph", true)
		}
		step := v.Steps[index]
		owned, err := r.PersistStepIntent(ctx, c, step.ID, c.Direction)
		if err != nil {
			return recordRepositoryFailure(err)
		}
		if !owned {
			return repository.ErrRecoveryConflict
		}
		if err := s.awaitReliabilityStepBoundary(ctx, c, index, "before_effect"); err != nil {
			return err
		}
		remote, cancel := context.WithTimeout(ctx, remoteTimeout)
		if c.Direction == "forward" {
			err = step.Execute(remote)
		} else {
			err = step.Compensate(remote)
		}
		cancel()
		if err == nil && c.Direction == "forward" && index == 1 {
			// A durable read may already prove collection even when no webhook
			// arrived. Persist a wake before S1's pause result can consume it.
			remoteRead, readCancel := context.WithTimeout(ctx, remoteTimeout)
			p, readErr := s.financialAuthority(remoteRead, *v.Input)
			readCancel()
			if readErr != nil {
				err = readErr
			} else if p.ProviderOutcome == "collected" {
				id, _ := uuid.Parse(p.Id)
				err = r.WakeSaga(ctx, v.BookingID, v.Generation, &id, "s1_collected_read")
			}
		}
		if err != nil && c.Direction == "forward" {
			// Only a winning terminal Booking decision permits destructive work. An
			// uncertain S2/S3 call remains forward unless a fresh read proves a loser.
			remoteRead, readCancel := context.WithTimeout(ctx, remoteTimeout)
			authority, readErr := s.bookingAuthority(remoteRead, *v.Input)
			readCancel()
			if readErr == nil && authority.Booking.Status != "CONFIRMED" {
				terminal := authority.Phase == "TERMINAL"
				definitive := errors.Is(err, errTerminal) || status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.NotFound
				if !terminal && definitive {
					kind := "fail"
					if authority.Phase == "CONFIRMING" {
						kind = "abort_confirmation"
					}
					remoteDecision, decisionCancel := context.WithTimeout(ctx, remoteTimeout)
					authority, readErr = s.decide(remoteDecision, *v.Input, authority, kind)
					decisionCancel()
					terminal = readErr == nil && authority.Phase == "TERMINAL" && authority.Booking.Status != "CONFIRMED"
				}
				if terminal {
					next, switchErr := r.SwitchToCompensation(ctx, c)
					if switchErr != nil {
						return recordRepositoryFailure(switchErr)
					}
					c = *next
					continue
				}
			}
		}
		if err == nil {
			if err := s.awaitReliabilityStepBoundary(ctx, c, index, "effect_before_result"); err != nil {
				return err
			}
		}
		result := domain.StepResult{StepID: step.ID, Direction: c.Direction, State: "completed"}
		if err != nil {
			result.State = "retryable"
			result.ErrorCategory = "infrastructure_unknown"
			delay := retryDelay(v.FailureAttempts+1, jitter)
			result.NextAttemptAt = time.Now().Add(delay)
			if v.FailureAttempts+1 >= maxAttempts {
				result.ErrorCategory = "retry_age_attention"
			}
			if errors.Is(err, errFinancialWait) {
				result.ErrorCategory = "financial_wait"
				result.NextAttemptAt = time.Now().Add(5 * time.Second)
			}
			if errors.Is(err, repository.ErrRecoveryConflict) || errors.Is(err, repository.ErrRecoveryAttention) {
				result.State = "attention"
				result.ErrorCategory = "invalid_authority"
			}
		}
		owned, persistErr := r.PersistStepResult(ctx, c, result)
		if persistErr != nil {
			return recordRepositoryFailure(persistErr)
		}
		if !owned {
			return repository.ErrRecoveryConflict
		}
		if err != nil {
			return err
		}
		if c.Direction == "forward" && (h.GetPaymentResponse() != nil || step.ShouldPauseForPayment) {
			return nil
		}
	}
	return repository.ErrRecoveryConflict
}

func (s *SagaService) persistRecoveryFailure(c domain.SagaClaim, original error, category string, attention bool, maxAttempts int, jitter func(time.Duration) time.Duration) error {
	r, err := s.replayRepository()
	if err != nil {
		return errors.Join(original, err)
	}
	attempts := 1
	if c.Saga != nil {
		attempts = c.Saga.FailureAttempts + 1
	}
	delay := retryDelay(attempts, jitter)
	if attention {
		delay = time.Minute
	} else if attempts >= maxAttempts {
		category = "retry_age_attention"
		delay = time.Minute
	}
	failureCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	owned, persistErr := r.PersistClaimFailure(failureCtx, c, category, time.Now().Add(delay), attention)
	if persistErr == nil && !owned {
		persistErr = repository.ErrRecoveryConflict
	}
	return errors.Join(original, persistErr)
}

func retryDelay(attempts int, jitter func(time.Duration) time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	shift := min(max(attempts-1, 0), 7)
	delay := min(time.Minute, 500*time.Millisecond*time.Duration(1<<shift))
	if jitter != nil {
		delay = jitter(delay)
	}
	if delay < 0 {
		return 0
	}
	return min(time.Minute, delay)
}

func (s *SagaService) SettleOrderSaga(ctx context.Context, booking uuid.UUID, generation int64, decision, payment, reason string) (*StartOrderSagaResponse, error) {
	if booking == uuid.Nil || generation != 1 || reason == "" {
		return nil, repository.ErrInvalidRecoveryInput
	}
	r, err := s.replayRepository()
	if err != nil {
		return nil, err
	}
	v, err := s.repo.GetSagaByBookingId(ctx, booking)
	if errors.Is(err, repository.ErrNotFound) {
		// Terminal dispatch can beat initial Start delivery. Build the immutable
		// graph from Booking authority first, then execute only close/resolution.
		b, readErr := s.bookingClient.GetLifecycleDisposition(ctx, &bookingv1.LifecycleRequest{BookingId: booking.String(), Generation: generation})
		if readErr != nil {
			return nil, readErr
		}
		if b == nil || b.Booking == nil || b.ReservationDeadline == nil || b.ReservationDeadline.CheckValid() != nil {
			return nil, repository.ErrRecoveryAttention
		}
		if b.Phase != "TERMINAL" {
			return nil, repository.ErrRecoveryConflict
		}
		deadline := b.ReservationDeadline.AsTime()
		var seats []string
		for _, item := range b.Booking.Items {
			if item == nil {
				return nil, repository.ErrRecoveryAttention
			}
			seats = append(seats, item.SeatIds...)
		}
		in, _, inputErr := s.startInput(ctx, &StartOrderSagaRequest{BookingId: booking.String(), UserId: b.Booking.UserId, EventID: b.Booking.EventId, SeatIds: seats, TotalCents: int(b.Booking.TotalAmountCents), LifecycleGeneration: generation, BookingVersion: b.Version, ReservationDeadline: &deadline, Currency: b.Currency, BookingMode: b.BookingMode})
		if inputErr != nil {
			return nil, inputErr
		}
		v, _, err = r.CreateOrGetSaga(ctx, in, initialSteps())
	}
	if err != nil {
		return nil, err
	}
	if v.Input == nil || !v.RecoveryManaged {
		return nil, repository.ErrRecoveryAttention
	}
	b, err := s.bookingAuthority(ctx, *v.Input)
	if err != nil {
		return nil, err
	}
	if decision != "" && b.DecisionId != decision {
		return nil, repository.ErrRecoveryConflict
	}
	var paymentID, obligation *uuid.UUID
	p, err := s.financialAuthority(ctx, *v.Input)
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	if err == nil {
		id, parseErr := uuid.Parse(p.Id)
		if parseErr != nil {
			return nil, repository.ErrRecoveryConflict
		}
		paymentID = &id
		if payment != "" && payment != p.Id {
			return nil, repository.ErrRecoveryConflict
		}
		if b.Phase == "TERMINAL" && b.Booking.Status != "CONFIRMED" && p.ProviderOutcome == "collected" {
			// Payment freezes one full refund per generation and terminal decision. This
			// projection is the same immutable obligation across subsequent wakeups.
			id = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:saga-refund:"+p.Id+":g1:"+b.DecisionId))
			obligation = &id
		}
	} else if payment != "" {
		return nil, repository.ErrRecoveryConflict
	}
	terminal := b.Phase == "TERMINAL" && b.Booking.Status != "CONFIRMED"
	if err = r.SettleSaga(ctx, booking, generation, paymentID, obligation, terminal, reason); err != nil {
		return nil, err
	}
	v, err = s.repo.GetSagaById(ctx, v.ID)
	if err != nil {
		return nil, err
	}
	return &StartOrderSagaResponse{SagaId: v.ID.String(), Status: string(v.Status), Message: "durable settlement accepted"}, nil
}
