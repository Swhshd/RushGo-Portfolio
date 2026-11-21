package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/ticketbox/pkg/providerevidence"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/admission"
)

const (
	admissionEvidenceMaxAge = 10 * time.Second
	admissionRPCDeadline    = 5 * time.Second
	admissionProviderLimit  = 15 * time.Second
	admissionRecoveryBatch  = 16
)

// AdmissionEligibilityChecker must derive evidence from Payment's managed
// financial record and its configured provider. Callers cannot supply an
// eligibility decision.
type AdmissionEligibilityChecker interface {
	CheckAdmissionFinancialEligibility(context.Context, uuid.UUID, uuid.UUID, int64) domain.AdmissionProviderEvidence
}

// AdmissionCoordinator owns Payment permits and resolves Booking outcomes.
// It performs all RPC/provider work outside repository transactions.
type AdmissionCoordinator struct {
	repo        repository.AdmissionRepository
	eligibility AdmissionEligibilityChecker
	resolver    admission.OutcomeResolver
}

func NewAdmissionCoordinator(repo repository.AdmissionRepository, eligibility AdmissionEligibilityChecker, resolver admission.OutcomeResolver) *AdmissionCoordinator {
	return &AdmissionCoordinator{repo: repo, eligibility: eligibility, resolver: resolver}
}

var _ admission.Coordinator = (*AdmissionCoordinator)(nil)

func (c *AdmissionCoordinator) PrepareAdmission(ctx context.Context, identity admission.Identity) (admission.Permit, error) {
	if c == nil || c.repo == nil || c.eligibility == nil || c.resolver == nil {
		return admission.Permit{}, admission.ErrUnavailable
	}
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	known, err := c.repo.GetAdmission(ctx, identity)
	if err == nil {
		if err := known.ValidateFor(identity); err != nil {
			return admission.Permit{}, err
		}
		return known, nil
	}
	if !errors.Is(err, admission.ErrNotFound) {
		return admission.Permit{}, err
	}

	proofCtx, cancelProof := context.WithTimeout(ctx, admissionRPCDeadline)
	proof, err := c.resolver.ResolveAdmission(proofCtx, identity, uuid.Nil, 0, false)
	cancelProof()
	if err != nil {
		return admission.Permit{}, admissionResolverError("verify Booking intent", err)
	}
	if err := validateNewAdmissionIntent(identity, proof); err != nil {
		return admission.Permit{}, err
	}

	providerCtx, cancelProvider := context.WithTimeout(ctx, admissionProviderLimit)
	evidence := c.eligibility.CheckAdmissionFinancialEligibility(providerCtx, identity.PaymentID, identity.BookingID, identity.Generation)
	cancelProvider()
	if err := validateAdmissionProviderEvidence(evidence, time.Now().UTC()); err != nil {
		return admission.Permit{}, err
	}
	permit, err := c.repo.GrantAdmission(ctx, identity, evidence)
	if err != nil {
		return admission.Permit{}, err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	return permit, nil
}

func (c *AdmissionCoordinator) GetAdmission(ctx context.Context, identity admission.Identity) (admission.Permit, error) {
	if c == nil || c.repo == nil {
		return admission.Permit{}, admission.ErrUnavailable
	}
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	permit, err := c.repo.GetAdmission(ctx, identity)
	if err != nil {
		return admission.Permit{}, err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	return permit, nil
}

func (c *AdmissionCoordinator) ReconcileAdmission(ctx context.Context, identity admission.Identity, token uuid.UUID, fence int64) (admission.Permit, error) {
	if c == nil || c.repo == nil || c.resolver == nil {
		return admission.Permit{}, admission.ErrUnavailable
	}
	if err := identity.Validate(); err != nil {
		return admission.Permit{}, err
	}
	if token == uuid.Nil || fence <= 0 {
		return admission.Permit{}, admission.ErrStale
	}
	permit, err := c.repo.GetAdmission(ctx, identity)
	if err != nil {
		return admission.Permit{}, err
	}
	if err := permit.ValidateFor(identity); err != nil {
		return admission.Permit{}, err
	}
	if permit.Token != token || permit.Fence != fence {
		return admission.Permit{}, admission.ErrStale
	}
	if permit.State.Terminal() {
		return permit, nil
	}

	resolveCtx, cancel := context.WithTimeout(ctx, admissionRPCDeadline)
	outcome, err := c.resolver.ResolveAdmission(resolveCtx, identity, token, fence, false)
	cancel()
	if err != nil {
		return admission.Permit{}, admissionResolverError("read Booking outcome", err)
	}
	if err := validatePermitOutcome(identity, permit, outcome); err != nil {
		return admission.Permit{}, err
	}
	if outcome.State == admission.Pending {
		return permit, nil
	}
	return c.repo.SettleAdmission(ctx, identity, token, fence, outcome.State)
}

// RunAdmissionRecoveryOnce claims and processes at most sixteen due permits.
// A due permit has already waited through its initial recovery grace period;
// asking Booking to abort is durable and still resolves against the intent row.
func (c *AdmissionCoordinator) RunAdmissionRecoveryOnce(ctx context.Context) (int, error) {
	if c == nil || c.repo == nil || c.resolver == nil {
		return 0, admission.ErrUnavailable
	}
	claims, err := c.repo.ClaimDueAdmissions(ctx, admissionRecoveryBatch)
	if err != nil {
		return 0, err
	}
	var firstErr error
	for _, claim := range claims {
		permit := claim.Permit
		if err := permit.ValidateFor(permit.Identity); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		resolveCtx, cancel := context.WithTimeout(ctx, admissionRPCDeadline)
		outcome, resolveErr := c.resolver.ResolveAdmission(resolveCtx, permit.Identity, permit.Token, permit.Fence, true)
		cancel()
		if resolveErr != nil {
			if err := c.scheduleAdmissionRetry(ctx, permit, claim.Attempt, "booking_unavailable"); err != nil && firstErr == nil {
				firstErr = err
			} else if firstErr == nil {
				firstErr = admissionResolverError("recover Booking outcome", resolveErr)
			}
			continue
		}
		if err := validatePermitOutcome(permit.Identity, permit, outcome); err != nil {
			if scheduleErr := c.scheduleAdmissionRetry(ctx, permit, claim.Attempt, "booking_outcome_mismatch"); scheduleErr != nil && firstErr == nil {
				firstErr = scheduleErr
			} else if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if outcome.State == admission.Pending {
			if err := c.scheduleAdmissionRetry(ctx, permit, claim.Attempt, "booking_intent_pending"); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := c.repo.SettleAdmission(ctx, permit.Identity, permit.Token, permit.Fence, outcome.State); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(claims), firstErr
}

func (c *AdmissionCoordinator) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_, _ = c.RunAdmissionRecoveryOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *AdmissionCoordinator) scheduleAdmissionRetry(ctx context.Context, permit admission.Permit, attempt int, reason string) error {
	if attempt < 1 {
		attempt = 1
	}
	return c.repo.ScheduleAdmissionRetry(ctx, permit.Identity, permit.Token, permit.Fence, attempt, reason)
}

func validateNewAdmissionIntent(identity admission.Identity, outcome admission.Outcome) error {
	if outcome.Identity != identity {
		return admission.ErrConflict
	}
	if outcome.State != admission.Pending {
		return admission.ErrBlocked
	}
	if outcome.Token != uuid.Nil || outcome.Fence != 0 {
		return admission.ErrStale
	}
	return nil
}

func validatePermitOutcome(identity admission.Identity, permit admission.Permit, outcome admission.Outcome) error {
	if outcome.Identity != identity {
		return admission.ErrConflict
	}
	if outcome.Token != permit.Token || outcome.Fence != permit.Fence {
		return admission.ErrStale
	}
	if !outcome.State.Valid() {
		return admission.ErrUnavailable
	}
	return nil
}

func validateAdmissionProviderEvidence(evidence domain.AdmissionProviderEvidence, now time.Time) error {
	if evidence.EligibilityState == domain.AdmissionEligibilityDenied {
		return admission.ErrBlocked
	}
	if evidence.EligibilityState != domain.AdmissionEligibilityEligible || evidence.DisputeState != domain.DisputeEvidenceNoDispute ||
		evidence.RefundState != domain.RefundEvidenceNoRefund || !providerevidence.AllowedSource(evidence.Source) || evidence.ObservedAt.IsZero() {
		return admission.ErrUnavailable
	}
	age := now.Sub(evidence.ObservedAt)
	if age < 0 || age > admissionEvidenceMaxAge {
		return admission.ErrUnavailable
	}
	return nil
}

func admissionResolverError(operation string, err error) error {
	if errors.Is(err, admission.ErrInvalid) || errors.Is(err, admission.ErrConflict) || errors.Is(err, admission.ErrBlocked) ||
		errors.Is(err, admission.ErrNotFound) || errors.Is(err, admission.ErrStale) {
		return err
	}
	return fmt.Errorf("%w: %s", admission.ErrUnavailable, operation)
}
