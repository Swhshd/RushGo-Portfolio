package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/admission"
)

type admissionCoordinatorRepositoryFake struct {
	permit      admission.Permit
	getErr      error
	grantPermit admission.Permit
	grantErr    error
	claims      []repository.AdmissionRecoveryClaim
	claimLimit  int
	claimErr    error
	settled     []admission.State
	settleErr   error
	scheduled   int
	scheduleErr error
	grantCalls  int
}

func (r *admissionCoordinatorRepositoryFake) GetAdmission(_ context.Context, _ admission.Identity) (admission.Permit, error) {
	if r.getErr != nil {
		return admission.Permit{}, r.getErr
	}
	return r.permit, nil
}

func (r *admissionCoordinatorRepositoryFake) GrantAdmission(_ context.Context, _ admission.Identity, _ domain.AdmissionProviderEvidence) (admission.Permit, error) {
	r.grantCalls++
	return r.grantPermit, r.grantErr
}

func (r *admissionCoordinatorRepositoryFake) SettleAdmission(_ context.Context, _ admission.Identity, _ uuid.UUID, _ int64, state admission.State) (admission.Permit, error) {
	r.settled = append(r.settled, state)
	return r.permit, r.settleErr
}

func (r *admissionCoordinatorRepositoryFake) ScheduleAdmissionRetry(context.Context, admission.Identity, uuid.UUID, int64, int, string) error {
	r.scheduled++
	return r.scheduleErr
}

func (r *admissionCoordinatorRepositoryFake) ClaimDueAdmissions(_ context.Context, limit int) ([]repository.AdmissionRecoveryClaim, error) {
	r.claimLimit = limit
	return r.claims, r.claimErr
}

type admissionEligibilityFake struct {
	evidence domain.AdmissionProviderEvidence
	calls    int
}

func (f *admissionEligibilityFake) CheckAdmissionFinancialEligibility(context.Context, uuid.UUID, uuid.UUID, int64) domain.AdmissionProviderEvidence {
	f.calls++
	return f.evidence
}

type admissionOutcomeResolverFake struct {
	outcome  admission.Outcome
	err      error
	calls    int
	identity admission.Identity
	token    uuid.UUID
	fence    int64
	abort    bool
}

func (f *admissionOutcomeResolverFake) ResolveAdmission(_ context.Context, identity admission.Identity, token uuid.UUID, fence int64, abort bool) (admission.Outcome, error) {
	f.calls++
	f.identity, f.token, f.fence, f.abort = identity, token, fence, abort
	return f.outcome, f.err
}

func admissionCoordinatorIdentity() admission.Identity {
	actor := uuid.New()
	var keyDigest, fingerprint [32]byte
	keyDigest[0], fingerprint[0] = 1, 2
	return admission.Identity{
		RequestID: admission.RequestIDFor(actor, keyDigest), PaymentID: uuid.New(), BookingID: uuid.New(),
		TicketID: uuid.New(), EventID: uuid.New(), UserID: uuid.New(), ActorID: actor,
		Generation: 1, KeyDigest: keyDigest, Fingerprint: fingerprint,
	}
}

func admissionPermitFor(identity admission.Identity, state admission.State) admission.Permit {
	now := time.Now().UTC()
	return admission.Permit{Identity: identity, Token: uuid.New(), Fence: 1, State: state,
		Source: "stripe_test_api", ObservedAt: now, CreatedAt: now, NextAttemptAt: now.Add(time.Second)}
}

func eligibleAdmissionEvidence() domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{EligibilityState: domain.AdmissionEligibilityEligible,
		DisputeState: domain.DisputeEvidenceNoDispute, RefundState: domain.RefundEvidenceNoRefund,
		ObservedAt: time.Now().UTC(), Source: "stripe_test_api"}
}

func TestPrepareAdmissionReturnsKnownPermitBeforeProviderOrResolver(t *testing.T) {
	identity := admissionCoordinatorIdentity()
	want := admissionPermitFor(identity, admission.Pending)
	repo := &admissionCoordinatorRepositoryFake{permit: want}
	eligibility := &admissionEligibilityFake{evidence: eligibleAdmissionEvidence()}
	resolver := &admissionOutcomeResolverFake{}
	coordinator := NewAdmissionCoordinator(repo, eligibility, resolver)

	got, err := coordinator.PrepareAdmission(context.Background(), identity)
	if err != nil || got != want {
		t.Fatalf("PrepareAdmission = %+v, %v; want stored permit %+v", got, err, want)
	}
	if eligibility.calls != 0 || resolver.calls != 0 || repo.grantCalls != 0 {
		t.Fatalf("known permit caused new decision work: eligibility=%d resolver=%d grants=%d", eligibility.calls, resolver.calls, repo.grantCalls)
	}
}

func TestPrepareAdmissionRequiresPersistedPendingIntentAndFreshEligibleEvidence(t *testing.T) {
	identity := admissionCoordinatorIdentity()
	want := admissionPermitFor(identity, admission.Pending)
	resolver := &admissionOutcomeResolverFake{outcome: admission.Outcome{Identity: identity, State: admission.Pending}}
	eligibility := &admissionEligibilityFake{evidence: eligibleAdmissionEvidence()}
	repo := &admissionCoordinatorRepositoryFake{getErr: admission.ErrNotFound, grantPermit: want}
	coordinator := NewAdmissionCoordinator(repo, eligibility, resolver)

	got, err := coordinator.PrepareAdmission(context.Background(), identity)
	if err != nil || got != want {
		t.Fatalf("eligible prepare = %+v, %v; want grant %+v", got, err, want)
	}
	if resolver.calls != 1 || resolver.identity != identity || resolver.token != uuid.Nil || resolver.fence != 0 || resolver.abort {
		t.Fatalf("Booking proof call = %+v; want exact pending-intent proof with no token and no abort", resolver)
	}
	if eligibility.calls != 1 || repo.grantCalls != 1 {
		t.Fatalf("new grant work counts: eligibility=%d grants=%d; want one each", eligibility.calls, repo.grantCalls)
	}
}

func TestPrepareAdmissionFailsClosedOnNonPendingIntentOrStaleEvidence(t *testing.T) {
	for _, test := range []struct {
		name     string
		outcome  admission.Outcome
		evidence domain.AdmissionProviderEvidence
		wantErr  error
	}{
		{name: "booking intent is already terminal", outcome: admission.Outcome{State: admission.Aborted}, evidence: eligibleAdmissionEvidence(), wantErr: admission.ErrBlocked},
		{name: "provider evidence is stale", outcome: admission.Outcome{State: admission.Pending}, evidence: func() domain.AdmissionProviderEvidence {
			e := eligibleAdmissionEvidence()
			e.ObservedAt = time.Now().Add(-11 * time.Second)
			return e
		}(), wantErr: admission.ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := admissionCoordinatorIdentity()
			test.outcome.Identity = identity
			repo := &admissionCoordinatorRepositoryFake{getErr: admission.ErrNotFound}
			resolver := &admissionOutcomeResolverFake{outcome: test.outcome}
			eligibility := &admissionEligibilityFake{evidence: test.evidence}
			coordinator := NewAdmissionCoordinator(repo, eligibility, resolver)

			_, err := coordinator.PrepareAdmission(context.Background(), identity)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("PrepareAdmission error=%v; want %v", err, test.wantErr)
			}
			if repo.grantCalls != 0 {
				t.Fatalf("unsafe proof/evidence reached grant repository %d times", repo.grantCalls)
			}
		})
	}
}

func TestReconcileAdmissionRejectsOutcomeForDifferentTokenOrFence(t *testing.T) {
	identity := admissionCoordinatorIdentity()
	permit := admissionPermitFor(identity, admission.Pending)
	repo := &admissionCoordinatorRepositoryFake{permit: permit}
	resolver := &admissionOutcomeResolverFake{outcome: admission.Outcome{Identity: identity, Token: uuid.New(), Fence: permit.Fence, State: admission.Committed}}
	coordinator := NewAdmissionCoordinator(repo, &admissionEligibilityFake{}, resolver)

	_, err := coordinator.ReconcileAdmission(context.Background(), identity, permit.Token, permit.Fence)
	if !errors.Is(err, admission.ErrStale) {
		t.Fatalf("ReconcileAdmission error=%v; want stale outcome rejection", err)
	}
	if resolver.token != permit.Token || resolver.fence != permit.Fence || resolver.abort || len(repo.settled) != 0 {
		t.Fatalf("reconcile did not bind to exact permit: resolver=%+v settled=%v", resolver, repo.settled)
	}
}

func TestAdmissionRecoveryUsesBoundedBatchAndOnlySettlesDurableTerminalOutcome(t *testing.T) {
	identity := admissionCoordinatorIdentity()
	permit := admissionPermitFor(identity, admission.Pending)
	repo := &admissionCoordinatorRepositoryFake{claims: []repository.AdmissionRecoveryClaim{{Permit: permit, Attempt: 3}}}
	resolver := &admissionOutcomeResolverFake{outcome: admission.Outcome{Identity: identity, Token: permit.Token, Fence: permit.Fence, State: admission.Pending}}
	coordinator := NewAdmissionCoordinator(repo, &admissionEligibilityFake{}, resolver)

	processed, err := coordinator.RunAdmissionRecoveryOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("recovery processed=%d err=%v; want one processed claim", processed, err)
	}
	if repo.claimLimit != 16 || !resolver.abort || len(repo.settled) != 0 || repo.scheduled != 1 {
		t.Fatalf("recovery did not stay bounded and pending: limit=%d abort=%v settled=%v scheduled=%d", repo.claimLimit, resolver.abort, repo.settled, repo.scheduled)
	}

	repo.claims = []repository.AdmissionRecoveryClaim{{Permit: permit, Attempt: 4}}
	resolver.outcome = admission.Outcome{Identity: identity, Token: permit.Token, Fence: permit.Fence, State: admission.Aborted}
	processed, err = coordinator.RunAdmissionRecoveryOnce(context.Background())
	if err != nil || processed != 1 || len(repo.settled) != 1 || repo.settled[0] != admission.Aborted {
		t.Fatalf("verified abort recovery processed=%d err=%v settled=%v", processed, err, repo.settled)
	}
}
