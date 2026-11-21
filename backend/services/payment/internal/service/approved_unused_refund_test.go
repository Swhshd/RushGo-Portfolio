package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type approvedUnusedRefundRepository struct {
	repository.PaymentRepositoryInterface
	view       *domain.FinancialView
	accepted   int
	obligation domain.RefundObligation
	identity   repository.ApprovedUnusedRefundIdentity
	acceptErr  error
	getCalls   int
	getHook    func(int) *domain.FinancialView
}

func (r *approvedUnusedRefundRepository) GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error) {
	r.getCalls++
	if r.getHook != nil {
		return r.getHook(r.getCalls), nil
	}
	return r.view, nil
}

func (r *approvedUnusedRefundRepository) RequireApprovedUnusedRefund(_ context.Context, obligation domain.RefundObligation, identity repository.ApprovedUnusedRefundIdentity) (*domain.FinancialView, error) {
	r.accepted++
	r.obligation = obligation
	r.identity = identity
	return r.view, r.acceptErr
}

func TestGetUnusedRefundPaymentSnapshotOnlyChecksProviderEligibilityBeforeAnObligationExists(t *testing.T) {
	newEligibleView := func() *domain.FinancialView {
		paymentID, bookingID, buyerID := uuid.New(), uuid.New(), uuid.New()
		mode := false
		return &domain.FinancialView{
			Payment: &domain.Payment{ID: paymentID, UserId: buyerID, BookingId: &bookingID, Status: domain.PaymentSuccess,
				Price: 725, Currency: "usd", PaymentIntentId: "pi_unused_refund_fixture"},
			Generation: 1, Managed: true, ProviderAccount: "acct_test_fixture", LiveMode: &mode,
			ProviderOutcome: "collected", CreateState: "create_ready", CancelState: "not_requested",
			ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
		}
	}

	t.Run("no obligation returns fresh read-only eligibility", func(t *testing.T) {
		view := newEligibleView()
		repo := &approvedUnusedRefundRepository{view: view}
		provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityEligible,
			DisputeState:     domain.DisputeEvidenceNoDispute, RefundState: domain.RefundEvidenceNoRefund,
			ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
		}}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *view.Payment.BookingId, view.Payment.ID, 1, true)
		if err != nil || !eligible || got != view || provider.calls != 1 || repo.getCalls < 1 {
			t.Fatalf("snapshot=%+v eligible=%v providerCalls=%d localReads=%d err=%v", got, eligible, provider.calls, repo.getCalls, err)
		}
	})

	t.Run("existing decision skips provider and remains status-readable", func(t *testing.T) {
		view := newEligibleView()
		decisionID := uuid.New()
		view.RefundDecisionID = &decisionID
		view.RefundState, view.RefundAmountCents, view.RefundStatus = "provider_pending", 725, "pending"
		repo := &approvedUnusedRefundRepository{view: view}
		provider := &admissionEvidenceProvider{}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *view.Payment.BookingId, view.Payment.ID, 1, true)
		if err != nil || eligible || got != view || provider.calls != 0 || repo.getCalls < 1 {
			t.Fatalf("existing obligation snapshot=%+v eligible=%v providerCalls=%d localReads=%d err=%v", got, eligible, provider.calls, repo.getCalls, err)
		}
	})

	t.Run("refund added during provider read cancels eligibility", func(t *testing.T) {
		initial := newEligibleView()
		updated := *initial
		decisionID := uuid.New()
		updated.RefundDecisionID = &decisionID
		updated.RefundState, updated.RefundAmountCents = "pending", int64(initial.Payment.Price)
		updated.RefundReason = domain.RefundReasonConfirmedUnusedTicket
		repo := &approvedUnusedRefundRepository{view: initial}
		repo.getHook = func(call int) *domain.FinancialView {
			if call >= 3 {
				return &updated
			}
			return initial
		}
		provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityEligible,
			DisputeState:     domain.DisputeEvidenceNoDispute, RefundState: domain.RefundEvidenceNoRefund,
			ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
		}}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *initial.Payment.BookingId, initial.Payment.ID, 1, true)
		if err != nil || eligible || got != &updated || provider.calls != 1 || repo.getCalls < 3 {
			t.Fatalf("concurrent refund snapshot=%+v eligible=%v providerCalls=%d localReads=%d err=%v", got, eligible, provider.calls, repo.getCalls, err)
		}
	})

	t.Run("unknown provider evidence fails closed", func(t *testing.T) {
		view := newEligibleView()
		repo := &approvedUnusedRefundRepository{view: view}
		provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityUnknown,
			DisputeState:     domain.DisputeEvidenceUnknown, RefundState: domain.RefundEvidenceUnknown,
		}}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *view.Payment.BookingId, view.Payment.ID, 1, true)
		if err != nil || eligible || got != view || provider.calls != 1 {
			t.Fatalf("unknown provider evidence snapshot=%+v eligible=%v providerCalls=%d err=%v", got, eligible, provider.calls, err)
		}
	})

	t.Run("orphan refund reason fails closed without provider read", func(t *testing.T) {
		view := newEligibleView()
		view.RefundReason = domain.RefundReasonConfirmedUnusedTicket
		repo := &approvedUnusedRefundRepository{view: view}
		provider := &admissionEvidenceProvider{}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *view.Payment.BookingId, view.Payment.ID, 1, true)
		if err != nil || eligible || got != view || provider.calls != 0 {
			t.Fatalf("orphan refund reason snapshot=%+v eligible=%v providerCalls=%d err=%v", got, eligible, provider.calls, err)
		}
	})

	t.Run("status read is local only unless approval is requested", func(t *testing.T) {
		view := newEligibleView()
		repo := &approvedUnusedRefundRepository{view: view}
		provider := &admissionEvidenceProvider{evidence: domain.AdmissionProviderEvidence{
			EligibilityState: domain.AdmissionEligibilityEligible,
			DisputeState:     domain.DisputeEvidenceNoDispute, RefundState: domain.RefundEvidenceNoRefund,
			ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
		}}
		svc := &PaymentService{repo: repo, account: "acct_test_fixture", admission: provider}
		got, eligible, err := svc.GetUnusedRefundPaymentSnapshot(context.Background(), *view.Payment.BookingId, view.Payment.ID, 1, false)
		if err != nil || eligible || got != view || provider.calls != 0 || repo.getCalls < 1 {
			t.Fatalf("status snapshot=%+v eligible=%v providerCalls=%d localReads=%d err=%v", got, eligible, provider.calls, repo.getCalls, err)
		}
	})
}

type approvedUnusedRefundAuthorityStub struct {
	decision   *bookingv1.ApprovedUnusedRefundDecision
	err        error
	booking    uuid.UUID
	decisionID uuid.UUID
	calls      int
}

func (a *approvedUnusedRefundAuthorityStub) GetApprovedUnusedRefundDecision(_ context.Context, bookingID, decisionID uuid.UUID) (*bookingv1.ApprovedUnusedRefundDecision, error) {
	a.calls++
	a.booking, a.decisionID = bookingID, decisionID
	return a.decision, a.err
}

func TestAcceptApprovedUnusedRefundDecisionPersistsOnlyVerifiedFullUSDObligation(t *testing.T) {
	paymentID, bookingID, buyerID, eventID, decisionID, approverID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	mode := false
	view := &domain.FinancialView{
		Payment: &domain.Payment{ID: paymentID, UserId: buyerID, BookingId: &bookingID, Status: domain.PaymentSuccess,
			Price: 725, Currency: "usd", PaymentIntentId: "pi_unused_refund_fixture"},
		Generation: 1, Managed: true, ProviderAccount: "acct_test_fixture", LiveMode: &mode,
		ProviderOutcome: "collected", CreateState: "create_ready", CancelState: "not_requested",
		ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
	}
	eventStartsAt := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	eventQualifiedAt := eventStartsAt.Add(-time.Hour)
	approvedAt := eventStartsAt.Add(15 * time.Minute)
	decision := &bookingv1.ApprovedUnusedRefundDecision{
		RefundDecisionId: decisionID.String(), Status: "APPROVED", BuyerId: buyerID.String(), BookingId: bookingID.String(),
		EventId: eventID.String(), PaymentId: paymentID.String(), PaymentIntentId: view.Payment.PaymentIntentId,
		ProviderAccount: "acct_test_fixture", Livemode: false, Generation: 1, AmountCents: 725, Currency: "usd",
		ApprovedAt: timestamppb.New(approvedAt), EventStartsAt: timestamppb.New(eventStartsAt), ApproverId: approverID.String(),
		QualificationAttemptId: decisionID.String(), EventQualifiedAt: timestamppb.New(eventQualifiedAt), EventAuthorityVersion: "17",
	}
	repo := &approvedUnusedRefundRepository{view: view}
	authority := &approvedUnusedRefundAuthorityStub{decision: decision}
	svc := &PaymentService{repo: repo, account: "acct_test_fixture", booking: nil, approvedUnusedRefundDecisionReader: authority}

	got, err := svc.AcceptApprovedUnusedRefundDecision(context.Background(), bookingID, decisionID, paymentID, 1)
	if err != nil || got != view {
		t.Fatalf("accepted decision result=%p err=%v, want the persisted Payment view", got, err)
	}
	if authority.calls != 1 || authority.booking != bookingID || authority.decisionID != decisionID {
		t.Fatalf("Booking authority lookup was not bound to requested identity: %+v", authority)
	}
	if repo.accepted != 1 {
		t.Fatalf("RequireApprovedUnusedRefund calls=%d, want 1", repo.accepted)
	}
	if repo.obligation.PaymentID != paymentID || repo.obligation.DecisionID != decisionID || repo.obligation.Generation != 1 ||
		repo.obligation.AmountCents != 725 || repo.obligation.Currency != "usd" || repo.obligation.Reason != domain.RefundReasonConfirmedUnusedTicket ||
		repo.obligation.Key != "ticketbox:payment:"+paymentID.String()+":g1:refund" {
		t.Fatalf("Payment created the wrong immutable refund obligation: %+v", repo.obligation)
	}
	if repo.identity.BookingID != bookingID || repo.identity.UserID != buyerID || repo.identity.Generation != 1 ||
		repo.identity.ProviderAccount != "acct_test_fixture" || repo.identity.LiveMode || repo.identity.AmountCents != 725 ||
		repo.identity.Currency != "usd" || repo.identity.PaymentIntentID != "pi_unused_refund_fixture" {
		t.Fatalf("atomic Payment identity did not include the verified provider and buyer binding: %+v", repo.identity)
	}
}

func TestAcceptApprovedUnusedRefundDecisionAcceptsLegacyConsistentProof(t *testing.T) {
	paymentID, bookingID, buyerID, eventID, decisionID, approverID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	mode := false
	view := &domain.FinancialView{
		Payment: &domain.Payment{ID: paymentID, UserId: buyerID, BookingId: &bookingID, Status: domain.PaymentSuccess,
			Price: 725, Currency: "usd", PaymentIntentId: "pi_unused_refund_fixture"},
		Generation: 1, Managed: true, ProviderAccount: "acct_test_fixture", LiveMode: &mode,
		ProviderOutcome: "collected", CreateState: "create_ready", CancelState: "not_requested",
		ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
	}
	approvedAt := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	decision := &bookingv1.ApprovedUnusedRefundDecision{
		RefundDecisionId: decisionID.String(), Status: "APPROVED", BuyerId: buyerID.String(), BookingId: bookingID.String(),
		EventId: eventID.String(), PaymentId: paymentID.String(), PaymentIntentId: view.Payment.PaymentIntentId,
		ProviderAccount: "acct_test_fixture", Livemode: false, Generation: 1, AmountCents: 725, Currency: "usd",
		ApprovedAt: timestamppb.New(approvedAt), EventStartsAt: timestamppb.New(approvedAt.Add(time.Hour)), ApproverId: approverID.String(),
	}
	repo := &approvedUnusedRefundRepository{view: view}
	authority := &approvedUnusedRefundAuthorityStub{decision: decision}
	svc := &PaymentService{repo: repo, account: "acct_test_fixture", approvedUnusedRefundDecisionReader: authority}

	got, err := svc.AcceptApprovedUnusedRefundDecision(context.Background(), bookingID, decisionID, paymentID, 1)
	if err != nil || got != view || repo.accepted != 1 {
		t.Fatalf("consistent legacy proof result=%p accepted=%d err=%v", got, repo.accepted, err)
	}
}

func TestAcceptApprovedUnusedRefundDecisionRejectsInvalidQualificationProof(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bookingv1.ApprovedUnusedRefundDecision)
	}{
		{name: "missing approval timestamp", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.ApprovedAt = nil
		}},
		{name: "zero approval timestamp", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.ApprovedAt = timestamppb.New(time.Time{})
		}},
		{name: "missing qualification time", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.EventQualifiedAt = nil
		}},
		{name: "missing qualification attempt", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.QualificationAttemptId = ""
		}},
		{name: "qualification at event start", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.EventQualifiedAt = proof.EventStartsAt
		}},
		{name: "qualification after event start", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.EventQualifiedAt = timestamppb.New(proof.EventStartsAt.AsTime().Add(time.Nanosecond))
		}},
		{name: "attempt does not match refund decision", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.QualificationAttemptId = uuid.NewString()
		}},
		{name: "invalid event authority version", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.EventAuthorityVersion = "0"
		}},
		{name: "nonnumeric event authority version", mutate: func(proof *bookingv1.ApprovedUnusedRefundDecision) {
			proof.EventAuthorityVersion = "17x"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			paymentID, bookingID, buyerID, eventID, decisionID, approverID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			mode := false
			view := &domain.FinancialView{
				Payment: &domain.Payment{ID: paymentID, UserId: buyerID, BookingId: &bookingID, Status: domain.PaymentSuccess,
					Price: 725, Currency: "usd", PaymentIntentId: "pi_unused_refund_fixture"},
				Generation: 1, Managed: true, ProviderAccount: "acct_test_fixture", LiveMode: &mode,
				ProviderOutcome: "collected", CreateState: "create_ready", CancelState: "not_requested",
				ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
			}
			eventStartsAt := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
			proof := &bookingv1.ApprovedUnusedRefundDecision{
				RefundDecisionId: decisionID.String(), Status: "APPROVED", BuyerId: buyerID.String(), BookingId: bookingID.String(),
				EventId: eventID.String(), PaymentId: paymentID.String(), PaymentIntentId: view.Payment.PaymentIntentId,
				ProviderAccount: "acct_test_fixture", Livemode: false, Generation: 1, AmountCents: 725, Currency: "usd",
				ApprovedAt: timestamppb.New(eventStartsAt.Add(time.Hour)), EventStartsAt: timestamppb.New(eventStartsAt),
				ApproverId: approverID.String(), QualificationAttemptId: decisionID.String(),
				EventQualifiedAt: timestamppb.New(eventStartsAt.Add(-time.Hour)), EventAuthorityVersion: "17",
			}
			tc.mutate(proof)
			repo := &approvedUnusedRefundRepository{view: view}
			authority := &approvedUnusedRefundAuthorityStub{decision: proof}
			svc := &PaymentService{repo: repo, account: "acct_test_fixture", approvedUnusedRefundDecisionReader: authority}
			if _, err := svc.AcceptApprovedUnusedRefundDecision(context.Background(), bookingID, decisionID, paymentID, 1); err == nil {
				t.Fatal("invalid qualification proof was accepted")
			}
			if repo.accepted != 0 {
				t.Fatalf("Payment repository persisted invalid qualification proof %d times", repo.accepted)
			}
		})
	}
}

func TestAcceptApprovedUnusedRefundDecisionRejectsAuthorityIdentityAndEligibilityMismatches(t *testing.T) {
	base := func() (*PaymentService, *approvedUnusedRefundRepository, *approvedUnusedRefundAuthorityStub, uuid.UUID, uuid.UUID, uuid.UUID) {
		paymentID, bookingID, buyerID, eventID, decisionID, approverID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
		mode := false
		view := &domain.FinancialView{
			Payment: &domain.Payment{ID: paymentID, UserId: buyerID, BookingId: &bookingID, Status: domain.PaymentSuccess,
				Price: 725, Currency: "usd", PaymentIntentId: "pi_unused_refund_fixture"},
			Generation: 1, Managed: true, ProviderAccount: "acct_test_fixture", LiveMode: &mode,
			ProviderOutcome: "collected", CreateState: "create_ready", CancelState: "not_requested",
			ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
		}
		approvedAt := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
		decision := &bookingv1.ApprovedUnusedRefundDecision{
			RefundDecisionId: decisionID.String(), Status: "APPROVED", BuyerId: buyerID.String(), BookingId: bookingID.String(),
			EventId: eventID.String(), PaymentId: paymentID.String(), PaymentIntentId: view.Payment.PaymentIntentId,
			ProviderAccount: "acct_test_fixture", Livemode: false, Generation: 1, AmountCents: 725, Currency: "usd",
			ApprovedAt: timestamppb.New(approvedAt), EventStartsAt: timestamppb.New(approvedAt.Add(time.Hour)), ApproverId: approverID.String(),
		}
		repo := &approvedUnusedRefundRepository{view: view}
		authority := &approvedUnusedRefundAuthorityStub{decision: decision}
		return &PaymentService{repo: repo, account: "acct_test_fixture", approvedUnusedRefundDecisionReader: authority}, repo, authority, paymentID, bookingID, decisionID
	}

	tests := []struct {
		name   string
		mutate func(*PaymentService, *approvedUnusedRefundRepository, *approvedUnusedRefundAuthorityStub)
	}{
		{name: "not approved", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.Status = "REJECTED"
		}},
		{name: "decision id mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.RefundDecisionId = uuid.NewString()
		}},
		{name: "buyer mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.BuyerId = uuid.NewString()
		}},
		{name: "event missing", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.EventId = ""
		}},
		{name: "payment mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.PaymentId = uuid.NewString()
		}},
		{name: "intent mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.PaymentIntentId = "pi_wrong"
		}},
		{name: "account mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.ProviderAccount = "acct_other"
		}},
		{name: "live account", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.Livemode = true
		}},
		{name: "amount mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.AmountCents++
		}},
		{name: "currency mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.Currency = "eur"
		}},
		{name: "generation mismatch", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.Generation = 2
		}},
		{name: "missing event authority time", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.EventStartsAt = nil
		}},
		{name: "approval not before event", mutate: func(_ *PaymentService, _ *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.EventStartsAt = authority.decision.ApprovedAt
		}},
		{name: "self approval", mutate: func(_ *PaymentService, repo *approvedUnusedRefundRepository, authority *approvedUnusedRefundAuthorityStub) {
			authority.decision.ApproverId = repo.view.Payment.UserId.String()
		}},
		{name: "configured account mismatch", mutate: func(svc *PaymentService, _ *approvedUnusedRefundRepository, _ *approvedUnusedRefundAuthorityStub) {
			svc.account = "acct_other"
		}},
		{name: "local identity mismatch", mutate: func(_ *PaymentService, repo *approvedUnusedRefundRepository, _ *approvedUnusedRefundAuthorityStub) {
			repo.view.ProviderOutcome = "unknown"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, authority, paymentID, bookingID, decisionID := base()
			tc.mutate(svc, repo, authority)
			if _, err := svc.AcceptApprovedUnusedRefundDecision(context.Background(), bookingID, decisionID, paymentID, 1); err == nil {
				t.Fatal("mismatched authority/Payment identity was accepted")
			}
			if repo.accepted != 0 {
				t.Fatalf("Payment repository persisted a rejected decision %d times", repo.accepted)
			}
		})
	}
}
