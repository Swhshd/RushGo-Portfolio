package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
)

type refundPreSendTestRepository struct {
	repository.FinancialRepository
	decision     repository.RefundPreSendResult
	guardErr     error
	guardCalls   int
	events       *[]string
	finish       domain.FinancialResult
	finishCall   int
	finishOwned  *bool
	finishErr    error
	auditRecords []domain.RefundAuditRecord
	auditErr     error
	deferCalls   int
	deferClaim   domain.FinancialClaim
	deferNext    time.Time
	deferErr     error
}

func (r *refundPreSendTestRepository) CheckRefundPreSend(_ context.Context, _ domain.FinancialClaim) (repository.RefundPreSendResult, error) {
	r.guardCalls++
	if r.events != nil {
		*r.events = append(*r.events, "guard")
	}
	return r.decision, r.guardErr
}

func (r *refundPreSendTestRepository) FinishFinancialWork(_ context.Context, _ domain.FinancialClaim, result domain.FinancialResult) (bool, error) {
	r.finishCall++
	r.finish = result
	if r.events != nil {
		*r.events = append(*r.events, "finish")
	}
	owned := true
	if r.finishOwned != nil {
		owned = *r.finishOwned
	}
	return owned, r.finishErr
}

func (r *refundPreSendTestRepository) AppendRefundAudit(_ context.Context, record domain.RefundAuditRecord) error {
	r.auditRecords = append(r.auditRecords, record)
	if r.events != nil {
		*r.events = append(*r.events, "append")
	}
	return r.auditErr
}

func (r *refundPreSendTestRepository) ListRefundAudit(context.Context, domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error) {
	return nil, nil
}

func (r *refundPreSendTestRepository) DeferRefundForAdmission(_ context.Context, claim domain.FinancialClaim, _ repository.RefundPreSendResult, next time.Time) (bool, error) {
	r.deferCalls++
	r.deferClaim, r.deferNext = claim, next
	if r.events != nil {
		*r.events = append(*r.events, "defer")
	}
	return true, r.deferErr
}

type refundPreSendTestProvider struct {
	paymentgateway.FinancialProvider
	events         []string
	createClaims   []domain.FinancialClaim
	createPI       []string
	retrieveIDs    []string
	retrieveNil    bool
	inspectResults []*domain.ProviderObservation
	inspectCalls   int
	createNil      bool
	createStatus   string
}

func (p *refundPreSendTestProvider) InspectSucceededRefunds(_ context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	p.events = append(p.events, "inspect")
	if p.inspectCalls < len(p.inspectResults) {
		result := p.inspectResults[p.inspectCalls]
		p.inspectCalls++
		return result, nil
	}
	p.inspectCalls++
	return &domain.ProviderObservation{Account: account, LiveMode: live, IntentID: intentID, Status: "no_refunds", Collected: true}, nil
}

func (p *refundPreSendTestProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	p.events = append(p.events, "create")
	p.createClaims = append(p.createClaims, claim)
	p.createPI = append(p.createPI, intentID)
	if p.createNil {
		return nil, nil
	}
	amount := claim.Refund.AmountCents
	refundID := "re_pre_send_fixture"
	status := p.createStatus
	if status == "" {
		status = "pending"
	}
	if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
		capture.RecordResponse(domain.RefundAuditResponse{
			Classification: domain.RefundAuditResponseObserved, RefundID: refundID, PaymentIntentID: intentID,
			ProviderStatus: status, Currency: claim.Refund.Currency, AmountCents: &amount, SummarySource: "provider_object",
		})
	}
	return &domain.ProviderObservation{
		Account: claim.Operation.Identity.ProviderAccount, LiveMode: claim.Operation.Identity.LiveMode,
		PaymentID: claim.PaymentID, BookingID: claim.Operation.Identity.BookingID, Generation: claim.Operation.Identity.Generation,
		IntentID: intentID, RefundID: refundID, Status: status, AmountCents: amount,
		Currency: claim.Refund.Currency, Collected: true,
	}, nil
}

func (p *refundPreSendTestProvider) RetrieveRefund(_ context.Context, _ string, _ bool, refundID string) (*domain.ProviderObservation, error) {
	p.events = append(p.events, "retrieve")
	p.retrieveIDs = append(p.retrieveIDs, refundID)
	if p.retrieveNil {
		return nil, nil
	}
	return &domain.ProviderObservation{RefundID: refundID, Status: "pending", Collected: true}, nil
}

func TestExecuteRefundChecksCurrentStateBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		decision    repository.RefundPreSendResult
		guardErr    error
		wantCreates int
		wantGets    int
		retrieveNil bool
		wantState   string
		wantError   string
	}{
		{name: "allow preserves frozen claim", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendAllow}, wantCreates: 1, wantState: "provider_pending"},
		{name: "already succeeded releases owned claim without evidence", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendSucceeded}, wantState: "provider_pending"},
		{name: "current refund id is retrieved", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendExistingResult, RefundID: "re_current"}, wantGets: 1, wantState: "provider_pending"},
		{name: "nil current refund result retries safely", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendExistingResult, RefundID: "re_current"}, wantGets: 1, retrieveNil: true, wantState: "retryable", wantError: "refund_pre_send_retrieve_unknown"},
		{name: "stale owner suppresses create", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendStaleOwner}, wantState: "retryable", wantError: "refund_pre_send_owner_lost"},
		{name: "command mismatch suppresses create with stable category", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendCommandMismatch, Reason: "stable_key"}, wantState: "attention", wantError: "refund_pre_send_stable_key"},
		{name: "state not allowed suppresses create", decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendNotAllowed}, wantState: "attention", wantError: "refund_pre_send_not_allowed"},
		{name: "database unknown suppresses create and retries", guardErr: errors.New("read unavailable"), wantState: "retryable", wantError: "refund_pre_send_db_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := refundPreSendTestClaim()
			provider := &refundPreSendTestProvider{retrieveNil: tc.retrieveNil}
			decision := tc.decision
			if decision.Decision == repository.RefundPreSendAllow {
				decision.AttemptID = uuid.New()
			}
			repo := &refundPreSendTestRepository{decision: decision, guardErr: tc.guardErr, events: &provider.events}
			worker := &FinancialWorker{repo: repo, provider: provider}

			if err := worker.executeRefund(context.Background(), claim); err != nil {
				t.Fatalf("executeRefund returned error: %v", err)
			}
			if repo.guardCalls != 1 || repo.finishCall != 1 {
				t.Fatalf("guard/finish calls = %d/%d; want 1/1", repo.guardCalls, repo.finishCall)
			}
			if len(provider.createClaims) != tc.wantCreates || len(provider.retrieveIDs) != tc.wantGets {
				t.Fatalf("provider calls: creates=%d retrieves=%d events=%v; want %d/%d", len(provider.createClaims), len(provider.retrieveIDs), provider.events, tc.wantCreates, tc.wantGets)
			}
			if repo.finish.State != tc.wantState || repo.finish.ErrorCategory != tc.wantError {
				t.Fatalf("finish result = state:%q error:%q; want state:%q error:%q", repo.finish.State, repo.finish.ErrorCategory, tc.wantState, tc.wantError)
			}
			if len(provider.events) < 2 || provider.events[0] != "inspect" || provider.events[1] != "guard" {
				t.Fatalf("provider inspection did not precede the pre-send decision: %v", provider.events)
			}
			if tc.wantCreates == 1 {
				if len(provider.createClaims) != 1 || !reflect.DeepEqual(provider.createClaims[0], claim) ||
					provider.createPI[0] != "pi_pre_send_fixture" || provider.createClaims[0].Key != claim.Key ||
					provider.createClaims[0].Refund.Key != claim.Refund.Key {
					t.Fatalf("CreateFullRefund changed the frozen claim or key: got=%+v", provider.createClaims)
				}
				if len(provider.events) != 6 || provider.events[2] != "create" || provider.events[3] != "inspect" || provider.events[4] != "finish" || provider.events[5] != "append" {
					t.Fatalf("allow path call order=%v; want inspect, guard, create, inspect, finish, append", provider.events)
				}
			}
			if tc.wantGets == 1 && (len(provider.retrieveIDs) != 1 || provider.retrieveIDs[0] != "re_current") {
				t.Fatalf("current refund id was not retrieved: %v", provider.retrieveIDs)
			}
		})
	}
}

func TestExecuteRefundTreatsNilInitialInspectionAsUnknown(t *testing.T) {
	claim := refundPreSendTestClaim()
	provider := &refundPreSendTestProvider{inspectResults: []*domain.ProviderObservation{nil}}
	repo := &refundPreSendTestRepository{events: &provider.events}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if provider.inspectCalls != 1 || len(provider.createClaims) != 0 || repo.guardCalls != 0 {
		t.Fatalf("nil inspection proceeded to send: inspect=%d creates=%d guards=%d", provider.inspectCalls, len(provider.createClaims), repo.guardCalls)
	}
	if repo.finishCall != 1 || repo.finish.State != "retryable" || repo.finish.ErrorCategory != "provider_refund_unknown" || repo.finish.Observation != nil {
		t.Fatalf("nil inspection result = %+v; want retryable unknown without fabricated observation", repo.finish)
	}
}

func TestExecuteRefundTreatsNilInitialRefundRetrievalAsUnknown(t *testing.T) {
	claim := refundPreSendTestClaim()
	claim.View.RefundID = "re_current_fixture"
	provider := &refundPreSendTestProvider{retrieveNil: true}
	repo := &refundPreSendTestRepository{events: &provider.events}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if repo.finishCall != 1 || repo.finish.State != "retryable" || repo.finish.ErrorCategory != "provider_refund_retrieve_unknown" || repo.finish.Observation != nil {
		t.Fatalf("nil initial RetrieveRefund result=%+v; want retryable unknown without fabricated observation", repo.finish)
	}
	if len(provider.createClaims) != 0 || repo.guardCalls != 0 || len(provider.retrieveIDs) != 1 || provider.retrieveIDs[0] != claim.View.RefundID {
		t.Fatalf("nil initial retrieval proceeded to a new send: creates=%d guards=%d retrieves=%v", len(provider.createClaims), repo.guardCalls, provider.retrieveIDs)
	}
}

func TestExecuteRefundDoesNotCreateWithoutPreparedAttemptID(t *testing.T) {
	claim := refundPreSendTestClaim()
	provider := &refundPreSendTestProvider{}
	repo := &refundPreSendTestRepository{decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendAllow}, events: &provider.events}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if len(provider.createClaims) != 0 || repo.guardCalls != 1 || repo.finishCall != 1 || len(repo.auditRecords) != 0 {
		t.Fatalf("allow without attempt id did not fail closed: creates=%d guards=%d finish=%d audits=%d", len(provider.createClaims), repo.guardCalls, repo.finishCall, len(repo.auditRecords))
	}
	if repo.finish.State != "retryable" || repo.finish.ErrorCategory != "refund_pre_send_attempt_missing" {
		t.Fatalf("missing attempt result=%+v; want retryable attempt-missing", repo.finish)
	}
}

func TestExecuteRefundCoordinationWaitUsesNeutralDeferWithoutExhaustionOrCreate(t *testing.T) {
	claim := refundPreSendTestClaim()
	claim.Attempts = 32
	provider := &refundPreSendTestProvider{}
	repo := &refundPreSendTestRepository{
		decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendCoordinationWait, Reason: "pending_admission"},
		events:   &provider.events,
	}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if repo.guardCalls != 1 || repo.deferCalls != 1 || repo.finishCall != 0 || repo.deferClaim.Token != claim.Token ||
		repo.deferClaim.Fence != claim.Fence || !repo.deferNext.After(time.Now()) {
		t.Fatalf("coordination wait guard/defer/finish = %d/%d/%d claim=%+v next=%v", repo.guardCalls, repo.deferCalls, repo.finishCall, repo.deferClaim, repo.deferNext)
	}
	if len(provider.createClaims) != 0 || len(repo.auditRecords) != 0 || len(provider.events) != 3 ||
		provider.events[0] != "inspect" || provider.events[1] != "guard" || provider.events[2] != "defer" {
		t.Fatalf("coordination wait did not stop before Create/PREPARED: events=%v creates=%d audit=%d", provider.events, len(provider.createClaims), len(repo.auditRecords))
	}
}

func TestExecuteRefundTreatsNilCreateAsUnknownAndStillAppendsAudit(t *testing.T) {
	claim := refundPreSendTestClaim()
	provider := &refundPreSendTestProvider{createNil: true}
	repo := &refundPreSendTestRepository{decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendAllow, AttemptID: uuid.New()}, events: &provider.events}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if repo.finish.State != "retryable" || repo.finish.ErrorCategory != "provider_refund_response_incomplete" || repo.finish.Observation != nil {
		t.Fatalf("nil Create result=%+v; want retryable unknown without fabricated provider observation", repo.finish)
	}
	if len(repo.auditRecords) != 1 || repo.auditRecords[0].Kind != domain.RefundAuditKindResponse || repo.auditRecords[0].AttemptID != repo.decision.AttemptID {
		t.Fatalf("nil Create audit=%+v; want one response linked to original attempt", repo.auditRecords)
	}
	if repo.auditRecords[0].Response == nil || repo.auditRecords[0].Response.Classification != domain.RefundAuditOutcomeUnknown || repo.auditRecords[0].Response.ObservedTransportCalls != -1 {
		t.Fatalf("nil Create response audit=%+v; want unknown result and unknown transport count", repo.auditRecords[0].Response)
	}
}

func TestExecuteRefundAppendsResponseAfterFinishRegardlessOfOwnershipOrFinishError(t *testing.T) {
	finishError := errors.New("fixture finish unavailable")
	for _, tc := range []struct {
		name        string
		owned       bool
		finishErr   error
		wantOutcome string
		wantError   bool
	}{
		{name: "owned and committed", owned: true, wantOutcome: domain.RefundAuditFinishPersisted},
		{name: "ownership lost", owned: false, wantOutcome: domain.RefundAuditFinishNotOwned},
		{name: "finish commit unknown", owned: true, finishErr: finishError, wantOutcome: domain.RefundAuditFinishCommitUnknown, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := refundPreSendTestClaim()
			provider := &refundPreSendTestProvider{}
			owned := tc.owned
			repo := &refundPreSendTestRepository{
				decision:    repository.RefundPreSendResult{Decision: repository.RefundPreSendAllow, AttemptID: uuid.New()},
				finishOwned: &owned, finishErr: tc.finishErr, events: &provider.events,
			}
			worker := &FinancialWorker{repo: repo, provider: provider}

			err := worker.executeRefund(context.Background(), claim)
			if tc.wantError != (err != nil) {
				t.Fatalf("executeRefund error=%v; want error=%t", err, tc.wantError)
			}
			if len(repo.auditRecords) != 1 || repo.auditRecords[0].Response == nil || repo.auditRecords[0].Response.FinancialFinishOutcome != tc.wantOutcome {
				t.Fatalf("finish outcome was not preserved in audit: %+v", repo.auditRecords)
			}
			if len(provider.events) < 2 || provider.events[len(provider.events)-2] != "finish" || provider.events[len(provider.events)-1] != "append" {
				t.Fatalf("finish/append order=%v; want append after FinishFinancialWork", provider.events)
			}
		})
	}
}

func TestExecuteRefundPreservesTerminalCreateAgainstPendingFollowup(t *testing.T) {
	claim := refundPreSendTestClaim()
	provider := &refundPreSendTestProvider{
		createStatus: "failed",
		inspectResults: []*domain.ProviderObservation{
			{Collected: true, Status: "no_refunds"},
			{Collected: true, Status: "pending"},
		},
	}
	repo := &refundPreSendTestRepository{
		decision: repository.RefundPreSendResult{Decision: repository.RefundPreSendAllow, AttemptID: uuid.New()},
		events:   &provider.events,
	}
	worker := &FinancialWorker{repo: repo, provider: provider}

	if err := worker.executeRefund(context.Background(), claim); err != nil {
		t.Fatalf("executeRefund returned error: %v", err)
	}
	if repo.finish.State != "attention" || repo.finish.ErrorCategory != "provider_refund_terminal" {
		t.Fatalf("pending follow-up changed terminal Create result: state=%q category=%q; want terminal attention", repo.finish.State, repo.finish.ErrorCategory)
	}
}

func refundPreSendTestClaim() domain.FinancialClaim {
	paymentID, bookingID, userID, decisionID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	key := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	firstAttempt := time.Now().UTC()
	booking := bookingID
	mode := false
	identity := domain.FinancialIdentity{BookingID: bookingID, UserID: userID, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"}
	frozen := []byte(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	identity.Fingerprint, _ = domain.FinancialFingerprint(identity, frozen)
	return domain.FinancialClaim{
		PaymentID: paymentID, Token: uuid.New(), Fence: 1, Kind: "refund", Key: key, FirstAttemptAt: &firstAttempt, Attempts: 1,
		Operation: domain.CreateOperation{PaymentID: paymentID, Key: key, Identity: identity, FrozenParams: frozen},
		View: &domain.FinancialView{Payment: &domain.Payment{ID: paymentID, UserId: userID, BookingId: &booking, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", PaymentIntentId: "pi_pre_send_fixture"},
			Generation: 1, Managed: true, ProviderAccount: "acct_fixture", LiveMode: &mode, ProviderOutcome: "collected", RefundState: "claimed", RefundAmountCents: 725},
		Refund: &domain.RefundObligation{PaymentID: paymentID, DecisionID: decisionID, Generation: 1, AmountCents: 725, Currency: "usd", Reason: "EXPIRED", Key: key},
	}
}
