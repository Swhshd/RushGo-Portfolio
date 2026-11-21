package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"go.uber.org/zap"
)

type refundWebhookAuditRepository struct {
	repository.PaymentRepositoryInterface
	acceptedErr error
	appendErr   error
	listErr     error
	events      []string
	candidates  []domain.RefundAuditRecord
	queries     []domain.RefundAuditQuery
	appended    []domain.RefundAuditRecord
}

func (r *refundWebhookAuditRepository) AcceptProviderReceipt(_ context.Context, receipt domain.ProviderReceipt) (*domain.ProviderReceipt, bool, error) {
	r.events = append(r.events, "accept")
	if r.acceptedErr != nil {
		return nil, false, r.acceptedErr
	}
	return &receipt, true, nil
}

func (r *refundWebhookAuditRepository) ListRefundAudit(_ context.Context, query domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error) {
	r.events = append(r.events, "list")
	r.queries = append(r.queries, query)
	if r.listErr != nil {
		return nil, r.listErr
	}
	var result []domain.RefundAuditRecord
	for _, candidate := range r.candidates {
		if query.Kind != "" && candidate.Kind != query.Kind {
			continue
		}
		if query.ProviderRequestID != "" && candidate.ProviderRequestID != query.ProviderRequestID {
			continue
		}
		if query.ProviderRefundID != "" && candidate.ProviderRefundID != query.ProviderRefundID {
			continue
		}
		result = append(result, candidate)
	}
	return result, nil
}

func (r *refundWebhookAuditRepository) AppendRefundAudit(_ context.Context, record domain.RefundAuditRecord) error {
	r.events = append(r.events, "append")
	r.appended = append(r.appended, record)
	return r.appendErr
}

func TestAcceptProviderReceiptAppendsExactVerifiedRefundLinkAfterReceipt(t *testing.T) {
	candidate := refundAuditResponseRecordForTest(t, "req_webhook_link", "re_webhook_link", "pi_pre_send_fixture", "acct_fixture", false, 725, "usd")
	repo := &refundWebhookAuditRepository{candidates: []domain.RefundAuditRecord{candidate}}
	service := NewPaymentService(repo, zap.NewNop(), nil)
	receipt := verifiedRefundReceiptForTest(candidate.Response.ProviderRequestID, candidate.Response.RefundID, candidate.Response.PaymentIntentID, candidate.Response.AmountCents, candidate.Response.Currency)
	receipt.RefundAudit.ObservedProviderAccount = cloneStringForTest(candidate.Response.ObservedProviderAccount)
	receipt.RefundAudit.ObservedLiveMode = cloneBoolForTest(candidate.Response.ObservedLiveMode)

	if err := service.AcceptProviderReceipt(context.Background(), receipt); err != nil {
		t.Fatalf("AcceptProviderReceipt returned error: %v", err)
	}
	if len(repo.events) != 4 || repo.events[0] != "accept" || repo.events[1] != "list" || repo.events[2] != "list" || repo.events[3] != "append" {
		t.Fatalf("receipt/audit order=%v; want accept, indexed lookups, append", repo.events)
	}
	if len(repo.queries) != 2 || repo.queries[0].ProviderRequestID != "req_webhook_link" || repo.queries[1].ProviderRefundID != "re_webhook_link" {
		t.Fatalf("link queries were not indexed by exact IDs: %+v", repo.queries)
	}
	if len(repo.appended) != 1 {
		t.Fatalf("appended audits=%d; want one verified event", len(repo.appended))
	}
	linked := repo.appended[0]
	if linked.Kind != domain.RefundAuditKindVerifiedEvent || linked.AttemptID != candidate.AttemptID || linked.PaymentID != candidate.PaymentID || linked.VerifiedEvent == nil || linked.VerifiedEvent.LinkClassification != domain.RefundAuditLinkExactRefundIdentity || linked.VerifiedEvent.LinkSource != domain.RefundAuditLinkSourceRequestAndID {
		t.Fatalf("verified event did not carry exact response link: %+v", linked)
	}
}

func TestAcceptProviderReceiptPreservesAmbiguousAndMissingScopeWithoutGuessing(t *testing.T) {
	candidateA := refundAuditResponseRecordForTest(t, "req_shared", "re_shared", "pi_pre_send_fixture", "acct_fixture", false, 725, "usd")
	candidateB := refundAuditResponseRecordForTest(t, "req_shared", "re_shared", "pi_pre_send_fixture", "acct_fixture", false, 725, "usd")
	for _, tc := range []struct {
		name                string
		candidates          []domain.RefundAuditRecord
		removeEventScope    bool
		removeResponseScope bool
		wantLink            string
		wantAttempt         uuid.UUID
	}{
		{name: "shared refund identity remains ambiguous", candidates: []domain.RefundAuditRecord{candidateA, candidateB}, wantLink: domain.RefundAuditLinkAmbiguous},
		{name: "missing actual event scope remains unlinked", candidates: []domain.RefundAuditRecord{candidateA}, removeEventScope: true, wantLink: domain.RefundAuditLinkUncorrelated},
		{name: "missing actual response account remains unlinked", candidates: []domain.RefundAuditRecord{candidateA}, removeResponseScope: true, wantLink: domain.RefundAuditLinkUncorrelated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &refundWebhookAuditRepository{candidates: tc.candidates}
			service := NewPaymentService(repo, zap.NewNop(), nil)
			receipt := verifiedRefundReceiptForTest("req_shared", "re_shared", "pi_pre_send_fixture", candidateA.Response.AmountCents, candidateA.Response.Currency)
			if tc.removeEventScope {
				receipt.RefundAudit.ObservedProviderAccount = nil
				receipt.RefundAudit.ObservedLiveMode = nil
			}
			if tc.removeResponseScope {
				tc.candidates[0].Response.ObservedProviderAccount = nil
				repo.candidates = tc.candidates
			}
			if err := service.AcceptProviderReceipt(context.Background(), receipt); err != nil {
				t.Fatalf("AcceptProviderReceipt returned error: %v", err)
			}
			if len(repo.appended) != 1 || repo.appended[0].VerifiedEvent == nil || repo.appended[0].VerifiedEvent.LinkClassification != tc.wantLink || repo.appended[0].AttemptID != tc.wantAttempt {
				t.Fatalf("ambiguous/missing link was guessed: %+v", repo.appended)
			}
		})
	}
}

func TestAcceptProviderReceiptRejectsConflictingOrAdapterFailedResponseLink(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domain.RefundAuditRecord, *domain.VerifiedRefundEventAudit)
	}{
		{
			name: "adapter failure is not an exact response",
			mutate: func(response *domain.RefundAuditRecord, _ *domain.VerifiedRefundEventAudit) {
				response.Response.Classification = domain.RefundAuditAdapterFailure
			},
		},
		{
			name: "pre-mapping conflicting facts are not an exact response",
			mutate: func(response *domain.RefundAuditRecord, _ *domain.VerifiedRefundEventAudit) {
				response.Response.ConflictingFields = []string{"amount_cents"}
			},
		},
		{
			name: "matching event facts that disagree with immutable command are a conflict",
			mutate: func(response *domain.RefundAuditRecord, event *domain.VerifiedRefundEventAudit) {
				amount := int64(900)
				response.Response.AmountCents = &amount
				event.AmountCents = &amount
			},
		},
		{
			name: "mutually observed scope that contradicts command is a conflict",
			mutate: func(response *domain.RefundAuditRecord, event *domain.VerifiedRefundEventAudit) {
				account := "acct_other_synthetic"
				response.Response.ObservedProviderAccount = &account
				event.ObservedProviderAccount = &account
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := refundAuditResponseRecordForTest(t, "req_conflict", "re_conflict", "pi_pre_send_fixture", "acct_fixture", false, 725, "usd")
			receipt := verifiedRefundReceiptForTest("req_conflict", "re_conflict", "pi_pre_send_fixture", candidate.Response.AmountCents, candidate.Response.Currency)
			receipt.RefundAudit.ObservedProviderAccount = cloneStringForTest(candidate.Response.ObservedProviderAccount)
			receipt.RefundAudit.ObservedLiveMode = cloneBoolForTest(candidate.Response.ObservedLiveMode)
			tc.mutate(&candidate, receipt.RefundAudit)
			repo := &refundWebhookAuditRepository{candidates: []domain.RefundAuditRecord{candidate}}
			service := NewPaymentService(repo, zap.NewNop(), nil)

			if err := service.AcceptProviderReceipt(context.Background(), receipt); err != nil {
				t.Fatalf("AcceptProviderReceipt returned error: %v", err)
			}
			if len(repo.appended) != 1 || repo.appended[0].VerifiedEvent == nil ||
				repo.appended[0].VerifiedEvent.LinkClassification != domain.RefundAuditLinkConflict ||
				repo.appended[0].PaymentID != uuid.Nil || repo.appended[0].AttemptID != uuid.Nil {
				t.Fatalf("conflicting response was linked: %+v", repo.appended)
			}
		})
	}
}

func TestAcceptProviderReceiptUsesRefundIDWhenEventRequestIDDiffersOrIsMissing(t *testing.T) {
	for _, eventRequestID := range []string{"req_update_synthetic", ""} {
		name := "missing event Request-ID"
		if eventRequestID != "" {
			name = "different event Request-ID"
		}
		t.Run(name, func(t *testing.T) {
			candidate := refundAuditResponseRecordForTest(t, "req_create_synthetic", "re_update_synthetic", "pi_pre_send_fixture", "acct_fixture", false, 725, "usd")
			receipt := verifiedRefundReceiptForTest(eventRequestID, "re_update_synthetic", "pi_pre_send_fixture", candidate.Response.AmountCents, candidate.Response.Currency)
			receipt.RefundAudit.ObservedProviderAccount = cloneStringForTest(candidate.Response.ObservedProviderAccount)
			receipt.RefundAudit.ObservedLiveMode = cloneBoolForTest(candidate.Response.ObservedLiveMode)
			repo := &refundWebhookAuditRepository{candidates: []domain.RefundAuditRecord{candidate}}
			service := NewPaymentService(repo, zap.NewNop(), nil)

			if err := service.AcceptProviderReceipt(context.Background(), receipt); err != nil {
				t.Fatalf("AcceptProviderReceipt returned error: %v", err)
			}
			if len(repo.appended) != 1 || repo.appended[0].VerifiedEvent == nil ||
				repo.appended[0].VerifiedEvent.LinkClassification != domain.RefundAuditLinkExactRefundIdentity ||
				repo.appended[0].VerifiedEvent.LinkSource != domain.RefundAuditLinkSourceRefundID ||
				repo.appended[0].AttemptID != candidate.AttemptID {
				t.Fatalf("object-only association was not preserved: %+v", repo.appended)
			}
		})
	}
}

func TestAcceptProviderReceiptAuditFailureDoesNotRejectReceipt(t *testing.T) {
	auditErr := errors.New("synthetic audit store unavailable")
	repo := &refundWebhookAuditRepository{appendErr: auditErr}
	service := NewPaymentService(repo, zap.NewNop(), nil)
	receipt := verifiedRefundReceiptForTest("", "", "", nil, "")
	receipt.EventType = "charge.refunded"
	receipt.RefundAudit.EventType = "charge.refunded"
	receipt.RefundAudit.ObjectType = "charge"
	receipt.RefundAudit.RefundID = ""
	receipt.RefundAudit.LinkClassification = domain.RefundAuditLinkUncorrelated

	if err := service.AcceptProviderReceipt(context.Background(), receipt); err != nil {
		t.Fatalf("audit append failure rejected accepted receipt: %v", err)
	}
	if len(repo.events) != 2 || repo.events[0] != "accept" || repo.events[1] != "append" || len(repo.appended) != 1 {
		t.Fatalf("receipt/audit events=%v audits=%d", repo.events, len(repo.appended))
	}
}

func TestAcceptProviderReceiptFailureDoesNotAppendVerifiedSidecar(t *testing.T) {
	receiptErr := errors.New("synthetic receipt acceptance failed")
	repo := &refundWebhookAuditRepository{acceptedErr: receiptErr}
	service := NewPaymentService(repo, zap.NewNop(), nil)
	receipt := verifiedRefundReceiptForTest("req_unaccepted", "re_unaccepted", "pi_pre_send_fixture", nil, "usd")

	if err := service.AcceptProviderReceipt(context.Background(), receipt); !errors.Is(err, receiptErr) {
		t.Fatalf("AcceptProviderReceipt error=%v; want receipt acceptance error", err)
	}
	if len(repo.events) != 1 || repo.events[0] != "accept" || len(repo.appended) != 0 {
		t.Fatalf("audit appended before legitimate receipt acceptance: events=%v audits=%d", repo.events, len(repo.appended))
	}
}

func refundAuditResponseRecordForTest(t *testing.T, requestID, refundID, intentID, account string, live bool, amount int64, currency string) domain.RefundAuditRecord {
	t.Helper()
	claim := refundPreSendTestClaim()
	prepared, err := domain.NewRefundPreparedAudit(claim, uuid.New())
	if err != nil {
		t.Fatalf("NewRefundPreparedAudit: %v", err)
	}
	accountValue, liveValue := account, live
	response, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, FinancialFinishOutcome: domain.RefundAuditFinishPersisted,
		ProviderRequestID: requestID, RefundID: refundID, ChargeID: "ch_webhook_link", PaymentIntentID: intentID,
		ProviderStatus: "pending", Currency: currency, AmountCents: &amount,
		ObservedProviderAccount: &accountValue, ObservedLiveMode: &liveValue,
		TransportObservationState: "observed", ObservedTransportCalls: 1,
	})
	if err != nil {
		t.Fatalf("NewRefundResponseAudit: %v", err)
	}
	return response
}

func verifiedRefundReceiptForTest(requestID, refundID, intentID string, amount *int64, currency string) domain.ProviderReceipt {
	receiptID := uuid.New()
	eventID := "evt_verified_refund_fixture"
	account, live := "acct_fixture", false
	return domain.ProviderReceipt{
		ID:          receiptID,
		Observation: domain.ProviderObservation{EventID: eventID},
		EventType:   "refund.updated",
		RefundAudit: &domain.VerifiedRefundEventAudit{
			ReceiptID: receiptID, EventID: eventID, EventType: "refund.updated", ObjectType: "refund", ObjectID: refundID,
			ProviderRequestID: requestID, ObservedProviderAccount: &account, ObservedLiveMode: &live,
			RefundID: refundID, ChargeID: "ch_webhook_link", PaymentIntentID: intentID, ProviderStatus: "pending",
			Currency: currency, AmountCents: amount, LinkClassification: domain.RefundAuditLinkUncorrelated,
		},
	}
}

func cloneStringForTest(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoolForTest(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
