package main

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestExplainLoadedRecordsReportsBoundedWindowAsPartial(t *testing.T) {
	prepared, _ := explainFixture(t, uuid.New(), "re_window_fixture", "req_window_fixture")
	result := explainLoadedRecords(prepared.PaymentID, refundExplanationRecords{
		records:                 []domain.RefundAuditRecord{prepared},
		primaryRowsReturned:     refundExplainRecordLimit,
		relatedIdentityQueries:  refundExplainRelatedIdentityLimit,
		mayBeTruncated:          true,
		relatedSearchWasLimited: true,
	})
	if result.Status != "PARTIAL" || !result.MayBeTruncated || result.QueryLimit != refundExplainRecordLimit ||
		result.PrimaryRowsIncluded != refundExplainRecordLimit || result.RelatedIdentityQueries != refundExplainRelatedIdentityLimit {
		t.Fatalf("bounded window was not exposed as partial: %+v", result)
	}
}

func TestExplainRecordsEmitsSharedIdentityAmbiguityStatuses(t *testing.T) {
	paymentID := uuid.New()
	firstPrepared, firstResponse := explainFixture(t, paymentID, "re_shared_fixture", "req_shared_fixture")
	secondPrepared, secondResponse := explainFixture(t, paymentID, "re_shared_fixture", "req_shared_fixture")
	result := explainRecords(paymentID, []domain.RefundAuditRecord{firstPrepared, firstResponse, secondPrepared, secondResponse})
	if result.Status != "PARTIAL" || len(result.Attempts) != 2 {
		t.Fatalf("shared identifiers were not reported as partial: %+v", result)
	}
	for _, attempt := range result.Attempts {
		if attempt.Status != "AMBIGUOUS_SHARED_REFUND_ID" {
			t.Fatalf("status mutation was not reflected in emitted attempt: %+v", attempt)
		}
	}
}

func TestLoadRefundExplanationRecordsFindsUnlinkedIndexedEventCandidates(t *testing.T) {
	paymentID := uuid.New()
	prepared, response := explainFixture(t, paymentID, "re_candidate_fixture", "req_candidate_fixture")
	event, err := domain.NewRefundVerifiedEventRecord(domain.VerifiedRefundEventAudit{
		ReceiptID: uuid.New(), EventID: "evt_candidate_fixture", EventType: "refund.updated",
		ProviderRequestID: "req_candidate_fixture", RefundID: "re_candidate_fixture",
		LinkClassification: domain.RefundAuditLinkUncorrelated,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := &explainAuditReader{
		paymentID: paymentID,
		primary:   []domain.RefundAuditRecord{prepared, response},
		candidate: event,
	}
	loaded, err := loadRefundExplanationRecords(context.Background(), reader, paymentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.queries) != 3 || reader.queries[0].PaymentID != paymentID ||
		reader.queries[1].ProviderRefundID == "" || reader.queries[1].ProviderRequestID != "" ||
		reader.queries[2].ProviderRequestID == "" || reader.queries[2].ProviderRefundID != "" {
		t.Fatalf("expected one payment query and separate indexed identity queries, got %+v", reader.queries)
	}
	if len(loaded.relatedEvents) != 1 {
		t.Fatalf("unlinked event was not surfaced as a candidate: %+v", loaded.relatedEvents)
	}
	candidate := loaded.relatedEvents[0]
	if candidate.EventID != "evt_candidate_fixture" || candidate.SamePaymentScope || len(candidate.MatchedBy) != 2 ||
		candidate.MatchedBy[0] != "provider_refund_id" || candidate.MatchedBy[1] != "provider_request_id" ||
		candidate.LinkClassification != domain.RefundAuditLinkUncorrelated {
		t.Fatalf("candidate was treated as a forced link or lost search facts: %+v", candidate)
	}
	result := explainLoadedRecords(paymentID, loaded)
	if result.Status != "PARTIAL" || len(result.RelatedEventCandidates) != 1 {
		t.Fatalf("candidate event did not keep explanation partial: %+v", result)
	}
}

func TestRefundAuditRelatedIdentityQueriesBoundAndDeduplicate(t *testing.T) {
	records := make([]domain.RefundAuditRecord, 0, refundExplainRelatedIdentityLimit+1)
	for i := 0; i < refundExplainRelatedIdentityLimit; i++ {
		prepared, response := explainFixture(t, uuid.New(), "re_related_"+uuid.NewString(), "req_related_"+uuid.NewString())
		_ = prepared
		records = append(records, response)
	}
	queries, limited := refundAuditRelatedIdentityQueries(records)
	if !limited || len(queries) != refundExplainRelatedIdentityLimit {
		t.Fatalf("related identity query plan was not bounded: len=%d limited=%v", len(queries), limited)
	}
	for _, query := range queries {
		if query.Limit != refundExplainProbeLimit || (query.ProviderRefundID == "") == (query.ProviderRequestID == "") {
			t.Fatalf("query does not use exactly one bounded indexed identity: %+v", query)
		}
	}
}

type explainAuditReader struct {
	paymentID uuid.UUID
	primary   []domain.RefundAuditRecord
	candidate domain.RefundAuditRecord
	queries   []domain.RefundAuditQuery
}

func (r *explainAuditReader) ListRefundAudit(_ context.Context, query domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error) {
	r.queries = append(r.queries, query)
	if query.PaymentID != uuid.Nil {
		if query.PaymentID != r.paymentID || query.Limit != refundExplainProbeLimit {
			return nil, domain.ErrInvalidRefundAuditRecord
		}
		return append([]domain.RefundAuditRecord(nil), r.primary...), nil
	}
	if query.Limit != refundExplainProbeLimit || (query.ProviderRequestID == "") == (query.ProviderRefundID == "") {
		return nil, domain.ErrInvalidRefundAuditRecord
	}
	if r.candidate.VerifiedEvent == nil {
		return nil, nil
	}
	if query.ProviderRequestID != "" && query.ProviderRequestID == r.candidate.ProviderRequestID {
		return []domain.RefundAuditRecord{r.candidate}, nil
	}
	if query.ProviderRefundID != "" && query.ProviderRefundID == r.candidate.ProviderRefundID {
		return []domain.RefundAuditRecord{r.candidate}, nil
	}
	return nil, nil
}

func explainFixture(t *testing.T, paymentID uuid.UUID, refundID, requestID string) (domain.RefundAuditRecord, domain.RefundAuditRecord) {
	t.Helper()
	bookingID, decisionID, attemptID := uuid.New(), uuid.New(), uuid.New()
	commandDigest := sha256.Sum256([]byte("command:" + paymentID.String()))
	prepared := domain.RefundAuditRecord{
		Version: domain.RefundAuditSchemaVersion, RecordID: attemptID, PaymentID: paymentID, BookingID: bookingID,
		Generation: 1, CommandDigest: commandDigest, AttemptID: attemptID, Kind: domain.RefundAuditKindPrepared,
		RefundDecisionID: decisionID, Provider: "stripe", IntendedAccount: "acct_fixture", IntendedLiveMode: boolPointer(false),
		ScopeSource: "frozen_payment_command", TargetAmountCents: int64Pointer(700), Currency: "usd", PaymentIntentID: "pi_fixture",
		IdempotencyKeyDigest: sha256.Sum256([]byte("key_fixture")), ClaimTokenDigest: sha256.Sum256([]byte("token_fixture")),
		ClaimFence: 1, Prepared: &domain.RefundAuditPrepared{Version: domain.RefundAuditSchemaVersion, Stage: "pre_create"},
	}
	if err := domain.ValidateRefundAuditRecord(prepared); err != nil {
		t.Fatalf("invalid prepared fixture: %v", err)
	}
	response, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, ProviderRequestID: requestID, RefundID: refundID,
		PaymentIntentID: "pi_fixture", Currency: "usd", AmountCents: int64Pointer(700),
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := domain.ValidateRefundAuditRecord(response); err != nil {
		t.Fatalf("invalid response fixture: %v", err)
	}
	return prepared, response
}

func int64Pointer(value int64) *int64 { return &value }
func boolPointer(value bool) *bool    { return &value }
