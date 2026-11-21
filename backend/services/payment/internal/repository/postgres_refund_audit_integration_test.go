//go:build recoverycomponents

package repository

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestRefundPreSendAuditCommitsWithAllowAndKeepsSecretsAsDigestsIntegration(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)
	op := seedRefundPreSendPayment(t, ctx, repo)
	claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)

	result, err := repo.CheckRefundPreSend(ctx, claim)
	if err != nil || result.Decision != RefundPreSendAllow || result.AttemptID == uuid.Nil {
		t.Fatalf("pre-send result = %+v err=%v; want committed allow with attempt identity", result, err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit
		WHERE application_attempt_id=$1 AND record_kind='PREPARED'`, result.AttemptID).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("prepared audit rows=%d err=%v; want exactly one", rows, err)
	}

	records, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{PaymentID: op.PaymentID})
	if err != nil || len(records) != 1 || records[0].Kind != domain.RefundAuditKindPrepared || records[0].AttemptID != result.AttemptID {
		t.Fatalf("indexed audit query = %+v err=%v; want the durable preparation", records, err)
	}
	wantKeyDigest := sha256.Sum256([]byte(claim.Refund.Key))
	wantTokenDigest := sha256.Sum256([]byte(claim.Token.String()))
	if records[0].IdempotencyKeyDigest != wantKeyDigest || records[0].ClaimTokenDigest != wantTokenDigest {
		t.Fatal("preparation did not retain key/token digests")
	}
	var rawKeyColumn, rawTokenColumn bool
	if err := pool.QueryRow(ctx, `SELECT
		EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='payment_refund_audit' AND column_name='idempotency_key'),
		EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='payment_refund_audit' AND column_name='claim_token')`).Scan(&rawKeyColumn, &rawTokenColumn); err != nil {
		t.Fatal(err)
	}
	if rawKeyColumn || rawTokenColumn {
		t.Fatal("audit schema contains raw idempotency key or claim token columns")
	}
}

func TestRefundPreSendAuditInsertFailureRollsBackAllowIntegration(t *testing.T) {
	pool, ctx, repo := newRefundComponentRepository(t)
	op := seedRefundPreSendPayment(t, ctx, repo)
	claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
	if _, err := pool.Exec(ctx, `ALTER TABLE payment_refund_audit ADD CONSTRAINT refund_audit_injected_failure CHECK (false)`); err != nil {
		t.Fatal(err)
	}

	result, err := repo.CheckRefundPreSend(ctx, claim)
	if err == nil || result.Decision == RefundPreSendAllow || result.AttemptID != uuid.Nil {
		t.Fatalf("audit insert failure returned result=%+v err=%v; want fail-closed error", result, err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, op.PaymentID).Scan(&state); err != nil || state != "claimed" {
		t.Fatalf("Payment state after failed preparation=%q err=%v; want unchanged claimed", state, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1`, op.PaymentID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("audit rows after failed preparation=%d err=%v; want transaction rollback", count, err)
	}
}

func TestRefundAuditAppendIsIdempotentForVolatileTimesAndConflictsOnChangedFactsIntegration(t *testing.T) {
	_, ctx, repo := newRefundComponentRepository(t)
	op := seedRefundPreSendPayment(t, ctx, repo)
	claim := claimRefundPreSendPayment(t, ctx, repo, op.PaymentID)
	guard, err := repo.CheckRefundPreSend(ctx, claim)
	if err != nil || guard.Decision != RefundPreSendAllow || guard.AttemptID == uuid.Nil {
		t.Fatalf("pre-send guard = %+v err=%v", guard, err)
	}
	prepared, err := domain.NewRefundPreparedAudit(claim, guard.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC()
	response, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, ProviderRequestID: "req_audit_fixture",
		RefundID: "re_audit_fixture", ProviderStatus: "succeeded", Currency: "usd",
		AmountCents: int64Pointer(700), ObservedAt: &observed,
		FinancialFinishOutcome:    domain.RefundAuditFinishPersisted,
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AppendRefundAudit(ctx, response); err != nil {
		t.Fatal(err)
	}

	replayObserved := observed.Add(2 * time.Second)
	replay, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, ProviderRequestID: "req_audit_fixture",
		RefundID: "re_audit_fixture", ProviderStatus: "succeeded", Currency: "usd",
		AmountCents: int64Pointer(700), ObservedAt: &replayObserved,
		FinancialFinishOutcome:    domain.RefundAuditFinishPersisted,
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AppendRefundAudit(ctx, replay); err != nil {
		t.Fatalf("same stable response with a different observation time = %v; want idempotent replay", err)
	}

	conflicting, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, ProviderRequestID: "req_other_fixture",
		RefundID: "re_audit_fixture", ProviderStatus: "succeeded", Currency: "usd",
		AmountCents: int64Pointer(700), ObservedAt: &replayObserved,
		FinancialFinishOutcome:    domain.RefundAuditFinishPersisted,
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AppendRefundAudit(ctx, conflicting); !errors.Is(err, ErrRefundAuditConflict) {
		t.Fatalf("changed response facts append error=%v; want explicit identity conflict", err)
	}
}

func TestRefundVerifiedEventWithoutExactRefundIdentityRemainsUnlinkedIntegration(t *testing.T) {
	_, ctx, repo := newRefundComponentRepository(t)
	receiptID := uuid.New()
	event := domain.VerifiedRefundEventAudit{
		ReceiptID: receiptID, EventID: "evt_unlinked_audit_fixture", EventType: "refund.updated",
		ObjectType: "refund", ObjectID: "re_unlinked_fixture", RefundID: "re_unlinked_fixture",
		ObservedProviderAccount: stringPointer("acct_observed_fixture"), AmountCents: int64Pointer(700),
		LinkClassification: domain.RefundAuditLinkUncorrelated,
	}
	record, err := domain.NewRefundVerifiedEventRecord(event, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.AppendRefundAudit(ctx, record); err != nil {
		t.Fatal(err)
	}
	changedEvent := event
	changedEvent.AmountCents = int64Pointer(701)
	changedRecord, err := domain.NewRefundVerifiedEventRecord(changedEvent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if changedRecord.ProviderEventID != record.ProviderEventID || changedRecord.RecordID == record.RecordID {
		t.Fatal("changed sidecar facts did not get a distinct immutable observation identity")
	}
	if err := repo.AppendRefundAudit(ctx, changedRecord); err != nil {
		t.Fatalf("append changed observation of same verified event: %v", err)
	}
	records, err := repo.ListRefundAudit(ctx, domain.RefundAuditQuery{ProviderEventID: event.EventID})
	if err != nil || len(records) != 2 {
		t.Fatalf("event-indexed query = %d records err=%v", len(records), err)
	}
	for _, stored := range records {
		if stored.PaymentID != uuid.Nil || stored.AttemptID != uuid.Nil || stored.CommandDigest != ([32]byte{}) {
			t.Fatal("verified event acquired Payment/attempt linkage without an exact response identity")
		}
	}
}

func int64Pointer(value int64) *int64    { return &value }
func stringPointer(value string) *string { return &value }
