package domain

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRefundPreparedAuditSeparatesLogicalCommandFromApplicationAttempt(t *testing.T) {
	claim := refundAuditClaimFixture()
	first, err := NewRefundPreparedAudit(claim, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	secondClaim := claim
	secondClaim.Token = uuid.New()
	secondClaim.Fence++
	second, err := NewRefundPreparedAudit(secondClaim, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if first.CommandDigest != second.CommandDigest {
		t.Fatal("a legal application retry changed the frozen logical command digest")
	}
	if first.AttemptID == second.AttemptID || first.RecordID == second.RecordID {
		t.Fatal("separate application invocations shared an attempt identity")
	}
	if first.IdempotencyKeyDigest != sha256.Sum256([]byte(claim.Refund.Key)) ||
		first.ClaimTokenDigest != sha256.Sum256([]byte(claim.Token.String())) {
		t.Fatal("prepared audit did not use stable key/token digests")
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), claim.Refund.Key) || strings.Contains(string(raw), claim.Token.String()) {
		t.Fatal("prepared audit serialization exposed the raw idempotency key or claim token")
	}
}

func TestRefundResponseCaptureIsBoundedConcurrentAndMergesWithoutErasingFacts(t *testing.T) {
	capture := NewRefundResponseCapture(uuid.New())
	capture.MarkTransportObserverAttached()
	var wg sync.WaitGroup
	for i := 0; i < MaxRefundHTTPObservationDetails+3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			capture.RecordHTTPObservation(RefundHTTPObservation{StatusCode: 200, RequestID: "req_fixture", ResultClass: "http_response"})
		}(i)
	}
	wg.Wait()
	capture.RecordResponse(RefundAuditResponse{
		Classification: RefundAuditResponseObserved, RefundID: "re_refund_fixture", AmountCents: int64Ptr(700),
		SummarySource: "transport_allowlist",
	})
	capture.RecordResponse(RefundAuditResponse{
		PaymentIntentID: "pi_fixture", ProviderStatus: "succeeded", ProviderRequestID: "req_sdk_fixture",
		SummarySource: "stripe_response_meta",
	})
	capture.RecordResponse(RefundAuditResponse{RefundID: "re_changed_fixture"})

	got := capture.Snapshot()
	if got.ObservedTransportCalls != MaxRefundHTTPObservationDetails+3 || !got.TransportObservationsTruncated ||
		len(got.TransportObservations) != MaxRefundHTTPObservationDetails || got.TransportObservations[0].Ordinal != 1 ||
		got.TransportObservations[len(got.TransportObservations)-1].Ordinal != MaxRefundHTTPObservationDetails {
		t.Fatalf("transport capture did not retain a bounded detail set and exact total: %+v", got)
	}
	if got.AmountCents == nil || *got.AmountCents != 700 || got.PaymentIntentID != "pi_fixture" || got.ProviderStatus != "succeeded" {
		t.Fatal("later mapped metadata erased earlier allowlisted response facts")
	}
	if len(got.ConflictingFields) != 1 || got.ConflictingFields[0] != "refund_id" || got.RefundID != "re_refund_fixture" {
		t.Fatalf("changed safe response fact was not preserved as a conflict: %+v", got)
	}
}

func TestVerifiedRefundEventRequiresExactResponseIdentityAndIgnoresObservedTimeForIdentity(t *testing.T) {
	claim := refundAuditClaimFixture()
	prepared, err := NewRefundPreparedAudit(claim, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	response, err := NewRefundResponseAudit(prepared, RefundAuditResponse{
		Classification: RefundAuditResponseObserved, ProviderRequestID: "req_verified_fixture",
		RefundID: "re_verified_fixture", PaymentIntentID: "pi_refund_fixture", Currency: "usd",
		AmountCents: int64Ptr(700), ObservedProviderAccount: stringPtr("acct_fixture"), ObservedLiveMode: boolPtr(false),
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC()
	event := VerifiedRefundEventAudit{
		ReceiptID: uuid.New(), EventID: "evt_verified_fixture", EventType: "refund.updated", ObjectType: "refund",
		ObjectID: "re_verified_fixture", RefundID: "re_verified_fixture", PaymentIntentID: "pi_refund_fixture",
		AmountCents: int64Ptr(700), Currency: "usd",
		ProviderRequestID: "req_verified_fixture", ObservedProviderAccount: stringPtr("acct_fixture"),
		ObservedLiveMode: boolPtr(false), LinkClassification: RefundAuditLinkExactRefundIdentity,
		LinkSource: RefundAuditLinkSourceRequestAndID, ObservedAt: &observed,
	}
	record, err := NewRefundVerifiedEventRecord(event, &response)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRefundAuditRecord(record); err != nil {
		t.Fatalf("constructed exact event record failed validation: %v", err)
	}
	if record.PaymentID != claim.PaymentID || record.AttemptID != prepared.AttemptID {
		t.Fatalf("exact verified event link = %+v", record)
	}
	for _, requestID := range []string{"req_later_status_fixture", ""} {
		objectOnlyEvent := event
		objectOnlyEvent.ProviderRequestID = requestID
		objectOnlyEvent.LinkSource = RefundAuditLinkSourceRefundID
		objectOnly, err := NewRefundVerifiedEventRecord(objectOnlyEvent, &response)
		if err != nil || objectOnly.AttemptID != prepared.AttemptID || objectOnly.VerifiedEvent.LinkSource != RefundAuditLinkSourceRefundID {
			t.Fatalf("object-only association with event request ID %q failed: record=%+v err=%v", requestID, objectOnly, err)
		}
	}
	wrongObservedScope := event
	wrongObservedScope.ObservedProviderAccount = stringPtr("acct_other_fixture")
	if _, err := NewRefundVerifiedEventRecord(wrongObservedScope, &response); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("event observed-account mismatch error=%v; want exact-link rejection", err)
	}
	unknownResponseScope, err := NewRefundResponseAudit(prepared, RefundAuditResponse{
		Classification: RefundAuditResponseObserved, ProviderRequestID: "req_verified_fixture", RefundID: "re_verified_fixture",
		PaymentIntentID: "pi_refund_fixture", Currency: "usd", AmountCents: int64Ptr(700),
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRefundVerifiedEventRecord(event, &unknownResponseScope); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("unknown response scope error=%v; want exact link to remain unavailable", err)
	}
	missingFacts := event
	missingFacts.AmountCents = nil
	if _, err := NewRefundVerifiedEventRecord(missingFacts, &response); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("missing signed refund amount error=%v; want exact link to remain unavailable", err)
	}
	commandMismatch := event
	commandMismatch.AmountCents = int64Ptr(701)
	if _, err := NewRefundVerifiedEventRecord(commandMismatch, &response); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("command-inconsistent signed amount error=%v; want exact-link rejection", err)
	}
	later := observed.Add(time.Minute)
	event.ObservedAt = &later
	replayed, err := NewRefundVerifiedEventRecord(event, &response)
	if err != nil || replayed.RecordID != record.RecordID {
		t.Fatalf("event receipt time changed stable identity: record=%s replay=%s err=%v", record.RecordID, replayed.RecordID, err)
	}
	changed := event
	changed.RefundID = "re_other_fixture"
	if _, err := NewRefundVerifiedEventRecord(changed, &response); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("changed RefundID link error=%v; want rejected conflict", err)
	}

	unlinked := event
	unlinked.LinkClassification, unlinked.LinkSource = RefundAuditLinkUncorrelated, ""
	unlinked.PaymentIntentID = ""
	noLink, err := NewRefundVerifiedEventRecord(unlinked, nil)
	if err != nil || noLink.PaymentID != uuid.Nil || noLink.AttemptID != uuid.Nil {
		t.Fatalf("unlinked verified event acquired a Payment relationship: %+v err=%v", noLink, err)
	}
}

func TestVerifiedRefundEventObservationIdentityRetainsChangedStableFacts(t *testing.T) {
	observed := time.Now().UTC()
	event := VerifiedRefundEventAudit{
		ReceiptID: uuid.New(), EventID: "evt_changed_facts_fixture", EventType: "refund.updated",
		RefundID: "re_changed_facts_fixture", AmountCents: int64Ptr(700),
		LinkClassification: RefundAuditLinkUncorrelated, ObservedAt: &observed,
	}
	first, err := NewRefundVerifiedEventRecord(event, nil)
	if err != nil {
		t.Fatal(err)
	}
	later := observed.Add(time.Minute)
	event.ObservedAt = &later
	replay, err := NewRefundVerifiedEventRecord(event, nil)
	if err != nil || first.RecordID != replay.RecordID || first.ObservationDigest != replay.ObservationDigest {
		t.Fatalf("local observation time changed immutable observation identity: first=%s replay=%s err=%v", first.RecordID, replay.RecordID, err)
	}
	changed := event
	changed.AmountCents = int64Ptr(701)
	secondObservation, err := NewRefundVerifiedEventRecord(changed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ProviderEventID != secondObservation.ProviderEventID || first.RecordID == secondObservation.RecordID ||
		first.ObservationDigest == secondObservation.ObservationDigest {
		t.Fatal("changed safe facts were not retained as a distinct observation of the same provider event")
	}
}

func TestRefundAuditQueryRequiresIndexedSelector(t *testing.T) {
	if err := (RefundAuditQuery{}).Validate(); !errors.Is(err, ErrInvalidRefundAuditRecord) {
		t.Fatalf("unbounded audit query validation=%v; want rejection", err)
	}
	if err := (RefundAuditQuery{PaymentID: uuid.New(), Limit: 200}).Validate(); err != nil {
		t.Fatalf("bounded Payment query validation=%v", err)
	}
}

func refundAuditClaimFixture() FinancialClaim {
	paymentID, bookingID, userID := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":700,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	identity := FinancialIdentity{
		BookingID: bookingID, UserID: userID, Generation: 1, ProviderAccount: "acct_fixture", AmountCents: 700, Currency: "usd",
	}
	identity.Fingerprint, _ = FinancialFingerprint(identity, frozen)
	booking := bookingID
	refundKey := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	return FinancialClaim{
		PaymentID: paymentID, Token: uuid.New(), Fence: 2, Kind: "refund", Key: refundKey,
		Operation: CreateOperation{Identity: identity, PaymentID: paymentID, Key: refundKey, FrozenParams: frozen},
		View:      &FinancialView{Payment: &Payment{ID: paymentID, UserId: userID, BookingId: &booking, PaymentIntentId: "pi_refund_fixture"}},
		Refund:    &RefundObligation{PaymentID: paymentID, DecisionID: uuid.New(), Generation: 1, AmountCents: 700, Currency: "usd", Reason: "EXPIRED", Key: refundKey},
	}
}

func int64Ptr(value int64) *int64    { return &value }
func stringPtr(value string) *string { return &value }
func boolPtr(value bool) *bool       { return &value }
