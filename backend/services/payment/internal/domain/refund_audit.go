package domain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const RefundAuditSchemaVersion int16 = 1

const MaxRefundHTTPObservationDetails = 32

type RefundAuditKind string

const (
	RefundAuditKindPrepared      RefundAuditKind = "PREPARED"
	RefundAuditKindResponse      RefundAuditKind = "RESPONSE"
	RefundAuditKindVerifiedEvent RefundAuditKind = "VERIFIED_EVENT"
)

const (
	RefundAuditOutcomeUnknown          = "outcome_unknown"
	RefundAuditResponseObserved        = "response_observed"
	RefundAuditAdapterFailure          = "adapter_failure"
	RefundAuditIncompleteResponse      = "incomplete_response"
	RefundAuditMappedResponse          = "mapped_response"
	RefundAuditLinkExactRefundIdentity = "exact_refund_identity"
	RefundAuditLinkUncorrelated        = "uncorrelated"
	RefundAuditLinkAmbiguous           = "ambiguous"
	RefundAuditLinkConflict            = "conflict"
	RefundAuditLinkSourceRefundID      = "verified_refund_id"
	RefundAuditLinkSourceRequestAndID  = "verified_request_id_and_refund_id"
	RefundAuditFinishPersisted         = "persisted"
	RefundAuditFinishNotOwned          = "not_owned"
	RefundAuditFinishCommitUnknown     = "commit_unknown"
)

var (
	ErrInvalidRefundAuditRecord = errors.New("invalid refund audit record")
	ErrRefundAuditConflict      = errors.New("refund audit record identity conflict")
	ErrRefundAuditUnavailable   = errors.New("refund audit storage unavailable")
)

// RefundAuditRecord is a typed, versioned audit row. Zero UUIDs, zero digests,
// empty strings, and nil pointers represent facts that were not observed.
// Intended scope always comes from the frozen local command; response/event
// scope belongs in the observed fields on the typed payloads below.
type RefundAuditRecord struct {
	Version              int16
	RecordID             uuid.UUID
	PaymentID            uuid.UUID
	BookingID            uuid.UUID
	Generation           int64
	CommandDigest        [32]byte
	ObservationDigest    [32]byte
	AttemptID            uuid.UUID
	Kind                 RefundAuditKind
	RefundDecisionID     uuid.UUID
	Provider             string
	IntendedAccount      string
	IntendedLiveMode     *bool
	ScopeSource          string
	TargetAmountCents    *int64
	Currency             string
	PaymentIntentID      string
	IdempotencyKeyDigest [32]byte
	ClaimTokenDigest     [32]byte
	ClaimFence           int64
	ProviderRequestID    string
	ProviderRefundID     string
	ProviderEventID      string
	ObservedAt           *time.Time
	RecordedAt           time.Time
	Prepared             *RefundAuditPrepared
	Response             *RefundAuditResponse
	VerifiedEvent        *VerifiedRefundEventAudit
}

type RefundAuditPrepared struct {
	Version int16  `json:"version"`
	Stage   string `json:"stage"`
}

// RefundAuditResponse contains only allowlisted response facts. It deliberately
// has no raw body, headers, credentials, original key, or error text.
type RefundAuditResponse struct {
	Version                        int16                   `json:"version"`
	Classification                 string                  `json:"classification"`
	ErrorCategory                  string                  `json:"error_category,omitempty"`
	FinancialFinishOutcome         string                  `json:"financial_finish_outcome,omitempty"`
	ProviderRequestID              string                  `json:"provider_request_id,omitempty"`
	RefundID                       string                  `json:"refund_id,omitempty"`
	ChargeID                       string                  `json:"charge_id,omitempty"`
	PaymentIntentID                string                  `json:"payment_intent_id,omitempty"`
	ProviderStatus                 string                  `json:"provider_status,omitempty"`
	Currency                       string                  `json:"currency,omitempty"`
	AmountCents                    *int64                  `json:"amount_cents,omitempty"`
	ObservedProviderAccount        *string                 `json:"observed_provider_account,omitempty"`
	ObservedLiveMode               *bool                   `json:"observed_live_mode,omitempty"`
	SummarySource                  string                  `json:"summary_source,omitempty"`
	ObservedAt                     *time.Time              `json:"observed_at,omitempty"`
	TransportObservationState      string                  `json:"transport_observation_state"`
	ObservedTransportCalls         int64                   `json:"observed_transport_calls"`
	TransportObservationsTruncated bool                    `json:"transport_observations_truncated"`
	TransportObservations          []RefundHTTPObservation `json:"transport_observations,omitempty"`
	ConflictingFields              []string                `json:"conflicting_fields,omitempty"`
}

// RefundHTTPObservation is one observed HTTP RoundTrip. It stores only the
// response status, Request-ID, local observation time, ordinal, and a bounded
// result category; request/response contents and headers are never retained.
type RefundHTTPObservation struct {
	Ordinal     int64      `json:"ordinal"`
	StatusCode  int        `json:"status_code,omitempty"`
	RequestID   string     `json:"request_id,omitempty"`
	ResultClass string     `json:"result_class,omitempty"`
	ObservedAt  *time.Time `json:"observed_at,omitempty"`
}

// VerifiedRefundEventAudit is an optional sidecar populated only after the
// existing webhook signature and scope checks accept a receipt. Missing
// account/mode/request fields remain nil or empty. The idempotency key is kept
// as a digest only.
type VerifiedRefundEventAudit struct {
	Version                 int16      `json:"version"`
	ReceiptID               uuid.UUID  `json:"receipt_id"`
	EventID                 string     `json:"event_id"`
	EventType               string     `json:"event_type,omitempty"`
	ObjectType              string     `json:"object_type,omitempty"`
	ObjectID                string     `json:"object_id,omitempty"`
	ProviderRequestID       string     `json:"provider_request_id,omitempty"`
	IdempotencyKeyDigestHex *string    `json:"idempotency_key_digest,omitempty"`
	ObservedProviderAccount *string    `json:"observed_provider_account,omitempty"`
	ObservedLiveMode        *bool      `json:"observed_live_mode,omitempty"`
	RefundID                string     `json:"refund_id,omitempty"`
	ChargeID                string     `json:"charge_id,omitempty"`
	PaymentIntentID         string     `json:"payment_intent_id,omitempty"`
	ProviderStatus          string     `json:"provider_status,omitempty"`
	Currency                string     `json:"currency,omitempty"`
	AmountCents             *int64     `json:"amount_cents,omitempty"`
	LinkClassification      string     `json:"link_classification"`
	LinkSource              string     `json:"link_source,omitempty"`
	EventCreatedAt          *time.Time `json:"event_created_at,omitempty"`
	ObservedAt              *time.Time `json:"observed_at,omitempty"`
}

// RefundAuditQuery requires an indexed identity selector. Every populated
// selector is an additional AND predicate. Limit is bounded by the repository
// to prevent historical table scans and prevent union searches from becoming
// implicit correlation rules.
type RefundAuditQuery struct {
	PaymentID         uuid.UUID
	BookingID         uuid.UUID
	Generation        int64
	CommandDigest     [32]byte
	AttemptID         uuid.UUID
	ProviderRequestID string
	ProviderRefundID  string
	ProviderEventID   string
	Kind              RefundAuditKind
	Limit             int
}

func (q RefundAuditQuery) Validate() error {
	if q.PaymentID == uuid.Nil &&
		(q.BookingID == uuid.Nil || q.Generation < 1) &&
		q.CommandDigest == ([32]byte{}) && q.AttemptID == uuid.Nil &&
		q.ProviderRequestID == "" && q.ProviderRefundID == "" && q.ProviderEventID == "" {
		return fmt.Errorf("%w: query requires an indexed identity", ErrInvalidRefundAuditRecord)
	}
	if q.Kind != "" && q.Kind != RefundAuditKindPrepared && q.Kind != RefundAuditKindResponse && q.Kind != RefundAuditKindVerifiedEvent {
		return fmt.Errorf("%w: unknown query kind", ErrInvalidRefundAuditRecord)
	}
	if q.Limit < 0 || q.Limit > 1000 {
		return fmt.Errorf("%w: query limit outside 0..1000", ErrInvalidRefundAuditRecord)
	}
	return nil
}

// NewRefundPreparedAudit builds the immutable command identity after the guard
// has confirmed the current owner and frozen command. It stores only digests
// for the stable key and claim token.
func NewRefundPreparedAudit(claim FinancialClaim, attemptID uuid.UUID) (RefundAuditRecord, error) {
	if attemptID == uuid.Nil || claim.Kind != "refund" || claim.PaymentID == uuid.Nil || claim.Token == uuid.Nil || claim.Fence < 1 || claim.Refund == nil || claim.View == nil || claim.View.Payment == nil {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	identity := claim.Operation.Identity
	if identity.BookingID == uuid.Nil || identity.UserID == uuid.Nil || identity.Generation < 1 || identity.AmountCents < 1 || identity.Currency == "" || identity.ProviderAccount == "" {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	if claim.Operation.PaymentID != claim.PaymentID || claim.Refund.PaymentID != claim.PaymentID || claim.Refund.Generation != identity.Generation ||
		claim.Refund.DecisionID == uuid.Nil || claim.Refund.AmountCents < 1 || claim.Refund.Currency != identity.Currency || claim.Refund.Key == "" ||
		claim.Key != claim.Refund.Key || claim.Operation.Key != claim.Refund.Key || claim.View.Payment.PaymentIntentId == "" {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	wantRefundKey := fmt.Sprintf("ticketbox:payment:%s:g%d:refund", claim.PaymentID, identity.Generation)
	if claim.Refund.Key != wantRefundKey {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	fingerprint, err := FinancialFingerprint(identity, claim.Operation.FrozenParams)
	if err != nil || fingerprint != identity.Fingerprint {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	keyDigest := sha256.Sum256([]byte(claim.Refund.Key))
	tokenDigest := sha256.Sum256([]byte(claim.Token.String()))
	reasonDigest := sha256.Sum256([]byte(claim.Refund.Reason))
	mode := identity.LiveMode
	target := claim.Refund.AmountCents

	canonical, err := json.Marshal(struct {
		Version              int    `json:"version"`
		PaymentID            string `json:"payment_id"`
		BookingID            string `json:"booking_id"`
		Generation           int64  `json:"generation"`
		RefundDecisionID     string `json:"refund_decision_id"`
		Provider             string `json:"provider"`
		IntendedAccount      string `json:"intended_account"`
		IntendedLiveMode     bool   `json:"intended_live_mode"`
		TargetAmountCents    int64  `json:"target_amount_cents"`
		Currency             string `json:"currency"`
		PaymentIntentID      string `json:"payment_intent_id"`
		FrozenRequestDigest  string `json:"frozen_request_digest"`
		IdempotencyKeyDigest string `json:"idempotency_key_digest"`
		RefundReasonDigest   string `json:"refund_reason_digest"`
	}{
		Version: int(RefundAuditSchemaVersion), PaymentID: claim.PaymentID.String(), BookingID: identity.BookingID.String(),
		Generation: identity.Generation, RefundDecisionID: claim.Refund.DecisionID.String(), Provider: "stripe",
		IntendedAccount: identity.ProviderAccount, IntendedLiveMode: identity.LiveMode, TargetAmountCents: target,
		Currency: identity.Currency, PaymentIntentID: claim.View.Payment.PaymentIntentId,
		FrozenRequestDigest: hex.EncodeToString(fingerprint[:]), IdempotencyKeyDigest: hex.EncodeToString(keyDigest[:]),
		RefundReasonDigest: hex.EncodeToString(reasonDigest[:]),
	})
	if err != nil {
		return RefundAuditRecord{}, fmt.Errorf("%w: encode command digest", ErrInvalidRefundAuditRecord)
	}
	commandDigest := sha256.Sum256(canonical)

	return RefundAuditRecord{
		Version: RefundAuditSchemaVersion, RecordID: attemptID, PaymentID: claim.PaymentID, BookingID: identity.BookingID,
		Generation: identity.Generation, CommandDigest: commandDigest, AttemptID: attemptID, Kind: RefundAuditKindPrepared,
		RefundDecisionID: claim.Refund.DecisionID, Provider: "stripe", IntendedAccount: identity.ProviderAccount,
		IntendedLiveMode: &mode, ScopeSource: "frozen_payment_command", TargetAmountCents: &target, Currency: identity.Currency,
		PaymentIntentID: claim.View.Payment.PaymentIntentId, IdempotencyKeyDigest: keyDigest, ClaimTokenDigest: tokenDigest,
		ClaimFence: claim.Fence, Prepared: &RefundAuditPrepared{Version: RefundAuditSchemaVersion, Stage: "pre_create"},
	}, nil
}

// NewRefundResponseAudit preserves the prepared command identity while adding
// one attempt-bound response result. A deterministic record ID makes retries
// of the same append idempotent and changed facts an explicit conflict.
func NewRefundResponseAudit(prepared RefundAuditRecord, response RefundAuditResponse) (RefundAuditRecord, error) {
	if prepared.Kind != RefundAuditKindPrepared || prepared.RecordID == uuid.Nil || prepared.AttemptID == uuid.Nil || prepared.CommandDigest == ([32]byte{}) {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	response.Version = RefundAuditSchemaVersion
	if response.Classification == "" {
		response.Classification = RefundAuditOutcomeUnknown
	}
	response = cloneRefundAuditResponse(response)
	record := prepared
	record.RecordID = refundAuditRecordID("attempt:" + prepared.AttemptID.String() + ":response")
	record.Kind = RefundAuditKindResponse
	record.Prepared = nil
	record.Response = &response
	record.ObservedAt = cloneTime(response.ObservedAt)
	record.ProviderRequestID = response.ProviderRequestID
	record.ProviderRefundID = response.RefundID
	return record, nil
}

// NewRefundVerifiedEventRecord materializes a verified receipt sidecar. link
// must be a persisted RESPONSE record whose RefundID exactly matches the event;
// metadata-only relationships are rejected. A nil link leaves command scope
// and attempt identity unknown while retaining the verified event itself.
func NewRefundVerifiedEventRecord(event VerifiedRefundEventAudit, link *RefundAuditRecord) (RefundAuditRecord, error) {
	if event.ReceiptID == uuid.Nil || event.EventID == "" {
		return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
	}
	event.Version = RefundAuditSchemaVersion
	if event.LinkClassification == "" && link == nil {
		event.LinkClassification = RefundAuditLinkUncorrelated
	}
	event = cloneVerifiedRefundEvent(event)
	record := RefundAuditRecord{
		Version: RefundAuditSchemaVersion,
		Kind:    RefundAuditKindVerifiedEvent, ProviderEventID: event.EventID,
		ProviderRequestID: event.ProviderRequestID, ProviderRefundID: event.RefundID,
		ObservedAt: cloneTime(event.ObservedAt), VerifiedEvent: &event,
	}
	if link != nil {
		if ValidateRefundAuditRecord(*link) != nil || link.Kind != RefundAuditKindResponse || link.Response == nil || link.CommandDigest == ([32]byte{}) ||
			event.LinkClassification != RefundAuditLinkExactRefundIdentity ||
			(event.LinkSource != RefundAuditLinkSourceRefundID && event.LinkSource != RefundAuditLinkSourceRequestAndID) ||
			event.RefundID == "" || event.RefundID != link.Response.RefundID || link.PaymentIntentID == "" ||
			(event.LinkSource == RefundAuditLinkSourceRequestAndID && (event.ProviderRequestID == "" || event.ProviderRequestID != link.Response.ProviderRequestID)) ||
			!refundAuditEventMatchesResponse(event, *link) {
			return RefundAuditRecord{}, ErrInvalidRefundAuditRecord
		}
		record.PaymentID, record.BookingID, record.Generation = link.PaymentID, link.BookingID, link.Generation
		record.CommandDigest, record.AttemptID = link.CommandDigest, link.AttemptID
		record.RefundDecisionID, record.Provider = link.RefundDecisionID, link.Provider
		record.IntendedAccount, record.IntendedLiveMode = link.IntendedAccount, cloneBool(link.IntendedLiveMode)
		record.ScopeSource = link.ScopeSource
		record.TargetAmountCents, record.Currency, record.PaymentIntentID = cloneInt64(link.TargetAmountCents), link.Currency, link.PaymentIntentID
		record.IdempotencyKeyDigest, record.ClaimTokenDigest, record.ClaimFence = link.IdempotencyKeyDigest, link.ClaimTokenDigest, link.ClaimFence
	}
	digest, err := RefundAuditObservationDigest(record)
	if err != nil {
		return RefundAuditRecord{}, err
	}
	record.ObservationDigest = digest
	record.RecordID = refundAuditObservationRecordID(event, digest)
	if err := ValidateRefundAuditRecord(record); err != nil {
		return RefundAuditRecord{}, err
	}
	return record, nil
}

func refundAuditEventMatchesResponse(event VerifiedRefundEventAudit, responseRecord RefundAuditRecord) bool {
	response := responseRecord.Response
	if response == nil || response.Classification != RefundAuditResponseObserved || len(response.ConflictingFields) != 0 ||
		event.ObjectType != "refund" || event.ObjectID == "" || event.ObjectID != event.RefundID ||
		event.PaymentIntentID == "" || event.AmountCents == nil || event.Currency == "" ||
		response.PaymentIntentID == "" || response.AmountCents == nil || response.Currency == "" ||
		event.ObservedProviderAccount == nil || response.ObservedProviderAccount == nil ||
		event.ObservedLiveMode == nil || response.ObservedLiveMode == nil ||
		*event.ObservedProviderAccount != *response.ObservedProviderAccount || *event.ObservedLiveMode != *response.ObservedLiveMode ||
		responseRecord.IntendedAccount != *event.ObservedProviderAccount || responseRecord.IntendedLiveMode == nil ||
		*responseRecord.IntendedLiveMode != *event.ObservedLiveMode {
		return false
	}
	if response.PaymentIntentID != "" && response.PaymentIntentID != responseRecord.PaymentIntentID {
		return false
	}
	if response.AmountCents != nil && (responseRecord.TargetAmountCents == nil || *response.AmountCents != *responseRecord.TargetAmountCents) {
		return false
	}
	if response.Currency != "" && response.Currency != responseRecord.Currency {
		return false
	}
	if event.PaymentIntentID != "" && (event.PaymentIntentID != responseRecord.PaymentIntentID ||
		(response.PaymentIntentID != "" && event.PaymentIntentID != response.PaymentIntentID)) {
		return false
	}
	if event.ChargeID != "" && response.ChargeID != "" && event.ChargeID != response.ChargeID {
		return false
	}
	if event.AmountCents != nil && (responseRecord.TargetAmountCents == nil || *event.AmountCents != *responseRecord.TargetAmountCents ||
		(response.AmountCents != nil && *event.AmountCents != *response.AmountCents)) {
		return false
	}
	if event.Currency != "" && (event.Currency != responseRecord.Currency ||
		(response.Currency != "" && event.Currency != response.Currency)) {
		return false
	}
	if event.ObjectType == "refund" && event.ObjectID != "" && event.ObjectID != event.RefundID {
		return false
	}
	if response.ObservedProviderAccount != nil && *response.ObservedProviderAccount != responseRecord.IntendedAccount {
		return false
	}
	if response.ObservedLiveMode != nil && responseRecord.IntendedLiveMode != nil && *response.ObservedLiveMode != *responseRecord.IntendedLiveMode {
		return false
	}
	return true
}

func refundAuditRecordID(identity string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("urn:ticketbox:payment:refund-audit:v1:"+identity))
}

// RefundResponseCapture is scoped to one application attempt in its context.
// It is safe for concurrent transport callbacks and bounds retained detail
// while preserving the exact number of observed RoundTrip calls.
type RefundResponseCapture struct {
	AttemptID uuid.UUID
	mu        sync.Mutex
	response  RefundAuditResponse
}

func NewRefundResponseCapture(attemptID uuid.UUID) *RefundResponseCapture {
	return &RefundResponseCapture{AttemptID: attemptID, response: RefundAuditResponse{
		Version: RefundAuditSchemaVersion, Classification: RefundAuditOutcomeUnknown,
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
		TransportObservations: make([]RefundHTTPObservation, 0, MaxRefundHTTPObservationDetails),
	}}
}

type refundResponseCaptureContextKey struct{}

func WithRefundResponseCapture(ctx context.Context, capture *RefundResponseCapture) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, refundResponseCaptureContextKey{}, capture)
}

func RefundResponseCaptureFromContext(ctx context.Context) *RefundResponseCapture {
	if ctx == nil {
		return nil
	}
	capture, _ := ctx.Value(refundResponseCaptureContextKey{}).(*RefundResponseCapture)
	return capture
}

func (c *RefundResponseCapture) RecordResponse(response RefundAuditResponse) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	response.Version = RefundAuditSchemaVersion
	if response.Classification == "" {
		response.Classification = c.response.Classification
	}
	if response.ErrorCategory == "" {
		response.ErrorCategory = c.response.ErrorCategory
	}
	if response.TransportObservationState == "" {
		response.TransportObservationState = c.response.TransportObservationState
	}
	response.ObservedTransportCalls = c.response.ObservedTransportCalls
	response.TransportObservationsTruncated = c.response.TransportObservationsTruncated
	response.TransportObservations = cloneRefundHTTPObservations(c.response.TransportObservations)
	response.ConflictingFields = append([]string(nil), c.response.ConflictingFields...)
	mergeRefundResponseFields(&response, c.response)
	if response.ObservedAt == nil {
		response.ObservedAt = cloneTime(c.response.ObservedAt)
		if response.ObservedAt == nil {
			now := time.Now().UTC()
			response.ObservedAt = &now
		}
	}
	c.response = cloneRefundAuditResponse(response)
}

// MarkTransportObserverAttached distinguishes an observed zero-call count from
// a transport that was not instrumented. Call this before invoking the SDK.
func (c *RefundResponseCapture) MarkTransportObserverAttached() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.response.ObservedTransportCalls < 0 {
		c.response.ObservedTransportCalls = 0
	}
	c.response.TransportObservationState = "instrumented"
}

func (c *RefundResponseCapture) RecordHTTPObservation(observation RefundHTTPObservation) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.response.ObservedTransportCalls < 0 {
		c.response.ObservedTransportCalls = 0
	}
	c.response.ObservedTransportCalls++
	observation.Ordinal = c.response.ObservedTransportCalls
	if observation.ObservedAt == nil {
		now := time.Now().UTC()
		observation.ObservedAt = &now
	}
	c.response.TransportObservationState = "observed"
	if len(c.response.TransportObservations) < MaxRefundHTTPObservationDetails {
		c.response.TransportObservations = append(c.response.TransportObservations, observation)
	} else {
		c.response.TransportObservationsTruncated = true
	}
}

func (c *RefundResponseCapture) SetResult(classification, errorCategory string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if classification != "" {
		c.response.Classification = classification
	}
	c.response.ErrorCategory = errorCategory
}

func (c *RefundResponseCapture) SetFinancialFinishOutcome(outcome string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.response.FinancialFinishOutcome = outcome
}

func (c *RefundResponseCapture) Snapshot() RefundAuditResponse {
	if c == nil {
		return RefundAuditResponse{Version: RefundAuditSchemaVersion, Classification: RefundAuditOutcomeUnknown, TransportObservationState: "unknown"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneRefundAuditResponse(c.response)
}

func cloneRefundAuditResponse(response RefundAuditResponse) RefundAuditResponse {
	response.AmountCents = cloneInt64(response.AmountCents)
	response.ObservedProviderAccount = cloneString(response.ObservedProviderAccount)
	response.ObservedLiveMode = cloneBool(response.ObservedLiveMode)
	response.ObservedAt = cloneTime(response.ObservedAt)
	response.TransportObservations = cloneRefundHTTPObservations(response.TransportObservations)
	response.ConflictingFields = append([]string(nil), response.ConflictingFields...)
	return response
}

func mergeRefundResponseFields(target *RefundAuditResponse, previous RefundAuditResponse) {
	mergeString := func(name string, dst *string, old string) {
		if *dst == "" {
			*dst = old
		} else if old != "" && old != *dst {
			target.ConflictingFields = appendUniqueString(target.ConflictingFields, name)
			*dst = old
		}
	}
	mergeString("provider_request_id", &target.ProviderRequestID, previous.ProviderRequestID)
	mergeString("refund_id", &target.RefundID, previous.RefundID)
	mergeString("charge_id", &target.ChargeID, previous.ChargeID)
	mergeString("payment_intent_id", &target.PaymentIntentID, previous.PaymentIntentID)
	mergeString("provider_status", &target.ProviderStatus, previous.ProviderStatus)
	mergeString("currency", &target.Currency, previous.Currency)
	mergeString("error_category", &target.ErrorCategory, previous.ErrorCategory)
	mergeString("financial_finish_outcome", &target.FinancialFinishOutcome, previous.FinancialFinishOutcome)
	if target.SummarySource == "" {
		target.SummarySource = previous.SummarySource
	} else if previous.SummarySource != "" && previous.SummarySource != target.SummarySource {
		target.SummarySource = "mixed"
	}
	if target.AmountCents == nil {
		target.AmountCents = cloneInt64(previous.AmountCents)
	} else if previous.AmountCents != nil && *target.AmountCents != *previous.AmountCents {
		target.ConflictingFields = appendUniqueString(target.ConflictingFields, "amount_cents")
		target.AmountCents = cloneInt64(previous.AmountCents)
	}
	if target.ObservedProviderAccount == nil {
		target.ObservedProviderAccount = cloneString(previous.ObservedProviderAccount)
	} else if previous.ObservedProviderAccount != nil && *target.ObservedProviderAccount != *previous.ObservedProviderAccount {
		target.ConflictingFields = appendUniqueString(target.ConflictingFields, "observed_provider_account")
		target.ObservedProviderAccount = cloneString(previous.ObservedProviderAccount)
	}
	if target.ObservedLiveMode == nil {
		target.ObservedLiveMode = cloneBool(previous.ObservedLiveMode)
	} else if previous.ObservedLiveMode != nil && *target.ObservedLiveMode != *previous.ObservedLiveMode {
		target.ConflictingFields = appendUniqueString(target.ConflictingFields, "observed_live_mode")
		target.ObservedLiveMode = cloneBool(previous.ObservedLiveMode)
	}
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func cloneRefundHTTPObservations(observations []RefundHTTPObservation) []RefundHTTPObservation {
	if observations == nil {
		return nil
	}
	cloned := make([]RefundHTTPObservation, len(observations))
	for i := range observations {
		cloned[i] = observations[i]
		cloned[i].ObservedAt = cloneTime(observations[i].ObservedAt)
	}
	return cloned
}

func cloneVerifiedRefundEvent(event VerifiedRefundEventAudit) VerifiedRefundEventAudit {
	event.ObservedProviderAccount = cloneString(event.ObservedProviderAccount)
	event.IdempotencyKeyDigestHex = cloneString(event.IdempotencyKeyDigestHex)
	event.ObservedLiveMode = cloneBool(event.ObservedLiveMode)
	event.AmountCents = cloneInt64(event.AmountCents)
	event.EventCreatedAt = cloneTime(event.EventCreatedAt)
	event.ObservedAt = cloneTime(event.ObservedAt)
	return event
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

// RefundAuditStableContent produces the canonical comparison form used when
// the same RecordID is appended twice. Database and observation timestamps are
// volatile; all other safe fields remain conflict-significant.
func RefundAuditStableContent(record RefundAuditRecord) ([]byte, error) {
	record.RecordedAt = time.Time{}
	record.ObservedAt = nil
	if record.Response != nil {
		response := cloneRefundAuditResponse(*record.Response)
		response.ObservedAt = nil
		for i := range response.TransportObservations {
			response.TransportObservations[i].ObservedAt = nil
		}
		record.Response = &response
	}
	if record.VerifiedEvent != nil {
		event := cloneVerifiedRefundEvent(*record.VerifiedEvent)
		event.ObservedAt = nil
		record.VerifiedEvent = &event
	}
	return json.Marshal(record)
}

// RefundAuditObservationDigest separates immutable observations of one
// provider event from the provider event identity. Local observation and DB
// insertion times are excluded; safe stable facts and any exact attempt link
// remain conflict-significant.
func RefundAuditObservationDigest(record RefundAuditRecord) ([32]byte, error) {
	if record.Kind != RefundAuditKindVerifiedEvent || record.VerifiedEvent == nil {
		return [32]byte{}, ErrInvalidRefundAuditRecord
	}
	record.RecordID = uuid.Nil
	record.ObservationDigest = [32]byte{}
	stable, err := RefundAuditStableContent(record)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(stable), nil
}

func RefundAuditDigestHex(digest [32]byte) string {
	return hex.EncodeToString(digest[:])
}

func ValidateRefundAuditRecord(record RefundAuditRecord) error {
	if record.Version != RefundAuditSchemaVersion || record.RecordID == uuid.Nil {
		return ErrInvalidRefundAuditRecord
	}
	payloadCount := 0
	if record.Prepared != nil {
		payloadCount++
	}
	if record.Response != nil {
		payloadCount++
	}
	if record.VerifiedEvent != nil {
		payloadCount++
	}
	if payloadCount != 1 {
		return ErrInvalidRefundAuditRecord
	}
	switch record.Kind {
	case RefundAuditKindPrepared:
		if record.Prepared == nil || record.Response != nil || record.VerifiedEvent != nil ||
			!refundAuditHasCommand(record) || record.ObservationDigest != ([32]byte{}) || record.RecordID != record.AttemptID ||
			record.Prepared.Version != RefundAuditSchemaVersion || record.Prepared.Stage != "pre_create" {
			return ErrInvalidRefundAuditRecord
		}
	case RefundAuditKindResponse:
		if record.Response == nil || record.Prepared != nil || record.VerifiedEvent != nil ||
			!refundAuditHasCommand(record) || record.ObservationDigest != ([32]byte{}) || record.AttemptID == uuid.Nil ||
			record.Response.Version != RefundAuditSchemaVersion || !validRefundAuditClassification(record.Response.Classification) ||
			record.ProviderRequestID != record.Response.ProviderRequestID || record.ProviderRefundID != record.Response.RefundID ||
			!validRefundAuditFinishOutcome(record.Response.FinancialFinishOutcome) ||
			!validRefundAuditTransport(record.Response) {
			return ErrInvalidRefundAuditRecord
		}
		if record.Response.ErrorCategory != "" && !isSafeRefundAuditCategory(record.Response.ErrorCategory) {
			return ErrInvalidRefundAuditRecord
		}
	case RefundAuditKindVerifiedEvent:
		if record.VerifiedEvent == nil || record.Prepared != nil || record.Response != nil ||
			record.ObservationDigest == ([32]byte{}) ||
			record.VerifiedEvent.Version != RefundAuditSchemaVersion || record.VerifiedEvent.ReceiptID == uuid.Nil ||
			record.VerifiedEvent.EventID == "" || record.ProviderEventID != record.VerifiedEvent.EventID ||
			record.ProviderRequestID != record.VerifiedEvent.ProviderRequestID || record.ProviderRefundID != record.VerifiedEvent.RefundID ||
			!validRefundAuditLink(record.VerifiedEvent.LinkClassification, record.VerifiedEvent.LinkSource) {
			return ErrInvalidRefundAuditRecord
		}
		if record.VerifiedEvent.LinkClassification == RefundAuditLinkExactRefundIdentity {
			event := record.VerifiedEvent
			if !refundAuditHasCommand(record) || event.ObservedProviderAccount == nil || event.ObservedLiveMode == nil ||
				*event.ObservedProviderAccount != record.IntendedAccount || record.IntendedLiveMode == nil ||
				*event.ObservedLiveMode != *record.IntendedLiveMode || event.RefundID == "" ||
				event.ObjectType != "refund" || event.ObjectID == "" || event.ObjectID != event.RefundID ||
				event.PaymentIntentID == "" || event.PaymentIntentID != record.PaymentIntentID ||
				event.AmountCents == nil || *event.AmountCents != *record.TargetAmountCents || event.Currency == "" || event.Currency != record.Currency {
				return ErrInvalidRefundAuditRecord
			}
		}
		digest, err := RefundAuditObservationDigest(record)
		if err != nil || digest != record.ObservationDigest ||
			record.RecordID != refundAuditObservationRecordID(*record.VerifiedEvent, digest) {
			return ErrInvalidRefundAuditRecord
		}
	default:
		return ErrInvalidRefundAuditRecord
	}
	return nil
}

func refundAuditObservationRecordID(event VerifiedRefundEventAudit, digest [32]byte) uuid.UUID {
	return refundAuditRecordID("receipt:" + event.ReceiptID.String() + ":event:" + event.EventID + ":observation:" + hex.EncodeToString(digest[:]))
}

func refundAuditHasCommand(record RefundAuditRecord) bool {
	return record.PaymentID != uuid.Nil && record.BookingID != uuid.Nil && record.Generation > 0 &&
		record.CommandDigest != ([32]byte{}) && record.AttemptID != uuid.Nil && record.RefundDecisionID != uuid.Nil &&
		record.Provider == "stripe" && record.IntendedAccount != "" && record.IntendedLiveMode != nil &&
		record.ScopeSource == "frozen_payment_command" && record.TargetAmountCents != nil && *record.TargetAmountCents > 0 &&
		record.Currency != "" && record.PaymentIntentID != "" && record.IdempotencyKeyDigest != ([32]byte{}) &&
		record.ClaimTokenDigest != ([32]byte{}) && record.ClaimFence > 0
}

func validRefundAuditClassification(value string) bool {
	switch value {
	case RefundAuditOutcomeUnknown, RefundAuditResponseObserved, RefundAuditAdapterFailure, RefundAuditIncompleteResponse, RefundAuditMappedResponse:
		return true
	default:
		return false
	}
}

func validRefundAuditFinishOutcome(value string) bool {
	return value == "" || value == RefundAuditFinishPersisted || value == RefundAuditFinishNotOwned || value == RefundAuditFinishCommitUnknown
}

func validRefundAuditTransport(response *RefundAuditResponse) bool {
	if response.TransportObservationState != "unknown" && response.TransportObservationState != "instrumented" && response.TransportObservationState != "observed" {
		return false
	}
	if response.TransportObservationState == "unknown" && response.ObservedTransportCalls != -1 {
		return false
	}
	if response.TransportObservationState == "instrumented" && response.ObservedTransportCalls < 0 {
		return false
	}
	if response.TransportObservationState == "observed" && response.ObservedTransportCalls < 1 {
		return false
	}
	if response.ObservedTransportCalls < -1 || len(response.TransportObservations) > MaxRefundHTTPObservationDetails {
		return false
	}
	if response.TransportObservationsTruncated != (response.ObservedTransportCalls > MaxRefundHTTPObservationDetails) {
		return false
	}
	for i, item := range response.TransportObservations {
		if item.Ordinal != int64(i+1) || item.StatusCode < 0 || item.StatusCode > 599 || len(item.RequestID) > 200 || len(item.ResultClass) > 64 {
			return false
		}
	}
	return true
}

func validRefundAuditLink(classification, source string) bool {
	switch classification {
	case RefundAuditLinkUncorrelated, RefundAuditLinkAmbiguous, RefundAuditLinkConflict:
		return source == ""
	case RefundAuditLinkExactRefundIdentity:
		return source == RefundAuditLinkSourceRefundID || source == RefundAuditLinkSourceRequestAndID
	default:
		return false
	}
}

func isSafeRefundAuditCategory(value string) bool {
	if len(value) > 80 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return value != ""
}
