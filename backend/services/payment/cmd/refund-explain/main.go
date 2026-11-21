package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

const (
	refundExplainRecordLimit          = 200
	refundExplainProbeLimit           = refundExplainRecordLimit + 1
	refundExplainRelatedIdentityLimit = 64
)

type explanation struct {
	Status                 string                  `json:"status"`
	EvidenceScope          string                  `json:"evidence_scope"`
	PaymentID              string                  `json:"payment_id"`
	QueryLimit             int                     `json:"query_limit,omitempty"`
	PrimaryRowsIncluded    int                     `json:"primary_rows_included,omitempty"`
	RelatedIdentityQueries int                     `json:"related_identity_queries,omitempty"`
	MayBeTruncated         bool                    `json:"may_be_truncated"`
	Commands               []commandExplanation    `json:"commands,omitempty"`
	Attempts               []attemptExplanation    `json:"attempts,omitempty"`
	RelatedEventCandidates []relatedEventCandidate `json:"related_event_candidates,omitempty"`
	Findings               []string                `json:"findings,omitempty"`
	Limitations            []string                `json:"limitations"`
}

type relatedEventCandidate struct {
	EventID                 string   `json:"event_id"`
	ObservationDigest       string   `json:"observation_digest"`
	EventType               string   `json:"event_type,omitempty"`
	RequestID               string   `json:"provider_request_id,omitempty"`
	RefundID                string   `json:"refund_id,omitempty"`
	ObservedProviderAccount *string  `json:"observed_provider_account,omitempty"`
	ObservedLiveMode        *bool    `json:"observed_livemode,omitempty"`
	LinkClassification      string   `json:"stored_link_classification"`
	MatchedBy               []string `json:"candidate_match_fields"`
	SamePaymentScope        bool     `json:"same_payment_scope"`
}

type commandExplanation struct {
	CommandDigest string   `json:"command_digest"`
	Generation    int64    `json:"generation"`
	AmountCents   *int64   `json:"target_amount_cents,omitempty"`
	Currency      string   `json:"currency,omitempty"`
	PaymentIntent string   `json:"payment_intent_id,omitempty"`
	Account       string   `json:"intended_account,omitempty"`
	LiveMode      *bool    `json:"intended_live_mode,omitempty"`
	AttemptIDs    []string `json:"application_attempt_ids"`
}

type responseExplanation struct {
	Classification          string   `json:"classification"`
	ErrorCategory           string   `json:"error_category,omitempty"`
	FinancialFinishOutcome  string   `json:"financial_finish_outcome,omitempty"`
	RequestID               string   `json:"provider_request_id,omitempty"`
	RefundID                string   `json:"refund_id,omitempty"`
	ChargeID                string   `json:"charge_id,omitempty"`
	PaymentIntentID         string   `json:"payment_intent_id,omitempty"`
	Status                  string   `json:"provider_status,omitempty"`
	AmountCents             *int64   `json:"amount_cents,omitempty"`
	Currency                string   `json:"currency,omitempty"`
	ObservedProviderAccount *string  `json:"observed_provider_account,omitempty"`
	ObservedLiveMode        *bool    `json:"observed_livemode,omitempty"`
	TransportState          string   `json:"transport_observation_state"`
	ObservedTransportCalls  *int64   `json:"observed_transport_calls,omitempty"`
	TransportDetailCount    int      `json:"transport_observation_detail_count"`
	TransportTruncated      bool     `json:"transport_observations_truncated"`
	ConflictingFields       []string `json:"conflicting_fields,omitempty"`
}

type eventExplanation struct {
	EventID                 string     `json:"event_id"`
	EventType               string     `json:"event_type,omitempty"`
	ObjectType              string     `json:"object_type,omitempty"`
	ObjectID                string     `json:"object_id,omitempty"`
	RequestID               string     `json:"provider_request_id,omitempty"`
	RefundID                string     `json:"refund_id,omitempty"`
	ChargeID                string     `json:"charge_id,omitempty"`
	PaymentIntentID         string     `json:"payment_intent_id,omitempty"`
	ProviderStatus          string     `json:"provider_status,omitempty"`
	Currency                string     `json:"currency,omitempty"`
	AmountCents             *int64     `json:"amount_cents,omitempty"`
	ObservedProviderAccount *string    `json:"observed_provider_account,omitempty"`
	ObservedLiveMode        *bool      `json:"observed_livemode,omitempty"`
	EventCreatedAt          *time.Time `json:"event_created_at,omitempty"`
	LinkClassification      string     `json:"link_classification"`
	LinkSource              string     `json:"link_source,omitempty"`
}

type attemptExplanation struct {
	AttemptID      string               `json:"application_attempt_id"`
	Prepared       bool                 `json:"prepared"`
	PreparedAt     *time.Time           `json:"prepared_recorded_at,omitempty"`
	CommandDigest  string               `json:"command_digest"`
	Status         string               `json:"status"`
	Response       *responseExplanation `json:"response,omitempty"`
	VerifiedEvents []eventExplanation   `json:"verified_events,omitempty"`
}

type refundExplainAttemptGroup struct {
	view         attemptExplanation
	seenResponse bool
}

func main() { os.Exit(run()) }

func run() int {
	flags := flag.NewFlagSet("refund-explain", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	paymentText := flags.String("payment-id", "", "Payment UUID in the isolated synthetic refund component database")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 {
		return 2
	}
	paymentID, err := uuid.Parse(*paymentText)
	if err != nil || paymentID == uuid.Nil {
		writeJSON(explanation{
			Status: "AUDIT_UNAVAILABLE", EvidenceScope: "SYNTHETIC_COMPONENT_ONLY", PaymentID: *paymentText,
			Findings:    []string{"A valid --payment-id UUID is required."},
			Limitations: []string{"This command reads only a validated, loopback, ownership-manifested synthetic Payment database."},
		})
		return 2
	}
	dsn := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	manifest := os.Getenv("REFUND_COMPONENT_OWNER_MANIFEST")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := repository.OpenRefundComponentDatabase(ctx, dsn, manifest)
	if err != nil {
		writeJSON(explanation{
			Status: "AUDIT_UNAVAILABLE", EvidenceScope: "SYNTHETIC_COMPONENT_ONLY", PaymentID: paymentID.String(),
			Findings:    []string{"The configured database did not pass the refund component ownership and PostgreSQL identity checks."},
			Limitations: []string{"No Provider request or financial write was attempted."},
		})
		return 2
	}
	defer pool.Close()
	repo := repository.NewPostgresPaymentRepository(pool)
	auditRepo, ok := any(repo).(repository.RefundAuditRepository)
	if !ok {
		writeJSON(explanation{
			Status: "AUDIT_UNAVAILABLE", EvidenceScope: "SYNTHETIC_COMPONENT_ONLY", PaymentID: paymentID.String(),
			Findings:    []string{"The Payment repository does not expose the optional refund audit capability."},
			Limitations: []string{"No Provider request or financial write was attempted."},
		})
		return 2
	}
	loaded, err := loadRefundExplanationRecords(ctx, auditRepo, paymentID)
	if err != nil {
		writeJSON(explanation{
			Status: "AUDIT_UNAVAILABLE", EvidenceScope: "SYNTHETIC_COMPONENT_ONLY", PaymentID: paymentID.String(),
			Findings:    []string{"Refund audit storage could not be queried; an absent table or query failure is not treated as legacy data."},
			Limitations: []string{"No Provider request or financial write was attempted."},
		})
		return 2
	}
	if len(loaded.records) == 0 {
		writeJSON(explanation{
			Status: "LEGACY_UNVERIFIED", EvidenceScope: "SYNTHETIC_COMPONENT_ONLY", PaymentID: paymentID.String(),
			QueryLimit: refundExplainRecordLimit, PrimaryRowsIncluded: 0,
			Findings:    []string{"No refund audit rows are present for this Payment."},
			Limitations: []string{"No historical source was inferred or backfilled. Unlinked events without a matching observed Request-ID or RefundID cannot be enumerated by Payment ID. This read-only command does not change Payment or Ticket authority."},
		})
		return 0
	}
	result := explainLoadedRecords(paymentID, loaded)
	writeJSON(result)
	return 0
}

type refundAuditReader interface {
	ListRefundAudit(context.Context, domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error)
}

type refundExplanationRecords struct {
	records                 []domain.RefundAuditRecord
	relatedEvents           []relatedEventCandidate
	primaryRowsReturned     int
	relatedIdentityQueries  int
	mayBeTruncated          bool
	relatedSearchWasLimited bool
}

func loadRefundExplanationRecords(ctx context.Context, reader refundAuditReader, paymentID uuid.UUID) (refundExplanationRecords, error) {
	primary, err := reader.ListRefundAudit(ctx, domain.RefundAuditQuery{PaymentID: paymentID, Limit: refundExplainProbeLimit})
	if err != nil {
		return refundExplanationRecords{}, err
	}
	loaded := refundExplanationRecords{primaryRowsReturned: min(len(primary), refundExplainRecordLimit)}
	if len(primary) > refundExplainRecordLimit {
		loaded.mayBeTruncated = true
		primary = primary[:refundExplainRecordLimit]
	}
	loaded.records = append([]domain.RefundAuditRecord(nil), primary...)

	queries, limited := refundAuditRelatedIdentityQueries(primary)
	loaded.relatedSearchWasLimited = limited
	loaded.relatedIdentityQueries = len(queries)
	selectedIDs := make(map[uuid.UUID]struct{}, len(primary))
	for _, record := range primary {
		selectedIDs[record.RecordID] = struct{}{}
	}
	candidateMatches := make(map[uuid.UUID]map[string]struct{})
	candidateRecords := make(map[uuid.UUID]domain.RefundAuditRecord)
	for _, query := range queries {
		related, err := reader.ListRefundAudit(ctx, query)
		if err != nil {
			return refundExplanationRecords{}, err
		}
		if len(related) > refundExplainRecordLimit {
			loaded.mayBeTruncated = true
			related = related[:refundExplainRecordLimit]
		}
		matchField := "provider_request_id"
		if query.ProviderRefundID != "" {
			matchField = "provider_refund_id"
		}
		for _, record := range related {
			if _, exists := selectedIDs[record.RecordID]; exists || record.Kind != domain.RefundAuditKindVerifiedEvent || record.VerifiedEvent == nil {
				continue
			}
			candidateRecords[record.RecordID] = record
			fields := candidateMatches[record.RecordID]
			if fields == nil {
				fields = make(map[string]struct{})
				candidateMatches[record.RecordID] = fields
			}
			fields[matchField] = struct{}{}
		}
	}
	for id, record := range candidateRecords {
		event := record.VerifiedEvent
		fields := make([]string, 0, len(candidateMatches[id]))
		for field := range candidateMatches[id] {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		loaded.relatedEvents = append(loaded.relatedEvents, relatedEventCandidate{
			EventID: event.EventID, ObservationDigest: domain.RefundAuditDigestHex(record.ObservationDigest),
			EventType: event.EventType, RequestID: event.ProviderRequestID, RefundID: event.RefundID,
			ObservedProviderAccount: event.ObservedProviderAccount, ObservedLiveMode: event.ObservedLiveMode,
			LinkClassification: event.LinkClassification, MatchedBy: fields, SamePaymentScope: record.PaymentID == paymentID,
		})
	}
	sort.Slice(loaded.relatedEvents, func(i, j int) bool {
		if loaded.relatedEvents[i].EventID != loaded.relatedEvents[j].EventID {
			return loaded.relatedEvents[i].EventID < loaded.relatedEvents[j].EventID
		}
		if loaded.relatedEvents[i].ObservationDigest != loaded.relatedEvents[j].ObservationDigest {
			return loaded.relatedEvents[i].ObservationDigest < loaded.relatedEvents[j].ObservationDigest
		}
		return strings.Join(loaded.relatedEvents[i].MatchedBy, ",") < strings.Join(loaded.relatedEvents[j].MatchedBy, ",")
	})
	return loaded, nil
}

func refundAuditRelatedIdentityQueries(records []domain.RefundAuditRecord) ([]domain.RefundAuditQuery, bool) {
	identities := make(map[string]domain.RefundAuditQuery)
	for _, record := range records {
		if record.Kind != domain.RefundAuditKindResponse || record.Response == nil {
			continue
		}
		if requestID := record.Response.ProviderRequestID; requestID != "" {
			identities["request:"+requestID] = domain.RefundAuditQuery{ProviderRequestID: requestID, Limit: refundExplainProbeLimit}
		}
		if refundID := record.Response.RefundID; refundID != "" {
			identities["refund:"+refundID] = domain.RefundAuditQuery{ProviderRefundID: refundID, Limit: refundExplainProbeLimit}
		}
	}
	keys := make([]string, 0, len(identities))
	for key := range identities {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	limited := len(keys) > refundExplainRelatedIdentityLimit
	if limited {
		keys = keys[:refundExplainRelatedIdentityLimit]
	}
	queries := make([]domain.RefundAuditQuery, 0, len(keys))
	for _, key := range keys {
		queries = append(queries, identities[key])
	}
	return queries, limited
}

func explainLoadedRecords(paymentID uuid.UUID, loaded refundExplanationRecords) explanation {
	result := explainRecords(paymentID, loaded.records)
	result.QueryLimit = refundExplainRecordLimit
	result.PrimaryRowsIncluded = loaded.primaryRowsReturned
	result.RelatedIdentityQueries = loaded.relatedIdentityQueries
	result.MayBeTruncated = loaded.mayBeTruncated || loaded.relatedSearchWasLimited
	result.RelatedEventCandidates = loaded.relatedEvents
	if loaded.mayBeTruncated {
		result.Findings = append(result.Findings, "The bounded audit query returned more rows than the displayed window; older records and additional identity matches may be omitted.")
	}
	if loaded.relatedSearchWasLimited {
		result.Findings = append(result.Findings, "The related event search exceeded its bounded identity query count; additional indexed candidates were not queried.")
	}
	if len(loaded.relatedEvents) > 0 {
		result.Findings = append(result.Findings, "Related verified events are candidate observations matched by a shared indexed identity; they are not assigned to a Payment or attempt by this query.")
	}
	if len(result.Findings) > 0 {
		result.Status = "PARTIAL"
	}
	sort.Strings(result.Findings)
	return result
}

func explainRecords(paymentID uuid.UUID, records []domain.RefundAuditRecord) explanation {
	result := explanation{
		Status: "EXPLAINED", EvidenceScope: "SYNTHETIC_PROVIDER_HTTP", PaymentID: paymentID.String(),
		Limitations: []string{
			"These records describe only synthetic local behavior and do not prove a real Provider request, execution, or refund.",
			"Audit records are explanatory only; they do not change Payment, Saga, admission, or Ticket authorization.",
		},
	}
	type commandGroup struct {
		view    commandExplanation
		attempt map[string]struct{}
	}
	commands := make(map[[32]byte]*commandGroup)
	attempts := make(map[uuid.UUID]*refundExplainAttemptGroup)
	refundAttempts := make(map[string]map[uuid.UUID]struct{})
	requestAttempts := make(map[string]map[uuid.UUID]struct{})
	verifiedLinks := make(map[uuid.UUID][]domain.VerifiedRefundEventAudit)

	for _, record := range records {
		if record.CommandDigest != ([32]byte{}) {
			group := commands[record.CommandDigest]
			if group == nil {
				group = &commandGroup{view: commandExplanation{
					CommandDigest: domain.RefundAuditDigestHex(record.CommandDigest), Generation: record.Generation,
					AmountCents: record.TargetAmountCents, Currency: record.Currency, PaymentIntent: record.PaymentIntentID,
					Account: record.IntendedAccount, LiveMode: record.IntendedLiveMode,
				}, attempt: make(map[string]struct{})}
				commands[record.CommandDigest] = group
			}
			if record.AttemptID != uuid.Nil {
				group.attempt[record.AttemptID.String()] = struct{}{}
			}
		}
		if record.AttemptID == uuid.Nil {
			if record.VerifiedEvent != nil {
				result.Findings = append(result.Findings, "A verified event has no exact application-attempt link.")
			}
			continue
		}
		group := attempts[record.AttemptID]
		if group == nil {
			group = &refundExplainAttemptGroup{view: attemptExplanation{
				AttemptID: record.AttemptID.String(), CommandDigest: domain.RefundAuditDigestHex(record.CommandDigest),
				Status: "PREPARATION_UNKNOWN",
			}}
			attempts[record.AttemptID] = group
		}
		switch record.Kind {
		case domain.RefundAuditKindPrepared:
			group.view.Prepared = true
			group.view.PreparedAt = timePointer(record.RecordedAt)
		case domain.RefundAuditKindResponse:
			group.seenResponse = true
			if record.Response != nil {
				response := record.Response
				view := &responseExplanation{
					Classification: response.Classification, ErrorCategory: response.ErrorCategory,
					FinancialFinishOutcome: response.FinancialFinishOutcome, RequestID: response.ProviderRequestID,
					RefundID: response.RefundID, ChargeID: response.ChargeID, PaymentIntentID: response.PaymentIntentID,
					Status: response.ProviderStatus, AmountCents: response.AmountCents, Currency: response.Currency,
					ObservedProviderAccount: response.ObservedProviderAccount, ObservedLiveMode: response.ObservedLiveMode,
					TransportState:       response.TransportObservationState,
					TransportDetailCount: len(response.TransportObservations), TransportTruncated: response.TransportObservationsTruncated,
					ConflictingFields: response.ConflictingFields,
				}
				if response.ObservedTransportCalls >= 0 {
					calls := response.ObservedTransportCalls
					view.ObservedTransportCalls = &calls
				}
				group.view.Response = view
				if len(response.ConflictingFields) > 0 {
					result.Findings = append(result.Findings, "A response contains conflicting safe object fields.")
				}
				if response.RefundID != "" {
					addAttempt(refundAttempts, response.RefundID, record.AttemptID)
				} else {
					result.Findings = append(result.Findings, "A response is missing a RefundID.")
				}
				if response.ProviderRequestID != "" {
					addAttempt(requestAttempts, response.ProviderRequestID, record.AttemptID)
				} else {
					result.Findings = append(result.Findings, "A response is missing a Provider Request-ID.")
				}
			}
		case domain.RefundAuditKindVerifiedEvent:
			if record.VerifiedEvent != nil {
				event := record.VerifiedEvent
				group.view.VerifiedEvents = append(group.view.VerifiedEvents, eventExplanation{
					EventID: event.EventID, EventType: event.EventType, ObjectType: event.ObjectType, ObjectID: event.ObjectID,
					RequestID: event.ProviderRequestID, RefundID: event.RefundID, ChargeID: event.ChargeID,
					PaymentIntentID: event.PaymentIntentID, ProviderStatus: event.ProviderStatus, Currency: event.Currency,
					AmountCents: event.AmountCents, ObservedProviderAccount: event.ObservedProviderAccount,
					ObservedLiveMode: event.ObservedLiveMode, EventCreatedAt: event.EventCreatedAt,
					LinkClassification: event.LinkClassification, LinkSource: event.LinkSource,
				})
				if event.LinkClassification != domain.RefundAuditLinkExactRefundIdentity {
					result.Findings = append(result.Findings, "A verified event is unlinked, ambiguous, or conflicting.")
				} else {
					verifiedLinks[record.AttemptID] = append(verifiedLinks[record.AttemptID], *event)
				}
			}
		}
	}

	for _, group := range commands {
		for attemptID := range group.attempt {
			group.view.AttemptIDs = append(group.view.AttemptIDs, attemptID)
		}
		sort.Strings(group.view.AttemptIDs)
		result.Commands = append(result.Commands, group.view)
	}
	sort.Slice(result.Commands, func(i, j int) bool { return result.Commands[i].CommandDigest < result.Commands[j].CommandDigest })
	for _, group := range attempts {
		switch {
		case group.view.Prepared && group.seenResponse:
			group.view.Status = "RESPONSE_RECORDED"
		case group.view.Prepared && !group.seenResponse:
			group.view.Status = "PREPARED_ONLY"
			result.Findings = append(result.Findings, "A prepared attempt has no response record; request dispatch and outcome remain unknown.")
		case !group.view.Prepared && group.seenResponse:
			group.view.Status = "RESPONSE_WITHOUT_PREPARATION"
			result.Findings = append(result.Findings, "A response record has no matching preparation record.")
		default:
			group.view.Status = "PREPARATION_UNKNOWN"
		}
		sort.Slice(group.view.VerifiedEvents, func(i, j int) bool {
			return group.view.VerifiedEvents[i].EventID < group.view.VerifiedEvents[j].EventID
		})
	}
	for attemptID, events := range verifiedLinks {
		group := attempts[attemptID]
		for _, event := range events {
			linked := group != nil && group.view.Response != nil && group.view.Response.RefundID != "" &&
				group.view.Response.RefundID == event.RefundID
			if linked && event.LinkSource == domain.RefundAuditLinkSourceRequestAndID {
				linked = group.view.Response.RequestID != "" && group.view.Response.RequestID == event.ProviderRequestID
			}
			if linked && event.PaymentIntentID != "" {
				linked = group.view.Response.PaymentIntentID == event.PaymentIntentID
			}
			if !linked {
				result.Findings = append(result.Findings, "A verified event's stored attempt link does not match the response facts.")
				if group != nil {
					setAttemptStatus(group, "CONFLICTING_VERIFIED_EVENT_LINK")
				}
			}
		}
	}
	for refundID, ids := range refundAttempts {
		if len(ids) > 1 {
			result.Findings = append(result.Findings, "Multiple attempts share a RefundID; the association is ambiguous: "+refundID)
			for attemptID := range ids {
				if group := attempts[attemptID]; group != nil {
					setAttemptStatus(group, "AMBIGUOUS_SHARED_REFUND_ID")
				}
			}
		}
	}
	for requestID, ids := range requestAttempts {
		if len(ids) > 1 {
			result.Findings = append(result.Findings, "Multiple attempts share a Provider Request-ID; the association is ambiguous: "+requestID)
			for attemptID := range ids {
				if group := attempts[attemptID]; group != nil {
					setAttemptStatus(group, "AMBIGUOUS_SHARED_REQUEST_ID")
				}
			}
		}
	}
	for _, group := range attempts {
		result.Attempts = append(result.Attempts, group.view)
	}
	sort.Slice(result.Attempts, func(i, j int) bool { return result.Attempts[i].AttemptID < result.Attempts[j].AttemptID })
	if len(result.Findings) > 0 {
		result.Status = "PARTIAL"
	}
	sort.Strings(result.Findings)
	return result
}

func setAttemptStatus(group *refundExplainAttemptGroup, status string) {
	if group == nil || attemptStatusPriority(status) <= attemptStatusPriority(group.view.Status) {
		return
	}
	group.view.Status = status
}

func attemptStatusPriority(status string) int {
	switch status {
	case "CONFLICTING_VERIFIED_EVENT_LINK":
		return 3
	case "AMBIGUOUS_SHARED_REFUND_ID":
		return 2
	case "AMBIGUOUS_SHARED_REQUEST_ID":
		return 1
	default:
		return 0
	}
}

func addAttempt(index map[string]map[uuid.UUID]struct{}, identity string, attempt uuid.UUID) {
	ids := index[identity]
	if ids == nil {
		ids = make(map[uuid.UUID]struct{})
		index[identity] = ids
	}
	ids[attempt] = struct{}{}
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}

func writeJSON(value explanation) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write refund explanation")
	}
}
