package recoverycomponent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Status string

const (
	StatusPass    Status = "PASS"
	StatusFail    Status = "FAIL"
	StatusUnknown Status = "UNKNOWN"
)

const (
	EvidenceSynthetic = "SYNTHETIC"

	FindingChargeAssociationNotPersisted = "PAYMENT_CHARGE_ASSOCIATION_NOT_PERSISTED"
)

var ErrProductSourceNotFound = errors.New("managed Payment source not found")

// QueryRower is implemented by pgxpool.Pool and pgx.Tx.
type QueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ProductSource is a read-only projection of an existing managed payments row.
// It deliberately has no ChargeID field: the Payment schema does not persist one.
type ProductSource struct {
	Managed         bool
	PaymentID       uuid.UUID
	BookingID       uuid.UUID
	Generation      int64
	ProviderAccount string
	LiveMode        *bool
	AmountCents     int64
	Currency        string
	PaymentIntentID string
	PaymentStatus   string
	ProviderOutcome string
	ProviderStatus  string
	CreateState     string
	CreateKey       string
	RequestHash     []byte
	RefundState     string
	RefundCommand   *PersistedRefundCommand
}

// PersistedRefundCommand is the durable command material stored by RequireRefund.
type PersistedRefundCommand struct {
	DecisionID  uuid.UUID
	AmountCents int64
	Currency    string
	Reason      string
	Key         string
}

// RequestObservation binds one synthetic observation to a request identity and time.
type RequestObservation struct {
	ID          string
	Operation   string
	AccountID   string
	LiveMode    *bool
	ObjectID    string
	RequestedAt time.Time
	ObservedAt  time.Time
}

type PaymentIntentFact struct {
	ID                  string
	Status              string
	AmountCents         int64
	AmountReceivedCents int64
	Currency            string
	LatestChargeID      string
	LiveMode            *bool
	Metadata            map[string]string
	Request             RequestObservation
}

type ChargeFact struct {
	ID              string
	PaymentIntentID string
	AmountCents     int64
	Currency        string
	Paid            *bool
	Captured        *bool
	LiveMode        *bool
	Metadata        map[string]string
	Request         RequestObservation
}

type RefundFact struct {
	ID              string
	PaymentIntentID string
	ChargeID        string
	Status          string
	AmountCents     int64
	Currency        string
	LiveMode        *bool
	Metadata        map[string]string
	Request         RequestObservation
}

// ProviderEvidence only accepts synthetic fixtures in this component scope.
type ProviderEvidence struct {
	Class         string
	PaymentIntent PaymentIntentFact
	Charge        ChargeFact
	Refund        RefundFact
}

// Validation keeps provider consistency, durable Payment command consistency,
// and end-to-end provenance separate. It never means a real Provider operation
// occurred or activates a formal Recovery predicate.
type Validation struct {
	ProviderFacts  Status
	ProductCommand Status
	Provenance     Status
	Overall        Status
	Findings       []string
}

type assessment struct {
	status   Status
	findings []string
	seen     map[string]struct{}
}

func newAssessment() *assessment {
	return &assessment{status: StatusPass, seen: make(map[string]struct{})}
}

func (a *assessment) add(status Status, finding string) {
	if status == StatusFail || (status == StatusUnknown && a.status == StatusPass) {
		a.status = status
	}
	if finding == "" {
		return
	}
	if _, ok := a.seen[finding]; ok {
		return
	}
	a.seen[finding] = struct{}{}
	a.findings = append(a.findings, finding)
}

func (a *assessment) unknown(finding string) { a.add(StatusUnknown, finding) }
func (a *assessment) fail(finding string)    { a.add(StatusFail, finding) }

const productSourceQuery = "SELECT id,booking_id,payment_generation,provider_account,livemode," +
	"price,currency,payment_intent_id,status::text,provider_outcome,provider_status,create_state," +
	"create_key,request_hash,refund_state,refund_decision_id,refund_amount_cents,refund_currency," +
	"refund_reason,refund_key FROM payments WHERE id=$1 AND managed"

func ReadProductSource(ctx context.Context, db QueryRower, paymentID uuid.UUID) (ProductSource, error) {
	if paymentID == uuid.Nil {
		return ProductSource{}, errors.New("invalid Payment source identity")
	}

	var source ProductSource
	var generation *int64
	var liveMode *bool
	var paymentIntentID, paymentStatus, providerOutcome, providerStatus *string
	var createState, createKey *string
	var refundState, refundKey *string
	var refundDecisionID *uuid.UUID
	var refundAmount *int64
	var refundCurrency, refundReason *string

	err := db.QueryRow(ctx, productSourceQuery, paymentID).Scan(
		&source.PaymentID, &source.BookingID, &generation, &source.ProviderAccount, &liveMode,
		&source.AmountCents, &source.Currency, &paymentIntentID, &paymentStatus, &providerOutcome,
		&providerStatus, &createState, &createKey, &source.RequestHash, &refundState,
		&refundDecisionID, &refundAmount, &refundCurrency, &refundReason, &refundKey,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProductSource{}, ErrProductSourceNotFound
	}
	if err != nil {
		return ProductSource{}, fmt.Errorf("read managed Payment source: %w", err)
	}

	source.Managed = true
	source.LiveMode = liveMode
	if generation != nil {
		source.Generation = *generation
	}
	if paymentIntentID != nil {
		source.PaymentIntentID = *paymentIntentID
	}
	if paymentStatus != nil {
		source.PaymentStatus = *paymentStatus
	}
	if providerOutcome != nil {
		source.ProviderOutcome = *providerOutcome
	}
	if providerStatus != nil {
		source.ProviderStatus = *providerStatus
	}
	if createState != nil {
		source.CreateState = *createState
	}
	if createKey != nil {
		source.CreateKey = *createKey
	}
	if refundState != nil {
		source.RefundState = *refundState
	}
	if refundDecisionID != nil {
		command := &PersistedRefundCommand{DecisionID: *refundDecisionID}
		if refundAmount != nil {
			command.AmountCents = *refundAmount
		}
		if refundCurrency != nil {
			command.Currency = *refundCurrency
		}
		if refundReason != nil {
			command.Reason = *refundReason
		}
		if refundKey != nil {
			command.Key = *refundKey
		}
		source.RefundCommand = command
	}
	return source, nil
}

// ValidateSingleFullRefund checks one succeeded full Refund against its
// PaymentIntent, Charge, and durable Payment/refund-command identities. The
// provider side is fixture-only. Marker ownership is checked separately by
// the existing repository FinishFinancialWork path.
func ValidateSingleFullRefund(source ProductSource, provider ProviderEvidence) Validation {
	product := assessProductCommand(source)
	providerFacts := assessProviderFacts(source, provider)

	// payments stores PaymentIntent and refund command identity but has no
	// persisted Charge identity to join to the provider's Charge object.
	provenance := newAssessment()
	provenance.unknown(FindingChargeAssociationNotPersisted)

	findings := append([]string{}, product.findings...)
	findings = appendUnique(findings, providerFacts.findings...)
	findings = appendUnique(findings, provenance.findings...)

	return Validation{
		ProviderFacts:  providerFacts.status,
		ProductCommand: product.status,
		Provenance:     provenance.status,
		Overall:        combine(product.status, providerFacts.status, provenance.status),
		Findings:       findings,
	}
}

func assessProductCommand(source ProductSource) *assessment {
	a := newAssessment()

	if !source.Managed {
		a.fail("PAYMENT_SOURCE_NOT_MANAGED")
	}
	if source.PaymentID == uuid.Nil || source.BookingID == uuid.Nil || source.ProviderAccount == "" ||
		source.PaymentIntentID == "" {
		a.unknown("PAYMENT_SOURCE_IDENTITY_MISSING")
	}
	if source.Generation != 1 || source.AmountCents < 1 || source.Currency != "usd" {
		a.fail("PAYMENT_SOURCE_IDENTITY_MISMATCH")
	}
	checkTestMode(a, source.LiveMode, "PAYMENT_TEST_MODE_MISSING", "PAYMENT_SOURCE_LIVE_MODE")
	if source.PaymentID != uuid.Nil {
		expectedCreateKey := "ticketbox:payment:" + source.PaymentID.String() + ":g1:create"
		switch {
		case source.CreateKey == "":
			a.unknown("PAYMENT_CREATE_COMMAND_MISSING")
		case source.CreateKey != expectedCreateKey:
			a.fail("PAYMENT_CREATE_COMMAND_MISMATCH")
		}
		if source.RefundCommand != nil {
			expectedRefundKey := "ticketbox:payment:" + source.PaymentID.String() + ":g1:refund"
			switch {
			case source.RefundCommand.Key == "":
				a.unknown("PAYMENT_REFUND_COMMAND_KEY_MISSING")
			case source.RefundCommand.Key != expectedRefundKey:
				a.fail("PAYMENT_REFUND_COMMAND_KEY_MISMATCH")
			}
		}
	}
	switch {
	case len(source.RequestHash) == 0:
		a.unknown("PAYMENT_CREATE_COMMAND_HASH_MISSING")
	case len(source.RequestHash) != 32:
		a.fail("PAYMENT_CREATE_COMMAND_HASH_INVALID")
	}
	if source.CreateState == "" || source.ProviderOutcome == "" || source.ProviderStatus == "" || source.PaymentStatus == "" {
		a.unknown("PAYMENT_CREATE_SOURCE_STATE_MISSING")
	} else if source.CreateState != "create_ready" || source.ProviderOutcome != "collected" ||
		source.ProviderStatus != "succeeded" || source.PaymentStatus != "success" {
		a.fail("PAYMENT_CREATE_SOURCE_STATE_MISMATCH")
	}

	if source.RefundCommand == nil {
		a.unknown("PAYMENT_REFUND_COMMAND_MISSING")
		return a
	}
	command := source.RefundCommand
	if command.DecisionID == uuid.Nil || command.Reason == "" {
		a.unknown("PAYMENT_REFUND_COMMAND_IDENTITY_MISSING")
	} else if source.BookingID == uuid.Nil || !refundDecisionMatches(source.BookingID, command.DecisionID, command.Reason) {
		a.fail("PAYMENT_REFUND_COMMAND_BUSINESS_IDENTITY_MISMATCH")
	}
	if command.AmountCents < 1 || command.Currency == "" {
		a.unknown("PAYMENT_REFUND_COMMAND_AMOUNT_MISSING")
	} else if command.AmountCents != source.AmountCents || command.Currency != source.Currency {
		a.fail("PAYMENT_REFUND_COMMAND_NOT_FULL_PAYMENT")
	}
	switch source.RefundState {
	case "":
		a.unknown("PAYMENT_REFUND_COMMAND_STATE_MISSING")
	case "not_required":
		a.fail("PAYMENT_REFUND_COMMAND_STATE_MISMATCH")
	case "pending", "claimed", "retryable", "provider_pending", "unknown", "succeeded", "attention":
	default:
		a.fail("PAYMENT_REFUND_COMMAND_STATE_INVALID")
	}
	return a
}

func refundDecisionMatches(bookingID, decisionID uuid.UUID, reason string) bool {
	candidates := []struct {
		kind, disposition string
	}{
		{"cancel", "CANCELLED"},
		{"expire", "EXPIRED"},
		{"fail", "FAILED"},
		{"abort_confirmation", "FAILED"},
	}
	for _, candidate := range candidates {
		if candidate.disposition != reason {
			continue
		}
		expected := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:"+candidate.kind))
		if decisionID == expected {
			return true
		}
	}
	return false
}

func assessProviderFacts(source ProductSource, provider ProviderEvidence) *assessment {
	a := newAssessment()
	if provider.Class != EvidenceSynthetic {
		a.unknown("PROVIDER_EVIDENCE_NOT_SYNTHETIC")
	}

	ids := make(map[string]struct{}, 3)
	checkRequest(a, source, provider.PaymentIntent.Request, "retrieve_payment_intent", provider.PaymentIntent.ID, ids)
	checkRequest(a, source, provider.Charge.Request, "retrieve_charge", provider.Charge.ID, ids)
	checkRequest(a, source, provider.Refund.Request, "retrieve_refund", provider.Refund.ID, ids)

	checkObjectMode(a, provider.PaymentIntent.LiveMode)
	checkObjectMode(a, provider.Charge.LiveMode)
	checkObjectMode(a, provider.Refund.LiveMode)

	if provider.PaymentIntent.ID == "" || source.PaymentIntentID == "" {
		a.unknown("PAYMENT_INTENT_IDENTITY_MISSING")
	} else if provider.PaymentIntent.ID != source.PaymentIntentID {
		a.fail("PAYMENT_INTENT_SOURCE_ID_MISMATCH")
	}
	if provider.PaymentIntent.Status == "" {
		a.unknown("PAYMENT_INTENT_STATUS_MISSING")
	} else if provider.PaymentIntent.Status != "succeeded" {
		a.fail("PAYMENT_INTENT_NOT_SUCCEEDED")
	}
	if source.AmountCents < 1 || provider.PaymentIntent.AmountCents != source.AmountCents ||
		provider.PaymentIntent.AmountReceivedCents != source.AmountCents {
		a.fail("PAYMENT_INTENT_AMOUNT_MISMATCH")
	}
	if source.Currency == "" || provider.PaymentIntent.Currency == "" {
		a.unknown("PAYMENT_INTENT_CURRENCY_MISSING")
	} else if provider.PaymentIntent.Currency != source.Currency {
		a.fail("PAYMENT_INTENT_CURRENCY_MISMATCH")
	}

	if provider.Charge.ID == "" || provider.PaymentIntent.LatestChargeID == "" ||
		provider.Charge.PaymentIntentID == "" {
		a.unknown("CHARGE_LINK_MISSING")
	} else if provider.Charge.ID != provider.PaymentIntent.LatestChargeID ||
		provider.Charge.PaymentIntentID != provider.PaymentIntent.ID {
		a.fail("PAYMENT_INTENT_CHARGE_LINK_MISMATCH")
	}
	if provider.Charge.AmountCents != source.AmountCents || source.AmountCents < 1 {
		a.fail("CHARGE_AMOUNT_MISMATCH")
	}
	if provider.Charge.Currency == "" {
		a.unknown("CHARGE_CURRENCY_MISSING")
	} else if provider.Charge.Currency != source.Currency {
		a.fail("CHARGE_CURRENCY_MISMATCH")
	}
	checkTrue(a, provider.Charge.Paid, "CHARGE_PAID_STATE_MISSING", "CHARGE_NOT_PAID")
	checkTrue(a, provider.Charge.Captured, "CHARGE_CAPTURE_STATE_MISSING", "CHARGE_NOT_CAPTURED")

	if provider.Refund.ID == "" || provider.Refund.PaymentIntentID == "" || provider.Refund.ChargeID == "" {
		a.unknown("REFUND_LINK_MISSING")
	} else if provider.Refund.PaymentIntentID != provider.PaymentIntent.ID ||
		provider.Refund.ChargeID != provider.Charge.ID {
		a.fail("REFUND_PAYMENT_OBJECT_LINK_MISMATCH")
	}
	if provider.Refund.Status == "" {
		a.unknown("REFUND_STATUS_MISSING")
	} else if provider.Refund.Status != "succeeded" {
		a.fail("REFUND_NOT_SUCCEEDED")
	}
	if source.AmountCents < 1 || provider.Refund.AmountCents != source.AmountCents {
		a.fail("REFUND_NOT_SINGLE_FULL_AMOUNT")
	}
	if provider.Refund.Currency == "" {
		a.unknown("REFUND_CURRENCY_MISSING")
	} else if provider.Refund.Currency != source.Currency {
		a.fail("REFUND_CURRENCY_MISMATCH")
	}

	checkMetadata(a, source, provider.PaymentIntent.Metadata)
	checkMetadata(a, source, provider.Charge.Metadata)
	checkMetadata(a, source, provider.Refund.Metadata)
	return a
}

func checkRequest(a *assessment, source ProductSource, request RequestObservation, operation, objectID string, ids map[string]struct{}) {
	if request.ID == "" || request.Operation == "" || request.ObjectID == "" || request.AccountID == "" ||
		request.RequestedAt.IsZero() || request.ObservedAt.IsZero() {
		a.unknown("PROVIDER_REQUEST_IDENTITY_OR_TIME_MISSING")
	} else {
		if _, exists := ids[request.ID]; exists {
			a.fail("PROVIDER_REQUEST_ID_REUSED")
		}
		ids[request.ID] = struct{}{}
		if request.Operation != operation || request.ObjectID != objectID {
			a.fail("PROVIDER_REQUEST_OBJECT_MISMATCH")
		}
		if request.ObservedAt.Before(request.RequestedAt) {
			a.fail("PROVIDER_OBSERVATION_PRECEDES_REQUEST")
		}
	}
	if request.AccountID == "" || source.ProviderAccount == "" {
		a.unknown("PROVIDER_ACCOUNT_IDENTITY_MISSING")
	} else if request.AccountID != source.ProviderAccount {
		a.fail("PROVIDER_ACCOUNT_IDENTITY_MISMATCH")
	}
	checkTestMode(a, request.LiveMode, "PROVIDER_REQUEST_MODE_MISSING", "PROVIDER_REQUEST_LIVE_MODE")
}

func checkObjectMode(a *assessment, liveMode *bool) {
	checkTestMode(a, liveMode, "PROVIDER_OBJECT_MODE_MISSING", "PROVIDER_OBJECT_LIVE_MODE")
}

func checkTestMode(a *assessment, liveMode *bool, missing, live string) {
	if liveMode == nil {
		a.unknown(missing)
		return
	}
	if *liveMode {
		a.fail(live)
	}
}

func checkTrue(a *assessment, value *bool, missing, falseFinding string) {
	if value == nil {
		a.unknown(missing)
		return
	}
	if !*value {
		a.fail(falseFinding)
	}
}

func checkMetadata(a *assessment, source ProductSource, metadata map[string]string) {
	checkMetadataValue(a, metadata, "ticketbox_payment_id", source.PaymentID.String())
	checkMetadataValue(a, metadata, "ticketbox_booking_id", source.BookingID.String())
	checkMetadataValue(a, metadata, "ticketbox_generation", fmt.Sprint(source.Generation))
}

func checkMetadataValue(a *assessment, metadata map[string]string, key, expected string) {
	if value := metadata[key]; value != "" && value != expected {
		a.fail("PROVIDER_METADATA_CONFLICT")
	}
}

func appendUnique(dst []string, extra ...string) []string {
	seen := make(map[string]struct{}, len(dst)+len(extra))
	for _, finding := range dst {
		seen[finding] = struct{}{}
	}
	for _, finding := range extra {
		if _, ok := seen[finding]; ok {
			continue
		}
		seen[finding] = struct{}{}
		dst = append(dst, finding)
	}
	return dst
}

func combine(statuses ...Status) Status {
	result := StatusPass
	for _, status := range statuses {
		if status == StatusFail {
			return StatusFail
		}
		if status == StatusUnknown {
			result = StatusUnknown
		}
	}
	return result
}
