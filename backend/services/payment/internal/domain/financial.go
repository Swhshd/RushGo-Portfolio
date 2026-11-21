package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

const RefundReasonConfirmedUnusedTicket = "CONFIRMED_UNUSED_TICKET"

// FinancialIdentity is the immutable authority for one managed payable lifecycle.
type FinancialIdentity struct {
	BookingID, UserID uuid.UUID
	Generation        int64
	ProviderAccount   string
	LiveMode          bool
	AmountCents       int64
	Currency          string
	Fingerprint       [32]byte
}

type CreateOperation struct {
	Identity     FinancialIdentity
	PaymentID    uuid.UUID
	Key          string
	FrozenParams json.RawMessage
}

type FrozenCreateParameters struct {
	Version                 int                     `json:"version"`
	Amount                  int64                   `json:"amount"`
	Currency                string                  `json:"currency"`
	AutomaticPaymentMethods AutomaticPaymentMethods `json:"automatic_payment_methods"`
	ReceiptEmail            string                  `json:"receipt_email,omitempty"`
}

// FinancialFingerprint uses canonical UUID/number lines and JSON-escaped strings.
// This also checks that persisted parameters cannot send a different provider charge.
func FinancialFingerprint(identity FinancialIdentity, frozen json.RawMessage) ([32]byte, error) {
	var params FrozenCreateParameters
	decoder := json.NewDecoder(bytes.NewReader(frozen))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&params); err != nil {
		return [32]byte{}, errors.New("invalid frozen create parameters")
	}
	if decoder.Decode(new(any)) != io.EOF || params.Version != 1 || params.Amount != identity.AmountCents ||
		params.Currency != identity.Currency || !params.AutomaticPaymentMethods.Enabled || params.AutomaticPaymentMethods.AllowRedirects != "always" {
		return [32]byte{}, errors.New("frozen create parameters disagree with financial identity")
	}
	quote := func(value string) string { raw, _ := json.Marshal(value); return string(raw) }
	mode := "test"
	if identity.LiveMode {
		mode = "live"
	}
	canonical := fmt.Sprintf("v1\n%s\n%s\n%d\n%d\n%s\n%s\n%s\ntrue\n%s\n%s\n",
		identity.BookingID, identity.UserID, identity.Generation, identity.AmountCents, quote(identity.Currency),
		quote(identity.ProviderAccount), mode, quote(params.AutomaticPaymentMethods.AllowRedirects), quote(params.ReceiptEmail))
	return sha256.Sum256([]byte(canonical)), nil
}

// ProviderObservation is trusted, normalized provider evidence, never a Kafka label.
type ProviderObservation struct {
	Account                             string
	LiveMode                            bool
	EventID, IntentID, RefundID, Status string
	PaymentID, BookingID                uuid.UUID
	Generation                          int64
	AmountCents, SucceededRefundCents   int64
	Currency                            string
	Collected                           bool
	ObservedAt                          time.Time
	EvidenceHash                        [32]byte
	SafeObject                          json.RawMessage
}

// FinancialView contains public facts, excluding frozen parameters and client secrets.
type FinancialView struct {
	Payment                                             *Payment
	Generation                                          int64
	Managed                                             bool
	ProviderAccount                                     string
	LiveMode                                            *bool
	ProviderOutcome, ProviderStatus, BookingDisposition string
	CreateState, CancelState, ReconciliationState       string
	RefundState, RefundID, RefundStatus, RefundReason   string
	RefundAmountCents                                   int64
	RefundDecisionID                                    *uuid.UUID
	NextAttemptAt, ObservedAt                           *time.Time
	ErrorCategory                                       string
}

type FinancialClaim struct {
	PaymentID, Token uuid.UUID
	Fence            int64
	Kind, Key        string
	LeaseUntil       time.Time
	FirstAttemptAt   *time.Time
	// These fields preserve refund claim bookkeeping if the final pre-send
	// admission fence returns a positive coordination wait. FirstAttemptAt is
	// the value after this claim; PriorRefundFirstAttemptAt is the exact value
	// observed before it incremented refund attempts.
	PriorRefundFirstAttemptAt *time.Time
	PriorRefundState          string
	PriorAttempts             int
	Operation                 CreateOperation
	View                      *FinancialView
	Refund                    *RefundObligation
	Attempts                  int
}

type FinancialResult struct {
	State, ErrorCategory string
	Observation          *ProviderObservation
	NextAttemptAt        time.Time
	BookingDisposition   string
	DecisionID           uuid.UUID
}

type RefundObligation struct {
	PaymentID, DecisionID uuid.UUID
	Generation            int64
	AmountCents           int64
	Currency, Reason, Key string
}

type ProviderReceipt struct {
	ID                                      uuid.UUID
	Observation                             ProviderObservation
	EventType, CorrelationState, RelayState string
	EnvelopeID                              string
	ReceivedAt                              time.Time
	RefundAudit                             *VerifiedRefundEventAudit `json:"-"`
}

type ReceiptClaim struct {
	ReceiptID, Token uuid.UUID
	Fence            int64
	Receipt          ProviderReceipt
	LeaseUntil       time.Time
}
