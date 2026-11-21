package domain

import (
	"time"

	"github.com/google/uuid"
)

type AdmissionEligibilityState string

const (
	AdmissionEligibilityUnknown  AdmissionEligibilityState = "UNKNOWN"
	AdmissionEligibilityDenied   AdmissionEligibilityState = "DENIED"
	AdmissionEligibilityEligible AdmissionEligibilityState = "ELIGIBLE"
)

type DisputeEvidenceState string

const (
	DisputeEvidenceUnknown   DisputeEvidenceState = "EVIDENCE_UNKNOWN"
	DisputeEvidenceNoDispute DisputeEvidenceState = "PROVIDER_OBSERVED_NO_DISPUTE"
	DisputeEvidencePresent   DisputeEvidenceState = "DISPUTE_PRESENT"
)

type RefundEvidenceState string

const (
	RefundEvidenceUnknown  RefundEvidenceState = "EVIDENCE_UNKNOWN"
	RefundEvidenceNoRefund RefundEvidenceState = "PROVIDER_OBSERVED_NO_REFUND"
	RefundEvidencePresent  RefundEvidenceState = "REFUND_PRESENT"
)

// AdmissionFinancialIdentity is loaded from RushGo's managed Payment record,
// never supplied by a buyer or scanner client.
type AdmissionFinancialIdentity struct {
	PaymentID, BookingID uuid.UUID
	Generation           int64
	ProviderAccount      string
	LiveMode             bool
	PaymentIntentID      string
	AmountCents          int64
	Currency             string
}

// AdmissionProviderEvidence records a fresh read-only provider observation.
// Zero ObservedAt means no complete decision-grade observation was made.
type AdmissionProviderEvidence struct {
	EligibilityState AdmissionEligibilityState
	DisputeState     DisputeEvidenceState
	RefundState      RefundEvidenceState
	ObservedAt       time.Time
	Source           string
	ReasonCode       string
}
