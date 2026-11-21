package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/admission"
)

const MaxPaymentEvidenceAge = 10 * time.Second

// PaymentEvidenceFreshAt evaluates evidence against a service-controlled
// decision timestamp. A zero, future, or older-than-budget observation fails
// closed. The ten-second budget is an internal candidate parameter, not a
// production timing guarantee.
func PaymentEvidenceFreshAt(observedAt, decisionAt time.Time) bool {
	if observedAt.IsZero() || decisionAt.IsZero() || observedAt.After(decisionAt) {
		return false
	}
	return decisionAt.Sub(observedAt) <= MaxPaymentEvidenceAge
}

// PaymentEvidenceElapsedWithinBound requires both process-local elapsed-time
// clocks to stay within the freshness budget. The monotonic clock avoids
// cross-host clock comparisons; the wall clock catches suspend intervals on
// platforms where the monotonic clock may stop while the machine sleeps.
func PaymentEvidenceElapsedWithinBound(monotonicElapsed, wallElapsed time.Duration) bool {
	return monotonicElapsed >= 0 && monotonicElapsed <= MaxPaymentEvidenceAge &&
		wallElapsed >= 0 && wallElapsed <= MaxPaymentEvidenceAge
}

var ErrInvalidTicketIssuanceEvent = errors.New("invalid ticket issuance event")

type TicketIssuanceEvent struct {
	ID, SagaID, BookingID, PaymentID, EventID uuid.UUID
	Generation                                int64
}

func (e TicketIssuanceEvent) Validate() error {
	if e.ID == uuid.Nil || e.SagaID == uuid.Nil || e.BookingID == uuid.Nil || e.PaymentID == uuid.Nil || e.EventID == uuid.Nil || e.Generation != 1 {
		return ErrInvalidTicketIssuanceEvent
	}
	return nil
}

type TicketIssuanceCandidate struct {
	BookingID, UserID, EventID, SagaID, PaymentID uuid.UUID
	Status                                        BookingStatus
	Managed                                       bool
	Generation                                    int64
	TotalAmountCents                              int64
	Currency                                      string
	Items                                         []BookingItem
}

type DisputeEvidenceState string
type RefundEvidenceState string

const (
	EvidenceUnknown  DisputeEvidenceState = "unknown"
	DisputeNoDispute DisputeEvidenceState = "provider_observed_no_dispute"
	DisputePresent   DisputeEvidenceState = "dispute_present"
	RefundUnknown    RefundEvidenceState  = "unknown"
	RefundNoRefund   RefundEvidenceState  = "provider_observed_no_refund"
	RefundPresent    RefundEvidenceState  = "refund_present"
)

type TicketAuthorityEvidence struct {
	SagaEligible, PaymentEligible, EventEligible bool
	PaymentDecisionKnown                         bool
	DisputeState                                 DisputeEvidenceState
	RefundState                                  RefundEvidenceState
	ObservedAt                                   time.Time
	PaymentQueryStartedAt                        time.Time // monotonic process-local start; never serialize or persist
	PaymentQueryWallStartedAt                    time.Time // wall-clock process-local start; never serialize or persist
	Source                                       string
	ReasonCode                                   string
}

type TicketIssuanceDecision struct {
	Eligible   bool
	ReasonCode string
}

func ValidateTicketIssuance(event TicketIssuanceEvent, booking TicketIssuanceCandidate, evidence TicketAuthorityEvidence) TicketIssuanceDecision {
	deny := func(reason string) TicketIssuanceDecision { return TicketIssuanceDecision{ReasonCode: reason} }
	if err := event.Validate(); err != nil {
		return deny("issuance_event_identity_invalid")
	}
	if booking.BookingID != event.BookingID || booking.EventID != event.EventID || booking.SagaID != event.SagaID ||
		booking.PaymentID != event.PaymentID || booking.Generation != event.Generation {
		return deny("booking_identity_mismatch")
	}
	if !booking.Managed || booking.UserID == uuid.Nil || booking.Status != StatusConfirmed || booking.Generation != 1 ||
		booking.TotalAmountCents < 1 || booking.Currency != "usd" {
		return deny("booking_not_issuable")
	}
	if len(booking.Items) == 0 || len(booking.Items) > 100 {
		return deny("booking_items_invalid")
	}
	seen := make(map[uuid.UUID]struct{})
	for _, item := range booking.Items {
		if item.ID == uuid.Nil || item.BookingID != booking.BookingID || item.TicketTierID == uuid.Nil || item.Quantity < 1 ||
			int(item.Quantity) != len(item.SeatIDs) {
			return deny("booking_seat_quantity_mismatch")
		}
		for _, seatID := range item.SeatIDs {
			if seatID == uuid.Nil {
				return deny("booking_seat_identity_invalid")
			}
			if _, duplicate := seen[seatID]; duplicate {
				return deny("booking_seat_identity_duplicate")
			}
			seen[seatID] = struct{}{}
		}
	}
	if !evidence.SagaEligible || !evidence.EventEligible {
		return deny("authoritative_booking_or_event_state_denied")
	}
	if !evidence.PaymentDecisionKnown || !evidence.PaymentEligible || evidence.DisputeState != DisputeNoDispute || evidence.RefundState != RefundNoRefund ||
		evidence.ObservedAt.IsZero() || !TicketAuthoritySourceAllowed(evidence.Source) {
		return deny("payment_evidence_not_current_and_complete")
	}
	return TicketIssuanceDecision{Eligible: true, ReasonCode: "ticket_issuance_eligible"}
}

type TicketStatus string

const (
	TicketIssued    TicketStatus = "ISSUED"
	TicketCheckedIn TicketStatus = "CHECKED_IN"
	TicketRevoked   TicketStatus = "REVOKED"
)

type Ticket struct {
	ID, BookingID, BookingItemID, EventID, SeatID, TicketTierID uuid.UUID
	Status                                                      TicketStatus
	BookingStatus                                               BookingStatus
	IssuedAt                                                    time.Time
	CheckedInAt                                                 *time.Time
}

type TicketCredential struct {
	TicketID          uuid.UUID
	Token             string // transient response value only; persistence accepts its digest.
	ExpiresAt         time.Time
	CredentialVersion int64
	Outcome           TicketCredentialRotationOutcome
}

type TicketCredentialRotationOutcome string

const (
	TicketCredentialRotationRotated        TicketCredentialRotationOutcome = "ROTATED"
	TicketCredentialRotationAlreadyApplied TicketCredentialRotationOutcome = "ALREADY_APPLIED"
)

type TicketCredentialRotationCommand struct {
	UserID, TicketID   uuid.UUID
	IdempotencyKey     string
	RequestFingerprint []byte
	CredentialDigest   []byte
	ExpiresAt          time.Time
}

type TicketCredentialRotationResult struct {
	Outcome           TicketCredentialRotationOutcome
	CredentialVersion int64
	ExpiresAt         time.Time
}

type TicketAdmissionEvidence struct {
	ObservedAt time.Time
	Source     string
	ReasonCode string
}

type TicketScanCandidate struct {
	Ticket                  Ticket
	Booking                 TicketIssuanceCandidate
	CredentialExpiresAt     time.Time
	BookingRefundDecisionID *uuid.UUID
}

type TicketScanCommand struct {
	EventID, StaffUserID, TicketID uuid.UUID
	CredentialDigest               []byte
	RequestFingerprint             []byte
	IdempotencyKey                 string
	Decision                       TicketIssuanceDecision
	Evidence                       TicketAuthorityEvidence
	Admission                      *admission.Permit
}

type TicketScanResult struct {
	ResultCode, ReasonCode, EvidenceSource string
	CheckedInAt, ObservedAt                *time.Time
	Replayed                               bool
}

type EventScannerAssignment struct {
	EventID, StaffUserID uuid.UUID
	Active               bool
	ChangedAt            time.Time
}

func NewTicketIssuanceReceipt(event TicketIssuanceEvent, decision TicketIssuanceDecision, evidence TicketAuthorityEvidence) (string, time.Time, error) {
	if err := event.Validate(); err != nil {
		return "", time.Time{}, err
	}
	if decision.Eligible && (evidence.ObservedAt.IsZero() || evidence.Source == "") {
		return "", time.Time{}, fmt.Errorf("eligible receipt requires provider observation")
	}
	if decision.Eligible {
		return "issued", evidence.ObservedAt.UTC(), nil
	}
	return "denied", evidence.ObservedAt.UTC(), nil
}
