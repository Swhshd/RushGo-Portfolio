package domain

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidUnusedRefundPaymentSnapshot = errors.New("invalid unused-refund Payment snapshot")

type RefundRequestStatus string

const (
	RefundRequestRequested                RefundRequestStatus = "REQUESTED"
	RefundRequestWaitingCheckInResolution RefundRequestStatus = "WAITING_CHECKIN_RESOLUTION"
	RefundRequestWithdrawn                RefundRequestStatus = "WITHDRAWN"
	RefundRequestRejected                 RefundRequestStatus = "REJECTED"
	RefundRequestApproved                 RefundRequestStatus = "APPROVED"
	RefundRequestProcessing               RefundRequestStatus = "REFUND_PROCESSING"
	RefundRequestRefunded                 RefundRequestStatus = "REFUNDED"
	RefundRequestNeedsAttention           RefundRequestStatus = "NEEDS_ATTENTION"
)

type RefundDecisionKind string

const (
	RefundDecisionApproved RefundDecisionKind = "APPROVED"
	RefundDecisionRejected RefundDecisionKind = "REJECTED"
)

const TypeUnusedTicketRefundApprovedV1 = "BookingUnusedTicketRefundApproved.v1"

type RefundApprovalAttemptState string

const (
	RefundApprovalAttemptPending  RefundApprovalAttemptState = "PENDING"
	RefundApprovalAttemptApproved RefundApprovalAttemptState = "APPROVED"
	RefundApprovalAttemptRejected RefundApprovalAttemptState = "REJECTED"
	RefundApprovalAttemptAborted  RefundApprovalAttemptState = "ABORTED"
)

type RefundApprovalAttempt struct {
	AttemptID, RequestID, BookingID, EventID, BuyerID, PaymentID, ApproverID uuid.UUID
	Generation                                                               int64
	KeyDigest, Fingerprint                                                   [32]byte
	Reason                                                                   string
	State                                                                    RefundApprovalAttemptState
	StartsAt, EventQualifiedAt                                               time.Time
	EventAuthorityVersion                                                    string
	ReleaseAcknowledged                                                      bool
	CreatedAt, TerminalAt                                                    time.Time
}

type RefundApprovalCandidate struct {
	BookingID, BuyerID, EventID, PaymentID uuid.UUID
	Generation                             int64
	AmountCents                            int64
	Currency                               string
	ForApproval                            bool
}

type BookingRefundRequest struct {
	RequestID, BookingID, EventID, BuyerID, PaymentID uuid.UUID
	Generation                                        int64
	AmountCents                                       int64
	Currency                                          string
	Status                                            RefundRequestStatus
	Reason, DecisionReason                            string
	DecisionID, ApproverID                            uuid.UUID
	QualificationAttemptID                            uuid.UUID
	PaymentIntentID, ProviderAccount                  string
	Livemode                                          bool
	EventStartsAt                                     *time.Time
	EventQualifiedAt                                  time.Time
	EventAuthorityVersion                             string
	RequestedAt, DecidedAt                            *time.Time
	PaymentRefundState, PaymentRefundStatus           string
	PaymentRefundAmountCents                          int64
	TicketRevocationState                             string
	TicketRevokedAt                                   *time.Time
}

type RefundApproverAssignment struct {
	EventID, StaffUserID uuid.UUID
	Active               bool
	ChangedAt            time.Time
}

type RefundEventSnapshot struct {
	EventID                uuid.UUID
	StartsAt               time.Time
	AuthorityVersion       string
	QualificationAttemptID uuid.UUID
	EventQualifiedAt       time.Time
}

type UnusedRefundPaymentSnapshot struct {
	BookingID, BuyerID, PaymentID uuid.UUID
	PaymentIntentID               string
	ProviderAccount               string
	Livemode                      bool
	Generation                    int64
	AmountCents                   int64
	Currency                      string
	RefundDecisionID              uuid.UUID
	RefundState                   string
	RefundAmountCents             int64
	RefundStatus                  string
	Collected                     bool
	EligibleForApproval           bool
}

func ValidateUnusedRefundPaymentSnapshot(candidate RefundApprovalCandidate, snapshot UnusedRefundPaymentSnapshot) error {
	if candidate.BookingID == uuid.Nil || candidate.BuyerID == uuid.Nil || candidate.EventID == uuid.Nil || candidate.PaymentID == uuid.Nil ||
		candidate.Generation != 1 || candidate.AmountCents < 1 || candidate.Currency != "usd" {
		return ErrInvalidUnusedRefundPaymentSnapshot
	}
	if snapshot.BookingID != candidate.BookingID || snapshot.BuyerID != candidate.BuyerID || snapshot.PaymentID != candidate.PaymentID ||
		snapshot.Generation != candidate.Generation || snapshot.AmountCents != candidate.AmountCents || snapshot.Currency != candidate.Currency ||
		strings.TrimSpace(snapshot.PaymentIntentID) == "" || strings.TrimSpace(snapshot.PaymentIntentID) != snapshot.PaymentIntentID ||
		strings.TrimSpace(snapshot.ProviderAccount) == "" || strings.TrimSpace(snapshot.ProviderAccount) != snapshot.ProviderAccount ||
		snapshot.Livemode || !snapshot.Collected || !snapshot.EligibleForApproval || snapshot.RefundDecisionID != uuid.Nil || snapshot.RefundAmountCents != 0 ||
		(snapshot.RefundState != "" && snapshot.RefundState != "not_required") || snapshot.RefundStatus != "" {
		return ErrInvalidUnusedRefundPaymentSnapshot
	}
	return nil
}

func ValidRefundRequestReason(reason string) bool {
	trimmed := strings.TrimSpace(reason)
	return len([]rune(trimmed)) >= 1 && len([]rune(trimmed)) <= 500
}

type RequestUnusedRefundCommand struct {
	RequestID, BookingID, BuyerID uuid.UUID
	Reason                        string
	KeyDigest, Fingerprint        [32]byte
}

type BeginUnusedRefundApprovalCommand struct {
	AttemptID, RequestID, EventID, ApproverID uuid.UUID
	Reason                                    string
	KeyDigest, Fingerprint                    [32]byte
}

type WithdrawUnusedRefundCommand struct {
	RequestID, BuyerID uuid.UUID
	KeyDigest          [32]byte
	Fingerprint        [32]byte
}

type DecideUnusedRefundCommand struct {
	DecisionID, RequestID, EventID, ApproverID uuid.UUID
	QualificationAttemptID                     uuid.UUID
	Kind                                       RefundDecisionKind
	Reason                                     string
	KeyDigest, Fingerprint                     [32]byte
	Event                                      RefundEventSnapshot
	Payment                                    UnusedRefundPaymentSnapshot
}

type RefundEventListQuery struct {
	EventID  uuid.UUID
	Limit    int
	BeforeAt *time.Time
	BeforeID uuid.UUID
}

type RefundDecisionSnapshot struct {
	BookingRefundRequest
	DecisionKind RefundDecisionKind
}
