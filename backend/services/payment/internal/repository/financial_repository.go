package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

var (
	ErrFinancialConflict        = errors.New("immutable financial identity conflict")
	ErrInvalidFinancialIdentity = errors.New("invalid financial identity")
	ErrProviderEvidenceConflict = errors.New("provider evidence does not match financial identity")
	ErrFinancialStateProtected  = errors.New("financial_state_protected")
	ErrRefundAuditConflict      = domain.ErrRefundAuditConflict
	ErrRefundAuditUnavailable   = domain.ErrRefundAuditUnavailable
)

type RefundPreSendDecision string

const (
	RefundPreSendAllow             RefundPreSendDecision = "allow"
	RefundPreSendSucceeded         RefundPreSendDecision = "succeeded"
	RefundPreSendExistingResult    RefundPreSendDecision = "existing_result"
	RefundPreSendStaleOwner        RefundPreSendDecision = "stale_owner"
	RefundPreSendCommandMismatch   RefundPreSendDecision = "command_mismatch"
	RefundPreSendNotAllowed        RefundPreSendDecision = "not_allowed"
	RefundPreSendCoordinationWait  RefundPreSendDecision = "coordination_wait"
	RefundPreSendAdmissionConflict RefundPreSendDecision = "admission_conflict"
)

type RefundPreSendResult struct {
	Decision              RefundPreSendDecision
	AttemptID             uuid.UUID
	RefundID              string
	Reason                string
	coordinationWaitProof bool
}

// RefundAuditRepository is intentionally optional so existing test doubles and
// read-only repository consumers do not need to implement audit persistence.
type RefundAuditRepository interface {
	AppendRefundAudit(context.Context, domain.RefundAuditRecord) error
	ListRefundAudit(context.Context, domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error)
}

// AdmissionRecoveryClaim is a short database claim over a due permit. Attempt
// counts are monotonic scheduling metadata; they never release a permit.
type AdmissionRecoveryClaim struct {
	Permit  admission.Permit
	Attempt int
}

// AdmissionRepository stores durable Payment-owned permits. Implementations
// recheck managed financial identity and refund preparation under the Payment
// row lock before granting a new permit.
type AdmissionRepository interface {
	GetAdmission(context.Context, admission.Identity) (admission.Permit, error)
	GrantAdmission(context.Context, admission.Identity, domain.AdmissionProviderEvidence) (admission.Permit, error)
	SettleAdmission(context.Context, admission.Identity, uuid.UUID, int64, admission.State) (admission.Permit, error)
	ScheduleAdmissionRetry(context.Context, admission.Identity, uuid.UUID, int64, int, string) error
	ClaimDueAdmissions(context.Context, int) ([]AdmissionRecoveryClaim, error)
}

// RefundAdmissionWorkRepository provides a neutral, owner-fenced release for
// claims whose final pre-send guard positively proved that no PREPARED record
// or Provider Create was attempted.
type RefundAdmissionWorkRepository interface {
	DeferRefundForAdmission(context.Context, domain.FinancialClaim, RefundPreSendResult, time.Time) (bool, error)
}

// ApprovedUnusedRefundIdentity is the Booking-approved binding Payment must
// recheck atomically against its managed Payment row before accepting the new
// refund reason. PaymentIntent and provider account remain Payment-owned facts.
type ApprovedUnusedRefundIdentity struct {
	BookingID, UserID uuid.UUID
	Generation        int64
	ProviderAccount   string
	LiveMode          bool
	AmountCents       int64
	Currency          string
	PaymentIntentID   string
}

// ApprovedUnusedRefundRepository is kept separate from the legacy financial
// interface so existing read and test adapters do not gain an unrelated
// terminal-refund contract.
type ApprovedUnusedRefundRepository interface {
	RequireApprovedUnusedRefund(context.Context, domain.RefundObligation, ApprovedUnusedRefundIdentity) (*domain.FinancialView, error)
}

// FinancialRepository's worker/receipt primitives are added in their owning tasks.
type FinancialRepository interface {
	CreateOrGet(context.Context, domain.CreateOperation) (*domain.FinancialView, bool, error)
	GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error)
	GetByLogicalIdentity(context.Context, uuid.UUID, int64) (*domain.FinancialView, error)
	RecordObservation(context.Context, domain.ProviderObservation) error
	ClaimFinancialWork(context.Context, int, time.Duration) ([]domain.FinancialClaim, error)
	ClaimCreatePayment(context.Context, uuid.UUID, time.Duration) (*domain.FinancialClaim, error)
	CheckRefundPreSend(context.Context, domain.FinancialClaim) (RefundPreSendResult, error)
	FinishFinancialWork(context.Context, domain.FinancialClaim, domain.FinancialResult) (bool, error)
	AcceptProviderReceipt(context.Context, domain.ProviderReceipt) (*domain.ProviderReceipt, bool, error)
	ClaimReceiptWork(context.Context, int, time.Duration) ([]domain.ReceiptClaim, error)
	ApplyReceiptObservation(context.Context, domain.ReceiptClaim, domain.ProviderObservation) (bool, error)
	FinishReceiptWork(context.Context, domain.ReceiptClaim, string, string, string, time.Time) (bool, error)
	RequireRefund(context.Context, domain.RefundObligation) (*domain.FinancialView, error)
	RequireCancel(context.Context, uuid.UUID, uuid.UUID) (*domain.FinancialView, error)
	FinancialBacklog(context.Context) (int64, int64, float64, error)
}
