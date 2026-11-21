package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

var (
	ErrTicketIdentityConflict      = errors.New("ticket issuance identity conflict")
	ErrTicketBookingMissing        = errors.New("ticket issuance booking missing")
	ErrTicketCredentialMissing     = errors.New("ticket credential unavailable")
	ErrTicketOwnerDenied           = errors.New("ticket owner authorization denied")
	ErrTicketNotEligible           = errors.New("ticket is not eligible for a credential")
	ErrScannerNotAssigned          = errors.New("scanner is not assigned to this event")
	ErrTicketIdempotencyConflict   = errors.New("ticket scan idempotency conflict")
	ErrTicketIdempotencyInProgress = errors.New("ticket operation is still in progress")
	ErrTicketIdempotencyKeyInvalid = errors.New("invalid ticket operation idempotency key")
)

type TicketIssuanceRepository interface {
	LookupTicketIssuanceReceipt(context.Context, domain.TicketIssuanceEvent) (bool, error)
	LoadTicketIssuanceCandidate(context.Context, domain.TicketIssuanceEvent) (domain.TicketIssuanceCandidate, error)
	ApplyTicketIssuance(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate, domain.TicketIssuanceDecision, domain.TicketAuthorityEvidence) error
}

type TicketRepository interface {
	TicketIssuanceRepository
	ListOwnedTickets(context.Context, uuid.UUID) ([]domain.Ticket, error)
	LoadTicketForCredential(context.Context, uuid.UUID, uuid.UUID) (domain.Ticket, domain.TicketIssuanceCandidate, error)
	LookupTicketCredentialRotation(context.Context, uuid.UUID, uuid.UUID, string, []byte) (domain.TicketCredentialRotationResult, bool, error)
	RotateTicketCredential(context.Context, domain.TicketCredentialRotationCommand) (domain.TicketCredentialRotationResult, error)
	IsScannerAssigned(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	SetScannerAssignment(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, bool) (domain.EventScannerAssignment, error)
	ListScannerAssignments(context.Context, uuid.UUID) ([]domain.EventScannerAssignment, error)
	LoadTicketScanCandidate(context.Context, []byte) (domain.TicketScanCandidate, error)
	LookupTicketScanRequest(context.Context, uuid.UUID, uuid.UUID, string, []byte) (domain.TicketScanResult, bool, error)
	CommitTicketCheckIn(context.Context, domain.TicketScanCommand) (domain.TicketScanResult, error)
}

func validTicketScanCommand(c domain.TicketScanCommand) error {
	if c.EventID == uuid.Nil || c.StaffUserID == uuid.Nil || c.TicketID == uuid.Nil || len(c.CredentialDigest) != 32 || len(c.RequestFingerprint) != 32 || len(c.IdempotencyKey) == 0 || len(c.IdempotencyKey) > 128 {
		return fmt.Errorf("invalid Ticket scan request")
	}
	if !validTicketScanKey(c.IdempotencyKey) {
		return fmt.Errorf("invalid Ticket scan idempotency key")
	}
	if c.Decision.Eligible && (!c.Evidence.PaymentDecisionKnown || !c.Evidence.PaymentEligible || !c.Evidence.SagaEligible || !c.Evidence.EventEligible || c.Evidence.DisputeState != domain.DisputeNoDispute || c.Evidence.RefundState != domain.RefundNoRefund || c.Evidence.ObservedAt.IsZero() || !domain.TicketAuthoritySourceAllowed(c.Evidence.Source)) {
		return fmt.Errorf("Ticket check-in cannot accept incomplete authority evidence")
	}
	return nil
}

func validTicketScanKey(value string) bool {
	return ValidTicketIdempotencyKey(value)
}

func ValidTicketIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func sameFingerprint(left, right []byte) bool { return bytes.Equal(left, right) }
