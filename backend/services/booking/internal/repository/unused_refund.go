package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

var (
	ErrRefundRequestNotFound                  = errors.New("Booking refund request not found")
	ErrRefundRequestNotEligible               = errors.New("Booking is not eligible for an unused-ticket refund")
	ErrRefundRequestOwnerDenied               = errors.New("Booking refund owner authorization denied")
	ErrRefundRequestConflict                  = errors.New("Booking refund request conflicts with an existing request")
	ErrRefundRequestFinal                     = errors.New("Booking refund request already has a final decision")
	ErrRefundRequestPendingAdmission          = errors.New("Booking refund waits for admission resolution")
	ErrRefundRequestApproverDenied            = errors.New("event refund approver authorization denied")
	ErrRefundRequestScannerDenied             = errors.New("active scanner cannot approve a Booking refund")
	ErrRefundRequestNotApproved               = errors.New("Booking refund decision is not currently approved")
	ErrRefundRequestPaymentConflict           = errors.New("Payment refund evidence conflicts with the approved Booking decision")
	ErrRefundRequestEventStarted              = errors.New("event has started before the refund decision")
	ErrRefundRequestEventAuthorityUnavailable = errors.New("trusted Event time authority unavailable")
	ErrRefundRequestAdmissionInactive         = errors.New("Booking Admission fence is not active")
	ErrRefundRequestIdempotencyConflict       = errors.New("Booking refund idempotency key conflicts with another request")
	ErrRefundApprovalAttemptConflict          = errors.New("Booking refund approval attempt conflicts with an existing attempt")
	ErrRefundApprovalAttemptFinal             = errors.New("Booking refund approval attempt is already final")
)

type UnusedTicketRefundRepository interface {
	PreviewUnusedRefund(context.Context, uuid.UUID, uuid.UUID) (domain.RefundApprovalCandidate, string, error)
	RequestUnusedRefund(context.Context, domain.RequestUnusedRefundCommand) (domain.BookingRefundRequest, error)
	GetUnusedRefundByBooking(context.Context, uuid.UUID, uuid.UUID) (domain.BookingRefundRequest, error)
	GetUnusedRefundByRequest(context.Context, uuid.UUID, uuid.UUID) (domain.BookingRefundRequest, error)
	GetEventUnusedRefundByID(context.Context, uuid.UUID, uuid.UUID) (domain.BookingRefundRequest, error)
	HasPendingBookingAdmission(context.Context, uuid.UUID) (bool, error)
	WithdrawUnusedRefund(context.Context, domain.WithdrawUnusedRefundCommand) (domain.BookingRefundRequest, error)
	BeginUnusedRefundApproval(context.Context, domain.BeginUnusedRefundApprovalCommand) (domain.RefundApprovalAttempt, error)
	FinalizeUnusedRefundApproval(context.Context, domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error)
	GetUnusedRefundApprovalAttempt(context.Context, uuid.UUID) (domain.RefundApprovalAttempt, error)
	AbortUnusedRefundApprovalAttempt(context.Context, uuid.UUID, string) (domain.RefundApprovalAttempt, error)
	ListUnusedRefundApprovalAttemptsForRecovery(context.Context, int) ([]domain.RefundApprovalAttempt, error)
	AcknowledgeUnusedRefundAttemptRelease(context.Context, uuid.UUID, string) error
	DecideUnusedRefund(context.Context, domain.DecideUnusedRefundCommand) (domain.BookingRefundRequest, error)
	ListEventUnusedRefunds(context.Context, domain.RefundEventListQuery) ([]domain.BookingRefundRequest, bool, error)
	SetEventRefundApprover(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, bool) (domain.RefundApproverAssignment, error)
	ListEventRefundApprovers(context.Context, uuid.UUID) ([]domain.RefundApproverAssignment, error)
	IsEventRefundApprover(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	CheckEventRefundDecisionActor(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error
	GetApprovedUnusedRefundDecision(context.Context, uuid.UUID, uuid.UUID) (domain.RefundDecisionSnapshot, error)
	ListUnusedRefundsForRecovery(context.Context, int) ([]domain.BookingRefundRequest, error)
	ApplyUnusedRefundPaymentSnapshot(context.Context, domain.BookingRefundRequest, domain.UnusedRefundPaymentSnapshot) (domain.BookingRefundRequest, error)
}
