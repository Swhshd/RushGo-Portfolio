package grpc

import (
	"errors"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Extend existing classifications to the internal coordinator; no public
// request/response schema or Scanner authorization changes.
func ticketCheckInError(err error) error {
	switch {
	case errors.Is(err, service.ErrTicketAuthorityUnknown), errors.Is(err, service.ErrTicketAdmissionUnknown),
		errors.Is(err, admission.ErrUnavailable), errors.Is(err, repository.ErrAdmissionInactive), errors.Is(err, repository.ErrAdmissionRequired):
		return status.Error(codes.Unavailable, "ticket authority unavailable")
	case errors.Is(err, admission.ErrConflict):
		return status.Error(codes.AlreadyExists, "idempotency key conflicts with another scan")
	case errors.Is(err, admission.ErrStale), errors.Is(err, admission.ErrBlocked), errors.Is(err, repository.ErrAdmissionTerminal):
		return status.Error(codes.FailedPrecondition, "ticket admission is not available for this request")
	default:
		return status.Error(codes.Internal, "ticket check-in failed")
	}
}
