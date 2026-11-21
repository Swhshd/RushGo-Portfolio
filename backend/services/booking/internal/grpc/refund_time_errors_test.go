package grpc

import (
	"fmt"
	"github.com/ticketbox/booking/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestRefundTimeTerminalAndConflictErrorsAreNotTransient(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{repository.ErrRefundApprovalAttemptConflict, codes.AlreadyExists},
		{repository.ErrRefundApprovalAttemptFinal, codes.FailedPrecondition},
	} {
		if got := status.Code(refundRPCError(fmt.Errorf("wrapped: %w", tc.err))); got != tc.want {
			t.Fatalf("%v mapped to %v, want %v", tc.err, got, tc.want)
		}
	}
}
