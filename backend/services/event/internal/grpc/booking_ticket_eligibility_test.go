package grpc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVerifyBookingTicketEligibilityRejectsMalformedInternalContract(t *testing.T) {
	server := NewEventServer(nil, zap.NewNop())
	booking, event, seat := uuid.NewString(), uuid.NewString(), uuid.NewString()
	tests := []struct {
		name    string
		request *eventv1.VerifyBookingTicketEligibilityRequest
	}{
		{name: "nil request"},
		{name: "invalid generation", request: &eventv1.VerifyBookingTicketEligibilityRequest{BookingId: booking, EventId: event, Generation: 2, SeatIds: []string{seat}}},
		{name: "duplicate seat", request: &eventv1.VerifyBookingTicketEligibilityRequest{BookingId: booking, EventId: event, Generation: 1, SeatIds: []string{seat, seat}}},
		{name: "invalid seat", request: &eventv1.VerifyBookingTicketEligibilityRequest{BookingId: booking, EventId: event, Generation: 1, SeatIds: []string{"invalid"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := server.VerifyBookingTicketEligibility(context.Background(), test.request); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("malformed request returned %v", err)
			}
		})
	}
}
