package grpc

import (
	"context"
	"testing"

	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPaidRecoveryTicketSnapshotRequiresSagaMTLSBeforeParsingRequest(t *testing.T) {
	server := NewBookingServer(nil, zap.NewNop())
	malformed := &bookingv1.LifecycleRequest{BookingId: "not-a-uuid", Generation: 2}
	tests := []struct {
		name string
		ctx  context.Context
		req  *bookingv1.LifecycleRequest
		want codes.Code
	}{
		{name: "plaintext", ctx: context.Background(), req: malformed, want: codes.Unauthenticated},
		{name: "unverified Saga", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentitySaga, false), req: malformed, want: codes.Unauthenticated},
		{name: "Gateway", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true), req: malformed, want: codes.PermissionDenied},
		{name: "Booking", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityBooking, true), req: malformed, want: codes.PermissionDenied},
		{name: "Payment", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentityPayment, true), req: malformed, want: codes.PermissionDenied},
		{name: "malformed Saga request", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentitySaga, true), req: malformed, want: codes.InvalidArgument},
		{name: "nil Saga request", ctx: adminOutboxRPCContext(grpcsecurity.ServiceIdentitySaga, true), req: nil, want: codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.GetPaidRecoveryTicketSnapshot(tc.ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("GetPaidRecoveryTicketSnapshot() code = %s, want %s (err=%v)", got, tc.want, err)
			}
		})
	}
}
