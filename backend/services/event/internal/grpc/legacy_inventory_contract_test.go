package grpc

import (
	"context"
	"testing"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The retained legacy method must never become an unguarded counter-editing
// shortcut around authoritative seat ownership and lifecycle accounting.
func TestLegacyBulkInventoryIsExplicitlyUnsupported(t *testing.T) {
	server := &EventServer{}
	response, err := server.UpdateBatchTicketAvailability(context.Background(), &eventv1.UpdateTicketAvailabilityRequest{
		TierId: "untrusted-tier", QuantityDelta: 100, ExpectedVersion: 1,
	})
	if response != nil || status.Code(err) != codes.Unimplemented {
		t.Fatalf("legacy batch response=%v code=%s, want Unimplemented", response, status.Code(err))
	}
}
