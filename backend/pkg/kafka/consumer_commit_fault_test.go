//go:build ticket_e2e

package kafka

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestTicketE2ECommitFaultLeavesOneOffsetUncommittedAndAllowsReplay(t *testing.T) {
	markerDir := t.TempDir()
	if err := ArmTicketE2ECommitFault(markerDir); err != nil {
		t.Fatal(err)
	}
	event := Event{ID: "event-123", Type: "TicketIssuanceRequested.v1"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := beforeOffsetCommit(ctx, event, "ticket.issuance.events.test", 0, 7); err == nil {
		t.Fatal("first delivery should block until the consumer process is stopped")
	}
	data, err := os.ReadFile(filepath.Join(markerDir, "ticket-consumer-before-offset-commit.json"))
	if err != nil {
		t.Fatalf("fault marker missing: %v", err)
	}
	var marker ticketE2ECommitFaultMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatalf("fault marker invalid: %v", err)
	}
	if marker.EventID != event.ID || marker.Topic != "ticket.issuance.events.test" || marker.Offset != 7 {
		t.Fatalf("fault marker identity mismatch: %+v", marker)
	}
	if err := beforeOffsetCommit(context.Background(), event, marker.Topic, marker.Partition, marker.Offset); err != nil {
		t.Fatalf("replayed event should pass the one-shot fault: %v", err)
	}
	if err := ArmTicketE2ECommitFault(markerDir); err == nil {
		t.Fatal("fault must not be rearmed after durable marker exists")
	}
}
