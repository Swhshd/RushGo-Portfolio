package main

import (
	"github.com/google/uuid"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/pkg/refundtime"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"testing"
	"time"
)

func TestRefundTimeAdapterChecksFullBinding(t *testing.T) {
	b := refundtime.Binding{AttemptID: uuid.New(), RequestID: uuid.New(), BookingID: uuid.New(), PaymentID: uuid.New(), EventID: uuid.New(), ApproverID: uuid.New(), Generation: 1, Fingerprint: strings.Repeat("a", 64)}
	now := time.Now().UTC()
	p := &eventv1.RefundTimePermit{Binding: timeBindingProto(b), State: "QUALIFIED", StartsAt: timestamppb.New(now.Add(time.Hour)), QualifiedAt: timestamppb.New(now), Version: 1}
	if _, err := timePermitResult(b, p); err != nil {
		t.Fatal(err)
	}
	p.Binding.AttemptId = uuid.NewString()
	if _, err := timePermitResult(b, p); err == nil {
		t.Fatal("wrong attempt")
	}
	p.Binding = timeBindingProto(b)
	p.QualifiedAt = p.StartsAt
	if _, err := timePermitResult(b, p); err == nil {
		t.Fatal("late qualification")
	}
	p.State = "RELEASED"
	p.StartsAt = nil
	p.QualifiedAt = nil
	p.Version = 0
	if _, err := timePermitResult(b, p); err != nil {
		t.Fatalf("pre-acquire release tombstone %v", err)
	}
	p.Binding.Fingerprint = strings.Repeat("b", 64)
	if _, err := timePermitResult(b, p); err == nil {
		t.Fatal("changed replay")
	}
}
