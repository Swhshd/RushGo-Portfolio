package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/service"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"github.com/ticketbox/pkg/refundtime"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type refundTimeAuthorityStub struct {
	permit       refundtime.Permit
	err          error
	acquireCalls int
	releaseCalls int
	lastBinding  refundtime.Binding
	lastOutcome  string
}

func (s *refundTimeAuthorityStub) Acquire(_ context.Context, binding refundtime.Binding) (refundtime.Permit, error) {
	s.acquireCalls++
	s.lastBinding = binding
	return s.permit, s.err
}

func (s *refundTimeAuthorityStub) Release(_ context.Context, binding refundtime.Binding, outcome string) (refundtime.Permit, error) {
	s.releaseCalls++
	s.lastBinding = binding
	s.lastOutcome = outcome
	return s.permit, s.err
}

func TestRefundTimeAcquireRejectsMalformedBinding(t *testing.T) {
	authority := &refundTimeAuthorityStub{}
	server := NewEventServer((*service.EventService)(nil), zap.NewNop())
	server.SetRefundTimeAuthority(authority)

	_, err := server.AcquireUnusedRefundTimePermit(context.Background(), &eventv1.RefundTimeBinding{AttemptId: "bad"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed binding status=%s, want InvalidArgument: %v", status.Code(err), err)
	}
	if authority.acquireCalls != 0 {
		t.Fatalf("malformed binding reached authority %d times", authority.acquireCalls)
	}
}

func TestRefundTimeAcquireReturnsQualifiedPermit(t *testing.T) {
	binding := refundtime.Binding{
		AttemptID: uuid.New(), RequestID: uuid.New(), BookingID: uuid.New(), PaymentID: uuid.New(),
		EventID: uuid.New(), ApproverID: uuid.New(), Generation: 1,
		Fingerprint: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	startsAt := time.Date(2031, 4, 5, 12, 30, 0, 0, time.UTC)
	qualifiedAt := startsAt.Add(-time.Minute)
	authority := &refundTimeAuthorityStub{permit: refundtime.Permit{Binding: binding, State: "QUALIFIED", StartsAt: startsAt, QualifiedAt: qualifiedAt, Version: 7}}
	server := NewEventServer((*service.EventService)(nil), zap.NewNop())
	server.SetRefundTimeAuthority(authority)

	got, err := server.AcquireUnusedRefundTimePermit(context.Background(), refundTimeBindingMessage(binding))
	if err != nil {
		t.Fatalf("acquire qualified permit: %v", err)
	}
	if got.State != "QUALIFIED" || got.Version != 7 || got.Binding.AttemptId != binding.AttemptID.String() {
		t.Fatalf("permit binding/state/version mismatch: %+v", got)
	}
	if got.StartsAt == nil || !got.StartsAt.AsTime().Equal(startsAt) || got.QualifiedAt == nil || !got.QualifiedAt.AsTime().Equal(qualifiedAt) {
		t.Fatalf("permit did not preserve qualification timestamps: %+v", got)
	}
	if authority.acquireCalls != 1 || authority.lastBinding != binding {
		t.Fatalf("authority call mismatch: calls=%d binding=%+v", authority.acquireCalls, authority.lastBinding)
	}
}

func TestRefundTimeReleaseRejectsUnsupportedOutcome(t *testing.T) {
	authority := &refundTimeAuthorityStub{}
	server := NewEventServer((*service.EventService)(nil), zap.NewNop())
	server.SetRefundTimeAuthority(authority)
	binding := grpcTestRefundTimeBinding()

	_, err := server.ReleaseUnusedRefundTimePermit(context.Background(), &eventv1.ReleaseUnusedRefundTimePermitRequest{Binding: refundTimeBindingMessage(binding), Outcome: "UNKNOWN"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported outcome status=%s, want InvalidArgument: %v", status.Code(err), err)
	}
	if authority.releaseCalls != 0 {
		t.Fatalf("unsupported outcome reached authority %d times", authority.releaseCalls)
	}
}

func TestRefundTimeReleasePassesOutcomeAndAllowsTombstone(t *testing.T) {
	binding := grpcTestRefundTimeBinding()
	authority := &refundTimeAuthorityStub{permit: refundtime.Permit{Binding: binding, State: "RELEASED", Version: 0}}
	server := NewEventServer((*service.EventService)(nil), zap.NewNop())
	server.SetRefundTimeAuthority(authority)

	got, err := server.ReleaseUnusedRefundTimePermit(context.Background(), &eventv1.ReleaseUnusedRefundTimePermitRequest{Binding: refundTimeBindingMessage(binding), Outcome: "ABORTED"})
	if err != nil {
		t.Fatalf("release before acquire: %v", err)
	}
	if got.State != "RELEASED" || got.Version != 0 || got.StartsAt != nil || got.QualifiedAt != nil {
		t.Fatalf("release tombstone response mismatch: %+v", got)
	}
	if authority.releaseCalls != 1 || authority.lastBinding != binding || authority.lastOutcome != "ABORTED" {
		t.Fatalf("release call mismatch: calls=%d outcome=%q", authority.releaseCalls, authority.lastOutcome)
	}
}

func TestRefundTimeAuthorityFailsClosedWhenUnset(t *testing.T) {
	server := NewEventServer((*service.EventService)(nil), zap.NewNop())
	_, err := server.AcquireUnusedRefundTimePermit(context.Background(), refundTimeBindingMessage(grpcTestRefundTimeBinding()))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unset authority status=%s, want Unavailable: %v", status.Code(err), err)
	}
}

func grpcTestRefundTimeBinding() refundtime.Binding {
	return refundtime.Binding{
		AttemptID: uuid.New(), RequestID: uuid.New(), BookingID: uuid.New(), PaymentID: uuid.New(),
		EventID: uuid.New(), ApproverID: uuid.New(), Generation: 1,
		Fingerprint: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
	}
}

func refundTimeBindingMessage(binding refundtime.Binding) *eventv1.RefundTimeBinding {
	return &eventv1.RefundTimeBinding{
		AttemptId: binding.AttemptID.String(), RequestId: binding.RequestID.String(), BookingId: binding.BookingID.String(),
		PaymentId: binding.PaymentID.String(), EventId: binding.EventID.String(), ApproverId: binding.ApproverID.String(),
		Generation: binding.Generation, Fingerprint: binding.Fingerprint,
	}
}
