package main

import (
	"context"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
	"testing"

	"github.com/ticketbox/saga/internal/service"
)

type recoveryEventRouteStub struct {
	eventv1.EventServiceClient
	reads, checks int
}

func (s *recoveryEventRouteStub) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	s.reads++
	return &eventv1.EventDetail{}, nil
}

func (s *recoveryEventRouteStub) VerifyBookingTicketEligibility(context.Context, *eventv1.VerifyBookingTicketEligibilityRequest, ...grpc.CallOption) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	s.checks++
	return &eventv1.VerifyBookingTicketEligibilityResponse{}, nil
}

func TestRecoveryEventRoutesAuthoritySeparately(t *testing.T) {
	legacy, secure := &recoveryEventRouteStub{}, &recoveryEventRouteStub{}
	client := recoveryEventClient{EventServiceClient: legacy, authority: secure}
	_, _ = client.GetEvent(context.Background(), &eventv1.GetEventRequest{})
	_, _ = client.VerifyBookingTicketEligibility(context.Background(), &eventv1.VerifyBookingTicketEligibilityRequest{})
	if legacy.reads != 1 || legacy.checks != 0 || secure.reads != 0 || secure.checks != 1 {
		t.Fatal("recovery eligibility did not use dedicated authority client")
	}
}

func TestRecoveryExitCodeDoesNotTreatRejectedReplayAsSuccess(t *testing.T) {
	rejectedReplay := &service.PaidUnissuedRecoveryReport{
		Eligible: false, Replayed: true, ResultCode: "payment_dispute_present",
	}
	if code := recoveryExitCode(rejectedReplay); code != 2 {
		t.Fatalf("rejected idempotent replay exit code = %d, want 2", code)
	}

	eligibleReplay := &service.PaidUnissuedRecoveryReport{Eligible: true, Replayed: true, ResultCode: "requeued"}
	if code := recoveryExitCode(eligibleReplay); code != 0 {
		t.Fatalf("eligible idempotent replay exit code = %d, want 0", code)
	}
}
