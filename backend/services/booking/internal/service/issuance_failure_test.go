package service

import (
	"context"
	"errors"
	"github.com/ticketbox/booking/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestIssuanceFailurePreservesCauseAndFirstStage(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	cause := errors.New("private database value")
	store := &ticketIssuanceStoreFake{candidate: candidate, receiptErr: cause}
	err := NewTicketIssuanceService(store, &ticketAuthorityFake{}).Process(context.Background(), event)
	var failure *domain.IssuanceFailure
	if !errors.Is(err, cause) || !errors.As(err, &failure) || failure.Stage != "lookup_receipt" || store.applyCalls != 0 {
		t.Fatalf("receipt boundary: %v", err)
	}
	authority, event, candidate, eventClient, _ := ticketAuthorityFixture()
	eventClient.err = status.Error(codes.PermissionDenied, "private peer certificate")
	store = &ticketIssuanceStoreFake{candidate: candidate}
	err = NewTicketIssuanceService(store, authority).Process(context.Background(), event)
	if !errors.As(err, &failure) || failure.Stage != "event_authority" || status.Code(err) != codes.PermissionDenied || store.applyCalls != 0 {
		t.Fatalf("RPC boundary: %v", err)
	}
}
