package main

import (
	"context"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"testing"
)

type recordingStore struct {
	repository.TicketIssuanceRepository
	calls int
}

func (s *recordingStore) ApplyTicketIssuance(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate, domain.TicketIssuanceDecision, domain.TicketAuthorityEvidence) error {
	s.calls++
	return nil
}

func TestDenialNeverCreatesReceipt(t *testing.T) {
	store := &recordingStore{}
	wrapper := guardedStore{store}
	if err := wrapper.ApplyTicketIssuance(context.Background(), domain.TicketIssuanceEvent{}, domain.TicketIssuanceCandidate{}, domain.TicketIssuanceDecision{ReasonCode: "provider_evidence_unknown"}, domain.TicketAuthorityEvidence{}); err == nil || store.calls != 0 {
		t.Fatal("denial reached persistence")
	}
	if err := wrapper.ApplyTicketIssuance(context.Background(), domain.TicketIssuanceEvent{}, domain.TicketIssuanceCandidate{}, domain.TicketIssuanceDecision{Eligible: true}, domain.TicketAuthorityEvidence{}); err != nil || store.calls != 1 {
		t.Fatal("eligible decision not delegated")
	}
}

func TestAuthorityAddressMustBeLoopbackBeforeTLS(t *testing.T) {
	t.Setenv("ORIGINAL_payment_ADDRESS", "example.com:443")
	c, err := connect("payment")
	if err == nil || c != nil {
		t.Fatal("foreign authority accepted")
	}
}
