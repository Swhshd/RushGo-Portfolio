package repository

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
)

func TestTicketScanHonorsBuildScopedAuthoritySource(t *testing.T) {
	command := domain.TicketScanCommand{
		EventID: uuid.New(), StaffUserID: uuid.New(), TicketID: uuid.New(),
		CredentialDigest: make([]byte, 32), RequestFingerprint: make([]byte, 32), IdempotencyKey: "authority-source-boundary",
		Decision: domain.TicketIssuanceDecision{Eligible: true},
		Evidence: domain.TicketAuthorityEvidence{
			PaymentDecisionKnown: true, PaymentEligible: true, SagaEligible: true, EventEligible: true,
			DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund,
			Source: "SIMULATED_PROVIDER_AUTHORITY", ObservedAt: time.Now().UTC(),
		},
	}
	err := validTicketScanCommand(command)
	if domain.TicketAuthoritySourceAllowed(command.Evidence.Source) {
		if err != nil {
			t.Fatalf("test build rejected explicitly allowed authority source: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("production build accepted simulated authority source")
	}
}
