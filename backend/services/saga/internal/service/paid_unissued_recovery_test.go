package service

import (
	"testing"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPaidRecoveryPaymentRequiresFreshTestAccountAndCleanProviderEvidence(t *testing.T) {
	now := time.Now().UTC()
	saga := recoveryPaymentSaga()
	snapshot := validRecoveryPaymentSnapshot(saga, now)
	if reason := paidRecoveryPaymentReason(snapshot, saga, "acct_test_expected", now); reason != "" {
		t.Fatalf("valid TEST payment rejected: %s", reason)
	}

	tests := []struct {
		name   string
		mutate func(*paymentv1.PaidRecoveryPaymentSnapshot)
	}{
		{name: "provider identity", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.ProviderIdentityVerified = false }},
		{name: "live mode", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.Livemode = true }},
		{name: "wrong provider account", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.ProviderAccount = "acct_other" }},
		{name: "wrong booking", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.Payment.BookingId = uuid.NewString() }},
		{name: "uncollected", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.Payment.ProviderOutcome = "unknown" }},
		{name: "refund present", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) { s.Payment.RefundState = "succeeded" }},
		{name: "dispute present", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) {
			s.Admission.DisputeState = paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_DISPUTE_PRESENT
		}},
		{name: "evidence unknown", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) {
			s.Admission.EligibilityState = paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN
		}},
		{name: "stale evidence", mutate: func(s *paymentv1.PaidRecoveryPaymentSnapshot) {
			s.Admission.ObservedAt = timestamppb.New(now.Add(-paidRecoveryEvidenceMaxAge - time.Second))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validRecoveryPaymentSnapshot(saga, now)
			test.mutate(candidate)
			if reason := paidRecoveryPaymentReason(candidate, saga, "acct_test_expected", now); reason == "" {
				t.Fatal("payment evidence outside the recovery contract was accepted")
			}
		})
	}
}

func TestPaidRecoveryTicketSnapshotRequiresExactZeroCountAndFreshEvidence(t *testing.T) {
	bookingID, sagaID := uuid.New(), uuid.New()
	good := &bookingv1.PaidRecoveryTicketSnapshot{
		BookingId: bookingID.String(), SagaId: sagaID.String(), Generation: 1,
		ObservedAt: timestamppb.New(time.Now().Add(-time.Second)),
	}
	if reason := paidRecoveryTicketReason(good, bookingID, sagaID, time.Now()); reason != "" {
		t.Fatalf("valid zero-ticket snapshot rejected: %s", reason)
	}
	wrong := proto.Clone(good).(*bookingv1.PaidRecoveryTicketSnapshot)
	wrong.SagaId = uuid.NewString()
	if paidRecoveryTicketReason(wrong, bookingID, sagaID, time.Now()) == "" {
		t.Fatal("ticket count snapshot from another Saga was accepted")
	}
	withTicket := proto.Clone(good).(*bookingv1.PaidRecoveryTicketSnapshot)
	withTicket.TicketCount = 1
	if paidRecoveryTicketReason(withTicket, bookingID, sagaID, time.Now()) != "ticket_already_exists" {
		t.Fatal("existing Ticket was not excluded from recovery")
	}
	withReceipt := proto.Clone(good).(*bookingv1.PaidRecoveryTicketSnapshot)
	withReceipt.IssuanceReceiptCount = 1
	if paidRecoveryTicketReason(withReceipt, bookingID, sagaID, time.Now()) != "issuance_receipt_already_exists" {
		t.Fatal("existing issuance receipt was not excluded from recovery")
	}
}

func TestPaidRecoveryEventRequiresEveryReferencedTierToBeAccountingReady(t *testing.T) {
	booking := &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Items: []*bookingv1.BookingItem{{TicketTierId: "tier-a"}}}}
	event := &eventv1.EventDetail{Id: uuid.NewString(), Tiers: []*eventv1.TicketTier{{Id: "tier-a", EventId: "wrong-event", AccountingReady: true}}}
	if _, reason := paidRecoveryEventObservation(event, booking); reason != "event_ticket_tier_mismatch" {
		t.Fatalf("wrong event tier accepted: %s", reason)
	}
	event.Tiers[0].EventId = event.Id
	event.Tiers[0].AccountingReady = false
	if _, reason := paidRecoveryEventObservation(event, booking); reason != "event_tier_accounting_not_ready" {
		t.Fatalf("unready Event tier accepted: %s", reason)
	}
}

func recoveryPaymentSaga() domain.Saga {
	return domain.Saga{BookingID: uuid.New(), Input: &domain.SagaInput{UserID: uuid.New(), AmountCents: 725, Currency: "usd", Generation: 1}}
}

func validRecoveryPaymentSnapshot(saga domain.Saga, now time.Time) *paymentv1.PaidRecoveryPaymentSnapshot {
	return &paymentv1.PaidRecoveryPaymentSnapshot{
		ProviderAccount: "acct_test_expected", ProviderIdentityVerified: true,
		Payment: &paymentv1.PaymentEntry{
			Id: uuid.NewString(), UserId: saga.Input.UserID.String(), BookingId: saga.BookingID.String(), PaymentIntentId: "pi_test_recovery",
			Status: "success", Price: int32(saga.Input.AmountCents), Currency: saga.Input.Currency, PaymentGeneration: 1,
			ProviderOutcome: "collected", ProviderStatus: "succeeded", ReconciliationState: "sale_verified",
			RefundState: "not_required", BookingDisposition: "CONFIRMED",
		},
		Admission: &paymentv1.AdmissionFinancialEligibilityResponse{
			EligibilityState: paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE,
			DisputeState:     paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE,
			RefundState:      paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND,
			ObservedAt:       timestamppb.New(now.Add(-time.Second)), Source: "stripe_test_api", ReasonCode: "provider_observed_clear",
		},
	}
}
