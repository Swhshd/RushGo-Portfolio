package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ticketEventClientFake struct {
	eventv1.EventServiceClient
	state   eventv1.BookingTicketEligibilityState
	err     error
	request *eventv1.VerifyBookingTicketEligibilityRequest
}

func (f *ticketEventClientFake) VerifyBookingTicketEligibility(_ context.Context, req *eventv1.VerifyBookingTicketEligibilityRequest, _ ...grpc.CallOption) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	f.request = req
	return &eventv1.VerifyBookingTicketEligibilityResponse{State: f.state}, f.err
}

type ticketSagaClientFake struct {
	sagav1.SagaOrchestratorServiceClient
	state sagav1.TicketIssuanceSagaState
	err   error
}

func (f *ticketSagaClientFake) VerifyTicketIssuanceSaga(context.Context, *sagav1.VerifyTicketIssuanceSagaRequest, ...grpc.CallOption) (*sagav1.VerifyTicketIssuanceSagaResponse, error) {
	return &sagav1.VerifyTicketIssuanceSagaResponse{State: f.state}, f.err
}

type ticketPaymentClientFake struct {
	paymentv1.PaymentServiceClient
	response *paymentv1.AdmissionFinancialEligibilityResponse
	err      error
	request  *paymentv1.AdmissionFinancialEligibilityRequest
}

func (f *ticketPaymentClientFake) CheckAdmissionFinancialEligibility(_ context.Context, req *paymentv1.AdmissionFinancialEligibilityRequest, _ ...grpc.CallOption) (*paymentv1.AdmissionFinancialEligibilityResponse, error) {
	f.request = req
	return f.response, f.err
}

func ticketAuthorityFixture() (*GRPCTicketAuthority, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate, *ticketEventClientFake, *ticketPaymentClientFake) {
	bookingID, eventID, sagaID, paymentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seatID := uuid.New()
	event := domain.TicketIssuanceEvent{ID: uuid.New(), SagaID: sagaID, BookingID: bookingID, PaymentID: paymentID, EventID: eventID, Generation: 1}
	candidate := domain.TicketIssuanceCandidate{BookingID: bookingID, UserID: uuid.New(), EventID: eventID, SagaID: sagaID, PaymentID: paymentID, Status: domain.StatusConfirmed, Managed: true, Generation: 1, TotalAmountCents: 1000, Currency: "usd", Items: []domain.BookingItem{{ID: uuid.New(), BookingID: bookingID, TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{seatID}}}}
	eventClient := &ticketEventClientFake{state: eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE}
	paymentClient := &ticketPaymentClientFake{response: &paymentv1.AdmissionFinancialEligibilityResponse{EligibilityState: paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE, DisputeState: paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE, RefundState: paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND, ObservedAt: timestamppb.New(time.Now().UTC()), Source: "stripe_test_api"}}
	return NewGRPCTicketAuthority(eventClient, &ticketSagaClientFake{state: sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_ELIGIBLE}, paymentClient), event, candidate, eventClient, paymentClient
}

func TestGRPCTicketAuthorityRequiresAllThreeCurrentAuthorities(t *testing.T) {
	authority, event, candidate, eventClient, paymentClient := ticketAuthorityFixture()
	evidence, err := authority.CheckTicketAuthorities(context.Background(), event, candidate)
	if err != nil || !evidence.SagaEligible || !evidence.EventEligible || !evidence.PaymentEligible || !evidence.PaymentDecisionKnown || evidence.DisputeState != domain.DisputeNoDispute || evidence.RefundState != domain.RefundNoRefund || evidence.ObservedAt.IsZero() {
		t.Fatalf("evidence=%+v error=%v", evidence, err)
	}
	if len(eventClient.request.SeatIds) != 1 || eventClient.request.SeatIds[0] != candidate.Items[0].SeatIDs[0].String() {
		t.Fatalf("event seat set=%v", eventClient.request.SeatIds)
	}
	if paymentClient.request.PaymentId != event.PaymentID.String() || paymentClient.request.BookingId != event.BookingID.String() {
		t.Fatalf("Payment identity not bound to issuance event: %+v", paymentClient.request)
	}
}

func TestGRPCTicketAuthorityPreservesDisputeDenialAndFailsClosedOnUnknown(t *testing.T) {
	authority, event, candidate, _, paymentClient := ticketAuthorityFixture()
	paymentClient.response.EligibilityState = paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_DENIED
	paymentClient.response.DisputeState = paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_DISPUTE_PRESENT
	evidence, err := authority.CheckTicketAuthorities(context.Background(), event, candidate)
	if err != nil || evidence.PaymentEligible || evidence.DisputeState != domain.DisputePresent {
		t.Fatalf("dispute evidence=%+v error=%v", evidence, err)
	}
	paymentClient.response.EligibilityState = paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN
	if _, err = authority.CheckTicketAuthorities(context.Background(), event, candidate); err == nil {
		t.Fatal("unknown Payment authority allowed issuance")
	}
}
