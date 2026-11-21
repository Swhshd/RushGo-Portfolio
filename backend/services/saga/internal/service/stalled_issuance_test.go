package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type stalledIssuanceRepositoryFake struct {
	repository.SagaRepositoryInterface
	state *repository.PaidUnissuedSagaState
}

func (f *stalledIssuanceRepositoryFake) GetPaidUnissuedSagaState(context.Context, uuid.UUID) (*repository.PaidUnissuedSagaState, error) {
	return f.state, nil
}

func (f *stalledIssuanceRepositoryFake) LookupPaidUnissuedRecoveryAudit(context.Context, repository.PaidUnissuedRecoveryCommand) (*repository.PaidUnissuedRecoveryAuditResult, error) {
	return nil, nil
}

func (f *stalledIssuanceRepositoryFake) RecordPaidUnissuedRecovery(context.Context, repository.PaidUnissuedRecoveryCommand) (*repository.PaidUnissuedRecoveryAuditResult, error) {
	return nil, nil
}

type stalledIssuanceBookingFake struct {
	bookingv1.BookingServiceClient
	view    *bookingv1.LifecycleDisposition
	tickets *bookingv1.PaidRecoveryTicketSnapshot
}

func (f *stalledIssuanceBookingFake) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	return f.view, nil
}

func (f *stalledIssuanceBookingFake) GetPaidRecoveryTicketSnapshot(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.PaidRecoveryTicketSnapshot, error) {
	return f.tickets, nil
}

type stalledIssuancePaymentFake struct {
	paymentv1.PaymentServiceClient
	entry    *paymentv1.PaymentEntry
	snapshot *paymentv1.PaidRecoveryPaymentSnapshot
}

func (f *stalledIssuancePaymentFake) GetFinancialPayment(context.Context, *paymentv1.FinancialPaymentRequest, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	return f.entry, nil
}

func (f *stalledIssuancePaymentFake) GetPaidRecoveryPaymentSnapshot(context.Context, *paymentv1.FinancialPaymentRequest, ...grpc.CallOption) (*paymentv1.PaidRecoveryPaymentSnapshot, error) {
	return f.snapshot, nil
}

type stalledIssuanceEventFake struct {
	eventv1.EventServiceClient
	event *eventv1.EventDetail
	seat  *eventv1.VerifyBookingTicketEligibilityResponse
}

func (f *stalledIssuanceEventFake) GetEvent(context.Context, *eventv1.GetEventRequest, ...grpc.CallOption) (*eventv1.EventDetail, error) {
	return f.event, nil
}

func (f *stalledIssuanceEventFake) VerifyBookingTicketEligibility(context.Context, *eventv1.VerifyBookingTicketEligibilityRequest, ...grpc.CallOption) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	return f.seat, nil
}

type stalledIssuanceInspectorFake struct {
	snapshot *pkgkafka.GroupSnapshot
	offset   int64
	found    bool
}

func (f *stalledIssuanceInspectorFake) InspectGroup(context.Context, string, string) (*pkgkafka.GroupSnapshot, error) {
	return f.snapshot, nil
}

func (f *stalledIssuanceInspectorFake) FindRecord(context.Context, string, int, int64, int64, int, func([]byte) bool) (int64, bool, int, error) {
	return f.offset, f.found, 1, nil
}

type stalledIssuanceFixture struct {
	service   *SagaService
	request   StalledIssuanceInspectRequest
	inspector *stalledIssuanceInspectorFake
	payment   *stalledIssuancePaymentFake
	state     *repository.PaidUnissuedSagaState
}

func newStalledIssuanceFixture(t *testing.T, members int, ticketCount, receiptCount int64) *stalledIssuanceFixture {
	t.Helper()
	bookingID, sagaID, userID, eventID, seatID, tierID, paymentID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	issuanceID := uuid.New()
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	saga := &domain.Saga{
		ID: sagaID, BookingID: bookingID, Status: domain.SAGA_COMPLETED, Generation: 1, PaymentID: paymentID,
		Input: &domain.SagaInput{
			BookingID: bookingID, UserID: userID, EventID: eventID, SeatIDs: []uuid.UUID{seatID},
			AmountCents: 100, Currency: "usd", Generation: 1, BookingMode: "pessimistic", ReservationDeadline: deadline,
		},
	}
	published := time.Now().UTC().Add(-time.Minute)
	state := &repository.PaidUnissuedSagaState{
		Saga: saga, OutboxCount: 1, OutboxPublished: 1, OutboxEventID: &issuanceID,
		OutboxPublishedAt: &published, OutboxPaymentID: &paymentID,
	}
	view := &bookingv1.LifecycleDisposition{
		Booking: &bookingv1.BookingDetail{
			Id: bookingID.String(), UserId: userID.String(), EventId: eventID.String(),
			Status: "CONFIRMED", TotalAmountCents: 100,
			Items: []*bookingv1.BookingItem{{TicketTierId: tierID.String(), Quantity: 1, SeatIds: []string{seatID.String()}}},
		},
		Generation: 1, Phase: "TERMINAL", Currency: "usd", BookingMode: "pessimistic",
		ReservationDeadline: timestamppb.New(deadline),
	}
	entry := &paymentv1.PaymentEntry{
		Id: paymentID.String(), BookingId: bookingID.String(), UserId: userID.String(),
		PaymentGeneration: 1, Price: 100, Currency: "usd", Status: "success",
		PaymentIntentId: "pi_r4_" + paymentID.String(),
		ProviderOutcome: "collected", ProviderStatus: "succeeded",
		ReconciliationState: "sale_verified", RefundState: "not_required", BookingDisposition: "CONFIRMED",
	}
	snapshot := &paymentv1.PaidRecoveryPaymentSnapshot{
		Payment: entry, ProviderAccount: "acct_r4_local", ProviderIdentityVerified: true,
		Admission: &paymentv1.AdmissionFinancialEligibilityResponse{
			EligibilityState: paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE,
			DisputeState:     paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE,
			RefundState:      paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND,
			Source:           "stripe_test_api", ObservedAt: timestamppb.Now(),
		},
	}
	event := &eventv1.EventDetail{Id: eventID.String(), Status: "published",
		Tiers: []*eventv1.TicketTier{{Id: tierID.String(), EventId: eventID.String(), AccountingReady: true}}}
	seat := &eventv1.VerifyBookingTicketEligibilityResponse{
		State: eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE,
	}
	tickets := &bookingv1.PaidRecoveryTicketSnapshot{
		BookingId: bookingID.String(), SagaId: sagaID.String(), Generation: 1,
		TicketCount: ticketCount, IssuanceReceiptCount: receiptCount, ObservedAt: timestamppb.Now(),
	}
	group := &pkgkafka.GroupSnapshot{
		Topic: "ticket.issuance.events", Group: "booking-ticket-issuance-v1", ObservedAt: time.Now().UTC(),
		Partitions: []pkgkafka.GroupPartitionSnapshot{{Topic: "ticket.issuance.events", Partition: 0, FirstOffset: 0, CommittedOffset: 0, LogEndOffset: 1}},
	}
	if members > 0 {
		group.Members = []pkgkafka.GroupMemberSnapshot{{MemberID: "member-1", ClientID: "booking-service",
			Topics: []pkgkafka.GroupMemberTopicSnapshot{{Topic: "ticket.issuance.events", Partitions: []int{0}}}}}
	}
	inspector := &stalledIssuanceInspectorFake{snapshot: group, offset: 0, found: true}
	service := &SagaService{
		repo:          &stalledIssuanceRepositoryFake{state: state},
		bookingClient: &stalledIssuanceBookingFake{view: view, tickets: tickets},
		paymentClient: &stalledIssuancePaymentFake{entry: entry, snapshot: snapshot},
		eventClient:   &stalledIssuanceEventFake{event: event, seat: seat},
	}
	return &stalledIssuanceFixture{
		service: service, inspector: inspector, payment: service.paymentClient.(*stalledIssuancePaymentFake), state: state,
		request: StalledIssuanceInspectRequest{
			BookingID: bookingID, SagaID: sagaID, ExpectedProviderAccount: "acct_r4_local",
			Operator: "operator", Reason: "verify stalled issuance", Topic: "ticket.issuance.events",
			Group: "booking-ticket-issuance-v1", SampleInterval: time.Millisecond, MaxScan: 10,
		},
	}
}

func TestInspectStalledIssuanceClassifiesPaidUncommittedOutbox(t *testing.T) {
	fixture := newStalledIssuanceFixture(t, 1, 0, 0)
	report, err := fixture.service.InspectStalledIssuance(context.Background(), fixture.request, fixture.inspector)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != StalledIssuanceStalledConsumer || !report.EligibleForRestart {
		t.Fatalf("classification=%q eligible=%v plan=%+v", report.Classification, report.EligibleForRestart, report.Plan)
	}
	if report.Plan == nil || report.Plan.Action != "restart_booking_consumer" || !report.Plan.RequiresIsolatedTarget {
		t.Fatalf("restart plan missing: %+v", report.Plan)
	}
	if !report.Kafka.RecordUncommitted || report.Kafka.Members != 1 || !report.Kafka.StableNoProgress {
		t.Fatalf("kafka observation = %+v", report.Kafka)
	}
	encoded, err := json.Marshal(report)
	if err != nil || len(encoded) == 0 {
		t.Fatalf("report did not marshal: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	kafka, ok := document["kafka"].(map[string]any)
	if !ok {
		t.Fatalf("kafka observation absent from JSON: %s", encoded)
	}
	if offset, present := kafka["record_offset"]; !present || offset != float64(0) {
		t.Fatalf("zero record offset must be serialized for the operator wrapper: %s", encoded)
	}
}

func TestInspectStalledIssuanceClassifiesConsumerDown(t *testing.T) {
	fixture := newStalledIssuanceFixture(t, 0, 0, 0)
	report, err := fixture.service.InspectStalledIssuance(context.Background(), fixture.request, fixture.inspector)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != StalledIssuanceConsumerDown || !report.EligibleForRestart {
		t.Fatalf("classification=%q eligible=%v", report.Classification, report.EligibleForRestart)
	}
}

func TestInspectStalledIssuanceDistinguishesTruncatedScanFromMissingRecord(t *testing.T) {
	fixture := newStalledIssuanceFixture(t, 1, 0, 0)
	fixture.inspector.found = false
	fixture.inspector.snapshot.Partitions[0].LogEndOffset = 50
	fixture.request.MaxScan = 10
	report, err := fixture.service.InspectStalledIssuance(context.Background(), fixture.request, fixture.inspector)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != StalledIssuanceScanTruncated || report.EligibleForRestart || report.Plan != nil {
		t.Fatalf("classification=%q eligible=%v plan=%+v", report.Classification, report.EligibleForRestart, report.Plan)
	}
	if report.Kafka == nil || !report.Kafka.ScanTruncated {
		t.Fatalf("truncated scan not reported: %+v", report.Kafka)
	}
}

func TestInspectStalledIssuanceRefusesUnknownPaymentAuthority(t *testing.T) {
	fixture := newStalledIssuanceFixture(t, 1, 0, 0)
	fixture.payment.snapshot.Admission.EligibilityState = paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN
	report, err := fixture.service.InspectStalledIssuance(context.Background(), fixture.request, fixture.inspector)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != StalledIssuanceBlocked || report.EligibleForRestart || report.Plan != nil {
		t.Fatalf("blocked payment produced classification=%q eligible=%v plan=%+v", report.Classification, report.EligibleForRestart, report.Plan)
	}
	if len(report.Reasons) == 0 {
		t.Fatal("blocked payment did not keep a reason")
	}
}

func TestInspectStalledIssuanceReportsAlreadyIssuedAndDenied(t *testing.T) {
	issued := newStalledIssuanceFixture(t, 1, 1, 1)
	report, err := issued.service.InspectStalledIssuance(context.Background(), issued.request, issued.inspector)
	if err != nil || report.Classification != StalledIssuanceAlreadyIssued || report.EligibleForRestart {
		t.Fatalf("issued classification=%v err=%v", report, err)
	}
	denied := newStalledIssuanceFixture(t, 1, 0, 1)
	report, err = denied.service.InspectStalledIssuance(context.Background(), denied.request, denied.inspector)
	if err != nil || report.Classification != StalledIssuanceDenied || report.EligibleForRestart {
		t.Fatalf("denied classification=%v err=%v", report, err)
	}
}

func TestInspectStalledIssuanceIncludesOffsetSnapshotForIssuedTicket(t *testing.T) {
	fixture := newStalledIssuanceFixture(t, 1, 1, 1)
	fixture.request.IncludeKafkaOnIssued = true
	report, err := fixture.service.InspectStalledIssuance(context.Background(), fixture.request, fixture.inspector)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != StalledIssuanceAlreadyIssued || report.EligibleForRestart {
		t.Fatalf("classification=%q eligible=%v", report.Classification, report.EligibleForRestart)
	}
	if report.Kafka == nil || report.Kafka.Group != fixture.request.Group || len(report.Kafka.Partitions) != 1 {
		t.Fatalf("issued ticket did not include the offset snapshot: %+v", report.Kafka)
	}
}

func TestClassifyStalledIssuanceSeparatesProcessingConsumedAndRetention(t *testing.T) {
	assigned := &pkgkafka.GroupSnapshot{
		Topic: "ticket.issuance.events",
		Members: []pkgkafka.GroupMemberSnapshot{{MemberID: "m",
			Topics: []pkgkafka.GroupMemberTopicSnapshot{{Topic: "ticket.issuance.events", Partitions: []int{0}}}}},
	}
	base := &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1, RecordUncommitted: true, StableNoProgress: true}
	cases := []struct {
		name    string
		kafka   *StalledIssuanceKafkaObservation
		elapsed time.Duration
		want    string
	}{
		{"processing_recent", base, time.Second, StalledIssuanceProcessing},
		{"processing_moving", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1, RecordUncommitted: true}, time.Minute, StalledIssuanceProcessing},
		{"consumed", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1, RecordConsumed: true}, time.Minute, StalledIssuanceConsumedNoTicket},
		{"not_retained", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1}, time.Minute, StalledIssuanceRecordNotRetained},
		{"scan_truncated", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1, ScanTruncated: true}, time.Minute, StalledIssuanceScanTruncated},
		{"multi", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 2, RecordUncommitted: true, StableNoProgress: true}, time.Minute, StalledIssuanceMultiInstance},
		{"unassigned", &StalledIssuanceKafkaObservation{Topic: "ticket.issuance.events", Members: 1, RecordUncommitted: true, StableNoProgress: true, RecordPartition: 9}, time.Minute, StalledIssuanceConsumerNotAssigned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classifyStalledIssuance(tc.kafka, assigned, tc.elapsed)
			if got != tc.want {
				t.Fatalf("classification=%q want %q", got, tc.want)
			}
		})
	}
}
