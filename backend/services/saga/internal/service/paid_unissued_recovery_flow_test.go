package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagapkg "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecoverPaidUnissuedBookingUsesReadOnlyAuthoritiesAndRecordsQualifiedPlan(t *testing.T) {
	for _, mode := range []struct {
		name    string
		execute bool
	}{
		{name: "dry run"},
		{name: "execute", execute: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			fixture := newPaidUnissuedRecoveryFlowFixture(t)
			request := RecoverPaidUnissuedBookingRequest{
				BookingID: fixture.saga.BookingID, SagaID: fixture.saga.ID, OperationKey: uuid.New(),
				Operator: "operator-test", Reason: "restore final paid ticket step", ExpectedProviderAccount: "acct_test_expected",
				Execute: mode.execute,
			}
			report, err := fixture.service.RecoverPaidUnissuedBooking(context.Background(), request)
			if err != nil {
				t.Fatalf("recovery qualification: %v", err)
			}
			if !report.Eligible || report.Requeued != mode.execute || report.ResultCode != fixture.repo.last.ResultCode {
				t.Fatalf("recovery report=%+v command=%+v", report, fixture.repo.last)
			}
			if fixture.repo.lookupCalls != 1 || fixture.repo.stateReads != 1 || fixture.repo.recordCalls != 1 {
				t.Fatalf("repository calls lookup=%d state-read=%d record=%d", fixture.repo.lookupCalls, fixture.repo.stateReads, fixture.repo.recordCalls)
			}
			if fixture.repo.last.SagaID != fixture.saga.ID || fixture.repo.last.BookingID != fixture.saga.BookingID ||
				fixture.repo.last.PaymentID == nil || *fixture.repo.last.PaymentID != uuid.MustParse(fixture.payment.entry.Id) ||
				fixture.repo.last.PaymentIntentID != fixture.payment.entry.PaymentIntentId ||
				fixture.repo.last.Mode != recoveryMode(mode.execute) || !fixture.repo.last.Eligible {
				t.Fatalf("recorded recovery command lost exact identity/binding: %+v", fixture.repo.last)
			}
			if fixture.booking.lifecycleReads != 1 || fixture.booking.ticketReads != 1 || fixture.payment.financialReads != 1 ||
				fixture.payment.snapshotReads != 1 || fixture.event.eventReads != 1 || fixture.event.eligibilityReads != 1 {
				t.Fatalf("authoritative reads booking=%d ticket=%d payment=%d payment-snapshot=%d event=%d seat=%d",
					fixture.booking.lifecycleReads, fixture.booking.ticketReads, fixture.payment.financialReads,
					fixture.payment.snapshotReads, fixture.event.eventReads, fixture.event.eligibilityReads)
			}
			if fixture.booking.writeCalls != 0 || fixture.payment.writeCalls != 0 || fixture.event.writeCalls != 0 || fixture.repo.directSagaWrites != 0 {
				t.Fatalf("recovery invoked business writes: booking=%d payment=%d event=%d saga=%d",
					fixture.booking.writeCalls, fixture.payment.writeCalls, fixture.event.writeCalls, fixture.repo.directSagaWrites)
			}
		})
	}
}

func TestRecoverPaidUnissuedBookingRejectsExistingTicketBeforeRequeue(t *testing.T) {
	fixture := newPaidUnissuedRecoveryFlowFixture(t)
	fixture.booking.ticket.TicketCount = 1
	report, err := fixture.service.RecoverPaidUnissuedBooking(context.Background(), RecoverPaidUnissuedBookingRequest{
		BookingID: fixture.saga.BookingID, SagaID: fixture.saga.ID, OperationKey: uuid.New(),
		Operator: "operator-test", Reason: "restore final paid ticket step", ExpectedProviderAccount: "acct_test_expected", Execute: true,
	})
	if err != nil {
		t.Fatalf("recovery qualification: %v", err)
	}
	if report.Eligible || report.Requeued || report.ResultCode != "ticket_already_exists" || fixture.repo.recordCalls != 1 ||
		fixture.repo.last.Eligible || fixture.repo.last.ResultCode != "ticket_already_exists" {
		t.Fatalf("existing Ticket did not stop execute: report=%+v command=%+v", report, fixture.repo.last)
	}
	if fixture.booking.writeCalls != 0 || fixture.payment.writeCalls != 0 || fixture.event.writeCalls != 0 || fixture.repo.directSagaWrites != 0 {
		t.Fatalf("ineligible recovery invoked business writes: booking=%d payment=%d event=%d saga=%d",
			fixture.booking.writeCalls, fixture.payment.writeCalls, fixture.event.writeCalls, fixture.repo.directSagaWrites)
	}
}

func TestRecoverPaidUnissuedBookingRejectsWrongGenerationOrExistingBindingBeforeProviderReads(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*paidUnissuedRecoveryFlowFixture)
	}{
		{name: "wrong generation", mutate: func(f *paidUnissuedRecoveryFlowFixture) { f.saga.Generation = 2 }},
		{name: "payment already bound", mutate: func(f *paidUnissuedRecoveryFlowFixture) { f.saga.PaymentID = uuid.New() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPaidUnissuedRecoveryFlowFixture(t)
			test.mutate(fixture)
			report, err := fixture.service.RecoverPaidUnissuedBooking(context.Background(), RecoverPaidUnissuedBookingRequest{
				BookingID: fixture.saga.BookingID, SagaID: fixture.saga.ID, OperationKey: uuid.New(),
				Operator: "operator-test", Reason: "restore final paid ticket step", ExpectedProviderAccount: "acct_test_expected", Execute: true,
			})
			if err != nil || report == nil || report.Eligible || report.Requeued || report.ResultCode != "saga_not_eligible_attention_state" {
				t.Fatalf("ineligible Saga was not stopped before qualification: report=%+v err=%v", report, err)
			}
			if fixture.booking.lifecycleReads != 0 || fixture.payment.financialReads != 0 || fixture.payment.snapshotReads != 0 ||
				fixture.event.eventReads != 0 || fixture.event.eligibilityReads != 0 || fixture.repo.recordCalls != 1 {
				t.Fatalf("ineligible Saga triggered authority reads: booking=%d payment=%d snapshot=%d event=%d eligibility=%d audit=%d",
					fixture.booking.lifecycleReads, fixture.payment.financialReads, fixture.payment.snapshotReads,
					fixture.event.eventReads, fixture.event.eligibilityReads, fixture.repo.recordCalls)
			}
		})
	}
}

type paidUnissuedRecoveryFlowFixture struct {
	service *SagaService
	repo    *paidUnissuedRecoveryFlowRepository
	booking *paidUnissuedRecoveryFlowBooking
	payment *paidUnissuedRecoveryFlowPayment
	event   *paidUnissuedRecoveryFlowEvent
	saga    *domain.Saga
}

func newPaidUnissuedRecoveryFlowFixture(t *testing.T) *paidUnissuedRecoveryFlowFixture {
	t.Helper()
	bookingID, userID, eventID, tierID, seatID, sagaID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	input, _, err := domain.CanonicalInput(domain.SagaInput{
		BookingID: bookingID, UserID: userID, EventID: eventID, Generation: 1, SeatIDs: []uuid.UUID{seatID},
		AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("build canonical Saga input: %v", err)
	}
	status, index := domain.SAGA_PROCESSING, 4
	saga := &domain.Saga{
		ID: sagaID, BookingID: bookingID, Name: "booking", Status: status, NullableStatus: &status,
		CurrentStepIndex: index, NullableCurrentStepIndex: &index, Generation: 1, RecoveryManaged: true,
		Input: &input, Direction: "forward", RecoveryState: "attention", ErrorCategory: "paid_unissued_binding_missing",
	}
	for i, name := range []sagapkg.SagaStepName{
		sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED,
		sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER,
	} {
		stepStatus, executeState := domain.SAGA_STEP_COMPLETED, "completed"
		var executedAt *time.Time
		if i == 4 {
			stepStatus, executeState = domain.SAGA_STEP_EXECUTING, "intended"
		} else {
			completed := time.Now().UTC()
			executedAt = &completed
		}
		order, pause := int32(i), i == 1
		saga.Steps = append(saga.Steps, domain.SagaStep{
			ID: uuid.New(), SagaID: sagaID, Name: string(name), Order: int16(i), NullableOrder: &order,
			ShouldPauseForPayment: pause, NullablePause: &pause, Status: stepStatus, NullableStatus: &stepStatus,
			ExecuteState: executeState, CompensateState: "pending", ExecutedAt: executedAt,
		})
	}

	paymentSnapshot := validRecoveryPaymentSnapshot(*saga, time.Now().UTC())
	paymentSnapshot.Payment.Id = uuid.NewString()
	paymentSnapshot.Payment.PaymentIntentId = "pi_paid_unissued_test"
	booking := &paidUnissuedRecoveryFlowBooking{
		disposition: &bookingv1.LifecycleDisposition{
			Booking: &bookingv1.BookingDetail{
				Id: bookingID.String(), UserId: userID.String(), EventId: eventID.String(), Status: "CONFIRMED", TotalAmountCents: 725,
				Items: []*bookingv1.BookingItem{{TicketTierId: tierID.String(), Quantity: 1, SeatIds: []string{seatID.String()}}},
			},
			Generation: 1, Phase: "TERMINAL", Currency: "usd", BookingMode: "pessimistic",
			ReservationDeadline: timestamppb.New(input.ReservationDeadline),
		},
		ticket: &bookingv1.PaidRecoveryTicketSnapshot{
			BookingId: bookingID.String(), SagaId: sagaID.String(), Generation: 1, ObservedAt: timestamppb.New(time.Now().UTC()),
		},
	}
	payment := &paidUnissuedRecoveryFlowPayment{entry: paymentSnapshot.Payment, snapshot: paymentSnapshot}
	event := &paidUnissuedRecoveryFlowEvent{
		bookingID: bookingID.String(), seatID: seatID.String(),
		detail: &eventv1.EventDetail{
			Id: eventID.String(), Status: "PUBLISHED",
			Tiers: []*eventv1.TicketTier{{Id: tierID.String(), EventId: eventID.String(), AccountingReady: true}},
		},
		eligibility: &eventv1.VerifyBookingTicketEligibilityResponse{
			State: eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE,
		},
	}
	repo := &paidUnissuedRecoveryFlowRepository{state: &repository.PaidUnissuedSagaState{Saga: saga}}
	service := NewSagaService(zap.NewNop(), repo, nil, booking, payment, event)
	return &paidUnissuedRecoveryFlowFixture{service: service, repo: repo, booking: booking, payment: payment, event: event, saga: saga}
}

func recoveryMode(execute bool) string {
	if execute {
		return "execute"
	}
	return "dry_run"
}

type paidUnissuedRecoveryFlowRepository struct {
	repository.SagaRepositoryInterface
	state            *repository.PaidUnissuedSagaState
	last             repository.PaidUnissuedRecoveryCommand
	lookupCalls      int
	stateReads       int
	recordCalls      int
	directSagaWrites int
}

func (r *paidUnissuedRecoveryFlowRepository) LookupPaidUnissuedRecoveryAudit(context.Context, repository.PaidUnissuedRecoveryCommand) (*repository.PaidUnissuedRecoveryAuditResult, error) {
	r.lookupCalls++
	return nil, nil
}

func (r *paidUnissuedRecoveryFlowRepository) GetPaidUnissuedSagaState(context.Context, uuid.UUID) (*repository.PaidUnissuedSagaState, error) {
	r.stateReads++
	return r.state, nil
}

func (r *paidUnissuedRecoveryFlowRepository) RecordPaidUnissuedRecovery(_ context.Context, command repository.PaidUnissuedRecoveryCommand) (*repository.PaidUnissuedRecoveryAuditResult, error) {
	r.recordCalls++
	r.last = command
	return &repository.PaidUnissuedRecoveryAuditResult{
		AuditID: uuid.New(), ResultCode: command.ResultCode, Eligible: command.Eligible,
		Requeued: command.Eligible && command.Mode == "execute",
	}, nil
}

func (r *paidUnissuedRecoveryFlowRepository) Create(context.Context, *domain.Saga) error {
	r.directSagaWrites++
	return nil
}

func (r *paidUnissuedRecoveryFlowRepository) UpdateSaga(context.Context, *domain.Saga, *uuid.UUID) error {
	r.directSagaWrites++
	return nil
}

type paidUnissuedRecoveryFlowBooking struct {
	bookingv1.BookingServiceClient
	disposition    *bookingv1.LifecycleDisposition
	ticket         *bookingv1.PaidRecoveryTicketSnapshot
	lifecycleReads int
	ticketReads    int
	writeCalls     int
}

func (b *paidUnissuedRecoveryFlowBooking) GetLifecycleDisposition(_ context.Context, req *bookingv1.LifecycleRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	b.lifecycleReads++
	if req.BookingId != b.disposition.Booking.Id || req.Generation != b.disposition.Generation {
		return nil, fmt.Errorf("unexpected Booking lifecycle identity")
	}
	return b.disposition, nil
}

func (b *paidUnissuedRecoveryFlowBooking) GetPaidRecoveryTicketSnapshot(_ context.Context, req *bookingv1.LifecycleRequest, _ ...grpc.CallOption) (*bookingv1.PaidRecoveryTicketSnapshot, error) {
	b.ticketReads++
	if req.BookingId != b.ticket.BookingId || req.Generation != b.ticket.Generation {
		return nil, fmt.Errorf("unexpected paid-recovery ticket identity")
	}
	return b.ticket, nil
}

func (b *paidUnissuedRecoveryFlowBooking) CreateBooking(context.Context, *bookingv1.CreateBookingRequest, ...grpc.CallOption) (*bookingv1.BookingDetail, error) {
	b.writeCalls++
	return nil, fmt.Errorf("unexpected Booking write")
}

func (b *paidUnissuedRecoveryFlowBooking) UpdateBookingStatusById(context.Context, *bookingv1.UpdateBookingStatusByIdReq, ...grpc.CallOption) (*emptypb.Empty, error) {
	b.writeCalls++
	return nil, fmt.Errorf("unexpected Booking write")
}

func (b *paidUnissuedRecoveryFlowBooking) DecideLifecycle(context.Context, *bookingv1.LifecycleDecisionRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	b.writeCalls++
	return nil, fmt.Errorf("unexpected Booking lifecycle decision")
}

type paidUnissuedRecoveryFlowPayment struct {
	paymentv1.PaymentServiceClient
	entry          *paymentv1.PaymentEntry
	snapshot       *paymentv1.PaidRecoveryPaymentSnapshot
	financialReads int
	snapshotReads  int
	writeCalls     int
}

func (p *paidUnissuedRecoveryFlowPayment) GetFinancialPayment(_ context.Context, req *paymentv1.FinancialPaymentRequest, _ ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	p.financialReads++
	if req.BookingId != p.entry.BookingId || req.Generation != p.entry.PaymentGeneration {
		return nil, fmt.Errorf("unexpected financial-payment lookup identity")
	}
	return p.entry, nil
}

func (p *paidUnissuedRecoveryFlowPayment) GetPaidRecoveryPaymentSnapshot(_ context.Context, req *paymentv1.FinancialPaymentRequest, _ ...grpc.CallOption) (*paymentv1.PaidRecoveryPaymentSnapshot, error) {
	p.snapshotReads++
	if req.PaymentId != p.entry.Id || req.BookingId != p.entry.BookingId || req.Generation != p.entry.PaymentGeneration {
		return nil, fmt.Errorf("unexpected paid-recovery Payment identity")
	}
	return p.snapshot, nil
}

func (p *paidUnissuedRecoveryFlowPayment) CreatePayment(context.Context, *paymentv1.CreatePaymentRequest, ...grpc.CallOption) (*paymentv1.CreatePaymentResponse, error) {
	p.writeCalls++
	return nil, fmt.Errorf("unexpected Payment creation")
}

func (p *paidUnissuedRecoveryFlowPayment) MakeRefundPayment(context.Context, *paymentv1.MakeRefundPaymentReq, ...grpc.CallOption) (*emptypb.Empty, error) {
	p.writeCalls++
	return nil, fmt.Errorf("unexpected refund")
}

func (p *paidUnissuedRecoveryFlowPayment) RequestPaymentResolution(context.Context, *paymentv1.PaymentResolutionRequest, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	p.writeCalls++
	return nil, fmt.Errorf("unexpected Payment resolution")
}

func (p *paidUnissuedRecoveryFlowPayment) AcceptApprovedUnusedRefundDecision(context.Context, *paymentv1.AcceptApprovedUnusedRefundDecisionRequest, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	p.writeCalls++
	return nil, fmt.Errorf("unexpected approved refund decision")
}

func (p *paidUnissuedRecoveryFlowPayment) UpdatePaymentStatusByPaymentIntentId(context.Context, *paymentv1.UpdatePaymentStatusByPaymentIntentIdReq, ...grpc.CallOption) (*emptypb.Empty, error) {
	p.writeCalls++
	return nil, fmt.Errorf("unexpected Payment mutation")
}

type paidUnissuedRecoveryFlowEvent struct {
	eventv1.EventServiceClient
	bookingID        string
	seatID           string
	detail           *eventv1.EventDetail
	eligibility      *eventv1.VerifyBookingTicketEligibilityResponse
	eventReads       int
	eligibilityReads int
	writeCalls       int
}

func (e *paidUnissuedRecoveryFlowEvent) GetEvent(_ context.Context, req *eventv1.GetEventRequest, _ ...grpc.CallOption) (*eventv1.EventDetail, error) {
	e.eventReads++
	if req.EventId != e.detail.Id {
		return nil, fmt.Errorf("unexpected Event identity")
	}
	return e.detail, nil
}

func (e *paidUnissuedRecoveryFlowEvent) VerifyBookingTicketEligibility(_ context.Context, req *eventv1.VerifyBookingTicketEligibilityRequest, _ ...grpc.CallOption) (*eventv1.VerifyBookingTicketEligibilityResponse, error) {
	e.eligibilityReads++
	if req.BookingId != e.bookingID || req.EventId != e.detail.Id || req.Generation != 1 || len(req.SeatIds) != 1 || req.SeatIds[0] != e.seatID {
		return nil, fmt.Errorf("unexpected Event eligibility identity")
	}
	return e.eligibility, nil
}

func (e *paidUnissuedRecoveryFlowEvent) ApplySeatLifecycle(context.Context, *eventv1.SeatLifecycleRequest, ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	e.writeCalls++
	return nil, fmt.Errorf("unexpected Event seat write")
}

func (e *paidUnissuedRecoveryFlowEvent) ApplySaleAccounting(context.Context, *eventv1.SeatLifecycleRequest, ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	e.writeCalls++
	return nil, fmt.Errorf("unexpected Event accounting write")
}
