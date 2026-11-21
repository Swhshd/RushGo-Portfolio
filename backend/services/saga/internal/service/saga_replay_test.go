package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagapkg "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type replayBooking struct {
	bookingv1.BookingServiceClient
	view            *bookingv1.LifecycleDisposition
	reject, ackLost bool
	blockLifecycle  <-chan struct{}
}

func (b *replayBooking) GetBooking(context.Context, *bookingv1.GetBookingRequest, ...grpc.CallOption) (*bookingv1.BookingDetail, error) {
	return b.view.Booking, nil
}
func (b *replayBooking) UpdateBookingStatusById(context.Context, *bookingv1.UpdateBookingStatusByIdReq, ...grpc.CallOption) (*emptypb.Empty, error) {
	if b.reject {
		return nil, status.Error(codes.FailedPrecondition, "confirmation rejected")
	}
	return &emptypb.Empty{}, nil
}
func (b *replayBooking) GetLifecycleDisposition(ctx context.Context, _ *bookingv1.LifecycleRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if b.blockLifecycle != nil {
		select {
		case <-b.blockLifecycle:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.view, nil
}
func (b *replayBooking) DecideLifecycle(_ context.Context, req *bookingv1.LifecycleDecisionRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if req.Kind == "confirm" && b.reject {
		b.view.Booking.Status = "FAILED"
		b.view.Phase = "TERMINAL"
		b.view.DecisionId = decisionID(b.view.Booking.Id, "abort_confirmation")
		return nil, status.Error(codes.FailedPrecondition, "confirmation rejected")
	}
	switch req.Kind {
	case "claim_confirmation":
		b.view.Phase = "CONFIRMING"
		b.view.ConfirmationDecisionId = req.DecisionId
	case "confirm":
		b.view.Phase = "TERMINAL"
		b.view.Booking.Status = "CONFIRMED"
		b.view.DecisionId = req.DecisionId
	case "fail", "abort_confirmation":
		b.view.Phase = "TERMINAL"
		b.view.Booking.Status = "FAILED"
		b.view.DecisionId = req.DecisionId
	}
	b.view.Version++
	if req.Kind == "confirm" && b.ackLost {
		b.ackLost = false
		return nil, status.Error(codes.Unavailable, "commit ACK lost")
	}
	return b.view, nil
}
func decisionID(booking, kind string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking+":g1:"+kind)).String()
}

type replayPayment struct {
	paymentv1.PaymentServiceClient
	entry                              *paymentv1.PaymentEntry
	resolutions, legacyWrites, creates int
	createLost                         bool
	createReq                          *paymentv1.CreatePaymentRequest
}

func TestSagaFailedWakeCannotInventMoneyOrStrandKnownFailureIntegration(t *testing.T) {
	for _, outcome := range []string{"unknown", "not_collected", "collected"} {
		t.Run(outcome, func(t *testing.T) {
			s, r, db, v, b, p, e := storedReplayFixture(t)
			b.view.Phase = "OPEN"
			p.entry.ProviderOutcome = outcome
			p.entry.ProviderStatus = "requires_payment_method"
			p.entry.Status = "fail"
			if _, err := db.Exec(context.Background(), `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(context.Background(), `UPDATE sagas SET current_step_index=2,status='WAIT_FOR_PAYMENT' WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			if err := s.HandleSagaAfterPaymentFailure(context.Background(), []byte(`{"Data":{"object":{"id":"pi_replay","object":"payment_intent","status":"failed"}}}`)); err != nil {
				t.Fatal(err)
			}
			_ = claimAndRun(t, s, r, v.ID)
			got, err := r.GetSagaById(context.Background(), v.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch outcome {
			case "unknown":
				if got.Direction != "forward" || b.view.Booking.Status != "PENDING" || p.resolutions != 0 || e.close != 0 {
					t.Fatal("failure label selected terminal money authority")
				}
			case "not_collected":
				if got.Direction != "compensate" || b.view.Booking.Status != "FAILED" || p.resolutions == 0 {
					t.Fatal("known provider failure stranded at S2")
				}
			case "collected":
				if got.Status != domain.SAGA_COMPLETED || b.view.Booking.Status != "CONFIRMED" || p.legacyWrites != 0 {
					t.Fatal("stale failed label defeated durable collection")
				}
			}
		})
	}
}

func TestSagaRegistryCannotBypassManagedIntentIntegration(t *testing.T) {
	s, _, _, v, b, p, e := storedReplayFixture(t)
	b.view.Phase = "OPEN"
	h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Execute(context.Background(), 0); err == nil {
		t.Fatal("unclaimed managed graph executed")
	}
	if e.reserve != 0 {
		t.Fatal("managed legacy registry emitted effect before intent")
	}
}

func TestSagaTerminalSettlementBeforeStartCreatesCloseResponsibilityIntegration(t *testing.T) {
	s, r, _, v, b, p, e := storedReplayFixture(t)
	ctx := context.Background()
	// A terminal dispatch may win before Start is ever delivered.
	other := uuid.New()
	b.view.Booking.Id = other.String()
	p.entry.BookingId = other.String()
	b.view.Version = 1
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "CANCELLED"
	b.view.DecisionId = decisionID(other.String(), "cancel")
	if _, err := s.SettleOrderSaga(ctx, other, 1, b.view.DecisionId, p.entry.Id, "cancel_before_start"); err != nil {
		t.Fatal("terminal settlement lost responsibility before Start", err)
	}
	got, err := r.GetSagaByBookingId(ctx, other)
	if err != nil || got.ID == v.ID {
		t.Fatal("no original terminal settlement graph", err)
	}
	_ = claimAndRun(t, s, r, got.ID)
	if e.reserve != 0 || e.close != 1 {
		t.Fatal("terminal-before-start failed to close or re-reserved seats")
	}
}

func (p *replayPayment) CreatePayment(_ context.Context, req *paymentv1.CreatePaymentRequest, _ ...grpc.CallOption) (*paymentv1.CreatePaymentResponse, error) {
	p.creates++
	p.createReq = req
	if p.createLost {
		return nil, status.Error(codes.Unavailable, "create ACK lost")
	}
	return &paymentv1.CreatePaymentResponse{Id: p.entry.Id, PaymentIntentId: p.entry.PaymentIntentId, PaymentIntentClientSecret: "pi_secret_replay", CreateState: "create_ready"}, nil
}
func (p *replayPayment) GetFinancialPayment(context.Context, *paymentv1.FinancialPaymentRequest, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	return p.entry, nil
}
func (p *replayPayment) GetPaymentByPaymentIntentId(context.Context, *paymentv1.GetPaymentByIntentIdReq, ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	return p.entry, nil
}
func (p *replayPayment) RequestPaymentResolution(_ context.Context, _ *paymentv1.PaymentResolutionRequest, _ ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	p.resolutions++
	if p.entry.RefundState != "succeeded" {
		p.entry.RefundState = "pending"
	}
	return p.entry, nil
}
func (p *replayPayment) UpdatePaymentStatusByPaymentIntentId(context.Context, *paymentv1.UpdatePaymentStatusByPaymentIntentIdReq, ...grpc.CallOption) (*emptypb.Empty, error) {
	p.legacyWrites++
	return nil, errors.New("generic financial write prohibited")
}

type replayEvent struct {
	eventv1.EventServiceClient
	account, close, reserve, book int
	operations                    []string
	requests                      []*eventv1.SeatLifecycleRequest
	bookErr, accountErr           error
	emptyReply, nilReply          bool
}

func (e *replayEvent) ApplySeatLifecycle(_ context.Context, req *eventv1.SeatLifecycleRequest, _ ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	e.operations = append(e.operations, req.OperationId)
	e.requests = append(e.requests, req)
	if e.nilReply {
		return nil, nil
	}
	if e.emptyReply {
		return &eventv1.SeatLifecycleResponse{}, nil
	}
	switch req.Action {
	case "reserve":
		e.reserve++
		return &eventv1.SeatLifecycleResponse{State: "held"}, nil
	case "book":
		e.book++
		if e.bookErr != nil {
			return nil, e.bookErr
		}
		return &eventv1.SeatLifecycleResponse{State: "booked"}, nil
	case "close":
		e.close++
		return &eventv1.SeatLifecycleResponse{State: "closed"}, nil
	}
	return nil, errors.New("unexpected action")
}

func TestSagaMissingEventRepliesNeverSelectTerminalIntegration(t *testing.T) {
	for _, order := range []int{0, 2} {
		for _, nilReply := range []bool{false, true} {
			t.Run(fmt.Sprintf("S%d_nil%v", order, nilReply), func(t *testing.T) {
				s, r, db, v, b, p, e := storedReplayFixture(t)
				b.view.Phase = "OPEN"
				e.nilReply = nilReply
				e.emptyReply = !nilReply
				if order == 2 {
					b.view.Phase = "CONFIRMING"
					if _, err := db.Exec(context.Background(), `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(context.Background(), `UPDATE sagas SET current_step_index=2 WHERE id=$1`, v.ID); err != nil {
						t.Fatal(err)
					}
				}
				_ = claimAndRun(t, s, r, v.ID)
				got, err := r.GetSagaById(context.Background(), v.ID)
				if err != nil {
					t.Fatal(err)
				}
				if b.view.Booking.Status != "PENDING" || got.Direction != "forward" || p.resolutions != 0 || e.close != 0 {
					t.Fatal("empty Event acknowledgement destructively compensated")
				}
			})
		}
	}
}
func (e *replayEvent) ApplySaleAccounting(_ context.Context, req *eventv1.SeatLifecycleRequest, _ ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	e.account++
	e.operations = append(e.operations, req.OperationId)
	e.requests = append(e.requests, req)
	if e.accountErr != nil {
		return nil, e.accountErr
	}
	if req.Action != "account" {
		return nil, errors.New("account action required")
	}
	return &eventv1.SeatLifecycleResponse{State: "booked", AccountingState: "applied"}, nil
}

func replayLiteralFixture() (*SagaService, *domain.Saga, *replayBooking, *replayPayment, *replayEvent) {
	booking, user, event, tier, seat := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	input, _, _ := domain.CanonicalInput(domain.SagaInput{BookingID: booking, UserID: user, EventID: event, Generation: 1, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().Add(time.Minute), SeatIDs: []uuid.UUID{seat}})
	b := &replayBooking{view: &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: booking.String(), UserId: user.String(), EventId: event.String(), Status: "PENDING", TotalAmountCents: 725, Items: []*bookingv1.BookingItem{{TicketTierId: tier.String(), Quantity: 1, SeatIds: []string{seat.String()}, UnitPriceCents: 725}}}, Generation: 1, Phase: "CONFIRMING", Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: timestamppb.New(input.ReservationDeadline), ConfirmationDecisionId: decisionID(booking.String(), "claim_confirmation")}}
	p := &replayPayment{entry: &paymentv1.PaymentEntry{Id: uuid.NewString(), UserId: user.String(), BookingId: booking.String(), Price: 725, Currency: "usd", PaymentGeneration: 1, PaymentIntentId: "pi_replay", Status: "success", ProviderOutcome: "collected", RefundState: "not_required"}}
	e := &replayEvent{}
	s := NewSagaService(zap.NewNop(), nil, registry.NewSagaStepRegistry(), b, p, e)
	// Legacy processors exercise the old swallowed-error boundary during RED.
	s.sagaStepRegistry.Register(string(sagapkg.RESERVED_SEAT_STEP), registry.SagaStepProcessor{Compensate: func(context.Context, *eventv1.ReservedOrCompensateBatchSeatsReq) error { return nil }})
	s.sagaStepRegistry.Register(string(sagapkg.CREATE_PAYMENT_INTENT_STEP), registry.SagaStepProcessor{Compensate: func(context.Context, *paymentv1.MakeRefundPaymentReq) error { return nil }})
	s.sagaStepRegistry.Register(string(sagapkg.UPDATE_SEAT_BOOKED), registry.SagaStepProcessor{Execute: func(context.Context, *eventv1.UpdateBatchSeatStatusRequest) error { return nil }})
	s.sagaStepRegistry.Register(string(sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED), registry.SagaStepProcessor{Execute: s.UpdateBookingStatus})
	s.sagaStepRegistry.Register(string(sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER), registry.SagaStepProcessor{Execute: func(context.Context, *eventv1.UpdateTicketAvailabilityRequest) error { return nil }})
	v := &domain.Saga{ID: uuid.New(), BookingID: booking, Input: &input, Generation: 1, RecoveryManaged: true, Direction: "forward", RecoveryState: "ready", Status: domain.SAGA_PROCESSING, CurrentStepIndex: 3}
	for i, name := range []sagapkg.SagaStepName{sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED, sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER} {
		st := domain.SagaStep{ID: uuid.New(), SagaID: v.ID, Name: string(name), Order: int16(i), Status: domain.SAGA_STEP_PENDING, ExecuteState: "pending", CompensateState: "pending", ShouldPauseForPayment: i == 1}
		v.Steps = append(v.Steps, st)
	}
	return s, v, b, p, e
}

type capturedPaymentWakeRepository struct {
	repository.SagaRepositoryInterface
	repository.ReplayRepository
	bookingID   uuid.UUID
	generation  int64
	paymentID   *uuid.UUID
	reason      string
	wakeCalls   int
	booksAtWake int
	bookCount   func() int
	wakeErr     error
}

func (r *capturedPaymentWakeRepository) WakeSaga(_ context.Context, bookingID uuid.UUID, generation int64, paymentID *uuid.UUID, reason string) error {
	r.bookingID = bookingID
	r.generation = generation
	if paymentID != nil {
		id := *paymentID
		r.paymentID = &id
	}
	r.reason = reason
	r.wakeCalls++
	if r.bookCount != nil {
		r.booksAtWake = r.bookCount()
	}
	return r.wakeErr
}

func TestSagaCollectedS2BindsPaymentBeforeBookingEffect(t *testing.T) {
	s, v, b, p, e := replayLiteralFixture()
	b.view.Phase = "CONFIRMING"
	v.PaymentID = uuid.Nil
	wakeRepo := &capturedPaymentWakeRepository{bookCount: func() int { return e.book }}
	s.repo = wakeRepo

	if err := s.executeStoredStep(context.Background(), v, 2, nil); err != nil {
		t.Fatalf("collected S2 execution: %v", err)
	}
	expectedPaymentID, err := uuid.Parse(p.entry.Id)
	if err != nil {
		t.Fatal(err)
	}
	if wakeRepo.wakeCalls != 1 || wakeRepo.bookingID != v.BookingID || wakeRepo.generation != v.Generation || wakeRepo.paymentID == nil || *wakeRepo.paymentID != expectedPaymentID || wakeRepo.reason != "s2_collected_read" || wakeRepo.booksAtWake != 0 {
		t.Fatalf("collected payment was not durably bound before S2 booking: calls=%d booking=%s generation=%d payment=%v reason=%q books_at_wake=%d", wakeRepo.wakeCalls, wakeRepo.bookingID, wakeRepo.generation, wakeRepo.paymentID, wakeRepo.reason, wakeRepo.booksAtWake)
	}
	if e.book != 1 {
		t.Fatalf("S2 booking effect count=%d, want 1", e.book)
	}
}

func TestSagaS2RequiresValidCollectionAndPaymentBindingBeforeBooking(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*domain.Saga, *replayPayment, *capturedPaymentWakeRepository)
		wantErr   error
		wantWake  int
	}{
		{
			name: "payment not collected",
			configure: func(_ *domain.Saga, payment *replayPayment, _ *capturedPaymentWakeRepository) {
				payment.entry.ProviderOutcome = "unknown"
			},
			wantErr: errFinancialWait,
		},
		{
			name: "financial identity mismatch",
			configure: func(_ *domain.Saga, payment *replayPayment, _ *capturedPaymentWakeRepository) {
				payment.entry.BookingId = uuid.NewString()
			},
			wantErr: repository.ErrRecoveryConflict,
		},
		{
			name: "already bound to another payment",
			configure: func(saga *domain.Saga, _ *replayPayment, _ *capturedPaymentWakeRepository) {
				saga.PaymentID = uuid.New()
			},
			wantErr: repository.ErrRecoveryConflict,
		},
		{
			name: "payment binding rejected",
			configure: func(_ *domain.Saga, _ *replayPayment, wake *capturedPaymentWakeRepository) {
				wake.wakeErr = repository.ErrRecoveryConflict
			},
			wantErr:  repository.ErrRecoveryConflict,
			wantWake: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, v, b, p, e := replayLiteralFixture()
			b.view.Phase = "CONFIRMING"
			wakeRepo := &capturedPaymentWakeRepository{bookCount: func() int { return e.book }}
			tt.configure(v, p, wakeRepo)
			s.repo = wakeRepo

			err := s.executeStoredStep(context.Background(), v, 2, nil)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("S2 error=%v, want %v", err, tt.wantErr)
			}
			if wakeRepo.wakeCalls != tt.wantWake || e.book != 0 {
				t.Fatalf("invalid or conflicting payment reached S2 effect: wake_calls=%d book_calls=%d", wakeRepo.wakeCalls, e.book)
			}
		})
	}
}

func TestSagaConfirmationRejectionAndForwardRecovery(t *testing.T) {
	t.Run("definitive_rejection_propagates", func(t *testing.T) {
		s, v, b, p, _ := replayLiteralFixture()
		b.reject = true
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.GetSaga().Steps[3].Execute(context.Background()); err == nil {
			t.Fatal("S3 confirmation rejection swallowed")
		}
	})
	t.Run("confirm_commit_ack_loss_recovers_forward", func(t *testing.T) {
		s, v, b, p, e := replayLiteralFixture()
		b.ackLost = true
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.GetSaga().Steps[3].Execute(context.Background()); err != nil {
			t.Fatal("committed confirmation not recovered", err)
		}
		if b.view.Booking.Status != "CONFIRMED" {
			t.Fatal("confirmation authority not committed")
		}
		if err := h.GetSaga().Steps[4].Execute(context.Background()); err != nil || e.account != 1 || e.close != 0 || p.resolutions != 0 {
			t.Fatal("confirmed booking failed forward accounting", err)
		}
	})
	t.Run("S0_original_deadline_and_identity", func(t *testing.T) {
		s, v, b, p, e := replayLiteralFixture()
		b.view.Phase = "OPEN"
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if h.GetSaga().Steps[0].Execute == nil {
			t.Fatal("stored S0 replay missing")
		}
		if err = h.GetSaga().Steps[0].Execute(context.Background()); err != nil {
			t.Fatal(err)
		}
		req := e.requests[0]
		if req.OperationId != v.Steps[0].ID.String() || req.BookingId != v.BookingID.String() || req.Generation != 1 || !req.ReservationDeadline.AsTime().Equal(v.Input.ReservationDeadline) {
			t.Fatal("reserve changed immutable input")
		}
		b.view.Phase = "TERMINAL"
		b.view.Booking.Status = "CANCELLED"
		if err = h.GetSaga().Steps[0].Execute(context.Background()); err == nil || e.reserve != 1 {
			t.Fatal("terminal booking reserved again")
		}
	})
	t.Run("S1_response_loss_reads_original_payment", func(t *testing.T) {
		s, v, b, p, _ := replayLiteralFixture()
		b.view.Phase = "OPEN"
		p.createLost = true
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if h.GetSaga().Steps[1].Execute == nil {
			t.Fatal("stored S1 replay missing")
		}
		if err = h.GetSaga().Steps[1].Execute(context.Background()); err != nil {
			t.Fatal("durable original collection not recovered", err)
		}
		if p.creates != 1 || p.createReq.Price != 725 || p.createReq.PaymentGeneration != 1 || p.createReq.BookingId != v.BookingID.String() {
			t.Fatal("payment replay changed financial identity")
		}
	})
	t.Run("S2_claim_precedes_book", func(t *testing.T) {
		s, v, b, p, e := replayLiteralFixture()
		b.view.Phase = "OPEN"
		b.view.ConfirmationDecisionId = ""
		wakeRepo := &capturedPaymentWakeRepository{bookCount: func() int { return e.book }}
		s.repo = wakeRepo
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.GetSaga().Steps[2].Execute(context.Background()); err != nil {
			t.Fatal(err)
		}
		if b.view.Phase != "CONFIRMING" || e.book != 1 || e.operations[0] != v.Steps[2].ID.String() || wakeRepo.wakeCalls != 1 || wakeRepo.booksAtWake != 0 {
			t.Fatal("book bypassed confirmation authority")
		}
	})
	t.Run("S4_infrastructure_failure_never_compensates_confirmed", func(t *testing.T) {
		s, v, b, p, e := replayLiteralFixture()
		b.view.Phase = "TERMINAL"
		b.view.Booking.Status = "CONFIRMED"
		e.accountErr = status.Error(codes.Unavailable, "account outage")
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.GetSaga().Steps[4].Execute(context.Background()); err == nil {
			t.Fatal("account error swallowed")
		}
		if err = h.GetSaga().Steps[1].Compensate(context.Background()); err == nil || p.resolutions != 0 || e.close != 0 {
			t.Fatal("confirmed lifecycle compensated")
		}
	})
}

func TestSagaReplayRetainsOriginalStepIdentity(t *testing.T) {
	s, v, _, p, _ := replayLiteralFixture()
	ids := make([]uuid.UUID, 5)
	for i := range v.Steps {
		ids[i] = v.Steps[i].ID
	}
	for attempt := 0; attempt < 3; attempt++ {
		h, err := s.reBuildSagaHandler(context.Background(), v, p.entry)
		if err != nil {
			t.Fatal(err)
		}
		for i, st := range h.GetSaga().Steps {
			if st.ID != ids[i] || st.SagaID != v.ID {
				t.Fatal("replay replaced original step identity")
			}
		}
	}
}

func TestSagaMalformedPaymentWakeFailsClosed(t *testing.T) {
	s, _, _, p, _ := replayLiteralFixture()
	for _, raw := range []string{`null`, `{}`, `{"Data":null}`, `{"Data":{"object":{"id":3}}}`, `{"Data":{"object":{"id":"pi_spoof","status":"succeeded"}}}`} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("untrusted payload panicked: %v", r)
				}
			}()
			if err := s.HandleSagaAferPaymentSuccess(context.Background(), []byte(raw)); err == nil {
				t.Error("invalid wake accepted")
			}
		}()
	}
	if p.legacyWrites != 0 {
		t.Fatal("event label invented financial state")
	}
}

func TestSagaPaymentFailureWakeUsesDurablePaymentTruth(t *testing.T) {
	s, _, b, p, _ := replayLiteralFixture()
	p.entry.Status = "failed"
	p.entry.ProviderOutcome = "not_collected"
	p.entry.ReconciliationState = "resolved"
	raw := []byte(`{"Data":{"object":{"id":"pi_replay","object":"payment_intent","status":"succeeded"}}}`)
	if err := s.HandleSagaAfterPaymentFailure(context.Background(), raw); !errors.Is(err, repository.ErrRecoveryAttention) {
		t.Fatalf("expected recovery attention without a repository, got %v", err)
	}
	if b.view.Booking.Status == "CONFIRMED" || p.resolutions != 0 {
		t.Fatal("payment failure wake invented a confirmed or refund outcome")
	}
}

func replayPG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(os.Getenv("SAGA_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25438" || u.Path != "/ticketbox_saga_test" {
		t.Fatal("BLOCKED: isolated Saga fixture required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "saga_replay_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.Command("migrate", "-path", "../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

type claimExecutor interface {
	ExecuteClaim(context.Context, domain.SagaClaim) error
}
type replayClaimRepository interface {
	ClaimSaga(context.Context, uuid.UUID, time.Duration) (*domain.SagaClaim, error)
}
type settlementService interface {
	SettleOrderSaga(context.Context, uuid.UUID, int64, string, string, string) (*StartOrderSagaResponse, error)
}

func claimAndRun(t *testing.T, s *SagaService, r *repository.SagaRepository, id uuid.UUID) error {
	t.Helper()
	cr, ok := any(r).(replayClaimRepository)
	if !ok {
		t.Fatal("specific durable claim absent")
	}
	c, err := cr.ClaimSaga(context.Background(), id, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("work not claimable")
	}
	ex, ok := any(s).(claimExecutor)
	if !ok {
		t.Fatal("durable executor absent")
	}
	return ex.ExecuteClaim(context.Background(), *c)
}
func storedReplayFixture(t *testing.T) (*SagaService, *repository.SagaRepository, *pgxpool.Pool, *domain.Saga, *replayBooking, *replayPayment, *replayEvent) {
	t.Helper()
	p := replayPG(t)
	s, v, b, pay, e := replayLiteralFixture()
	r := repository.NewSagaRepository(p)
	v, _, err := r.CreateOrGetSaga(context.Background(), *v.Input, v.Steps)
	if err != nil {
		t.Fatal(err)
	}
	s.repo = r
	return s, r, p, v, b, pay, e
}
func TestSagaDurableExecutorBoundariesIntegration(t *testing.T) {
	for _, scenario := range []string{"confirmation_rejected", "confirmation_ack_lost", "book_uncertain", "book_impossible", "account_outage"} {
		t.Run(scenario, func(t *testing.T) {
			s, r, db, v, b, p, e := storedReplayFixture(t)
			ctx := context.Background()
			// Persisted S0/S1 represent a completed checkout, with original IDs intact.
			now := time.Now().UTC()
			if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=$2 WHERE saga_id=$1 AND "order"<2`, v.ID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=2,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "confirmation_rejected":
				b.reject = true
			case "confirmation_ack_lost":
				b.ackLost = true
			case "book_uncertain":
				e.bookErr = status.Error(codes.Unavailable, "ACK unknown")
			case "book_impossible":
				e.bookErr = status.Error(codes.FailedPrecondition, "closed receipt")
			case "account_outage":
				e.accountErr = status.Error(codes.Unavailable, "account outage")
			}
			_ = claimAndRun(t, s, r, v.ID)
			got, err := r.GetSagaById(ctx, v.ID)
			if err != nil {
				t.Fatal(err)
			}
			for i := range got.Steps {
				if got.Steps[i].ID != v.Steps[i].ID {
					t.Fatal("durable graph identity changed")
				}
			}
			switch scenario {
			case "confirmation_rejected", "book_impossible":
				if b.view.Booking.Status != "FAILED" || got.Direction != "compensate" || got.Status == domain.SAGA_COMPLETED || got.Status == domain.SAGA_ROLLED_BACK || e.account != 0 || p.resolutions == 0 {
					t.Fatalf("rejection lost compensation responsibility: %s/%s Booking=%s resolution=%d", got.Status, got.Direction, b.view.Booking.Status, p.resolutions)
				}
			case "confirmation_ack_lost":
				if got.Status != domain.SAGA_COMPLETED || b.view.Booking.Status != "CONFIRMED" || e.account != 1 || e.close != 0 || p.resolutions != 0 {
					t.Fatal("ACK loss did not recover forward")
				}
			case "book_uncertain":
				if got.Direction != "forward" || got.Status == domain.SAGA_ROLLED_BACK || b.view.Phase != "CONFIRMING" || e.close != 0 || p.resolutions != 0 {
					t.Fatal("uncertainty destructively compensated")
				}
			case "account_outage":
				if got.Direction != "forward" || b.view.Booking.Status != "CONFIRMED" || got.CurrentStepIndex != 4 || e.close != 0 || p.resolutions != 0 {
					t.Fatal("confirmed accounting outage rolled back")
				}
			}
		})
	}
}

func TestSagaCollectionWakeFencesRollbackReadIntegration(t *testing.T) {
	for _, window := range []string{"S1_read_before_result", "S1_completed_at_S0"} {
		t.Run(window, func(t *testing.T) {
			s, r, db, v, b, p, _ := storedReplayFixture(t)
			ctx := context.Background()
			b.view.Phase = "TERMINAL"
			b.view.Booking.Status = "EXPIRED"
			b.view.DecisionId = decisionID(v.BookingID.String(), "expire")
			index := 1
			if window == "S1_completed_at_S0" {
				index = 0
			}
			if _, err := db.Exec(ctx, `UPDATE saga_steps SET status='COMPENSATED',compensate_state='completed',compensated_at=clock_timestamp() WHERE saga_id=$1 AND "order">$2`, v.ID, index); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `UPDATE sagas SET status='ROLLING_BACK',direction='compensate',current_step_index=$2 WHERE id=$1`, v.ID, index); err != nil {
				t.Fatal(err)
			}
			c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
			if err != nil || c == nil {
				t.Fatal(err)
			}
			if ok, err := r.PersistStepIntent(ctx, *c, v.Steps[index].ID, "compensate"); err != nil || !ok {
				t.Fatal(err)
			}
			if _, err := s.SettleOrderSaga(ctx, v.BookingID, 1, b.view.DecisionId, p.entry.Id, "new_collection"); err != nil {
				t.Fatal(err)
			}
			ok, err := r.PersistStepResult(ctx, *c, domain.StepResult{StepID: v.Steps[index].ID, Direction: "compensate", State: "completed"})
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("old noncollection/close result erased new refund responsibility")
			}
			got, err := r.GetSagaById(ctx, v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.CurrentStepIndex != 1 || got.Steps[1].CompensateState == "completed" || got.Status != domain.SAGA_ROLLING_BACK {
				t.Fatal("new obligation not scheduled at original S1")
			}
		})
	}
}

func TestSagaCollectedS1SchedulesForwardWithoutWebhookIntegration(t *testing.T) {
	s, r, db, v, b, p, _ := storedReplayFixture(t)
	b.view.Phase = "OPEN"
	p.createLost = true
	if _, err := db.Exec(context.Background(), `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE sagas SET current_step_index=1,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatal(err)
	}
	got, err := r.GetSagaById(context.Background(), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == domain.SAGA_WAIT_FOR_PAYMENT {
		t.Fatal("known collected S1 stranded waiting for absent webhook")
	}
	if got.Status != domain.SAGA_COMPLETED {
		if err := claimAndRun(t, s, r, v.ID); err != nil {
			t.Fatal(err)
		}
	}
	got, err = r.GetSagaById(context.Background(), v.ID)
	if err != nil || got.Status != domain.SAGA_COMPLETED {
		t.Fatal("known collection failed forward recovery", err)
	}
}

func TestSagaWaitingPaymentRecoversWithoutWebhookBindsPaymentAndCreatesIssuanceOutboxIntegration(t *testing.T) {
	s, r, db, v, b, p, e := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "OPEN"
	p.entry.ProviderOutcome = "unknown"
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=1,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatalf("initial payment wait: %v", err)
	}
	waiting, err := r.GetSagaById(ctx, v.ID)
	if err != nil || waiting.Status != domain.SAGA_WAIT_FOR_PAYMENT || waiting.CurrentStepIndex != 2 {
		t.Fatalf("payment wait was not durable: status=%s index=%d err=%v", waiting.Status, waiting.CurrentStepIndex, err)
	}
	p.entry.ProviderOutcome = "collected"
	if _, err := db.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatal("due WAIT row was not recovered without WakeSaga", err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_COMPLETED || got.CurrentStepIndex != 5 || e.book != 1 || e.account != 1 {
		t.Fatalf("waiting payment did not resume forward: status=%s index=%d book=%d account=%d err=%v", got.Status, got.CurrentStepIndex, e.book, e.account, err)
	}
	wantPaymentID, err := uuid.Parse(p.entry.Id)
	if err != nil || got.PaymentID != wantPaymentID {
		t.Fatalf("polled collection did not bind authoritative payment: saved=%s want=%s err=%v", got.PaymentID, wantPaymentID, err)
	}
	var matchingOutboxRows int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM saga_ticket_issuance_outbox WHERE saga_id=$1 AND payment_id=$2`, v.ID, wantPaymentID).Scan(&matchingOutboxRows); err != nil || matchingOutboxRows != 1 {
		t.Fatalf("final saga did not atomically write one issuance event with its payment binding: rows=%d err=%v", matchingOutboxRows, err)
	}
}

func TestSagaAuthorityReadsUseRecoveryDeadlineIntegration(t *testing.T) {
	s, r, db, v, b, _, _ := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "CONFIRMING"
	b.blockLifecycle = make(chan struct{})
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<2`, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET current_step_index=2,status='PROCESSING' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = s.executeClaimWithJitter(ctx, *c, 50*time.Millisecond, 10, nil)
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("authority call exceeded recovery deadline: err=%v elapsed=%s", err, time.Since(started))
	}
}

func TestSagaVerifiedResolutionContinuesToOriginalS0Integration(t *testing.T) {
	s, r, db, v, b, p, e := storedReplayFixture(t)
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "EXPIRED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "expire")
	p.entry.RefundState = "succeeded"
	p.entry.RefundAmountCents = 725
	if _, err := db.Exec(context.Background(), `UPDATE sagas SET direction='compensate',status='ROLLING_BACK',current_step_index=1 WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatal(err)
	}
	got, err := r.GetSagaById(context.Background(), v.ID)
	if err != nil || got.Status != domain.SAGA_ROLLED_BACK || e.close != 1 {
		t.Fatal("successful S1 compensation paused before S0 close", err)
	}
}
func TestSagaLateCollectionCompensationOnlyReentry(t *testing.T) {
	s, r, db, v, b, p, e := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "EXPIRED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "expire")
	now := time.Now().UTC()
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET compensate_state='completed',status='COMPENSATED',compensated_at=$2 WHERE saga_id=$1`, v.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET direction='compensate',status='ROLLED_BACK',current_step_index=-1 WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	target, ok := any(s).(settlementService)
	if !ok {
		t.Fatal("authoritative settlement absent")
	}
	for i := 0; i < 2; i++ {
		if _, err := target.SettleOrderSaga(ctx, v.BookingID, 1, b.view.DecisionId, p.entry.Id, "late_collection"); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.SAGA_ROLLING_BACK || got.Direction != "compensate" {
		t.Fatal("late collection not reopened for compensation only")
	}
	_ = claimAndRun(t, s, r, v.ID)
	got, err = r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == domain.SAGA_ROLLED_BACK || e.reserve != 0 || e.book != 0 || e.account != 0 || p.resolutions == 0 {
		t.Fatal("pending refund treated as settled")
	}
	p.entry.RefundState = "succeeded"
	p.entry.RefundAmountCents = 725
	if _, err = target.SettleOrderSaga(ctx, v.BookingID, 1, b.view.DecisionId, p.entry.Id, "refund_progress"); err != nil {
		t.Fatal(err)
	}
	if err = claimAndRun(t, s, r, v.ID); err != nil {
		t.Fatal(err)
	}
	got, err = r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_ROLLED_BACK {
		t.Fatal("verified full refund did not settle", err)
	}
	if _, err = target.SettleOrderSaga(ctx, v.BookingID, 1, b.view.DecisionId, p.entry.Id, "duplicate"); err != nil {
		t.Fatal(err)
	}
	got, err = r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_ROLLED_BACK {
		t.Fatal("same obligation reopened twice", err)
	}
}
func TestSagaStartRecoversStoredGraphIntegration(t *testing.T) {
	s, r, _, v, b, p, e := storedReplayFixture(t)
	b.view.Phase = "OPEN"
	p.entry.ProviderOutcome = "unknown"
	req := &StartOrderSagaRequest{BookingId: v.BookingID.String(), SeatIds: []string{v.Input.SeatIDs[0].String()}, UserId: v.Input.UserID.String(), EventID: v.Input.EventID.String(), TotalCents: 725}
	first, err := s.StartOrderSaga(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.StartOrderSaga(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.SagaId != v.ID.String() || again.SagaId != v.ID.String() || e.reserve != 1 {
		t.Fatal("start retry duplicated original lifecycle")
	}
	if first.PaymentClientSecret != "pi_secret_replay" || again.PaymentClientSecret != "pi_secret_replay" || p.creates < 2 {
		t.Fatalf("start retry did not recover the payment secret through the stable create identity: first=%q again=%q creates=%d", first.PaymentClientSecret, again.PaymentClientSecret, p.creates)
	}
	got, err := r.GetSagaById(context.Background(), v.ID)
	if err != nil || got.Status != domain.SAGA_WAIT_FOR_PAYMENT || len(got.Steps) != 5 {
		t.Fatal("start did not retain complete original graph", err)
	}
	req.TotalCents = 726
	if _, err = s.StartOrderSaga(context.Background(), req); err == nil {
		t.Fatal("untrusted amount accepted")
	}
	req.TotalCents = 725
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "EXPIRED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "expire")
	if _, err = s.StartOrderSaga(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if e.reserve != 1 {
		t.Fatal("terminal start revived seats")
	}
}

func TestSagaInvalidGraphFailureBecomesDurableAttentionIntegration(t *testing.T) {
	s, r, db, v, _, _, _ := storedReplayFixture(t)
	ctx := context.Background()
	c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
	if err != nil || c == nil {
		t.Fatal("claim missing", err)
	}
	if _, err := db.Exec(ctx, `UPDATE saga_steps SET name='corrupt_step' WHERE saga_id=$1 AND "order"=0`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ExecuteClaim(ctx, *c); err == nil {
		t.Fatal("invalid graph was treated as successful")
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryState != "attention" || got.ErrorCategory != "invalid_persisted_graph" || got.NextAttemptAt == nil || got.FailureAttempts != 1 {
		t.Fatalf("invalid graph responsibility was not durably classified: state=%q category=%q due=%v failures=%d", got.RecoveryState, got.ErrorCategory, got.NextAttemptAt, got.FailureAttempts)
	}
}

func TestSagaStartRetainsSecretAfterPaymentPollAndActiveClaimIntegration(t *testing.T) {
	s, r, db, v, b, p, e := storedReplayFixture(t)
	ctx := context.Background()
	b.view.Phase = "OPEN"
	p.entry.ProviderOutcome = "unknown"
	req := &StartOrderSagaRequest{BookingId: v.BookingID.String(), UserId: v.Input.UserID.String(), EventID: v.Input.EventID.String(), TotalCents: 725, SeatIds: []string{v.Input.SeatIDs[0].String()}}
	if _, err := s.StartOrderSaga(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RunRecoveryOnce(ctx, 1); err != nil || n != 1 {
		t.Fatal("pending payment poll", n, err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_PROCESSING || got.CurrentStepIndex != 2 || got.ErrorCategory != "financial_wait" {
		t.Fatal("S2 poll not retained", err)
	}
	for _, active := range []bool{false, true} {
		if active {
			if _, err := db.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp() WHERE id=$1`, v.ID); err != nil {
				t.Fatal(err)
			}
			if c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second); err != nil || c == nil {
				t.Fatal("worker claim missing", err)
			}
		}
		response, err := s.StartOrderSaga(ctx, req)
		if err != nil || response.SagaId != v.ID.String() || response.PaymentClientSecret != "pi_secret_replay" {
			t.Fatalf("Start lost original active payment after poll/claim active=%v response=%+v err=%v", active, response, err)
		}
		if p.createReq.BookingId != v.BookingID.String() || p.createReq.UserId != v.Input.UserID.String() || p.createReq.PaymentGeneration != 1 || p.createReq.Price != 725 || p.createReq.Currency != "usd" {
			t.Fatal("secret recovery changed original financial identity")
		}
	}
	b.view.Phase = "TERMINAL"
	b.view.Booking.Status = "EXPIRED"
	b.view.DecisionId = decisionID(v.BookingID.String(), "expire")
	response, err := s.StartOrderSaga(ctx, req)
	if err != nil || response.PaymentClientSecret != "" {
		t.Fatal("terminal Start exposed payment secret", err)
	}
	got, err = r.GetSagaById(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, st := range got.Steps {
		if st.ID != v.Steps[i].ID {
			t.Fatal("poll/claim recovery allocated a new step identity")
		}
	}
	if e.reserve != 1 || e.book != 0 {
		t.Fatal("payment polling repeated seat effects")
	}
}
