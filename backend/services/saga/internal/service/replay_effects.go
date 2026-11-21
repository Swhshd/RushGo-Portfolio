package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagapkg "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errTerminal = errors.New("Booking lifecycle cannot confirm")
var errFinancialWait = errors.New("financial responsibility pending")

func bookingDecisionID(booking uuid.UUID, kind string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+booking.String()+":g1:"+kind)).String()
}

func (s *SagaService) bookingAuthority(ctx context.Context, in domain.SagaInput) (*bookingv1.LifecycleDisposition, error) {
	view, err := s.bookingClient.GetLifecycleDisposition(ctx, &bookingv1.LifecycleRequest{BookingId: in.BookingID.String(), Generation: in.Generation})
	if err != nil {
		return nil, err
	}
	if view == nil || view.Booking == nil || view.Generation != 1 || view.Booking.Id != in.BookingID.String() || view.Booking.UserId != in.UserID.String() || view.Booking.EventId != in.EventID.String() || view.Booking.TotalAmountCents != in.AmountCents || view.Currency != in.Currency || view.BookingMode != in.BookingMode || view.ReservationDeadline == nil || view.ReservationDeadline.CheckValid() != nil || !view.ReservationDeadline.AsTime().Equal(in.ReservationDeadline) {
		return nil, repository.ErrRecoveryConflict
	}
	var seats []string
	for _, item := range view.Booking.Items {
		if item == nil || item.Quantity < 1 || int(item.Quantity) != len(item.SeatIds) {
			return nil, repository.ErrRecoveryConflict
		}
		seats = append(seats, item.SeatIds...)
	}
	sort.Strings(seats)
	if len(seats) != len(in.SeatIDs) {
		return nil, repository.ErrRecoveryConflict
	}
	for i, id := range in.SeatIDs {
		if seats[i] != id.String() {
			return nil, repository.ErrRecoveryConflict
		}
	}
	switch view.Phase {
	case "OPEN", "CONFIRMING":
		if view.Booking.Status != "PENDING" {
			return nil, repository.ErrRecoveryAttention
		}
	case "TERMINAL":
		switch view.Booking.Status {
		case "CONFIRMED", "FAILED", "EXPIRED", "CANCELLED":
		default:
			return nil, repository.ErrRecoveryAttention
		}
	default:
		return nil, repository.ErrRecoveryAttention
	}
	return view, nil
}

func (s *SagaService) financialAuthority(ctx context.Context, in domain.SagaInput) (*paymentv1.PaymentEntry, error) {
	p, err := s.paymentClient.GetFinancialPayment(ctx, &paymentv1.FinancialPaymentRequest{BookingId: in.BookingID.String(), Generation: in.Generation})
	if err != nil {
		return nil, err
	}
	if p == nil || p.BookingId != in.BookingID.String() || p.UserId != in.UserID.String() || p.PaymentGeneration != 1 || int64(p.Price) != in.AmountCents || p.Currency != in.Currency {
		return nil, repository.ErrRecoveryConflict
	}
	id, err := uuid.Parse(p.Id)
	if err != nil || id == uuid.Nil {
		return nil, repository.ErrRecoveryConflict
	}
	return p, nil
}

func (s *SagaService) bindCollectedPayment(ctx context.Context, v *domain.Saga, payment *paymentv1.PaymentEntry) error {
	if v == nil || payment == nil {
		return repository.ErrRecoveryAttention
	}
	id, err := uuid.Parse(payment.Id)
	if err != nil || id == uuid.Nil {
		return repository.ErrRecoveryConflict
	}
	if v.PaymentID == id {
		return nil
	}
	if v.PaymentID != uuid.Nil {
		return repository.ErrRecoveryConflict
	}
	r, err := s.replayRepository()
	if err != nil {
		return err
	}
	return r.WakeSaga(ctx, v.BookingID, v.Generation, &id, "s2_collected_read")
}

// Every CAS response, including a lost commit acknowledgement, is resolved by a
// fresh authoritative read. No generic status setter can choose a winner here.
func (s *SagaService) decide(ctx context.Context, in domain.SagaInput, view *bookingv1.LifecycleDisposition, kind string) (*bookingv1.LifecycleDisposition, error) {
	id := bookingDecisionID(in.BookingID, kind)
	_, callErr := s.bookingClient.DecideLifecycle(ctx, &bookingv1.LifecycleDecisionRequest{BookingId: in.BookingID.String(), Generation: in.Generation, DecisionId: id, Kind: kind, Reason: "saga_" + kind, ExpectedVersion: view.Version, ConfirmationDecisionId: view.ConfirmationDecisionId})
	current, err := s.bookingAuthority(ctx, in)
	if err != nil {
		return nil, err
	}
	if current.Booking.Status == "CONFIRMED" {
		return current, nil
	}
	if current.DecisionId == id || kind == "claim_confirmation" && current.ConfirmationDecisionId == id && current.Phase == "CONFIRMING" {
		return current, nil
	}
	if current.Phase == "TERMINAL" {
		return current, errTerminal
	}
	if callErr != nil {
		return current, callErr
	}
	return current, errFinancialWait
}

func seatRequest(in domain.SagaInput, id uuid.UUID, action string) *eventv1.SeatLifecycleRequest {
	seats := make([]string, len(in.SeatIDs))
	for i, seat := range in.SeatIDs {
		seats[i] = seat.String()
	}
	hash := sha256.Sum256([]byte("v1\n" + in.EventID.String() + "\n" + strings.Join(seats, ",") + "\n"))
	return &eventv1.SeatLifecycleRequest{BookingId: in.BookingID.String(), EventId: in.EventID.String(), Generation: in.Generation, SeatIds: seats, SeatSetHash: hex.EncodeToString(hash[:]), OperationId: id.String(), Action: action, ReservationDeadline: timestamppb.New(in.ReservationDeadline)}
}
func compensationID(id uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:saga-step:"+id.String()+":compensate"))
}

func (s *SagaService) executeStoredStep(ctx context.Context, v *domain.Saga, index int, h *registry.SagaHandler) error {
	in := *v.Input
	view, err := s.bookingAuthority(ctx, in)
	if err != nil {
		return err
	}
	st := v.Steps[index]
	if view.Phase == "TERMINAL" && view.Booking.Status != "CONFIRMED" {
		return errTerminal
	}
	switch index {
	case 0:
		if view.Phase != "OPEN" {
			return repository.ErrRecoveryConflict
		}
		if !time.Now().Before(in.ReservationDeadline) {
			return errTerminal
		}
		result, err := s.eventClient.ApplySeatLifecycle(ctx, seatRequest(in, st.ID, "reserve"))
		if err != nil {
			return err
		}
		if result == nil || result.State != "held" {
			return errFinancialWait
		}
		return nil
	case 1:
		res, callErr := s.paymentClient.CreatePayment(ctx, &paymentv1.CreatePaymentRequest{BookingId: in.BookingID.String(), UserId: in.UserID.String(), PaymentGeneration: in.Generation, Price: int32(in.AmountCents), Currency: in.Currency})
		if callErr == nil && res != nil && res.CreateState == "create_ready" {
			h.SetPaymentResponse(res)
			return nil
		}
		// Payment owns the create identity/key. A timeout is not proof of failure and
		// may hide a successfully persisted original intent or collected outcome.
		p, readErr := s.financialAuthority(ctx, in)
		if readErr == nil && p.ProviderOutcome == "collected" {
			return nil
		}
		if readErr != nil && status.Code(readErr) != codes.NotFound {
			return readErr
		}
		if callErr != nil {
			return callErr
		}
		return errFinancialWait
	case 2:
		p, err := s.financialAuthority(ctx, in)
		if err != nil {
			return err
		}
		if p.ProviderOutcome == "not_collected" && (p.ProviderStatus == "canceled" || p.ProviderStatus == "requires_payment_method") {
			return errTerminal
		}
		if p.ProviderOutcome != "collected" {
			return errFinancialWait
		}
		if err := s.bindCollectedPayment(ctx, v, p); err != nil {
			return err
		}
		if view.Phase == "OPEN" {
			view, err = s.decide(ctx, in, view, "claim_confirmation")
			if err != nil {
				return err
			}
		}
		if view.Phase != "CONFIRMING" && view.Booking.Status != "CONFIRMED" {
			return errTerminal
		}
		res, err := s.eventClient.ApplySeatLifecycle(ctx, seatRequest(in, st.ID, "book"))
		if err != nil {
			return err
		}
		if res == nil || res.State != "booked" {
			return errFinancialWait
		}
		return nil
	case 3:
		if view.Booking.Status == "CONFIRMED" {
			return nil
		}
		if view.Phase != "CONFIRMING" {
			return errTerminal
		}
		view, err = s.decide(ctx, in, view, "confirm")
		if err != nil {
			return err
		}
		if view.Booking.Status != "CONFIRMED" {
			return errTerminal
		}
		return nil
	case 4:
		if view.Booking.Status != "CONFIRMED" {
			return errTerminal
		}
		req := seatRequest(in, st.ID, "account")
		for _, item := range view.Booking.Items {
			req.Tiers = append(req.Tiers, &eventv1.LifecycleTierQuantity{TierId: item.TicketTierId, Quantity: item.Quantity})
		}
		res, err := s.eventClient.ApplySaleAccounting(ctx, req)
		if err != nil {
			return err
		}
		if res == nil || res.State != "booked" || res.AccountingState != "applied" {
			return errFinancialWait
		}
		return nil
	}
	return repository.ErrRecoveryAttention
}

func (s *SagaService) compensateStoredStep(ctx context.Context, v *domain.Saga, index int) error {
	in := *v.Input
	view, err := s.bookingAuthority(ctx, in)
	if err != nil {
		return err
	}
	if view.Booking.Status == "CONFIRMED" {
		return repository.ErrRecoveryConflict
	}
	if view.Phase != "TERMINAL" {
		return errFinancialWait
	}
	switch index {
	case 0, 2:
		res, err := s.eventClient.ApplySeatLifecycle(ctx, seatRequest(in, compensationID(v.Steps[index].ID), "close"))
		if err != nil {
			return err
		}
		if res == nil || res.State != "closed" {
			return errFinancialWait
		}
	case 1:
		p, err := s.financialAuthority(ctx, in)
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		decision, err := uuid.Parse(view.DecisionId)
		if err != nil || decision == uuid.Nil {
			return repository.ErrRecoveryConflict
		}
		if _, err = s.paymentClient.RequestPaymentResolution(ctx, &paymentv1.PaymentResolutionRequest{PaymentId: p.Id, BookingId: in.BookingID.String(), Generation: in.Generation, DecisionId: decision.String(), Reason: view.Booking.Status}); err != nil {
			return err
		}
		p, err = s.financialAuthority(ctx, in)
		if err != nil {
			return err
		}
		if p.ProviderOutcome == "collected" {
			if p.RefundState != "succeeded" || p.RefundAmountCents != in.AmountCents {
				return errFinancialWait
			}
		} else if p.ProviderOutcome != "not_collected" || p.ReconciliationState != "resolved" {
			return errFinancialWait
		}
	case 3, 4:
		// The terminal Booking CAS owns abort; Event close owns debit reversal.
	default:
		return repository.ErrRecoveryAttention
	}
	return nil
}

func (s *SagaService) reBuildSagaHandler(_ context.Context, v *domain.Saga, payment *paymentv1.PaymentEntry) (*registry.SagaHandler, error) {
	if v == nil || !v.RecoveryManaged || v.Input == nil || len(v.Steps) != 5 {
		return nil, repository.ErrRecoveryAttention
	}
	in, _, err := domain.CanonicalInput(*v.Input)
	if err != nil || in.Fingerprint != v.Input.Fingerprint || v.BookingID != in.BookingID {
		return nil, repository.ErrRecoveryConflict
	}
	names := []sagapkg.SagaStepName{sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED, sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER}
	ids := map[uuid.UUID]bool{}
	for i, st := range v.Steps {
		if st.ID == uuid.Nil || ids[st.ID] || st.SagaID != v.ID || st.Order != int16(i) || st.Name != string(names[i]) || st.ShouldPauseForPayment != (i == 1) {
			return nil, repository.ErrRecoveryAttention
		}
		ids[st.ID] = true
	}
	if payment != nil && (payment.BookingId != v.BookingID.String() || payment.UserId != in.UserID.String() || payment.PaymentGeneration != 1) {
		return nil, repository.ErrRecoveryConflict
	}
	h := registry.NewSagaHandler(v, s.repo, s.logger)
	for i := range v.Steps {
		index := i
		v.Steps[i].Execute = func(ctx context.Context) error { return s.executeStoredStep(ctx, v, index, h) }
		v.Steps[i].Compensate = func(ctx context.Context) error { return s.compensateStoredStep(ctx, v, index) }
	}
	return h, nil
}
