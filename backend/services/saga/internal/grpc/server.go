package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"github.com/ticketbox/saga/internal/repository"
	"github.com/ticketbox/saga/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SagaOrchestratorServer struct {
	sagav1.UnimplementedSagaOrchestratorServiceServer
	service *service.SagaService
	logger  *zap.Logger
}

func NewSagaOrchestratorServer(service *service.SagaService, logger *zap.Logger) *SagaOrchestratorServer {
	return &SagaOrchestratorServer{
		service: service,
		logger:  logger,
	}
}

func (s *SagaOrchestratorServer) StartOrderSaga(ctx context.Context, req *sagav1.StartOrderSagaRequest) (*sagav1.OrderSagaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Saga input required")
	}
	var deadline *time.Time
	if req.ReservationDeadline != nil {
		if req.ReservationDeadline.CheckValid() != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid deadline")
		}
		v := req.ReservationDeadline.AsTime()
		deadline = &v
	}
	res, err := s.service.StartOrderSaga(ctx, &service.StartOrderSagaRequest{
		BookingId:           req.BookingId,
		SeatIds:             req.SeatIds,
		UserId:              req.UserId,
		TotalCents:          int(req.TotalCents),
		EventID:             req.EventId,
		LifecycleGeneration: req.LifecycleGeneration, BookingVersion: req.BookingVersion, ReservationDeadline: deadline, BookingMode: req.BookingMode, Currency: req.Currency,
	})
	if err != nil {
		return nil, sagaRPCError(err)
	}

	return &sagav1.OrderSagaResponse{
		Status:                    res.Status,
		Message:                   res.Message,
		PaymentIntentClientSecret: res.PaymentClientSecret,
		SagaId:                    res.SagaId,
	}, nil
}

func (s *SagaOrchestratorServer) CompensateOrderSaga(ctx context.Context, req *sagav1.CompensateOrderSagaRequest) (*sagav1.OrderSagaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Saga input required")
	}
	id, err := uuid.Parse(req.SagaId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Saga ID")
	}
	res, err := s.service.CompensateOrderSaga(ctx, id)
	if err != nil {
		return nil, sagaRPCError(err)
	}
	return orderResponse(res), nil
}

func (s *SagaOrchestratorServer) ExpireOrderSaga(ctx context.Context, req *sagav1.ExpireOrderSagaRequest) (*sagav1.ExpireOrderSagaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Saga input required")
	}
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, fmt.Errorf("invalid booking id: %w", err)
	}
	expired, sagaStatus, err := s.service.ExpireOrderSaga(ctx, bookingID)
	if err != nil {
		return nil, err
	}
	return &sagav1.ExpireOrderSagaResponse{
		Expired:    expired,
		SagaStatus: sagaStatus,
	}, nil
}

func (s *SagaOrchestratorServer) SettleOrderSaga(ctx context.Context, req *sagav1.SettleOrderSagaRequest) (*sagav1.OrderSagaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Saga input required")
	}
	id, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Booking ID")
	}
	res, err := s.service.SettleOrderSaga(ctx, id, req.Generation, req.DecisionId, req.PaymentId, req.Reason)
	if err != nil {
		return nil, sagaRPCError(err)
	}
	return orderResponse(res), nil
}

func (s *SagaOrchestratorServer) VerifyTicketIssuanceSaga(ctx context.Context, req *sagav1.VerifyTicketIssuanceSagaRequest) (*sagav1.VerifyTicketIssuanceSagaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Saga identity required")
	}
	parse := func(raw string) (uuid.UUID, error) {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil {
			return uuid.Nil, status.Error(codes.InvalidArgument, "invalid ticket issuance identity")
		}
		return id, nil
	}
	sagaID, err := parse(req.SagaId)
	if err != nil {
		return nil, err
	}
	bookingID, err := parse(req.BookingId)
	if err != nil {
		return nil, err
	}
	paymentID, err := parse(req.PaymentId)
	if err != nil {
		return nil, err
	}
	eventID, err := parse(req.EventId)
	if err != nil {
		return nil, err
	}
	if req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid lifecycle generation")
	}
	eligible, reason, err := s.service.VerifyTicketIssuanceSaga(ctx, sagaID, bookingID, paymentID, eventID, req.Generation)
	if err != nil {
		return nil, sagaRPCError(err)
	}
	state := sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_DENIED
	if eligible {
		state = sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_ELIGIBLE
	}
	return &sagav1.VerifyTicketIssuanceSagaResponse{State: state, ReasonCode: reason}, nil
}
func orderResponse(res *service.StartOrderSagaResponse) *sagav1.OrderSagaResponse {
	return &sagav1.OrderSagaResponse{Status: res.Status, Message: res.Message, SagaId: res.SagaId, PaymentIntentClientSecret: res.PaymentClientSecret}
}
func sagaRPCError(err error) error {
	switch {
	case errors.Is(err, repository.ErrInvalidRecoveryInput):
		return status.Error(codes.InvalidArgument, "invalid Saga input")
	case errors.Is(err, repository.ErrRecoveryConflict), errors.Is(err, repository.ErrRecoveryAttention):
		return status.Error(codes.FailedPrecondition, "Saga lifecycle requires recovery")
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, "Saga not found")
	default:
		return status.Error(codes.Unavailable, "Saga recovery unavailable; retry original identity")
	}
}
