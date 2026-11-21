package grpc

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func parseLifecycleRequest(req *eventv1.SeatLifecycleRequest) (domain.SeatLifecycleRequest, error) {
	var result domain.SeatLifecycleRequest
	invalid := status.Error(codes.InvalidArgument, "invalid seat lifecycle request")
	if req == nil || req.ReservationDeadline == nil || req.ReservationDeadline.CheckValid() != nil {
		return result, invalid
	}
	var err error
	if result.BookingID, err = uuid.Parse(req.BookingId); err != nil {
		return result, invalid
	}
	if result.EventID, err = uuid.Parse(req.EventId); err != nil {
		return result, invalid
	}
	if result.OperationID, err = uuid.Parse(req.OperationId); err != nil {
		return result, invalid
	}
	hash, err := hex.DecodeString(req.SeatSetHash)
	if err != nil || len(hash) != 32 {
		return result, invalid
	}
	copy(result.Fingerprint[:], hash)
	result.Generation, result.Action = req.Generation, req.Action
	result.ReservationDeadline = req.ReservationDeadline.AsTime()
	for _, s := range req.SeatIds {
		id, err := uuid.Parse(s)
		if err != nil {
			return result, invalid
		}
		result.SeatIDs = append(result.SeatIDs, id)
	}
	result.TierQuantities = make(map[uuid.UUID]int32, len(req.Tiers))
	for _, tier := range req.Tiers {
		if tier == nil {
			return result, invalid
		}
		id, err := uuid.Parse(tier.TierId)
		if err != nil || id == uuid.Nil || tier.Quantity <= 0 {
			return result, invalid
		}
		if _, exists := result.TierQuantities[id]; exists {
			return result, invalid
		}
		result.TierQuantities[id] = tier.Quantity
	}
	return result, nil
}

func lifecycleRPCError(err error) error {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, "seat lifecycle not found")
	case errors.Is(err, repository.ErrSaleNotAllowed), errors.Is(err, repository.ErrSeatNotAvailable), errors.Is(err, repository.ErrLifecycleClosed), errors.Is(err, repository.ErrLifecycleConflict), errors.Is(err, repository.ErrManagedSeat), errors.Is(err, repository.ErrAccountingBaseline):
		return status.Error(codes.FailedPrecondition, "seat lifecycle rejected")
	default:
		return status.Error(codes.Internal, "seat lifecycle outcome unavailable; replay original identity")
	}
}

func (s *EventServer) ApplySaleAccounting(ctx context.Context, req *eventv1.SeatLifecycleRequest) (*eventv1.SeatLifecycleResponse, error) {
	command, err := parseLifecycleRequest(req)
	if err != nil {
		return nil, err
	}
	result, err := s.service.ApplySaleAccounting(ctx, command)
	if err != nil {
		return nil, lifecycleRPCError(err)
	}
	return lifecycleResponse(result), nil
}

func (s *EventServer) ApplySeatLifecycle(ctx context.Context, req *eventv1.SeatLifecycleRequest) (*eventv1.SeatLifecycleResponse, error) {
	command, err := parseLifecycleRequest(req)
	if err != nil {
		return nil, err
	}
	result, err := s.service.ApplyLifecycle(ctx, command)
	if err != nil {
		return nil, lifecycleRPCError(err)
	}
	return lifecycleResponse(result), nil
}

func lifecycleResponse(result *domain.SeatLifecycleResult) *eventv1.SeatLifecycleResponse {
	response := &eventv1.SeatLifecycleResponse{State: result.State, AccountingState: result.AccountingState, Applied: result.Applied}
	if result.ReservationDeadline != nil {
		response.ReservationDeadline = timestamppb.New(*result.ReservationDeadline)
	}
	return response
}
