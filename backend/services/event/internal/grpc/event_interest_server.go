package grpc

import (
	"context"
	"errors"
	"os"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/repository"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *EventServer) ListMyEventInterests(ctx context.Context, req *eventv1.ListMyEventInterestsRequest) (*eventv1.ListMyEventInterestsResponse, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "list request required")
	}
	userID, err := parseInterestUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid user ID required")
	}
	if req.Limit < 1 || req.Limit > 100 || req.Offset < 0 || req.Offset > 100000 {
		return nil, status.Error(codes.InvalidArgument, "invalid page bounds")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	page, err := s.interestService.List(ctx, userID, req.Kind, req.Limit, req.Offset)
	if err != nil {
		return nil, interestStatus(err)
	}
	response := &eventv1.ListMyEventInterestsResponse{Limit: req.Limit, Offset: req.Offset, HasMore: page.HasMore, Items: make([]*eventv1.EventInterest, 0, len(page.Items))}
	for _, item := range page.Items {
		response.Items = append(response.Items, toProtoEventInterest(item))
	}
	return response, nil
}

func (s *EventServer) GetMyEventInterests(ctx context.Context, req *eventv1.GetMyEventInterestsRequest) (*eventv1.GetMyEventInterestsResponse, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "interest request required")
	}
	userID, err := parseInterestUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid user ID required")
	}
	eventID, err := parseInterestUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid event ID required")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	state, err := s.interestService.Get(ctx, userID, eventID)
	if err != nil {
		return nil, interestStatus(err)
	}
	response := &eventv1.GetMyEventInterestsResponse{EventId: eventID.String(), Favorite: state.Favorite, SaleStatus: state.SaleStatus, OnsaleAvailable: state.OnSaleAvailable, RestockStatus: state.RestockStatus, Subscriptions: make([]*eventv1.EventInterest, 0, len(state.Subscriptions))}
	for _, item := range state.Subscriptions {
		response.Subscriptions = append(response.Subscriptions, toProtoEventInterest(item))
	}
	return response, nil
}

func (s *EventServer) UpsertMyEventInterest(ctx context.Context, req *eventv1.UpsertMyEventInterestRequest) (*eventv1.EventInterest, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "interest request required")
	}
	userID, err := parseInterestUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid user ID required")
	}
	eventID, err := parseInterestUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid event ID required")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	item, err := s.interestService.Upsert(ctx, userID, eventID, req.Kind, req.EmailOptIn, req.EmailConsentVersion)
	if err != nil {
		return nil, interestStatus(err)
	}
	return toProtoEventInterest(item), nil
}

func (s *EventServer) WithdrawMyEventInterest(ctx context.Context, req *eventv1.WithdrawMyEventInterestRequest) (*eventv1.EventInterest, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "interest request required")
	}
	userID, err := parseInterestUUID(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid user ID required")
	}
	eventID, err := parseInterestUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid event ID required")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	item, err := s.interestService.Withdraw(ctx, userID, eventID, req.Kind)
	if err != nil {
		return nil, interestStatus(err)
	}
	return toProtoEventInterest(item), nil
}

func (s *EventServer) DisableMyEventAlertEmails(ctx context.Context, req *eventv1.DisableMyEventAlertEmailsRequest) (*eventv1.DisableMyEventAlertEmailsResponse, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "category email consent request required")
	}
	userID, err := parseInterestUUID(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid user ID required")
	}
	if req.GetKind() != repository.InterestOnSale && req.GetKind() != repository.InterestRestock {
		return nil, status.Error(codes.InvalidArgument, "valid event alert category required")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	updated, err := s.interestService.DisableAlertEmails(ctx, userID, req.GetKind())
	if err != nil {
		return nil, interestStatus(err)
	}
	return &eventv1.DisableMyEventAlertEmailsResponse{Kind: req.GetKind(), Updated: updated}, nil
}

func (s *EventServer) ReadEventInterestCounts(ctx context.Context, req *eventv1.ReadEventInterestCountsRequest) (*eventv1.EventInterestCounts, error) {
	if err := requireGatewayInterestCaller(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "event ID required")
	}
	eventID, err := parseInterestUUID(req.EventId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid event ID required")
	}
	if s.interestService == nil {
		return nil, status.Error(codes.Unavailable, "event interest service unavailable")
	}
	counts, err := s.interestService.Counts(ctx, eventID)
	if err != nil {
		return nil, interestStatus(err)
	}
	return &eventv1.EventInterestCounts{Favorites: counts.Favorites, Onsale: counts.OnSale, Restock: counts.Restock, ObservedAt: timestamppb.New(counts.ObservedAt)}, nil
}

func parseInterestUUID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, repository.ErrInvalidInterest
	}
	return id, nil
}

func toProtoEventInterest(item repository.EventInterest) *eventv1.EventInterest {
	response := &eventv1.EventInterest{Id: item.ID.String(), UserId: item.UserID.String(), EventId: item.EventID.String(), Kind: item.Kind, Status: item.Status,
		EmailOptIn: item.EmailOptIn, EmailConsentVersion: item.EmailConsentVersion, Version: item.Version,
		CreatedAt: timestamppb.New(item.CreatedAt), UpdatedAt: timestamppb.New(item.UpdatedAt)}
	if item.EmailConsentedAt != nil {
		response.EmailConsentedAt = timestamppb.New(*item.EmailConsentedAt)
	}
	if item.EmailRevokedAt != nil {
		response.EmailRevokedAt = timestamppb.New(*item.EmailRevokedAt)
	}
	return response
}

func interestStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repository.ErrInvalidInterest):
		return status.Error(codes.InvalidArgument, "invalid event interest")
	case errors.Is(err, repository.ErrInterestNotFound), errors.Is(err, repository.ErrNotFound):
		return status.Error(codes.NotFound, "event interest not found")
	case errors.Is(err, repository.ErrInterestAlreadyOnSale):
		return status.Error(codes.AlreadyExists, "event is already on sale")
	case errors.Is(err, repository.ErrInterestNotAvailable):
		return status.Error(codes.FailedPrecondition, "event interest is unavailable")
	default:
		return status.Error(codes.Unavailable, "event interest service unavailable")
	}
}

func requireGatewayInterestCaller(ctx context.Context) error {
	switch os.Getenv("RUSHGO_ENV") {
	case "", "local", "isolated_demo":
		// Local deployments intentionally use plaintext internal RPC. External
		// environments must pass the verified mTLS identity check below.
	case "stripe_test", "preproduction", "production":
		remote, ok := peer.FromContext(ctx)
		if !ok || remote == nil {
			return status.Error(codes.Unauthenticated, "verified Gateway identity required")
		}
		var info credentials.TLSInfo
		switch auth := remote.AuthInfo.(type) {
		case credentials.TLSInfo:
			info = auth
		case *credentials.TLSInfo:
			if auth == nil {
				return status.Error(codes.Unauthenticated, "verified Gateway identity required")
			}
			info = *auth
		default:
			return status.Error(codes.Unauthenticated, "verified Gateway identity required")
		}
		if len(info.State.PeerCertificates) == 0 || len(info.State.VerifiedChains) == 0 {
			return status.Error(codes.Unauthenticated, "verified Gateway identity required")
		}
		for _, identity := range info.State.PeerCertificates[0].URIs {
			if identity != nil && identity.String() == "spiffe://rushgo.internal/gateway" {
				return nil
			}
		}
		return status.Error(codes.PermissionDenied, "Gateway identity required")
	default:
		return status.Error(codes.Unauthenticated, "verified Gateway identity required")
	}
	return nil
}
