package grpc

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (s *BookingServer) managementAdmin(ctx context.Context) (uuid.UUID, error) {
	if s == nil || !s.adminBookingDetailSecure {
		return uuid.Nil, status.Error(codes.Unavailable, "management unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return uuid.Nil, err
	}
	p, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	if p.Role != "admin" {
		return uuid.Nil, status.Error(codes.PermissionDenied, "admin required")
	}
	return p.UserID, nil
}
func featureProto(v domain.FeatureFlag) *bookingv1.FeatureFlag {
	r := &bookingv1.FeatureFlag{Key: v.Key, Description: v.Description, DefaultEnabled: v.Default, Enabled: v.Enabled, Version: v.Version}
	if v.UpdatedAt != nil {
		r.UpdatedAt = timestamppb.New(*v.UpdatedAt)
	}
	if v.ActorID != nil {
		r.ActorId = v.ActorID.String()
	}
	return r
}
func (s *BookingServer) GetPublicFeatures(ctx context.Context, _ *emptypb.Empty) (*bookingv1.PublicFeatures, error) {
	if s == nil || !s.adminBookingDetailSecure {
		return nil, status.Error(codes.Unavailable, "features unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	flags, _, err := s.service.ListFeatureFlags(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "features unavailable")
	}
	result := &bookingv1.PublicFeatures{}
	for _, f := range flags {
		result.Flags = append(result.Flags, &bookingv1.PublicFeature{Key: f.Key, Enabled: f.Enabled})
	}
	return result, nil
}
func (s *BookingServer) ListFeatureFlags(ctx context.Context, _ *emptypb.Empty) (*bookingv1.FeatureFlags, error) {
	if _, err := s.managementAdmin(ctx); err != nil {
		return nil, err
	}
	flags, audit, err := s.service.ListFeatureFlags(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "features unavailable")
	}
	r := &bookingv1.FeatureFlags{ObservedAt: timestamppb.Now()}
	for _, f := range flags {
		r.Flags = append(r.Flags, featureProto(f))
	}
	for _, a := range audit {
		r.Audit = append(r.Audit, &bookingv1.FeatureChange{Key: a.Key, Before: a.Before, After: a.After, Version: a.Version, ActorId: a.ActorID.String(), ChangedAt: timestamppb.New(a.ChangedAt)})
	}
	return r, nil
}
func (s *BookingServer) ChangeFeatureFlag(ctx context.Context, req *bookingv1.ChangeFeatureFlagRequest) (*bookingv1.FeatureFlag, error) {
	actor, err := s.managementAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || req.ExpectedVersion < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid feature change")
	}
	v, err := s.service.ChangeFeatureFlag(ctx, req.Key, req.Enabled, req.ExpectedVersion, actor)
	if errors.Is(err, domain.ErrUnknownFeature) || errors.Is(err, domain.ErrManagementBounds) {
		return nil, status.Error(codes.InvalidArgument, "unknown feature or invalid version")
	}
	if errors.Is(err, domain.ErrFeatureConflict) {
		return nil, status.Error(codes.Aborted, "feature changed; reload its current version")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "feature update unavailable; reload before retry")
	}
	return featureProto(*v), nil
}
func (s *BookingServer) GetOperationsMetrics(ctx context.Context, req *bookingv1.OperationsQuery) (*bookingv1.OperationsMetrics, error) {
	if _, err := s.managementAdmin(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.From == nil || req.To == nil || !req.From.IsValid() || !req.To.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "bounded dates required")
	}
	from, to := req.From.AsTime(), req.To.AsTime()
	if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		return nil, status.Error(codes.InvalidArgument, "invalid metrics window")
	}
	v, err := s.service.OperationsMetrics(ctx, from, to)
	if errors.Is(err, domain.ErrManagementBounds) {
		return nil, status.Error(codes.ResourceExhausted, "narrow the date range")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "booking metrics unavailable")
	}
	return v, nil
}
