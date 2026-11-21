package grpc

import (
	"context"
	"strings"

	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func requireVerifiedGatewayPeer(ctx context.Context) error {
	remotePeer, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "verified Gateway identity required")
	}
	var tlsInfo credentials.TLSInfo
	switch authInfo := remotePeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = authInfo
	case *credentials.TLSInfo:
		if authInfo == nil {
			return status.Error(codes.Unauthenticated, "verified Gateway identity required")
		}
		tlsInfo = *authInfo
	default:
		return status.Error(codes.Unauthenticated, "verified Gateway identity required")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 || len(tlsInfo.State.VerifiedChains) == 0 {
		return status.Error(codes.Unauthenticated, "verified Gateway identity required")
	}
	identity := ""
	for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
		if uri == nil || !strings.HasPrefix(uri.String(), "spiffe://rushgo.internal/") {
			continue
		}
		candidate := uri.String()
		if identity != "" && identity != candidate {
			return status.Error(codes.PermissionDenied, "Booking RPC caller identity is not authorized")
		}
		identity = candidate
	}
	if identity != grpcsecurity.ServiceIdentityGateway {
		return status.Error(codes.PermissionDenied, "Booking RPC caller identity is not authorized")
	}
	return nil
}

func (s *BookingServer) ListAdminOutboxEvents(ctx context.Context, req *bookingv1.ListAdminOutboxEventsRequest) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	if s == nil || !s.adminOutboxSecure {
		return nil, status.Error(codes.Unavailable, "Admin outbox source unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticateAdminOutboxPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Role != "admin" {
		return nil, status.Error(codes.PermissionDenied, "Admin outbox authorization denied")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Admin outbox query")
	}
	if s.adminOutbox == nil {
		return nil, status.Error(codes.Unavailable, "Admin outbox source unavailable")
	}
	page, err := s.adminOutbox.List(ctx, service.AdminOutboxQuery{
		Source:    req.GetSource(),
		Status:    req.GetStatus(),
		EventType: req.GetEventType(),
		From:      req.GetFrom(),
		To:        req.GetTo(),
		Limit:     req.GetLimit(),
		Cursor:    req.GetCursor(),
	})
	if err != nil {
		switch {
		case err == service.ErrAdminOutboxInvalidQuery:
			return nil, status.Error(codes.InvalidArgument, "invalid Admin outbox query")
		case err == service.ErrAdminOutboxCapacityExceeded:
			return nil, status.Error(codes.ResourceExhausted, "Admin outbox source capacity exceeded")
		default:
			return nil, status.Error(codes.Unavailable, "Admin outbox source unavailable")
		}
	}
	response := &bookingv1.ListAdminOutboxEventsResponse{
		Records:             make([]*bookingv1.AdminOutboxEventRecord, 0, len(page.Records)),
		NextCursor:          page.NextCursor,
		HasMore:             page.HasMore,
		ObservedAt:          timestamppb.New(page.ObservedAt),
		From:                timestamppb.New(page.From),
		To:                  timestamppb.New(page.To),
		SupportedEventTypes: append([]string(nil), page.SupportedEventTypes...),
		SourceRecordLimit:   int32(page.SourceRecordLimit),
		ConsumerOutcome:     page.ConsumerOutcome,
	}
	for _, record := range page.Records {
		response.Records = append(response.Records, &bookingv1.AdminOutboxEventRecord{
			EventId:         record.EventID,
			SourceService:   record.SourceService,
			EventType:       record.EventType,
			CreatedAt:       timestamppb.New(record.CreatedAt),
			RecordedStatus:  record.RecordedStatus,
			ConsumerOutcome: record.ConsumerOutcome,
		})
	}
	return response, nil
}
