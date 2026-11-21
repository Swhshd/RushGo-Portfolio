package grpc

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxAdminOutboxBearerTokenBytes = 8192

type TokenRevocationChecker interface {
	IsRevoked(context.Context, string) (bool, error)
}

type AdminOutboxServer struct {
	notificationv1.UnimplementedNotificationAdminServiceServer
	outbox      *service.AdminOutboxService
	users       userv1.UserServiceClient
	revocations TokenRevocationChecker
	secure      bool
	inbox       *service.NotificationInboxService
}

func NewAdminOutboxServer(outbox *service.AdminOutboxService, users userv1.UserServiceClient, revocations TokenRevocationChecker, secure bool, inbox ...*service.NotificationInboxService) *AdminOutboxServer {
	server := &AdminOutboxServer{outbox: outbox, users: users, revocations: revocations, secure: secure}
	if len(inbox) > 0 {
		server.inbox = inbox[0]
	}
	return server
}

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
			return status.Error(codes.PermissionDenied, "Notification RPC caller identity is not authorized")
		}
		identity = candidate
	}
	if identity != grpcsecurity.ServiceIdentityGateway {
		return status.Error(codes.PermissionDenied, "Notification RPC caller identity is not authorized")
	}
	return nil
}

func (s *AdminOutboxServer) ListAdminOutboxEvents(ctx context.Context, req *bookingv1.ListAdminOutboxEventsRequest) (*bookingv1.ListAdminOutboxEventsResponse, error) {
	if s == nil || !s.secure {
		return nil, status.Error(codes.Unavailable, "Notification admin outbox source unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	if err := s.authenticateAdmin(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.GetSource() != service.AdminOutboxSource {
		return nil, status.Error(codes.InvalidArgument, "invalid Notification admin outbox query")
	}
	if s.outbox == nil {
		return nil, status.Error(codes.Unavailable, "Notification admin outbox source unavailable")
	}
	page, err := s.outbox.List(ctx, service.AdminOutboxQuery{
		Source: req.GetSource(), Status: req.GetStatus(), EventType: req.GetEventType(),
		From: req.GetFrom(), To: req.GetTo(), Limit: req.GetLimit(), Cursor: req.GetCursor(),
	})
	if err != nil {
		switch {
		case err == service.ErrAdminOutboxInvalidQuery:
			return nil, status.Error(codes.InvalidArgument, "invalid Notification admin outbox query")
		case err == service.ErrAdminOutboxCapacityExceeded:
			return nil, status.Error(codes.ResourceExhausted, "Notification admin outbox source capacity exceeded")
		default:
			return nil, status.Error(codes.Unavailable, "Notification admin outbox source unavailable")
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
			EventId: record.EventID, SourceService: record.SourceService, EventType: record.EventType,
			CreatedAt: timestamppb.New(record.CreatedAt), RecordedStatus: record.RecordedStatus,
			ConsumerOutcome: record.ConsumerOutcome,
		})
	}
	return response, nil
}

func (s *AdminOutboxServer) ListMyNotifications(ctx context.Context, req *notificationv1.ListMyNotificationsRequest) (*notificationv1.ListMyNotificationsResponse, error) {
	if s == nil || !s.secure || s.inbox == nil {
		return nil, status.Error(codes.Unavailable, "Notification inbox unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	userID, err := s.authenticateUser(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Notification inbox query")
	}
	page, err := s.inbox.List(ctx, userID, req.GetPage(), req.GetPageSize(), req.GetUnreadOnly())
	if err != nil {
		if errors.Is(err, repository.ErrInvalidInboxQuery) {
			return nil, status.Error(codes.InvalidArgument, "invalid Notification inbox query")
		}
		return nil, status.Error(codes.Unavailable, "Notification inbox unavailable")
	}
	response := &notificationv1.ListMyNotificationsResponse{
		Notifications: make([]*notificationv1.NotificationInboxItem, 0, len(page.Items)),
		Total:         page.Total, UnreadCount: page.UnreadCount, Page: req.GetPage(), PageSize: req.GetPageSize(),
	}
	if response.Page == 0 {
		response.Page = 1
	}
	if response.PageSize == 0 {
		response.PageSize = service.InboxDefaultPageSize
	}
	for _, item := range page.Items {
		projected := &notificationv1.NotificationInboxItem{
			Id: item.ID.String(), Type: item.Type, Locale: item.Locale, Title: item.Title, Body: item.Body,
			CreatedAt: timestamppb.New(item.CreatedAt), BookingId: item.BookingID.String(),
			EventId: item.EventID.String(), DetailPath: service.InboxRecordDetailPath(item), Source: service.InboxSource,
		}
		if item.BookingID == uuid.Nil {
			projected.BookingId = ""
		}
		if item.ReadAt != nil {
			projected.ReadAt = timestamppb.New(*item.ReadAt)
		}
		response.Notifications = append(response.Notifications, projected)
	}
	return response, nil
}

func (s *AdminOutboxServer) MarkMyNotificationRead(ctx context.Context, req *notificationv1.MarkMyNotificationReadRequest) (*notificationv1.MarkMyNotificationReadResponse, error) {
	if s == nil || !s.secure || s.inbox == nil {
		return nil, status.Error(codes.Unavailable, "Notification inbox unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	userID, err := s.authenticateUser(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "valid notification ID required")
	}
	notificationID, err := uuid.Parse(req.GetNotificationId())
	if err != nil || notificationID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "valid notification ID required")
	}
	readAt, err := s.inbox.MarkRead(ctx, userID, notificationID)
	if errors.Is(err, repository.ErrInboxNotFound) {
		return nil, status.Error(codes.NotFound, "notification not found")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Notification inbox unavailable")
	}
	return &notificationv1.MarkMyNotificationReadResponse{Id: notificationID.String(), ReadAt: timestamppb.New(readAt)}, nil
}

func (s *AdminOutboxServer) ListDeliveryExceptions(ctx context.Context, req *bookingv1.OperationsQuery) (*bookingv1.OperationsBookings, error) {
	if s == nil || !s.secure || s.inbox == nil {
		return nil, status.Error(codes.Unavailable, "Notification delivery exception source unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return nil, err
	}
	if err := s.authenticateAdmin(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.GetFrom() == nil || req.GetTo() == nil || !req.GetFrom().IsValid() || !req.GetTo().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "invalid Notification delivery exception query")
	}
	items, hasMore, observedAt, err := s.inbox.ListDeliveryExceptions(ctx, req.GetFrom().AsTime(), req.GetTo().AsTime(), req.GetLimit(), req.GetOffset())
	if err != nil {
		if errors.Is(err, service.ErrInvalidDeliveryExceptionQuery) {
			return nil, status.Error(codes.InvalidArgument, "invalid Notification delivery exception query")
		}
		return nil, status.Error(codes.Unavailable, "Notification delivery exception source unavailable")
	}
	response := &bookingv1.OperationsBookings{HasMore: hasMore, ObservedAt: timestamppb.New(observedAt), Scope: "notification.delivery_intents"}
	for _, item := range items {
		response.Items = append(response.Items, &bookingv1.OperationsBooking{
			BookingId: item.BookingID.String(), EventId: item.EventID.String(), CreatedAt: timestamppb.New(item.CreatedAt),
			Issues: []*bookingv1.OperationIssue{{Kind: "notification_delivery", Source: "notification.delivery_intents", State: item.State, ObservedAt: timestamppb.New(observedAt), NextAction: deliveryExceptionNextAction(item.State)}},
		})
	}
	return response, nil
}

func deliveryExceptionNextAction(state string) string {
	switch state {
	case "PENDING":
		return "Inspect the delivery worker and SMTP configuration before retrying."
	case "UNKNOWN":
		return "Reconcile provider acceptance using the stable Message-ID; avoid a blind resend."
	case "ATTENTION":
		return "Review the delivery exception and confirm the customer contact path."
	case "RETRYABLE":
		return "Inspect retry exhaustion and provider response before changing delivery state."
	default:
		return "Inspect the Notification delivery intent and provider evidence."
	}
}

func (s *AdminOutboxServer) authenticateUser(ctx context.Context) (uuid.UUID, error) {
	if s == nil || s.users == nil || s.revocations == nil {
		return uuid.Nil, status.Error(codes.Unavailable, "Notification authorization unavailable")
	}
	token, err := adminOutboxBearerTokenFromContext(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	revoked, err := s.revocations.IsRevoked(ctx, token)
	if err != nil {
		return uuid.Nil, status.Error(codes.Unavailable, "Notification authorization unavailable")
	}
	if revoked {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	validated, err := s.users.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.NotFound {
			return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
		}
		return uuid.Nil, status.Error(codes.Unavailable, "Notification authorization unavailable")
	}
	if validated == nil || !validated.GetValid() {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	userID, err := uuid.Parse(validated.GetUserId())
	if err != nil || userID == uuid.Nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	return userID, nil
}

func (s *AdminOutboxServer) authenticateAdmin(ctx context.Context) error {
	if s == nil || s.users == nil || s.revocations == nil {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	token, err := adminOutboxBearerTokenFromContext(ctx)
	if err != nil {
		return err
	}
	revoked, err := s.revocations.IsRevoked(ctx, token)
	if err != nil {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	if revoked {
		return status.Error(codes.Unauthenticated, "authentication required")
	}

	validated, err := s.users.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	if validated == nil {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	if !validated.GetValid() {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	userID, err := uuid.Parse(validated.GetUserId())
	if err != nil || userID == uuid.Nil {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	if validated.GetRole() != "admin" {
		return status.Error(codes.PermissionDenied, "admin access required")
	}

	current, err := s.users.ValidateToken(ctx, &userv1.ValidateTokenRequest{AccessToken: token, RequireCurrentRole: true})
	if err != nil {
		switch status.Code(err) {
		case codes.Unauthenticated, codes.NotFound:
			return status.Error(codes.PermissionDenied, "admin access required")
		default:
			return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
		}
	}
	if current == nil {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	if !current.GetValid() {
		return status.Error(codes.PermissionDenied, "admin access required")
	}
	if !current.GetCurrentRoleVerified() {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	currentUserID, err := uuid.Parse(current.GetUserId())
	if err != nil || currentUserID == uuid.Nil {
		return status.Error(codes.Unavailable, "Notification admin authorization unavailable")
	}
	if currentUserID != userID || current.GetRole() != "admin" {
		return status.Error(codes.PermissionDenied, "admin access required")
	}
	return nil
}

func adminOutboxBearerTokenFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "authentication required")
	}
	values := md.Get("authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return "", status.Error(codes.Unauthenticated, "authentication required")
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if token == "" || len(token) > maxAdminOutboxBearerTokenBytes || token != strings.TrimSpace(token) || strings.ContainsAny(token, " \t\r\n") {
		return "", status.Error(codes.Unauthenticated, "authentication required")
	}
	return token, nil
}
