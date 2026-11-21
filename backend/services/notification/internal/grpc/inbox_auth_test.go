package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type inboxRepositoryStub struct {
	owners     map[uuid.UUID]uuid.UUID
	items      map[uuid.UUID]domain.BusinessInboxRecord
	exceptions []repository.DeliveryException
	listCalls  int
	markCalls  int
}

func (r *inboxRepositoryStub) AcceptBusinessNotification(context.Context, *domain.BusinessNotificationV1, [32]byte, domain.InboxTemplate, string, bool) (bool, error) {
	return false, nil
}

func (r *inboxRepositoryStub) ListInbox(_ context.Context, userID uuid.UUID, page, pageSize int32, unreadOnly bool) (repository.InboxPage, error) {
	r.listCalls++
	result := repository.InboxPage{Items: make([]domain.BusinessInboxRecord, 0), UnreadCount: 1}
	for id, item := range r.items {
		if r.owners[id] != userID || unreadOnly && item.ReadAt != nil {
			continue
		}
		result.Items = append(result.Items, item)
		result.Total++
	}
	return result, nil
}

func (r *inboxRepositoryStub) MarkInboxRead(_ context.Context, userID, notificationID uuid.UUID) (time.Time, error) {
	r.markCalls++
	item, exists := r.items[notificationID]
	if !exists || r.owners[notificationID] != userID {
		return time.Time{}, repository.ErrInboxNotFound
	}
	if item.ReadAt == nil {
		readAt := time.Now().UTC()
		item.ReadAt = &readAt
		r.items[notificationID] = item
	}
	return *item.ReadAt, nil
}

func (r *inboxRepositoryStub) ReadDeliveryExceptions(context.Context, time.Time, time.Time, int32, int32) ([]repository.DeliveryException, bool, time.Time, error) {
	return append([]repository.DeliveryException(nil), r.exceptions...), false, time.Now().UTC(), nil
}

func TestNotificationInboxRPCBindsOwnerToCurrentUserAndFailsClosed(t *testing.T) {
	userID := uuid.MustParse("95000000-0000-4000-8000-000000000001")
	otherUserID := uuid.MustParse("95000000-0000-4000-8000-000000000002")
	notificationID := uuid.MustParse("95000000-0000-4000-8000-000000000003")
	bookingID := uuid.MustParse("95000000-0000-4000-8000-000000000004")
	item := domain.BusinessInboxRecord{ID: notificationID, BookingID: bookingID, EventID: uuid.New(), UserID: userID, Type: "tickets_issued", Locale: "en", Title: "Tickets are ready", Body: "View your order.", CreatedAt: time.Now().UTC()}

	t.Run("current user lists only owned items and marks read", func(t *testing.T) {
		store := &inboxRepositoryStub{owners: map[uuid.UUID]uuid.UUID{notificationID: userID}, items: map[uuid.UUID]domain.BusinessInboxRecord{notificationID: item}}
		users := &adminOutboxUserClientStub{initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID.String(), Role: "user"}}
		revocations := &adminOutboxRevocationStub{}
		server := NewAdminOutboxServer(nil, users, revocations, true, service.NewNotificationInboxService(store))
		ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer current-user")
		response, err := server.ListMyNotifications(ctx, &notificationv1.ListMyNotificationsRequest{Page: 1, PageSize: 20})
		if err != nil || len(response.GetNotifications()) != 1 || response.GetNotifications()[0].GetId() != notificationID.String() || response.GetNotifications()[0].GetDetailPath() != "/my-tickets/orders/"+bookingID.String() {
			t.Fatalf("current user inbox response=%+v err=%v", response, err)
		}
		if len(users.requests) != 1 || users.requests[0].GetAccessToken() != "current-user" || store.listCalls != 1 {
			t.Fatalf("owner binding calls: user=%+v list=%d", users.requests, store.listCalls)
		}
		marked, err := server.MarkMyNotificationRead(ctx, &notificationv1.MarkMyNotificationReadRequest{NotificationId: notificationID.String()})
		if err != nil || marked.GetId() != notificationID.String() || !marked.GetReadAt().IsValid() {
			t.Fatalf("owned mark-read response=%+v err=%v", marked, err)
		}
		if _, err := server.MarkMyNotificationRead(ctx, &notificationv1.MarkMyNotificationReadRequest{NotificationId: uuid.NewString()}); status.Code(err) != codes.NotFound {
			t.Fatalf("nonexistent item status=%v err=%v", status.Code(err), err)
		}
	})

	t.Run("cross-owner mark is not found", func(t *testing.T) {
		store := &inboxRepositoryStub{owners: map[uuid.UUID]uuid.UUID{notificationID: userID}, items: map[uuid.UUID]domain.BusinessInboxRecord{notificationID: item}}
		users := &adminOutboxUserClientStub{initial: &userv1.ValidateTokenResponse{Valid: true, UserId: otherUserID.String(), Role: "user"}}
		server := NewAdminOutboxServer(nil, users, &adminOutboxRevocationStub{}, true, service.NewNotificationInboxService(store))
		ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer other-user")
		response, err := server.MarkMyNotificationRead(ctx, &notificationv1.MarkMyNotificationReadRequest{NotificationId: notificationID.String()})
		if response != nil || status.Code(err) != codes.NotFound || store.markCalls != 1 {
			t.Fatalf("cross-owner mark response=%+v code=%s calls=%d err=%v", response, status.Code(err), store.markCalls, err)
		}
	})

	for _, tc := range []struct {
		name          string
		identity      string
		verified      bool
		authorization string
		revoked       bool
		revocationErr error
		wantCode      codes.Code
		wantRevokes   int
	}{
		{name: "missing mutual TLS", authorization: "Bearer current-user", wantCode: codes.Unauthenticated},
		{name: "untrusted Gateway identity", identity: "spiffe://rushgo.internal/booking", verified: true, authorization: "Bearer current-user", wantCode: codes.PermissionDenied},
		{name: "unverified Gateway certificate", identity: grpcsecurity.ServiceIdentityGateway, authorization: "Bearer current-user", wantCode: codes.Unauthenticated},
		{name: "revoked user token", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: "Bearer current-user", revoked: true, wantCode: codes.Unauthenticated, wantRevokes: 1},
		{name: "revocation store unavailable", identity: grpcsecurity.ServiceIdentityGateway, verified: true, authorization: "Bearer current-user", revocationErr: errors.New("revocation store unavailable"), wantCode: codes.Unavailable, wantRevokes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &inboxRepositoryStub{owners: map[uuid.UUID]uuid.UUID{notificationID: userID}, items: map[uuid.UUID]domain.BusinessInboxRecord{notificationID: item}}
			users := &adminOutboxUserClientStub{initial: &userv1.ValidateTokenResponse{Valid: true, UserId: userID.String(), Role: "user"}}
			revocations := &adminOutboxRevocationStub{revoked: tc.revoked, err: tc.revocationErr}
			server := NewAdminOutboxServer(nil, users, revocations, true, service.NewNotificationInboxService(store))
			ctx := adminOutboxRPCContext(tc.identity, tc.verified, tc.authorization)
			_, err := server.ListMyNotifications(ctx, &notificationv1.ListMyNotificationsRequest{Page: 1, PageSize: 20})
			if status.Code(err) != tc.wantCode || revocations.calls != tc.wantRevokes || store.listCalls != 0 {
				t.Fatalf("list status=%s revocation calls=%d reads=%d err=%v", status.Code(err), revocations.calls, store.listCalls, err)
			}
		})
	}
}

func TestNotificationDeliveryExceptionsRequiresCurrentAdminAndVerifiedGateway(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	adminID := uuid.MustParse("95000000-0000-4000-8000-000000000011")
	store := &inboxRepositoryStub{
		owners: map[uuid.UUID]uuid.UUID{}, items: map[uuid.UUID]domain.BusinessInboxRecord{},
		exceptions: []repository.DeliveryException{{BookingID: uuid.New(), EventID: uuid.New(), Type: "refund_succeeded", State: "UNKNOWN", CreatedAt: now}},
	}
	query := &bookingv1.OperationsQuery{From: timestamppb.New(now.Add(-time.Hour)), To: timestamppb.New(now), Limit: 20}
	ctx := adminOutboxRPCContext(grpcsecurity.ServiceIdentityGateway, true, "Bearer current-admin")

	t.Run("current admin reads safe projection", func(t *testing.T) {
		users := &adminOutboxUserClientStub{
			initial: &userv1.ValidateTokenResponse{Valid: true, UserId: adminID.String(), Role: "admin"},
			current: &userv1.ValidateTokenResponse{Valid: true, UserId: adminID.String(), Role: "admin", CurrentRoleVerified: true},
		}
		server := NewAdminOutboxServer(nil, users, &adminOutboxRevocationStub{}, true, service.NewNotificationInboxService(store))
		response, err := server.ListDeliveryExceptions(ctx, query)
		if err != nil || len(response.GetItems()) != 1 || response.GetItems()[0].GetIssues()[0].GetKind() != "notification_delivery" || len(users.requests) != 2 || store.listCalls != 0 {
			t.Fatalf("admin exception response=%+v calls=%d err=%v", response, len(users.requests), err)
		}
	})

	for _, tc := range []struct {
		name          string
		identity      string
		verified      bool
		role          string
		currentRole   string
		currentOK     bool
		revoked       bool
		revocationErr error
		wantCode      codes.Code
		wantRevokes   int
	}{
		{name: "demoted admin", identity: grpcsecurity.ServiceIdentityGateway, verified: true, role: "admin", currentRole: "user", currentOK: true, wantCode: codes.PermissionDenied, wantRevokes: 1},
		{name: "revoked admin", identity: grpcsecurity.ServiceIdentityGateway, verified: true, revoked: true, wantCode: codes.Unauthenticated, wantRevokes: 1},
		{name: "revocation service unavailable", identity: grpcsecurity.ServiceIdentityGateway, verified: true, revocationErr: errors.New("revocation store unavailable"), wantCode: codes.Unavailable, wantRevokes: 1},
		{name: "wrong client certificate", identity: "spiffe://rushgo.internal/booking", verified: true, wantCode: codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			users := &adminOutboxUserClientStub{
				initial: &userv1.ValidateTokenResponse{Valid: true, UserId: adminID.String(), Role: tc.role},
				current: &userv1.ValidateTokenResponse{Valid: true, UserId: adminID.String(), Role: tc.currentRole, CurrentRoleVerified: tc.currentOK},
			}
			revocations := &adminOutboxRevocationStub{revoked: tc.revoked, err: tc.revocationErr}
			server := NewAdminOutboxServer(nil, users, revocations, true, service.NewNotificationInboxService(store))
			callCtx := adminOutboxRPCContext(tc.identity, tc.verified, "Bearer current-admin")
			_, err := server.ListDeliveryExceptions(callCtx, query)
			if status.Code(err) != tc.wantCode || revocations.calls != tc.wantRevokes {
				t.Fatalf("admin exception status=%s revocation calls=%d err=%v", status.Code(err), revocations.calls, err)
			}
		})
	}
}
