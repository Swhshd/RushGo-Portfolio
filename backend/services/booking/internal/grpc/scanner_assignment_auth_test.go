package grpc

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const scannerAssignmentOtherEventID = "44444444-4444-4444-8444-444444444444"

type scannerAssignmentListRepository struct {
	repository.TicketRepository
	assignments []domain.EventScannerAssignment
	err         error
	calls       int
	eventID     uuid.UUID
}

func (r *scannerAssignmentListRepository) ListScannerAssignments(_ context.Context, eventID uuid.UUID) ([]domain.EventScannerAssignment, error) {
	r.calls++
	r.eventID = eventID
	return r.assignments, r.err
}

func callScannerAssignmentRPC(t *testing.T, server *BookingServer, ctx context.Context, req *bookingv1.ListEventScannerAssignmentsRequest) (*bookingv1.ListEventScannerAssignmentsResponse, error) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	bookingv1.RegisterBookingServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.DialContext(ctx, "passthrough:///booking-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial in-memory Booking gRPC server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return bookingv1.NewBookingServiceClient(conn).ListEventScannerAssignments(ctx, req)
}

func TestListEventScannerAssignmentsRequiresAuthenticatedAdmin(t *testing.T) {
	assignments := []domain.EventScannerAssignment{
		{EventID: uuid.MustParse(scannerAssignmentOtherEventID), StaffUserID: uuid.MustParse(ticketAuthOtherID), Active: true},
	}

	tests := []struct {
		name       string
		metadata   metadata.MD
		validation *userv1.ValidateTokenResponse
		wantCode   codes.Code
		wantCalls  int
	}{
		{name: "missing metadata", wantCode: codes.Unauthenticated},
		{name: "empty bearer", metadata: metadata.Pairs("authorization", "Bearer "), wantCode: codes.Unauthenticated},
		{name: "malformed bearer", metadata: metadata.Pairs("authorization", "Basic opaque"), wantCode: codes.Unauthenticated},
		{name: "expired token rejected by User service", metadata: metadata.Pairs("authorization", "Bearer expired-token"), validation: &userv1.ValidateTokenResponse{Valid: false}, wantCode: codes.Unauthenticated},
		{name: "invalid signature rejected by User service", metadata: metadata.Pairs("authorization", "Bearer bad-signature-token"), validation: &userv1.ValidateTokenResponse{Valid: false}, wantCode: codes.Unauthenticated},
		{name: "ordinary user", metadata: metadata.Pairs("authorization", "Bearer user-token"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}, wantCode: codes.PermissionDenied},
		{name: "ordinary user cannot elevate with forged role metadata", metadata: metadata.Pairs("authorization", "Bearer user-token", "role", "admin"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthUserID, Role: "user"}, wantCode: codes.PermissionDenied},
		{name: "valid admin can list a different event", metadata: metadata.Pairs("authorization", "Bearer admin-token"), validation: &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAuthOtherID, Role: "admin"}, wantCode: codes.OK, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &scannerAssignmentListRepository{assignments: assignments}
			server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
			tokens := &fakeTicketTokenClient{response: tt.validation}
			if tt.validation != nil {
				tokens.currentRole = &userv1.ValidateTokenResponse{Valid: tt.validation.Valid, UserId: tt.validation.UserId, Role: tt.validation.Role, CurrentRoleVerified: true}
			}
			configureAdminOutboxAuthenticators(server, tokens, &fakeTicketRevocationChecker{})

			ctx := context.Background()
			if tt.metadata != nil {
				ctx = metadata.NewOutgoingContext(ctx, tt.metadata)
			}
			resp, err := callScannerAssignmentRPC(t, server, ctx, &bookingv1.ListEventScannerAssignmentsRequest{EventId: scannerAssignmentOtherEventID})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("ListEventScannerAssignments status = %s, want %s (err=%v)", got, tt.wantCode, err)
			}
			if store.calls != tt.wantCalls {
				t.Fatalf("repository calls = %d, want %d", store.calls, tt.wantCalls)
			}
			if tt.wantCode == codes.OK {
				if resp == nil || len(resp.GetAssignments()) != len(assignments) {
					t.Fatalf("admin response assignments = %v, want %d", resp.GetAssignments(), len(assignments))
				}
				if got := resp.GetAssignments()[0].GetEventId(); got != scannerAssignmentOtherEventID {
					t.Fatalf("returned assignment event ID = %s, want requested event %s", got, scannerAssignmentOtherEventID)
				}
				if store.eventID.String() != scannerAssignmentOtherEventID {
					t.Fatalf("repository event ID = %s, want requested event %s", store.eventID, scannerAssignmentOtherEventID)
				}
				if tokens.calls != 2 || tokens.token != "admin-token" {
					t.Fatalf("admin bearer was not validated: calls=%d token=%q", tokens.calls, tokens.token)
				}
			} else if resp != nil {
				t.Fatalf("denied request returned a partial response: %+v", resp)
			}
		})
	}
}

func TestListEventScannerAssignmentsHidesPartialDataOnRepositoryError(t *testing.T) {
	store := &scannerAssignmentListRepository{
		assignments: []domain.EventScannerAssignment{{EventID: uuid.MustParse(ticketAuthEventID), StaffUserID: uuid.MustParse(ticketAuthOtherID), Active: true}},
		err:         errors.New("synthetic repository failure"),
	}
	server := NewBookingServer(nil, zap.NewNop(), service.NewTicketManager(store, nil))
	server.SetAdminOutboxPrincipalAuthenticator(staticTicketPrincipalAuthenticator{principal: TicketPrincipal{UserID: uuid.MustParse(ticketAuthUserID), Role: "admin"}})
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-admin"))
	resp, err := callScannerAssignmentRPC(t, server, ctx, &bookingv1.ListEventScannerAssignmentsRequest{EventId: ticketAuthEventID})
	if status.Code(err) != codes.Internal {
		t.Fatalf("repository error status = %s, want Internal (err=%v)", status.Code(err), err)
	}
	if resp != nil {
		t.Fatalf("repository error returned partial staff data: %+v", resp)
	}
	if store.calls != 1 {
		t.Fatalf("repository calls = %d, want 1", store.calls)
	}
}
