package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/gateway/internal/router"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ticketUserID = "11111111-1111-4111-8111-111111111111"
const ticketAdminID = "22222222-2222-4222-8222-222222222222"
const ticketEventID = "33333333-3333-4333-8333-333333333333"

type ticketAuthClient struct{ userv1.UserServiceClient }

func (ticketAuthClient) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	if req.AccessToken == "user" {
		return &userv1.ValidateTokenResponse{Valid: true, UserId: ticketUserID, Role: "user", CurrentRoleVerified: req.RequireCurrentRole}, nil
	}
	if req.AccessToken == "admin" {
		return &userv1.ValidateTokenResponse{Valid: true, UserId: ticketAdminID, Role: "admin", CurrentRoleVerified: req.RequireCurrentRole}, nil
	}
	return &userv1.ValidateTokenResponse{}, nil
}

type ticketBookingClient struct {
	bookingv1.BookingServiceClient
	listUser         string
	listAuth         []string
	scannerListCalls int
	scannerListAuth  []string
	scannerListError error
	grantCalls       int
	actor            string
	assignmentAuth   []string
	checkInRequest   *bookingv1.CheckInTicketRequest
	checkInAuth      []string
	rotateRequest    *bookingv1.RotateTicketCredentialRequest
	rotateAuth       []string
	rotateResponse   *bookingv1.TicketCredentialResponse
	rotateError      error
	checkInError     error
}

func (c *ticketBookingClient) ListOwnedTickets(ctx context.Context, req *bookingv1.ListOwnedTicketsRequest, _ ...grpc.CallOption) (*bookingv1.ListOwnedTicketsResponse, error) {
	c.listUser = req.UserId
	c.listAuth = outgoingAuthorization(ctx)
	return &bookingv1.ListOwnedTicketsResponse{Tickets: []*bookingv1.TicketDetail{{Id: "ticket-1", Status: "ISSUED", BookingStatus: "CONFIRMED"}}}, nil
}
func (c *ticketBookingClient) ListEventScannerAssignments(ctx context.Context, _ *bookingv1.ListEventScannerAssignmentsRequest, _ ...grpc.CallOption) (*bookingv1.ListEventScannerAssignmentsResponse, error) {
	c.scannerListCalls++
	c.scannerListAuth = outgoingAuthorization(ctx)
	if c.scannerListError != nil {
		return nil, c.scannerListError
	}
	return &bookingv1.ListEventScannerAssignmentsResponse{Assignments: []*bookingv1.EventScannerAssignmentResponse{{EventId: ticketEventID, StaffUserId: ticketUserID, Active: true, ChangedAt: timestamppb.New(time.Now())}}}, nil
}
func (c *ticketBookingClient) SetEventScannerAssignment(ctx context.Context, req *bookingv1.SetEventScannerAssignmentRequest, _ ...grpc.CallOption) (*bookingv1.EventScannerAssignmentResponse, error) {
	c.grantCalls++
	c.actor = req.ActorUserId
	c.assignmentAuth = outgoingAuthorization(ctx)
	return &bookingv1.EventScannerAssignmentResponse{EventId: req.EventId, StaffUserId: req.StaffUserId, Active: req.Active, ChangedAt: timestamppb.New(time.Now())}, nil
}
func (c *ticketBookingClient) CheckInTicket(ctx context.Context, req *bookingv1.CheckInTicketRequest, _ ...grpc.CallOption) (*bookingv1.CheckInTicketResponse, error) {
	c.checkInRequest = req
	c.checkInAuth = outgoingAuthorization(ctx)
	if c.checkInError != nil {
		return nil, c.checkInError
	}
	return &bookingv1.CheckInTicketResponse{ResultCode: "accepted", ReasonCode: "checked_in", Replayed: false}, nil
}
func (c *ticketBookingClient) RotateTicketCredential(ctx context.Context, req *bookingv1.RotateTicketCredentialRequest, _ ...grpc.CallOption) (*bookingv1.TicketCredentialResponse, error) {
	c.rotateRequest = req
	c.rotateAuth = outgoingAuthorization(ctx)
	if c.rotateError != nil {
		return nil, c.rotateError
	}
	if c.rotateResponse != nil {
		return c.rotateResponse, nil
	}
	return &bookingv1.TicketCredentialResponse{Credential: "opaque-test-credential", ExpiresAt: timestamppb.New(time.Now().Add(5 * time.Minute)), Outcome: "ROTATED", CredentialVersion: 2}, nil
}

func outgoingAuthorization(ctx context.Context) []string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil
	}
	return append([]string(nil), md.Get("authorization")...)
}

func ticketRouter(t *testing.T, client *ticketBookingClient) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:26379", MaxRetries: -1, DialTimeout: time.Second})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal("isolated Redis fixture unavailable")
	}
	return router.SetupRouter(ticketAuthClient{}, nil, client, nil, nil, rdb, nil)
}

func ticketRequest(r http.Handler, method, path, body, token string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestTicketWalletUsesAuthenticatedOwnerAndNoStore(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	if w := ticketRequest(r, http.MethodGet, "/api/tickets", "", "", nil); w.Code != http.StatusUnauthorized || client.listUser != "" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unauthenticated wallet request: code=%d user=%q", w.Code, client.listUser)
	}
	w := ticketRequest(r, http.MethodGet, "/api/tickets", "", "user", nil)
	if w.Code != http.StatusOK || client.listUser != ticketUserID || len(client.listAuth) != 1 || client.listAuth[0] != "Bearer user" || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), `"booking_status":"CONFIRMED"`) {
		t.Fatalf("wallet owner/cache contract failed code=%d user=%s cache=%s body=%s", w.Code, client.listUser, w.Header().Get("Cache-Control"), w.Body.String())
	}
}

func TestTicketScannerAssignmentIsAdminOnly(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	path := "/api/events/" + ticketEventID + "/scanners/" + ticketUserID
	if w := ticketRequest(r, http.MethodPut, path, "", "user", nil); w.Code != http.StatusForbidden || client.grantCalls != 0 {
		t.Fatalf("non-admin scanner grant code=%d calls=%d", w.Code, client.grantCalls)
	}
	w := ticketRequest(r, http.MethodPut, path, "", "admin", nil)
	if w.Code != http.StatusOK || client.grantCalls != 1 || client.actor != ticketAdminID || len(client.assignmentAuth) != 1 || client.assignmentAuth[0] != "Bearer admin" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("admin scanner grant failed code=%d calls=%d actor=%s", w.Code, client.grantCalls, client.actor)
	}
}

func TestTicketScannerAssignmentListForwardsAdminCredentialAndHidesBackendErrors(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	path := "/api/events/" + ticketEventID + "/scanners"
	if w := ticketRequest(r, http.MethodGet, path, "", "user", nil); w.Code != http.StatusForbidden || client.scannerListCalls != 0 {
		t.Fatalf("non-admin scanner list code=%d calls=%d", w.Code, client.scannerListCalls)
	}
	w := ticketRequest(r, http.MethodGet, path, "", "admin", nil)
	if w.Code != http.StatusOK || client.scannerListCalls != 1 || len(client.scannerListAuth) != 1 || client.scannerListAuth[0] != "Bearer admin" || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), ticketUserID) {
		t.Fatalf("admin scanner list failed code=%d calls=%d auth=%v cache=%s body=%s", w.Code, client.scannerListCalls, client.scannerListAuth, w.Header().Get("Cache-Control"), w.Body.String())
	}

	client.scannerListError = status.Error(codes.Internal, "private repository details")
	w = ticketRequest(r, http.MethodGet, path, "", "admin", nil)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "private repository details") || strings.Contains(w.Body.String(), ticketUserID) {
		t.Fatalf("backend error leaked details or staff data: code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestTicketCheckInUsesAuthenticatedStaffAndIdempotencyHeader(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	token := "do-not-echo-this-opaque-credential"
	w := ticketRequest(r, http.MethodPost, "/api/events/"+ticketEventID+"/check-in", `{"credential":"`+token+`"}`, "user", map[string]string{"Idempotency-Key": "scan-1"})
	if w.Code != http.StatusOK || client.checkInRequest == nil || client.checkInRequest.StaffUserId != ticketUserID || client.checkInRequest.IdempotencyKey != "scan-1" || client.checkInRequest.Credential != token || len(client.checkInAuth) != 1 || client.checkInAuth[0] != "Bearer user" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("check-in request contract failed code=%d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), token) {
		t.Fatal("scan response echoed the bearer credential")
	}
}

func TestTicketCredentialRotationUsesAuthenticatedOwnerAndIdempotencyContract(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	path := "/api/tickets/44444444-4444-4444-8444-444444444444/credential"
	key := "wallet-operation-1"
	w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": key})
	if w.Code != http.StatusOK || client.rotateRequest == nil || client.rotateRequest.UserId != ticketUserID || client.rotateRequest.IdempotencyKey != key || len(client.rotateAuth) != 1 || client.rotateAuth[0] != "Bearer user" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("rotation request contract failed code=%d request=%+v body=%s", w.Code, client.rotateRequest, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"outcome":"ROTATED"`) || !strings.Contains(w.Body.String(), `"credential_version":2`) || !strings.Contains(w.Body.String(), `"credential":"opaque-test-credential"`) {
		t.Fatalf("first rotation response omitted its one-time credential result: %s", w.Body.String())
	}

	client.rotateRequest = nil
	client.rotateResponse = &bookingv1.TicketCredentialResponse{ExpiresAt: timestamppb.New(time.Now().Add(5 * time.Minute)), Outcome: "ALREADY_APPLIED", CredentialVersion: 2}
	w = ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": key})
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"outcome":"ALREADY_APPLIED"`) || strings.Contains(w.Body.String(), `"credential"`) {
		t.Fatalf("idempotent replay exposed or omitted a credential: code=%d body=%s", w.Code, w.Body.String())
	}
	if client.rotateRequest == nil || client.rotateRequest.IdempotencyKey != key {
		t.Fatalf("idempotent replay did not reuse the request key: %+v", client.rotateRequest)
	}
}

func TestTicketCredentialRotationRejectsInvalidKeyAndMapsRetryableStates(t *testing.T) {
	client := &ticketBookingClient{}
	r := ticketRouter(t, client)
	path := "/api/tickets/44444444-4444-4444-8444-444444444444/credential"
	if w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": "bad key"}); w.Code != http.StatusBadRequest || client.rotateRequest != nil {
		t.Fatalf("invalid rotation key reached Booking: code=%d request=%+v", w.Code, client.rotateRequest)
	}
	client.rotateError = status.Error(codes.Aborted, "still running")
	if w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": "retry-this-key"}); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"idempotency_in_progress"`) || !strings.Contains(w.Body.String(), `"retryable":true`) {
		t.Fatalf("in-progress rotation response=%d %s", w.Code, w.Body.String())
	}
	client.rotateError = status.Error(codes.Unavailable, "authority unknown")
	if w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": "authority-key"}); w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"code":"ticket_authority_unavailable"`) {
		t.Fatalf("unknown authority mapping=%d %s", w.Code, w.Body.String())
	}
	client.rotateError = status.Error(codes.FailedPrecondition, "ticket not eligible")
	if w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": "ineligible-key"}); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"ticket_not_eligible"`) {
		t.Fatalf("permanently ineligible ticket mapping=%d %s", w.Code, w.Body.String())
	}
	client.rotateError = status.Error(codes.AlreadyExists, "key conflict")
	if w := ticketRequest(r, http.MethodPost, path, "", "user", map[string]string{"Idempotency-Key": "conflicting-key"}); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"idempotency_conflict"`) {
		t.Fatalf("idempotency conflict mapping=%d %s", w.Code, w.Body.String())
	}
}

func TestTicketCheckInMapsConcurrentOperationKeyWaitAsRetryable(t *testing.T) {
	client := &ticketBookingClient{checkInError: status.Error(codes.Aborted, "still running")}
	r := ticketRouter(t, client)
	w := ticketRequest(r, http.MethodPost, "/api/events/"+ticketEventID+"/check-in", `{"credential":"opaque"}`, "user", map[string]string{"Idempotency-Key": "same-checkin-key"})
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"idempotency_in_progress"`) || !strings.Contains(w.Body.String(), `"retryable":true`) {
		t.Fatalf("in-progress check-in response=%d %s", w.Code, w.Body.String())
	}
}
