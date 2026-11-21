package handler

import (
	"context"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TicketHandler struct {
	client bookingv1.BookingServiceClient
}

func NewTicketHandler(client bookingv1.BookingServiceClient) *TicketHandler {
	return &TicketHandler{client: client}
}

func (h *TicketHandler) ListOwned(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	response, err := h.client.ListOwnedTickets(ticketIdentityContext(c), &bookingv1.ListOwnedTicketsRequest{UserId: userID})
	if err != nil || response == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_list_unavailable", "error": "tickets are temporarily unavailable"})
		return
	}
	tickets := make([]gin.H, 0, len(response.GetTickets()))
	for _, ticket := range response.GetTickets() {
		tickets = append(tickets, gin.H{"id": ticket.Id, "booking_id": ticket.BookingId, "event_id": ticket.EventId, "seat_id": ticket.SeatId, "ticket_tier_id": ticket.TicketTierId, "status": ticket.Status, "booking_status": ticket.BookingStatus, "issued_at": timestampString(ticket.IssuedAt), "checked_in_at": timestampString(ticket.CheckedInAt)})
	}
	c.JSON(http.StatusOK, gin.H{"tickets": tickets})
}

func (h *TicketHandler) RotateCredential(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	keys := c.Request.Header.Values("Idempotency-Key")
	if len(keys) > 1 {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_idempotency_key", "error": "a single valid Idempotency-Key is required"})
		return
	}
	idempotencyKey := uuid.NewString() // preserve one-shot compatibility for clients predating the header
	if len(keys) == 1 {
		idempotencyKey = keys[0]
	}
	if !ticketScanKeyPattern.MatchString(idempotencyKey) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_idempotency_key", "error": "a single valid Idempotency-Key is required"})
		return
	}
	c.Header("Idempotency-Key", idempotencyKey)
	response, err := h.client.RotateTicketCredential(ticketIdentityContext(c), &bookingv1.RotateTicketCredentialRequest{UserId: userID, TicketId: c.Param("id"), IdempotencyKey: idempotencyKey})
	if err != nil {
		switch status.Code(err) {
		case codes.PermissionDenied:
			c.JSON(http.StatusForbidden, gin.H{"code": "ticket_owner_required", "error": "ticket is not available to this account"})
		case codes.NotFound:
			c.JSON(http.StatusNotFound, gin.H{"code": "ticket_unavailable", "error": "ticket is not available"})
		case codes.InvalidArgument:
			c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_idempotency_key", "error": "ticket id or Idempotency-Key is invalid"})
		case codes.AlreadyExists:
			c.JSON(http.StatusConflict, gin.H{"code": "idempotency_conflict", "error": "this key was used for a different ticket operation"})
		case codes.Aborted:
			c.JSON(http.StatusConflict, gin.H{"code": "idempotency_in_progress", "error": "the previous request is still processing; retry with the same key", "retryable": true})
		case codes.Unavailable:
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_authority_unavailable", "error": "ticket eligibility is temporarily unavailable"})
		case codes.FailedPrecondition:
			c.JSON(http.StatusConflict, gin.H{"code": "ticket_not_eligible", "error": "this ticket cannot create an entry credential"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_credential_unavailable", "error": "ticket credential service is temporarily unavailable"})
		}
		return
	}
	if response == nil || response.CredentialVersion < 1 || timestampString(response.ExpiresAt) == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_credential_unavailable", "error": "ticket credential response is incomplete"})
		return
	}
	payload := gin.H{"outcome": response.Outcome, "credential_version": response.CredentialVersion, "expires_at": timestampString(response.ExpiresAt)}
	switch response.Outcome {
	case "ROTATED":
		if response.Credential == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_credential_unavailable", "error": "ticket credential response is incomplete"})
			return
		}
		payload["credential"] = response.Credential
	case "ALREADY_APPLIED":
		if response.Credential != "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_credential_unavailable", "error": "ticket credential replay response was invalid"})
			return
		}
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_credential_unavailable", "error": "ticket credential response is incomplete"})
		return
	}
	c.JSON(http.StatusOK, payload)
}

func (h *TicketHandler) SetScannerAssignment(active bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		actorID, ok := authenticatedUserID(c)
		if !ok {
			return
		}
		response, err := h.client.SetEventScannerAssignment(ticketIdentityContext(c), &bookingv1.SetEventScannerAssignmentRequest{EventId: c.Param("id"), StaffUserId: c.Param("staffId"), ActorUserId: actorID, Active: active})
		if err != nil {
			if status.Code(err) == codes.InvalidArgument {
				c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_scanner_assignment", "error": "event or staff id is invalid"})
			} else {
				c.JSON(http.StatusServiceUnavailable, gin.H{"code": "scanner_assignment_unavailable", "error": "scanner assignment could not be updated"})
			}
			return
		}
		c.JSON(http.StatusOK, scannerAssignmentJSON(response))
	}
}

func (h *TicketHandler) ListScannerAssignments(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	response, err := h.client.ListEventScannerAssignments(ticketIdentityContext(c), &bookingv1.ListEventScannerAssignmentsRequest{EventId: c.Param("id")})
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "scanner_assignment_unavailable", "error": "scanner assignments are temporarily unavailable"})
		return
	}
	assignments := make([]gin.H, 0, len(response.GetAssignments()))
	for _, assignment := range response.GetAssignments() {
		assignments = append(assignments, scannerAssignmentJSON(assignment))
	}
	c.JSON(http.StatusOK, gin.H{"assignments": assignments})
}

func (h *TicketHandler) CheckIn(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	staffID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	keys := c.Request.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !ticketScanKeyPattern.MatchString(keys[0]) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_check_in_request", "error": "a valid Idempotency-Key is required"})
		return
	}
	var request struct {
		Credential string `json:"credential"`
	}
	if err := c.ShouldBindJSON(&request); err != nil || request.Credential == "" || len(request.Credential) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_check_in_request", "error": "ticket credential is required"})
		return
	}
	response, err := h.client.CheckInTicket(ticketIdentityContext(c), &bookingv1.CheckInTicketRequest{EventId: c.Param("id"), StaffUserId: staffID, Credential: request.Credential, IdempotencyKey: keys[0]})
	if err != nil {
		switch status.Code(err) {
		case codes.PermissionDenied:
			c.JSON(http.StatusForbidden, gin.H{"code": "scanner_not_assigned", "error": "you are not assigned to check in this event"})
		case codes.NotFound:
			c.JSON(http.StatusNotFound, gin.H{"code": "ticket_credential_invalid", "error": "ticket credential is invalid"})
		case codes.AlreadyExists:
			c.JSON(http.StatusConflict, gin.H{"code": "idempotency_conflict", "error": "this key was used for a different scan"})
		case codes.Aborted:
			c.JSON(http.StatusConflict, gin.H{"code": "idempotency_in_progress", "error": "the previous request is still processing; retry with the same key", "retryable": true})
		case codes.InvalidArgument:
			c.JSON(http.StatusBadRequest, gin.H{"code": "invalid_check_in_request", "error": "check-in request is invalid"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": "ticket_authority_unavailable", "error": "check-in is temporarily unavailable"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": response.ResultCode, "reason_code": response.ReasonCode, "checked_in_at": timestampString(response.CheckedInAt), "replayed": response.Replayed, "evidence_source": response.EvidenceSource, "observed_at": timestampString(response.ObservedAt)})
}

func authenticatedUserID(c *gin.Context) (string, bool) {
	userID, ok := c.Get("user_id")
	id, valid := userID.(string)
	if !ok || !valid || id == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return "", false
	}
	return id, true
}

func ticketIdentityContext(c *gin.Context) context.Context {
	ctx := c.Request.Context()
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Delete("authorization")
	md.Set("authorization", c.GetHeader("Authorization"))
	return metadata.NewOutgoingContext(ctx, md)
}

func timestampString(value *timestamppb.Timestamp) any {
	if value == nil {
		return nil
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

func scannerAssignmentJSON(value *bookingv1.EventScannerAssignmentResponse) gin.H {
	return gin.H{"event_id": value.EventId, "staff_user_id": value.StaffUserId, "active": value.Active, "changed_at": timestampString(value.ChangedAt)}
}

var ticketScanKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
