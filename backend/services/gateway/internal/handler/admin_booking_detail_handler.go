package handler

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adminBookingDetailTimeout = 10 * time.Second

type AdminBookingDetailHandler struct {
	bookingClient bookingv1.BookingServiceClient
}

func NewAdminBookingDetailHandler(bookingClient bookingv1.BookingServiceClient) *AdminBookingDetailHandler {
	return &AdminBookingDetailHandler{bookingClient: bookingClient}
}

func (h *AdminBookingDetailHandler) Get(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.bookingClient == nil {
		writeAdminBookingDetailError(c, http.StatusServiceUnavailable, "source_unavailable", "booking detail source unavailable")
		return
	}
	bookingID, err := uuid.Parse(c.Param("id"))
	if err != nil || bookingID == uuid.Nil {
		writeAdminBookingDetailError(c, http.StatusBadRequest, "invalid_booking_id", "invalid booking identifier")
		return
	}
	authorization := c.Request.Header.Values("Authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") ||
		strings.TrimPrefix(authorization[0], "Bearer ") == "" ||
		strings.ContainsAny(strings.TrimPrefix(authorization[0], "Bearer "), " \t\r\n") {
		writeAdminBookingDetailError(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), adminBookingDetailTimeout)
	defer cancel()
	ctx = metadataOutgoingContext(ctx, authorization[0])
	source, err := h.bookingClient.GetAdminBookingDetail(ctx, &bookingv1.GetAdminBookingDetailRequest{BookingId: bookingID.String()})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			writeAdminBookingDetailError(c, http.StatusNotFound, "not_found", "booking not found")
		case codes.InvalidArgument:
			writeAdminBookingDetailError(c, http.StatusBadRequest, "invalid_booking_id", "invalid booking identifier")
		case codes.PermissionDenied:
			writeAdminBookingDetailError(c, http.StatusForbidden, "forbidden", "booking access forbidden")
		default:
			writeAdminBookingDetailError(c, http.StatusServiceUnavailable, "source_unavailable", "booking detail source unavailable")
		}
		return
	}
	response, ok := projectAdminBookingDetail(source, bookingID)
	if !ok {
		writeAdminBookingDetailError(c, http.StatusServiceUnavailable, "source_unavailable", "booking detail source unavailable")
		return
	}
	c.JSON(http.StatusOK, response)
}

type adminBookingDetailResponse struct {
	BookingID           string                     `json:"booking_id"`
	UserID              string                     `json:"user_id"`
	EventID             string                     `json:"event_id"`
	BookingStatus       string                     `json:"booking_status"`
	TotalAmountCents    int64                      `json:"total_amount_cents"`
	Currency            string                     `json:"currency"`
	BookingMode         string                     `json:"booking_mode"`
	LifecycleGeneration int64                      `json:"lifecycle_generation"`
	LifecyclePhase      string                     `json:"lifecycle_phase"`
	CreatedAt           string                     `json:"created_at"`
	ObservedAt          string                     `json:"observed_at"`
	Items               []adminBookingDetailItem   `json:"items"`
	Tickets             []adminBookingDetailTicket `json:"tickets"`
	Receipt             *adminBookingDetailReceipt `json:"receipt"`
	Refund              *adminBookingDetailRefund  `json:"refund"`
	SourceService       string                     `json:"source_service"`
	Coverage            string                     `json:"coverage"`
}

type adminBookingDetailItem struct {
	ID             string   `json:"id"`
	TicketTierID   string   `json:"ticket_tier_id"`
	TierName       string   `json:"tier_name"`
	Quantity       int32    `json:"quantity"`
	UnitPriceCents int64    `json:"unit_price_cents"`
	SeatIDs        []string `json:"seat_ids"`
}

type adminBookingDetailTicket struct {
	TicketID     string  `json:"ticket_id"`
	Status       string  `json:"status"`
	SeatID       string  `json:"seat_id"`
	TicketTierID string  `json:"ticket_tier_id"`
	IssuedAt     string  `json:"issued_at"`
	CheckedInAt  *string `json:"checked_in_at"`
	RevokedAt    *string `json:"revoked_at"`
}

type adminBookingDetailReceipt struct {
	Outcome      string  `json:"outcome"`
	ReasonCode   string  `json:"reason_code"`
	TicketCount  int32   `json:"ticket_count"`
	ReceiptCount int32   `json:"receipt_count"`
	ObservedAt   *string `json:"observed_at"`
	CreatedAt    *string `json:"created_at"`
}

type adminBookingDetailRefund struct {
	RequestID             string  `json:"request_id"`
	Status                string  `json:"status"`
	Reason                string  `json:"reason"`
	DecisionReason        string  `json:"decision_reason"`
	RefundDecisionID      string  `json:"refund_decision_id"`
	AmountCents           int64   `json:"amount_cents"`
	Currency              string  `json:"currency"`
	RequestedAt           *string `json:"requested_at"`
	DecidedAt             *string `json:"decided_at"`
	PaymentRefundState    string  `json:"payment_refund_state"`
	TicketRevocationState string  `json:"ticket_revocation_state"`
	CanWithdraw           bool    `json:"can_withdraw"`
	ApprovalEnabled       bool    `json:"approval_enabled"`
}

func projectAdminBookingDetail(source *bookingv1.AdminBookingDetail, requestedID uuid.UUID) (adminBookingDetailResponse, bool) {
	if source == nil || source.GetBooking() == nil || source.GetObservedAt() == nil || source.GetObservedAt().CheckValid() != nil {
		return adminBookingDetailResponse{}, false
	}
	booking := source.GetBooking()
	if booking.GetId() != requestedID.String() || !validUUIDText(booking.GetUserId()) || !validUUIDText(booking.GetEventId()) ||
		booking.GetTotalAmountCents() < 0 || booking.GetCreatedAt() == nil || booking.GetCreatedAt().CheckValid() != nil {
		return adminBookingDetailResponse{}, false
	}
	statusText, ok := safeAdminEventMetadataLabel(booking.GetStatus(), 50, true)
	if !ok {
		return adminBookingDetailResponse{}, false
	}
	currency := strings.ToUpper(strings.TrimSpace(source.GetCurrency()))
	if currency == "" {
		currency = strings.ToUpper(strings.TrimSpace(booking.GetCurrency()))
	}
	if currency != "USD" {
		return adminBookingDetailResponse{}, false
	}
	mode, ok := safeAdminEventMetadataLabel(source.GetBookingMode(), 32, false)
	if !ok {
		return adminBookingDetailResponse{}, false
	}
	phase, ok := safeAdminEventMetadataLabel(source.GetLifecyclePhase(), 32, false)
	if !ok {
		return adminBookingDetailResponse{}, false
	}
	observedAt := source.GetObservedAt().AsTime().UTC()
	if source.GetLifecycleGeneration() < 0 {
		return adminBookingDetailResponse{}, false
	}
	response := adminBookingDetailResponse{
		BookingID: booking.GetId(), UserID: booking.GetUserId(), EventID: booking.GetEventId(),
		BookingStatus: statusText, TotalAmountCents: booking.GetTotalAmountCents(), Currency: currency,
		BookingMode: mode, LifecycleGeneration: source.GetLifecycleGeneration(), LifecyclePhase: phase,
		CreatedAt:     booking.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano),
		ObservedAt:    observedAt.Format(time.RFC3339Nano),
		Items:         make([]adminBookingDetailItem, 0, len(booking.GetItems())),
		Tickets:       make([]adminBookingDetailTicket, 0, len(source.GetTickets())),
		SourceService: "booking", Coverage: "booking_owned_exact",
	}
	if len(booking.GetItems()) > 1000 {
		return adminBookingDetailResponse{}, false
	}
	for _, item := range booking.GetItems() {
		if item == nil || !validUUIDText(item.GetId()) || !validUUIDText(item.GetTicketTierId()) || item.GetQuantity() < 1 || item.GetUnitPriceCents() < 0 {
			return adminBookingDetailResponse{}, false
		}
		tierName, ok := safeAdminEventMetadataLabel(item.GetTierName(), 240, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		seats := make([]string, 0, len(item.GetSeatIds()))
		for _, seat := range item.GetSeatIds() {
			if !validUUIDText(seat) {
				return adminBookingDetailResponse{}, false
			}
			seats = append(seats, seat)
		}
		response.Items = append(response.Items, adminBookingDetailItem{
			ID: item.GetId(), TicketTierID: item.GetTicketTierId(), TierName: tierName,
			Quantity: item.GetQuantity(), UnitPriceCents: item.GetUnitPriceCents(), SeatIDs: seats,
		})
	}
	if len(source.GetTickets()) > 5000 {
		return adminBookingDetailResponse{}, false
	}
	for _, ticket := range source.GetTickets() {
		if ticket == nil || !validUUIDText(ticket.GetTicketId()) || !validUUIDText(ticket.GetSeatId()) || !validUUIDText(ticket.GetTicketTierId()) ||
			ticket.GetIssuedAt() == nil || ticket.GetIssuedAt().CheckValid() != nil {
			return adminBookingDetailResponse{}, false
		}
		ticketStatus, ok := safeAdminEventMetadataLabel(ticket.GetStatus(), 32, true)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		response.Tickets = append(response.Tickets, adminBookingDetailTicket{
			TicketID: ticket.GetTicketId(), Status: ticketStatus, SeatID: ticket.GetSeatId(), TicketTierID: ticket.GetTicketTierId(),
			IssuedAt:    ticket.GetIssuedAt().AsTime().UTC().Format(time.RFC3339Nano),
			CheckedInAt: adminBookingTimestamp(ticket.GetCheckedInAt()), RevokedAt: adminBookingTimestamp(ticket.GetRevokedAt()),
		})
	}
	if source.GetReceiptPresent() {
		receipt := source.GetReceipt()
		if receipt == nil || (receipt.GetOutcome() != "issued" && receipt.GetOutcome() != "denied") || receipt.GetTicketCount() < 0 || receipt.GetReceiptCount() < 1 {
			return adminBookingDetailResponse{}, false
		}
		reasonCode, ok := safeAdminEventMetadataLabel(receipt.GetReasonCode(), 80, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		response.Receipt = &adminBookingDetailReceipt{
			Outcome: receipt.GetOutcome(), ReasonCode: reasonCode, TicketCount: receipt.GetTicketCount(), ReceiptCount: receipt.GetReceiptCount(),
			ObservedAt: adminBookingTimestamp(receipt.GetObservedAt()), CreatedAt: adminBookingTimestamp(receipt.GetCreatedAt()),
		}
	}
	if source.GetRefundPresent() {
		refund := source.GetRefund()
		if refund == nil || !validUUIDText(refund.GetRequestId()) || refund.GetBookingId() != booking.GetId() ||
			refund.GetEventId() != booking.GetEventId() || refund.GetAmountCents() < 0 || strings.ToUpper(refund.GetCurrency()) != "USD" {
			return adminBookingDetailResponse{}, false
		}
		refundStatus, ok := safeAdminEventMetadataLabel(refund.GetStatus(), 48, true)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		reason, ok := safeAdminEventMetadataLabel(refund.GetReason(), 500, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		decisionReason, ok := safeAdminEventMetadataLabel(refund.GetDecisionReason(), 500, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		if refund.GetRefundDecisionId() != "" && !validUUIDText(refund.GetRefundDecisionId()) {
			return adminBookingDetailResponse{}, false
		}
		paymentState, ok := safeAdminEventMetadataLabel(refund.GetPaymentRefundState(), 48, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		ticketState, ok := safeAdminEventMetadataLabel(refund.GetTicketRevocationState(), 48, false)
		if !ok {
			return adminBookingDetailResponse{}, false
		}
		response.Refund = &adminBookingDetailRefund{
			RequestID: refund.GetRequestId(), Status: refundStatus, Reason: reason, DecisionReason: decisionReason,
			RefundDecisionID: refund.GetRefundDecisionId(), AmountCents: refund.GetAmountCents(), Currency: "USD",
			RequestedAt: adminBookingTimestamp(refund.GetRequestedAt()), DecidedAt: adminBookingTimestamp(refund.GetDecidedAt()),
			PaymentRefundState: paymentState, TicketRevocationState: ticketState,
			CanWithdraw: refund.GetCanWithdraw(), ApprovalEnabled: refund.GetApprovalEnabled(),
		}
	}
	created, err := time.Parse(time.RFC3339Nano, response.CreatedAt)
	if err != nil || created.After(observedAt.Add(time.Minute)) {
		return adminBookingDetailResponse{}, false
	}
	return response, true
}

func adminBookingTimestamp(value *timestamppb.Timestamp) *string {
	if value == nil || value.CheckValid() != nil {
		return nil
	}
	formatted := value.AsTime().UTC().Format(time.RFC3339Nano)
	return &formatted
}

func validUUIDText(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil
}

func writeAdminBookingDetailError(c *gin.Context, code int, codeText, message string) {
	c.Header("Cache-Control", "no-store")
	c.JSON(code, gin.H{"code": codeText, "error": message})
}
