package handler

import (
	"context"
	"encoding/csv"
	"github.com/gin-gonic/gin"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ManagementHandler struct {
	booking bookingv1.BookingServiceClient
	secure  bool
}

func NewManagementHandler(b bookingv1.BookingServiceClient, secure bool) *ManagementHandler {
	return &ManagementHandler{booking: b, secure: secure}
}
func (h *ManagementHandler) available(c *gin.Context) bool {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.booking == nil || !h.secure {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return false
	}
	return true
}
func managementError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.Aborted:
		c.JSON(409, gin.H{"code": "version_conflict", "error": "Reload the current feature version before retrying."})
	case codes.ResourceExhausted:
		c.JSON(422, gin.H{"code": "narrow_date_range", "error": "Choose a smaller date range; results exceed 10000 records."})
	default:
		operationsError(c, err)
	}
}
func featureJSON(v *bookingv1.FeatureFlag) gin.H {
	var at any
	if v.UpdatedAt != nil {
		at = v.UpdatedAt.AsTime().Format(time.RFC3339)
	}
	return gin.H{"key": v.Key, "description": v.Description, "default_enabled": v.DefaultEnabled, "enabled": v.Enabled, "version": v.Version, "updated_at": at, "actor_id": v.ActorId}
}
func (h *ManagementHandler) PublicFeatures(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	fallback := gin.H{"source_state": "unavailable", "flags": gin.H{"discovery_advanced_filters": false}}
	if h == nil || h.booking == nil || !h.secure {
		c.JSON(503, fallback)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	r, err := h.booking.GetPublicFeatures(ctx, &emptypb.Empty{})
	if err != nil || r == nil {
		c.JSON(503, fallback)
		return
	}
	flags := gin.H{"discovery_advanced_filters": false}
	for _, v := range r.Flags {
		if v.Key == "discovery_advanced_filters" {
			flags[v.Key] = v.Enabled
		}
	}
	c.JSON(200, gin.H{"source_state": "available", "flags": flags})
}
func (h *ManagementHandler) Features(c *gin.Context) {
	if !h.available(c) {
		return
	}
	ctx, cancel := context.WithTimeout(ticketIdentityContext(c), 5*time.Second)
	defer cancel()
	r, err := h.booking.ListFeatureFlags(ctx, &emptypb.Empty{})
	if err != nil {
		managementError(c, err)
		return
	}
	if r == nil {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	flags := []gin.H{}
	audit := []gin.H{}
	for _, v := range r.Flags {
		flags = append(flags, featureJSON(v))
	}
	for _, v := range r.Audit {
		audit = append(audit, gin.H{"key": v.Key, "before": v.Before, "after": v.After, "version": v.Version, "actor_id": v.ActorId, "changed_at": v.ChangedAt.AsTime().Format(time.RFC3339)})
	}
	c.JSON(200, gin.H{"flags": flags, "audit": audit, "audit_limit": 50, "observed_at": r.ObservedAt.AsTime().Format(time.RFC3339), "source": "booking.presentation_feature_flags + presentation_feature_audit"})
}
func (h *ManagementHandler) ChangeFeature(c *gin.Context) {
	if !h.available(c) {
		return
	}
	var body struct {
		Enabled *bool  `json:"enabled"`
		Version *int64 `json:"expected_version"`
	}
	if !bindAuth(c, &body) {
		return
	}
	if body.Enabled == nil || body.Version == nil || *body.Version < 0 {
		c.JSON(400, gin.H{"code": "invalid_change"})
		return
	}
	ctx, cancel := context.WithTimeout(ticketIdentityContext(c), 5*time.Second)
	defer cancel()
	r, err := h.booking.ChangeFeatureFlag(ctx, &bookingv1.ChangeFeatureFlagRequest{Key: c.Param("key"), Enabled: *body.Enabled, ExpectedVersion: *body.Version})
	if err != nil {
		managementError(c, err)
		return
	}
	if r == nil {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	c.JSON(200, featureJSON(r))
}
func (h *ManagementHandler) Metrics(c *gin.Context) {
	if !h.available(c) {
		return
	}
	now := time.Now().UTC()
	from, e1 := time.Parse(time.RFC3339, c.DefaultQuery("from", now.Add(-7*24*time.Hour).Format(time.RFC3339)))
	to, e2 := time.Parse(time.RFC3339, c.DefaultQuery("to", now.Format(time.RFC3339)))
	format := c.DefaultQuery("format", "json")
	if e1 != nil || e2 != nil || !from.Before(to) || to.Sub(from) > 31*24*time.Hour || (format != "json" && format != "csv") {
		c.JSON(400, gin.H{"code": "invalid_query"})
		return
	}
	ctx, cancel := context.WithTimeout(ticketIdentityContext(c), 10*time.Second)
	defer cancel()
	r, err := h.booking.GetOperationsMetrics(ctx, &bookingv1.OperationsQuery{From: timestamppb.New(from), To: timestamppb.New(to)})
	if err != nil {
		managementError(c, err)
		return
	}
	if r == nil {
		c.JSON(503, gin.H{"code": "source_unavailable"})
		return
	}
	if format == "csv" {
		writeMetricsCSV(c, r, from, to)
		return
	}
	money := []gin.H{}
	for _, g := range r.Money {
		var net any
		state := "partial_known"
		if g.UnknownPayments == 0 {
			state = "recorded_complete"
			net = g.CollectedKnownMinor - g.RefundedKnownMinor
		}
		money = append(money, gin.H{"currency": g.Currency, "mode": g.Mode, "state": state, "payments": g.Payments, "unknown_payments": g.UnknownPayments, "confirmed_refunds": g.ConfirmedRefunds, "collected_known_minor": g.CollectedKnownMinor, "refunded_known_minor": g.RefundedKnownMinor, "net_minor": net})
	}
	var paymentAt any
	if r.PaymentObservedAt != nil {
		paymentAt = r.PaymentObservedAt.AsTime().Format(time.RFC3339)
	}
	c.JSON(http.StatusOK, gin.H{"from": from.Format(time.RFC3339), "to": to.Format(time.RFC3339), "record_limit": 10000, "scope": r.Scope, "booking": gin.H{"source": "booking.bookings/tickets/booking_refund_requests", "observed_at": r.BookingObservedAt.AsTime().Format(time.RFC3339), "orders": r.Orders, "issued_tickets": r.IssuedTickets, "checked_in_tickets": r.CheckedInTickets, "refund_requests": r.RefundRequests, "confirmed_refunds": r.ConfirmedRefunds}, "payment": gin.H{"source": "payment.payments", "source_state": r.PaymentSourceState, "unavailable_reason": r.PaymentUnavailableReason, "observed_at": paymentAt, "groups": money}})
}
func csvSafe(s string) string {
	if len(s) > 0 && strings.ContainsRune("=+-@\t\r\n", rune(s[0])) {
		return "'" + s
	}
	return s
}
func writeMetricsCSV(c *gin.Context, r *bookingv1.OperationsMetrics, from, to time.Time) {
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", `attachment; filename="rushgo-operations.csv"`)
	w := csv.NewWriter(c.Writer)
	_ = w.Write([]string{"metric", "currency", "mode", "value", "state", "source", "observed_at", "from", "to", "scope"})
	write := func(name, currency, mode, value, state, source, at string) {
		_ = w.Write([]string{name, csvSafe(currency), csvSafe(mode), value, state, source, at, from.Format(time.RFC3339), to.Format(time.RFC3339), r.Scope})
	}
	for _, v := range []struct {
		name  string
		count int64
	}{{"orders", r.Orders}, {"issued_tickets", r.IssuedTickets}, {"checked_in_tickets", r.CheckedInTickets}, {"refund_requests", r.RefundRequests}, {"confirmed_refunds", r.ConfirmedRefunds}} {
		write(v.name, "", "", strconv.FormatInt(v.count, 10), "recorded", "booking", r.BookingObservedAt.AsTime().Format(time.RFC3339))
	}
	if r.PaymentSourceState != "available" {
		write("payment_metrics", "", "", "", "unavailable", "payment.payments", "")
	} else {
		for _, g := range r.Money {
			state := "partial_known"
			if g.UnknownPayments == 0 {
				state = "recorded_complete"
			}
			at := r.PaymentObservedAt.AsTime().Format(time.RFC3339)
			write("collected_known_minor", g.Currency, g.Mode, strconv.FormatInt(g.CollectedKnownMinor, 10), state, "payment.payments", at)
			write("refunded_known_minor", g.Currency, g.Mode, strconv.FormatInt(g.RefundedKnownMinor, 10), state, "payment.payments", at)
			write("unknown_payments", g.Currency, g.Mode, strconv.FormatInt(g.UnknownPayments, 10), state, "payment.payments", at)
			net := ""
			if g.UnknownPayments == 0 {
				net = strconv.FormatInt(g.CollectedKnownMinor-g.RefundedKnownMinor, 10)
			}
			write("net_minor", g.Currency, g.Mode, net, state, "payment.payments", at)
		}
	}
	w.Flush()
}
