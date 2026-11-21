package grpc

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (s *BookingServer) SetUnusedTicketRefundService(v *service.UnusedTicketRefundService, secure bool) {
	s.refunds = v
	s.refundsSecure = secure
}
func (s *BookingServer) refundPrincipal(ctx context.Context, admin bool) (TicketPrincipal, error) {
	if s == nil || s.refunds == nil || !s.refundsSecure {
		return TicketPrincipal{}, status.Error(codes.Unavailable, "refund service unavailable")
	}
	if err := requireVerifiedGatewayPeer(ctx); err != nil {
		return TicketPrincipal{}, err
	}
	if admin {
		p, err := s.authenticateAdminOutboxPrincipal(ctx)
		if err != nil {
			return p, err
		}
		if p.Role != "admin" {
			return TicketPrincipal{}, status.Error(codes.PermissionDenied, "refund assignment management denied")
		}
		return p, nil
	}
	return s.authenticateTicketPrincipal(ctx)
}
func refundRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, service.ErrUnusedRefundInvalidRequest):
		return status.Error(codes.InvalidArgument, "invalid refund request")
	case errors.Is(err, repository.ErrRefundRequestNotFound):
		return status.Error(codes.NotFound, "refund request not found")
	case errors.Is(err, repository.ErrRefundRequestOwnerDenied), errors.Is(err, repository.ErrRefundRequestApproverDenied), errors.Is(err, repository.ErrRefundRequestScannerDenied):
		return status.Error(codes.PermissionDenied, "refund action denied")
	case errors.Is(err, repository.ErrRefundRequestIdempotencyConflict), errors.Is(err, repository.ErrRefundRequestConflict), errors.Is(err, repository.ErrRefundApprovalAttemptConflict):
		return status.Error(codes.AlreadyExists, "refund identity conflicts")
	case errors.Is(err, repository.ErrRefundRequestNotEligible), errors.Is(err, repository.ErrRefundRequestFinal), errors.Is(err, repository.ErrRefundRequestPaymentConflict), errors.Is(err, repository.ErrRefundRequestNotApproved), errors.Is(err, repository.ErrRefundApprovalAttemptFinal):
		return status.Error(codes.FailedPrecondition, "refund action not eligible")
	default:
		return status.Error(codes.Unavailable, "refund authority unavailable")
	}
}
func refundTimestamp(v *time.Time) *timestamppb.Timestamp {
	if v == nil {
		return nil
	}
	return timestamppb.New(*v)
}
func (s *BookingServer) refundProjection(v domain.BookingRefundRequest) *bookingv1.BookingRefund {
	rev := map[string]string{"not_started": "NOT_STARTED", "pending_payment": "PENDING", "revoked": "REVOKED", "needs_attention": "NEEDS_ATTENTION"}[v.TicketRevocationState]
	decision := ""
	if v.DecisionID != uuid.Nil {
		decision = v.DecisionID.String()
	}
	return &bookingv1.BookingRefund{RequestId: v.RequestID.String(), BookingId: v.BookingID.String(), EventId: v.EventID.String(), Status: string(v.Status), Reason: v.Reason, DecisionReason: v.DecisionReason, RefundDecisionId: decision, AmountCents: v.AmountCents, Currency: v.Currency, RequestedAt: refundTimestamp(v.RequestedAt), DecidedAt: refundTimestamp(v.DecidedAt), PaymentRefundState: v.PaymentRefundState, TicketRevocationState: rev, CanWithdraw: v.Status == domain.RefundRequestRequested || v.Status == domain.RefundRequestWaitingCheckInResolution, ApprovalEnabled: s.refunds.ApprovalEnabled(), EligibilityCode: s.refunds.ApprovalBlockReason()}
}
func refundIDs(values ...string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(values))
	for i, v := range values {
		u, e := uuid.Parse(v)
		if e != nil || u == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid refund identity")
		}
		out[i] = u
	}
	return out, nil
}
func (s *BookingServer) RequestBookingRefund(ctx context.Context, req *bookingv1.RequestBookingRefundRequest) (*bookingv1.BookingRefund, error) {
	p, err := s.refundPrincipal(ctx, false)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetBookingId())
	if err != nil {
		return nil, err
	}
	v, err := s.refunds.Request(ctx, p.UserID, ids[0], req.GetReason(), req.GetIdempotencyKey())
	if err != nil {
		return nil, refundRPCError(err)
	}
	return s.refundProjection(v), nil
}
func (s *BookingServer) GetBookingRefund(ctx context.Context, req *bookingv1.GetBookingRefundRequest) (*bookingv1.BookingRefund, error) {
	p, err := s.refundPrincipal(ctx, false)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetBookingId())
	if err != nil {
		return nil, err
	}
	v, err := s.refunds.Get(ctx, p.UserID, ids[0])
	if err != nil {
		return nil, refundRPCError(err)
	}
	return s.refundProjection(v), nil
}
func (s *BookingServer) WithdrawBookingRefund(ctx context.Context, req *bookingv1.WithdrawBookingRefundRequest) (*bookingv1.BookingRefund, error) {
	p, err := s.refundPrincipal(ctx, false)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetRequestId())
	if err != nil {
		return nil, err
	}
	v, err := s.refunds.Withdraw(ctx, p.UserID, ids[0], req.GetIdempotencyKey())
	if err != nil {
		return nil, refundRPCError(err)
	}
	return s.refundProjection(v), nil
}
func (s *BookingServer) DecideBookingRefund(ctx context.Context, req *bookingv1.DecideBookingRefundRequest) (*bookingv1.BookingRefund, error) {
	p, err := s.refundPrincipal(ctx, false)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetEventId(), req.GetRequestId())
	if err != nil {
		return nil, err
	}
	kind := domain.RefundDecisionApproved
	if req.GetDecision() == "REJECT" {
		kind = domain.RefundDecisionRejected
	} else if req.GetDecision() != "APPROVE" {
		return nil, status.Error(codes.InvalidArgument, "invalid refund decision")
	}
	v, err := s.refunds.Decide(ctx, p.UserID, ids[0], ids[1], kind, req.GetReason(), req.GetIdempotencyKey())
	if err != nil {
		return nil, refundRPCError(err)
	}
	return s.refundProjection(v), nil
}
func (s *BookingServer) ListEventBookingRefunds(ctx context.Context, req *bookingv1.ListEventBookingRefundsRequest) (*bookingv1.ListEventBookingRefundsResponse, error) {
	p, err := s.refundPrincipal(ctx, false)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetEventId())
	if err != nil {
		return nil, err
	}
	limit := int(req.GetPageSize())
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 {
		return nil, status.Error(codes.InvalidArgument, "invalid page size")
	}
	rows, next, _, err := s.refunds.List(ctx, p.UserID, ids[0], req.GetCursor(), limit)
	if err != nil {
		return nil, refundRPCError(err)
	}
	result := &bookingv1.ListEventBookingRefundsResponse{NextCursor: next, ApprovalEnabled: s.refunds.ApprovalEnabled(), ApprovalBlockReason: s.refunds.ApprovalBlockReason()}
	for _, v := range rows {
		result.Refunds = append(result.Refunds, s.refundProjection(v))
	}
	return result, nil
}
func refundApproverProjection(v domain.RefundApproverAssignment) *bookingv1.EventRefundApprover {
	return &bookingv1.EventRefundApprover{EventId: v.EventID.String(), StaffUserId: v.StaffUserID.String(), Active: v.Active, ChangedAt: timestamppb.New(v.ChangedAt)}
}
func (s *BookingServer) SetEventRefundApprover(ctx context.Context, req *bookingv1.SetEventRefundApproverRequest) (*bookingv1.EventRefundApprover, error) {
	p, err := s.refundPrincipal(ctx, true)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetEventId(), req.GetStaffUserId())
	if err != nil {
		return nil, err
	}
	v, err := s.refunds.SetApprover(ctx, p.UserID, ids[0], ids[1], req.GetActive())
	if err != nil {
		return nil, refundRPCError(err)
	}
	return refundApproverProjection(v), nil
}
func (s *BookingServer) ListEventRefundApprovers(ctx context.Context, req *bookingv1.ListEventRefundApproversRequest) (*bookingv1.ListEventRefundApproversResponse, error) {
	_, err := s.refundPrincipal(ctx, true)
	if err != nil {
		return nil, err
	}
	ids, err := refundIDs(req.GetEventId())
	if err != nil {
		return nil, err
	}
	rows, err := s.refunds.ListApprovers(ctx, ids[0])
	if err != nil {
		return nil, refundRPCError(err)
	}
	out := &bookingv1.ListEventRefundApproversResponse{}
	for _, v := range rows {
		out.Assignments = append(out.Assignments, refundApproverProjection(v))
	}
	return out, nil
}
func (s *BookingServer) GetApprovedUnusedRefundDecision(ctx context.Context, req *bookingv1.GetApprovedUnusedRefundDecisionRequest) (*bookingv1.ApprovedUnusedRefundDecision, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityPayment); err != nil {
		return nil, err
	}
	if s == nil || s.refunds == nil || !s.refundsSecure {
		return nil, status.Error(codes.Unavailable, "refund authority unavailable")
	}
	ids, err := refundIDs(req.GetBookingId(), req.GetRefundDecisionId())
	if err != nil {
		return nil, err
	}
	v, err := s.refunds.ApprovedDecision(ctx, ids[0], ids[1])
	if err != nil {
		return nil, refundRPCError(err)
	}
	var qualifiedAt *timestamppb.Timestamp
	if !v.EventQualifiedAt.IsZero() {
		qualifiedAt = timestamppb.New(v.EventQualifiedAt)
	}
	return &bookingv1.ApprovedUnusedRefundDecision{RefundDecisionId: v.DecisionID.String(), Status: string(v.DecisionKind), BuyerId: v.BuyerID.String(), BookingId: v.BookingID.String(), EventId: v.EventID.String(), PaymentId: v.PaymentID.String(), PaymentIntentId: v.PaymentIntentID, ProviderAccount: v.ProviderAccount, Livemode: v.Livemode, Generation: v.Generation, AmountCents: v.AmountCents, Currency: v.Currency, ApprovedAt: refundTimestamp(v.DecidedAt), EventStartsAt: refundTimestamp(v.EventStartsAt), EventAuthorityVersion: v.EventAuthorityVersion, ApproverId: v.ApproverID.String(), QualificationAttemptId: refundOptionalUUID(v.QualificationAttemptID), EventQualifiedAt: qualifiedAt}, nil
}

func refundOptionalUUID(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
