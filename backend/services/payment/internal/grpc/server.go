package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	"github.com/ticketbox/pkg/grpcsecurity"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PaymentServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	logger  *zap.Logger
	service *service.PaymentService
}

func NewPaymentServer(service *service.PaymentService, logger *zap.Logger) *PaymentServer {
	return &PaymentServer{
		service: service,
		logger:  logger,
	}
}

func (p *PaymentServer) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	userId, err := uuid.Parse(req.UserId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid user id")
	}
	bookingId, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid booking id")
	}
	paymentId := uuid.New()
	payment := domain.CreatePaymentRequest{
		Payment: domain.Payment{
			ID:            paymentId,
			UserId:        userId,
			BookingId:     &bookingId,
			Price:         req.Price,
			Currency:      req.Currency,
			PaymentMethod: req.PaymentMethod,
			Status:        domain.PaymentPending,
		},
		UserEmail:         req.UserEmail,
		PaymentGeneration: req.PaymentGeneration,
	}
	res, err := p.service.CreatePayment(ctx, &payment)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return &paymentv1.CreatePaymentResponse{
		Id:                        res.PaymentID.String(),
		Status:                    res.Status,
		PaymentIntentId:           res.PaymentItentId,
		PaymentIntentClientSecret: res.PaymentIntentClientSecret,
		CreateState:               res.CreateState,
		RetryAfterSeconds:         res.RetryAfterSeconds,
	}, nil
}

func toPaymentEntry(payment *domain.Payment) paymentv1.PaymentEntry {
	bookingId := uuid.UUID{}
	if payment.BookingId != nil {
		bookingId = *payment.BookingId
	}
	orderId := uuid.UUID{}
	if payment.OrderId != nil {
		orderId = *payment.OrderId
	}
	transactionId := uuid.UUID{}
	if payment.Transaction_id != nil {
		transactionId = *payment.Transaction_id
	}
	return paymentv1.PaymentEntry{
		Id:              payment.ID.String(),
		UserId:          payment.UserId.String(),
		BookingId:       bookingId.String(),
		OrderId:         orderId.String(),
		Status:          string(payment.Status),
		Price:           payment.Price,
		Currency:        payment.Currency,
		TransactionId:   transactionId.String(),
		PaymentIntentId: payment.PaymentIntentId,
		PaymentMethod:   payment.PaymentMethod,
		CreatedAt:       timestamppb.New(payment.CreatedAt),
		UpdatedAt:       timestamppb.New(payment.UpdateAt),
	}
}

func financialPaymentEntry(view *domain.FinancialView) *paymentv1.PaymentEntry {
	entry := toPaymentEntry(view.Payment)
	entry.ProviderOutcome, entry.ProviderStatus = view.ProviderOutcome, view.ProviderStatus
	entry.ReconciliationState, entry.RefundState = view.ReconciliationState, view.RefundState
	entry.RefundAmountCents, entry.RefundId, entry.RefundStatus = view.RefundAmountCents, view.RefundID, view.RefundStatus
	entry.PaymentGeneration, entry.BookingDisposition, entry.AttentionReason = view.Generation, view.BookingDisposition, view.ErrorCategory
	return &entry
}

func financialRPCError(err error) error {
	switch {
	case errors.Is(err, service.ErrAccountingNotReady):
		return status.Error(codes.FailedPrecondition, "accounting_not_ready")
	case errors.Is(err, service.ErrAccountingUnavailable):
		return status.Error(codes.Unavailable, "accounting_readiness_unavailable")
	case errors.Is(err, repository.RecordNotFound):
		return status.Error(codes.NotFound, "payment not found")
	case errors.Is(err, repository.ErrInvalidFinancialIdentity):
		return status.Error(codes.InvalidArgument, "invalid financial identity")
	case errors.Is(err, repository.ErrFinancialStateProtected):
		return status.Error(codes.FailedPrecondition, "financial_state_protected")
	case errors.Is(err, repository.ErrFinancialConflict), errors.Is(err, repository.ErrProviderEvidenceConflict):
		return status.Error(codes.FailedPrecondition, "financial identity or Booking disposition conflict")
	default:
		return status.Error(codes.Unavailable, "financial responsibility unavailable; retry same identity")
	}
}

func (p *PaymentServer) CheckAdmissionFinancialEligibility(ctx context.Context, req *paymentv1.AdmissionFinancialEligibilityRequest) (*paymentv1.AdmissionFinancialEligibilityResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "payment and booking identity required")
	}
	paymentID, err := uuid.Parse(req.PaymentId)
	if err != nil || paymentID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment id")
	}
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	if req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid payment generation")
	}
	evidence := p.service.CheckAdmissionFinancialEligibility(ctx, paymentID, bookingID, req.Generation)
	return admissionEvidenceToProto(evidence), nil
}

func admissionEvidenceToProto(evidence domain.AdmissionProviderEvidence) *paymentv1.AdmissionFinancialEligibilityResponse {
	response := &paymentv1.AdmissionFinancialEligibilityResponse{
		EligibilityState: admissionEligibilityToProto(evidence.EligibilityState),
		DisputeState:     admissionDisputeStateToProto(evidence.DisputeState),
		RefundState:      admissionRefundStateToProto(evidence.RefundState),
		Source:           evidence.Source,
		ReasonCode:       evidence.ReasonCode,
	}
	if !evidence.ObservedAt.IsZero() {
		response.ObservedAt = timestamppb.New(evidence.ObservedAt.UTC())
	}
	return response
}

func admissionEligibilityToProto(state domain.AdmissionEligibilityState) paymentv1.AdmissionEligibilityState {
	switch state {
	case domain.AdmissionEligibilityEligible:
		return paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE
	case domain.AdmissionEligibilityDenied:
		return paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_DENIED
	default:
		return paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN
	}
}

func admissionDisputeStateToProto(state domain.DisputeEvidenceState) paymentv1.AdmissionDisputeEvidenceState {
	switch state {
	case domain.DisputeEvidenceNoDispute:
		return paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE
	case domain.DisputeEvidencePresent:
		return paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_DISPUTE_PRESENT
	default:
		return paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_UNKNOWN
	}
}

func admissionRefundStateToProto(state domain.RefundEvidenceState) paymentv1.AdmissionRefundEvidenceState {
	switch state {
	case domain.RefundEvidenceNoRefund:
		return paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND
	case domain.RefundEvidencePresent:
		return paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_REFUND_PRESENT
	default:
		return paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_UNKNOWN
	}
}

func (p *PaymentServer) GetFinancialPayment(ctx context.Context, req *paymentv1.FinancialPaymentRequest) (*paymentv1.PaymentEntry, error) {
	var payment, booking uuid.UUID
	var err error
	if req.PaymentId != "" {
		payment, err = uuid.Parse(req.PaymentId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid payment id")
		}
	}
	if req.BookingId != "" {
		booking, err = uuid.Parse(req.BookingId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid booking id")
		}
	}
	view, err := p.service.GetFinancialPayment(ctx, payment, booking, req.Generation)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return financialPaymentEntry(view), nil
}

// GetPaidRecoveryPaymentSnapshot is Saga-only and read-only. It includes
// provider eligibility evidence for the exact Payment identity and is not a
// public Payment read or a financial decision/write.
func (p *PaymentServer) GetPaidRecoveryPaymentSnapshot(ctx context.Context, req *paymentv1.FinancialPaymentRequest) (*paymentv1.PaidRecoveryPaymentSnapshot, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentitySaga); err != nil {
		return nil, err
	}
	if req == nil || req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "valid paid recovery Payment identity required")
	}
	paymentID, err := uuid.Parse(req.GetPaymentId())
	if err != nil || paymentID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment id")
	}
	bookingID, err := uuid.Parse(req.GetBookingId())
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	if p == nil || p.service == nil {
		return nil, status.Error(codes.Unavailable, "paid recovery Payment authority unavailable")
	}
	snapshot, err := p.service.GetPaidRecoveryPaymentSnapshot(ctx, paymentID, bookingID, req.Generation)
	if err != nil {
		return nil, financialRPCError(err)
	}
	if snapshot == nil || snapshot.Financial == nil || snapshot.Financial.Payment == nil {
		return nil, status.Error(codes.Unavailable, "paid recovery Payment snapshot unavailable")
	}
	livemode := false
	if snapshot.Financial.LiveMode != nil {
		livemode = *snapshot.Financial.LiveMode
	}
	return &paymentv1.PaidRecoveryPaymentSnapshot{
		Payment:                  financialPaymentEntry(snapshot.Financial),
		ProviderAccount:          snapshot.Financial.ProviderAccount,
		Livemode:                 livemode,
		ProviderIdentityVerified: snapshot.ProviderIdentityVerified,
		Admission:                admissionEvidenceToProto(snapshot.Admission),
	}, nil
}

func (p *PaymentServer) RequestPaymentResolution(ctx context.Context, req *paymentv1.PaymentResolutionRequest) (*paymentv1.PaymentEntry, error) {
	payment, err := uuid.Parse(req.PaymentId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment id")
	}
	booking, err := uuid.Parse(req.BookingId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	decision, err := uuid.Parse(req.DecisionId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid decision id")
	}
	view, err := p.service.RequestPaymentResolution(ctx, payment, booking, req.Generation, decision, req.Reason)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return financialPaymentEntry(view), nil
}

// AcceptApprovedUnusedRefundDecision is Booking-only. It reloads the
// authoritative immutable decision before Payment persists the sole refund
// obligation for this Payment row.
func (p *PaymentServer) AcceptApprovedUnusedRefundDecision(ctx context.Context, req *paymentv1.AcceptApprovedUnusedRefundDecisionRequest) (*paymentv1.PaymentEntry, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	if req == nil || req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "valid refund decision identity required")
	}
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	decisionID, err := uuid.Parse(req.RefundDecisionId)
	if err != nil || decisionID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid refund decision id")
	}
	paymentID, err := uuid.Parse(req.PaymentId)
	if err != nil || paymentID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment id")
	}
	view, err := p.service.AcceptApprovedUnusedRefundDecision(ctx, bookingID, decisionID, paymentID, req.Generation)
	if err != nil {
		return nil, financialAuthorityRPCError(err)
	}
	return financialPaymentEntry(view), nil
}

// GetUnusedRefundPaymentSnapshot is a Booking-only internal projection. The
// provider identity in this response is never exposed through public HTTP.
func (p *PaymentServer) GetUnusedRefundPaymentSnapshot(ctx context.Context, req *paymentv1.UnusedRefundPaymentSnapshotRequest) (*paymentv1.UnusedRefundPaymentSnapshot, error) {
	if err := grpcsecurity.RequireAdmissionOwner(ctx, grpcsecurity.ServiceIdentityBooking); err != nil {
		return nil, err
	}
	if req == nil || req.Generation != 1 {
		return nil, status.Error(codes.InvalidArgument, "valid Payment snapshot identity required")
	}
	bookingID, err := uuid.Parse(req.BookingId)
	if err != nil || bookingID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid booking id")
	}
	paymentID, err := uuid.Parse(req.PaymentId)
	if err != nil || paymentID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payment id")
	}
	view, eligible, err := p.service.GetUnusedRefundPaymentSnapshot(ctx, bookingID, paymentID, req.Generation, req.GetForApproval())
	if err != nil {
		return nil, financialRPCError(err)
	}
	if view == nil || view.Payment == nil {
		return nil, status.Error(codes.Unavailable, "Payment snapshot unavailable")
	}
	decisionID := ""
	if view.RefundDecisionID != nil {
		decisionID = view.RefundDecisionID.String()
	}
	return &paymentv1.UnusedRefundPaymentSnapshot{
		BookingId: bookingID.String(), PaymentId: view.Payment.ID.String(), BuyerId: view.Payment.UserId.String(),
		PaymentIntentId: view.Payment.PaymentIntentId, ProviderAccount: view.ProviderAccount,
		Livemode: view.LiveMode != nil && *view.LiveMode, Generation: view.Generation,
		AmountCents: int64(view.Payment.Price), Currency: view.Payment.Currency,
		RefundDecisionId: decisionID, RefundState: view.RefundState, RefundAmountCents: view.RefundAmountCents,
		RefundStatus:        view.RefundStatus,
		Collected:           view.ProviderOutcome == "collected" && view.Payment.Status == domain.PaymentSuccess,
		EligibleForApproval: eligible,
	}, nil
}

func financialAuthorityRPCError(err error) error {
	if remote, ok := status.FromError(err); ok {
		return status.Error(remote.Code(), remote.Message())
	}
	return financialRPCError(err)
}

func (p *PaymentServer) GetPaymentById(ctx context.Context, req *paymentv1.GetPaymentByIdReq) (*paymentv1.PaymentEntry, error) {
	paymentId, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid payment id")
	}
	res, err := p.service.GetFinancialForRead(ctx, paymentId)
	if err != nil {
		p.logger.Sugar().Errorf("get payment by id fail: %w", err)
		if errors.Is(err, repository.RecordNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, err
	}
	if res == nil {
		return nil, status.Error(codes.Internal, "get payment by id return nil")
	}
	return financialPaymentEntry(res), nil
}

func (p *PaymentServer) GetPayments(ctx context.Context, req *paymentv1.GetPaymentsReqeust) (*paymentv1.PaymentList, error) {
	user, err := uuid.Parse(req.UserId)
	if err != nil || user == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "valid user id required")
	}
	condition := &domain.GetPaymentsByCondition{UserId: &user, Status: req.Status, Currency: req.Currency, Price: req.Price, PaymentMethod: req.PaymentMethod}
	if req.BookingId != "" {
		id, err := uuid.Parse(req.BookingId)
		if err != nil || id == uuid.Nil {
			return nil, status.Error(codes.InvalidArgument, "invalid booking id")
		}
		condition.BookingId = &id
	}
	payments, err := p.service.GetPaymentsByCondition(ctx, condition)
	if err != nil {
		return nil, financialRPCError(err)
	}
	response := &paymentv1.PaymentList{Payments: make([]*paymentv1.PaymentEntry, 0, len(payments))}
	for _, payment := range payments {
		view, err := p.service.GetFinancialForRead(ctx, payment.ID)
		if err != nil {
			return nil, financialRPCError(err)
		}
		response.Payments = append(response.Payments, financialPaymentEntry(view))
	}
	return response, nil
}

func (p *PaymentServer) UpdatePaymentStatus(ctx context.Context, req *paymentv1.UpdatePaymentStatusRequest) (*emptypb.Empty, error) {
	paymentId, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid payment id")
	}
	err = p.service.UpdatePaymentStatus(ctx, paymentId, domain.PaymentStatus(req.Status))
	if err != nil {
		return nil, financialRPCError(err)
	}
	return nil, nil
}

func (p *PaymentServer) UpdatePayment(ctx context.Context, req *paymentv1.UpdatePaymentRequest) (*emptypb.Empty, error) {

	orderId, err := uuid.Parse(req.OrderId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid Order Id")
	}
	paymentId, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid payment id")
	}
	transactionId, err := uuid.Parse(req.TransactionId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid transaction id")
	}
	payment := domain.Payment{
		OrderId:        &orderId,
		Transaction_id: &transactionId,
		ID:             paymentId,
	}
	err = p.service.UpdatePayment(ctx, &payment)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return nil, nil
}

func (p *PaymentServer) DeletePayment(ctx context.Context, req *paymentv1.DeletePaymentRequest) (*emptypb.Empty, error) {
	paymentId, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid payment id")
	}
	err = p.service.SoftDeletePayment(ctx, paymentId)
	if err != nil {
		return nil, financialRPCError(err)
	}
	return nil, nil
}

func (p *PaymentServer) UpdatePaymentStatusByPaymentIntentId(ctx context.Context, req *paymentv1.UpdatePaymentStatusByPaymentIntentIdReq) (*emptypb.Empty, error) {

	err := p.service.UpdatePaymentStatusByPaymentIntentId(ctx, req.PaymentIntentId, domain.PaymentStatus(req.Status))
	if err != nil {
		return nil, financialRPCError(err)
	}
	return nil, nil
}

func (p *PaymentServer) GetPaymentByPaymentIntentId(ctx context.Context, req *paymentv1.GetPaymentByIntentIdReq) (*paymentv1.PaymentEntry, error) {
	res, err := p.service.GetPaymentByPaymentIntentId(ctx, req.PaymentIntentId)
	if err != nil {
		p.logger.Sugar().Errorf("get payment by payment intent id fail: %w", err)
		if errors.Is(err, repository.RecordNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, err
	}
	if res == nil {
		return nil, status.Error(codes.Internal, "get payment by payment_intent_id return nil")
	}
	payment := toPaymentEntry(res)
	return &payment, nil
}

func (p *PaymentServer) MakeRefundPayment(ctx context.Context, req *paymentv1.MakeRefundPaymentReq) (*emptypb.Empty, error) {
	err := p.service.MakePaymentRefund(ctx, &domain.MakePaymentRefundRequest{
		PaymentIntentId: req.PaymentIntentId,
		Amount:          int(req.Price),
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (p *PaymentServer) GetPaymentsByBookingIds(ctx context.Context, req *paymentv1.GetPaymentsByBookingIdsReq) (*paymentv1.PaymentList, error) {
	if len(req.BookingIds) == 0 {
		p.logger.Error("[PaymentServer][GetPaymentsByBookingIds] Booking ids are required")
		return nil, fmt.Errorf("booking ids are required")
	}

	bookingUUIDs := make([]uuid.UUID, 0, len(req.BookingIds))
	for _, bookingId := range req.BookingIds {
		bookingUUID, err := uuid.Parse(bookingId)
		if err != nil {
			return nil, fmt.Errorf("[PaymentServer][GetPaymentsByBookingIds] exist booking id is invalid: %w", err)
		}
		bookingUUIDs = append(bookingUUIDs, bookingUUID)
	}

	res, err := p.service.GetPaymentsByBookingIds(ctx, bookingUUIDs)
	if err != nil {
		return nil, fmt.Errorf("[PaymentServer][GetPaymentsByBookingIds] get payments by bookingIds fails: %w", err)
	}

	paymentEntries := make([]*paymentv1.PaymentEntry, 0, len(res))
	for _, payment := range res {
		paymentEntry := toPaymentEntry(&payment)
		paymentEntries = append(paymentEntries, &paymentEntry)
	}
	return &paymentv1.PaymentList{
		Payments: paymentEntries,
	}, nil
}
