package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	payment_gateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
)

type PaymentService struct {
	repo                               repository.PaymentRepositoryInterface
	gateway                            payment_gateway.PaymentGatewayInterface
	logger                             *zap.Logger
	booking                            bookingv1.BookingServiceClient
	approvedUnusedRefundDecisionReader ApprovedUnusedRefundDecisionReader
	accounting                         AccountingReadinessChecker
	account                            string
	liveMode                           bool
	provider                           payment_gateway.FinancialProvider
	admission                          payment_gateway.AdmissionEvidenceProvider
	receiptReliability                 RecoveryHook
}

func NewPaymentService(repo repository.PaymentRepositoryInterface, logger *zap.Logger, gateway payment_gateway.PaymentGatewayInterface) *PaymentService {
	return &PaymentService{
		repo:    repo,
		logger:  logger,
		gateway: gateway,
	}
}

func (p *PaymentService) SetFinancialAuthority(booking bookingv1.BookingServiceClient, account string, live bool) {
	p.booking, p.account, p.liveMode = booking, account, live
	if booking == nil {
		p.approvedUnusedRefundDecisionReader = nil
	} else {
		p.approvedUnusedRefundDecisionReader = bookingApprovedUnusedRefundDecisionReader{client: booking}
	}
	p.provider, _ = p.gateway.(payment_gateway.FinancialProvider)
	p.admission, _ = p.gateway.(payment_gateway.AdmissionEvidenceProvider)
}

func (p *PaymentService) SetApprovedUnusedRefundDecisionReader(reader ApprovedUnusedRefundDecisionReader) {
	p.approvedUnusedRefundDecisionReader = reader
}

func (p *PaymentService) SetAccountingReadiness(checker AccountingReadinessChecker) {
	p.accounting = checker
}

func (p *PaymentService) SetReceiptReliabilityHook(hook RecoveryHook) {
	p.receiptReliability = hook
}

func (p *PaymentService) AcceptProviderReceipt(ctx context.Context, r domain.ProviderReceipt) error {
	stored, inserted, err := p.repo.AcceptProviderReceipt(ctx, r)
	if err != nil {
		return err
	}
	if err := p.observeW28Receipt(ctx, r, stored, inserted); err != nil {
		return err
	}
	if r.RefundAudit != nil {
		p.appendVerifiedRefundEventAudit(ctx, r)
	}
	return nil
}

func (p *PaymentService) appendVerifiedRefundEventAudit(ctx context.Context, receipt domain.ProviderReceipt) {
	if p == nil || receipt.RefundAudit == nil {
		return
	}
	event := *receipt.RefundAudit
	if event.ReceiptID != receipt.ID || event.EventID != receipt.Observation.EventID || event.EventType != receipt.EventType {
		p.logRefundAuditDiagnostic("verified_event", "sidecar_identity_mismatch")
		return
	}
	auditRepo, ok := p.repo.(repository.RefundAuditRepository)
	if !ok {
		p.logRefundAuditDiagnostic("verified_event", "audit_repository_unavailable")
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()

	link, classification, source := correlateVerifiedRefundEvent(auditCtx, auditRepo, event, p)
	event.LinkClassification = classification
	event.LinkSource = source
	record, err := domain.NewRefundVerifiedEventRecord(event, link)
	if err != nil && link != nil {
		// Preserve the accepted event facts even if the final constructor finds
		// a mismatch in the exact-link assertions. Never retry with a looser link.
		event.LinkClassification = domain.RefundAuditLinkConflict
		event.LinkSource = ""
		record, err = domain.NewRefundVerifiedEventRecord(event, nil)
	}
	if err != nil {
		p.logRefundAuditDiagnostic("verified_event", "sidecar_record_invalid")
		return
	}
	if err := auditRepo.AppendRefundAudit(auditCtx, record); err != nil {
		p.logRefundAuditDiagnostic("verified_event", "audit_append_failed")
	}
}

func correlateVerifiedRefundEvent(ctx context.Context, auditRepo repository.RefundAuditRepository, event domain.VerifiedRefundEventAudit, service *PaymentService) (*domain.RefundAuditRecord, string, string) {
	if event.RefundID == "" || event.ObjectType != "refund" || event.PaymentIntentID == "" || event.AmountCents == nil || event.Currency == "" ||
		event.ObservedProviderAccount == nil || *event.ObservedProviderAccount == "" || event.ObservedLiveMode == nil {
		return nil, domain.RefundAuditLinkUncorrelated, ""
	}
	var requestMatches []domain.RefundAuditRecord
	if event.ProviderRequestID != "" {
		var lookupErr error
		requestMatches, lookupErr = auditRepo.ListRefundAudit(ctx, domain.RefundAuditQuery{
			ProviderRequestID: event.ProviderRequestID, Kind: domain.RefundAuditKindResponse, Limit: 100,
		})
		if lookupErr != nil {
			service.logRefundAuditDiagnostic("verified_event_link", "request_lookup_failed")
			requestMatches = nil
		}
	}
	refundMatches, err := auditRepo.ListRefundAudit(ctx, domain.RefundAuditQuery{
		ProviderRefundID: event.RefundID, Kind: domain.RefundAuditKindResponse, Limit: 100,
	})
	if err != nil {
		service.logRefundAuditDiagnostic("verified_event_link", "refund_lookup_failed")
		return nil, domain.RefundAuditLinkUncorrelated, ""
	}
	if len(refundMatches) == 0 {
		return nil, domain.RefundAuditLinkUncorrelated, ""
	}
	if len(refundMatches) != 1 {
		return nil, domain.RefundAuditLinkAmbiguous, ""
	}
	candidate := refundMatches[0]
	switch refundResponseLinkClassification(candidate, event) {
	case domain.RefundAuditLinkUncorrelated:
		return nil, domain.RefundAuditLinkUncorrelated, ""
	case domain.RefundAuditLinkConflict:
		return nil, domain.RefundAuditLinkConflict, ""
	}
	source := domain.RefundAuditLinkSourceRefundID
	if event.ProviderRequestID != "" && candidate.Response.ProviderRequestID == event.ProviderRequestID && len(requestMatches) == 1 && requestMatches[0].RecordID == candidate.RecordID {
		source = domain.RefundAuditLinkSourceRequestAndID
	}
	return &candidate, domain.RefundAuditLinkExactRefundIdentity, source
}

func refundResponseLinkClassification(candidate domain.RefundAuditRecord, event domain.VerifiedRefundEventAudit) string {
	if candidate.Kind != domain.RefundAuditKindResponse || candidate.Response == nil || candidate.Response.Classification != domain.RefundAuditResponseObserved ||
		len(candidate.Response.ConflictingFields) != 0 || candidate.Response.RefundID == "" || candidate.Response.RefundID != event.RefundID || event.ObjectType != "refund" || event.ObjectID != event.RefundID {
		return domain.RefundAuditLinkConflict
	}
	if candidate.TargetAmountCents == nil || candidate.Response.AmountCents == nil || event.AmountCents == nil ||
		candidate.Currency == "" || candidate.Response.Currency == "" || event.Currency == "" ||
		candidate.PaymentIntentID == "" || candidate.Response.PaymentIntentID == "" || event.PaymentIntentID == "" {
		return domain.RefundAuditLinkUncorrelated
	}
	if *candidate.TargetAmountCents != *candidate.Response.AmountCents || *candidate.Response.AmountCents != *event.AmountCents ||
		candidate.Currency != candidate.Response.Currency || candidate.Response.Currency != event.Currency ||
		candidate.PaymentIntentID != candidate.Response.PaymentIntentID || candidate.Response.PaymentIntentID != event.PaymentIntentID {
		return domain.RefundAuditLinkConflict
	}
	if candidate.Response.ObservedProviderAccount == nil || *candidate.Response.ObservedProviderAccount == "" ||
		candidate.Response.ObservedLiveMode == nil || event.ObservedProviderAccount == nil || *event.ObservedProviderAccount == "" || event.ObservedLiveMode == nil {
		return domain.RefundAuditLinkUncorrelated
	}
	if *candidate.Response.ObservedProviderAccount != *event.ObservedProviderAccount || *candidate.Response.ObservedLiveMode != *event.ObservedLiveMode ||
		(candidate.IntendedAccount != "" && candidate.IntendedAccount != *candidate.Response.ObservedProviderAccount) ||
		(candidate.IntendedLiveMode != nil && *candidate.IntendedLiveMode != *candidate.Response.ObservedLiveMode) ||
		(candidate.IntendedAccount != "" && candidate.IntendedAccount != *event.ObservedProviderAccount) ||
		(candidate.IntendedLiveMode != nil && *candidate.IntendedLiveMode != *event.ObservedLiveMode) {
		return domain.RefundAuditLinkConflict
	}
	if event.ChargeID != "" && candidate.Response.ChargeID != "" && candidate.Response.ChargeID != event.ChargeID {
		return domain.RefundAuditLinkConflict
	}
	return domain.RefundAuditLinkExactRefundIdentity
}

func (p *PaymentService) logRefundAuditDiagnostic(stage, category string) {
	if p == nil || p.logger == nil {
		return
	}
	p.logger.Warn("refund provenance sidecar diagnostic", zap.String("stage", stage), zap.String("category", category))
}

func (p *PaymentService) GetFinancialPayment(ctx context.Context, payment, booking uuid.UUID, generation int64) (*domain.FinancialView, error) {
	if (payment == uuid.Nil) == (booking == uuid.Nil) || generation != 1 {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	if payment != uuid.Nil {
		view, err := p.repo.GetFinancial(ctx, payment)
		if err == nil && view.Generation != 1 {
			return nil, repository.ErrFinancialConflict
		}
		return view, err
	}
	return p.repo.GetByLogicalIdentity(ctx, booking, generation)
}

func (p *PaymentService) RequestPaymentResolution(ctx context.Context, payment, booking uuid.UUID, generation int64, decision uuid.UUID, reason string) (*domain.FinancialView, error) {
	if payment == uuid.Nil || booking == uuid.Nil || generation != 1 || decision == uuid.Nil || p.booking == nil {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	financial, err := p.repo.GetFinancial(ctx, payment)
	if err != nil {
		return nil, err
	}
	if financial.Generation != generation || financial.Payment.BookingId == nil || *financial.Payment.BookingId != booking {
		return nil, repository.ErrFinancialConflict
	}
	view, err := p.readEligibleBooking(ctx, booking, generation)
	if err != nil {
		return nil, err
	}
	disposition := repository.TerminalDisposition(booking, decision)
	if view.Phase != "TERMINAL" || view.DecisionId != decision.String() || view.Booking.Status != disposition || disposition == "" || view.Booking.UserId != financial.Payment.UserId.String() || view.Booking.TotalAmountCents != int64(financial.Payment.Price) || view.Currency != financial.Payment.Currency {
		return nil, repository.ErrFinancialConflict
	}
	// The caller's reason is diagnostic only; immutable obligation reason is
	// derived from Booking's winning terminal decision.
	_ = reason
	if financial.ProviderOutcome == "collected" {
		return p.repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: payment, DecisionID: decision, Generation: generation, AmountCents: int64(financial.Payment.Price), Currency: financial.Payment.Currency, Reason: disposition, Key: "ticketbox:payment:" + payment.String() + ":g1:refund"})
	}
	return p.repo.RequireCancel(ctx, payment, decision)
}

func (p *PaymentService) CreatePayment(ctx context.Context, req *domain.CreatePaymentRequest) (*domain.CreatePaymentResponse, error) {
	if req == nil || req.BookingId == nil || p.booking == nil || p.provider == nil || p.account == "" || p.liveMode {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	generation := req.PaymentGeneration
	if generation == 0 {
		generation = 1
	}
	if generation != 1 {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	view, err := p.readEligibleBooking(ctx, *req.BookingId, generation)
	if err != nil {
		return nil, err
	}
	if view.Booking == nil || view.Booking.UserId != req.UserId.String() || view.Booking.TotalAmountCents < 1 || view.Booking.TotalAmountCents > 2147483647 || int64(req.Price) != view.Booking.TotalAmountCents || req.Currency != "usd" || view.Currency != "usd" {
		return nil, repository.ErrFinancialConflict
	}
	if bookingPayable(view) {
		if err := checkAccountingReadiness(ctx, p.accounting, view); err != nil {
			return nil, err
		}
	}
	i := domain.FinancialIdentity{BookingID: *req.BookingId, UserID: req.UserId, Generation: generation, ProviderAccount: p.account, LiveMode: p.liveMode, AmountCents: view.Booking.TotalAmountCents, Currency: view.Currency}
	frozen, _ := json.Marshal(domain.FrozenCreateParameters{Version: 1, Amount: i.AmountCents, Currency: i.Currency, AutomaticPaymentMethods: domain.AutomaticPaymentMethods{Enabled: true, AllowRedirects: "always"}, ReceiptEmail: req.UserEmail})
	i.Fingerprint, err = domain.FinancialFingerprint(i, frozen)
	if err != nil {
		return nil, err
	}
	id := uuid.New()
	op := domain.CreateOperation{Identity: i, PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen}
	financial, _, err := p.repo.CreateOrGet(ctx, op)
	if err != nil {
		return nil, err
	}
	response := &domain.CreatePaymentResponse{PaymentID: financial.Payment.ID, CreateState: financial.CreateState, RetryAfterSeconds: 1, Status: string(financial.Payment.Status), Currency: i.Currency}
	if !bookingPayable(view) || financial.CreateState == "create_attention" {
		response.RetryAfterSeconds = 0
		return response, nil
	}
	if financial.Payment.PaymentIntentId != "" {
		providerCtx, providerCancel := context.WithTimeout(ctx, 10*time.Second)
		o, secret, err := p.provider.RetrieveIntent(providerCtx, p.account, p.liveMode, financial.Payment.PaymentIntentId)
		providerCancel()
		if err != nil {
			return nil, err
		}
		if o.PaymentID != financial.Payment.ID || o.BookingID != i.BookingID || o.AmountCents != i.AmountCents || o.Currency != i.Currency || o.Generation != generation {
			return nil, repository.ErrProviderEvidenceConflict
		}
		if err := p.repo.RecordObservation(ctx, *o); err != nil {
			return nil, err
		}
		response.PaymentItentId = o.IntentID
		response.CreateState = "create_ready"
		response.RetryAfterSeconds = 0
		latest, err := p.readEligibleBooking(ctx, i.BookingID, generation)
		if err != nil {
			return nil, err
		}
		if bookingPayable(latest) && o.Status != "succeeded" && o.Status != "canceled" {
			response.PaymentIntentClientSecret = secret
		}
		return response, nil
	}
	claim, err := p.repo.ClaimCreatePayment(ctx, financial.Payment.ID, 30*time.Second)
	if err != nil {
		return nil, err
	}
	if claim == nil {
		return response, nil
	}
	worker := NewFinancialWorker(p)
	secret, err := worker.executeCreate(ctx, *claim)
	if err != nil {
		return nil, err
	}
	financial, err = p.repo.GetFinancial(ctx, financial.Payment.ID)
	if err != nil {
		return nil, err
	}
	response.CreateState = financial.CreateState
	response.PaymentItentId = financial.Payment.PaymentIntentId
	if response.CreateState == "create_ready" {
		response.RetryAfterSeconds = 0
	}
	latest, err := p.readEligibleBooking(ctx, i.BookingID, generation)
	if err != nil {
		return nil, err
	}
	if bookingPayable(latest) && financial.ProviderOutcome != "collected" && financial.ProviderStatus != "canceled" {
		response.PaymentIntentClientSecret = secret
	}
	return response, nil
}

func bookingPayable(view *bookingv1.LifecycleDisposition) bool {
	return view != nil && (view.Phase == "OPEN" || view.Phase == "CONFIRMING") && view.ReservationDeadline != nil && view.ReservationDeadline.IsValid() && view.ReservationDeadline.AsTime().After(time.Now())
}

func (p *PaymentService) readEligibleBooking(ctx context.Context, id uuid.UUID, generation int64) (*bookingv1.LifecycleDisposition, error) {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	view, err := p.booking.GetLifecycleDisposition(bounded, &bookingv1.LifecycleRequest{BookingId: id.String(), Generation: generation})
	if err != nil {
		return nil, err
	}
	if view == nil || view.Generation != generation || view.Booking == nil || view.Booking.Id != id.String() || (view.Phase != "OPEN" && view.Phase != "CONFIRMING" && view.Phase != "TERMINAL") {
		return nil, errors.New("authoritative Booking disposition unavailable")
	}
	return view, nil
}

func (p *PaymentService) GetPaymentById(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	return p.repo.GetPaymentByID(ctx, id)
}

// Historical rows remain readable with explicit unknown financial fields.
func (p *PaymentService) GetFinancialForRead(ctx context.Context, id uuid.UUID) (*domain.FinancialView, error) {
	return p.repo.GetFinancial(ctx, id)
}

func (p *PaymentService) GetPaymentsByCondition(ctx context.Context, req *domain.GetPaymentsByCondition) ([]domain.Payment, error) {
	if req == nil || req.UserId == nil || *req.UserId == uuid.Nil {
		return nil, repository.ErrInvalidFinancialIdentity
	}
	return p.repo.GetListPaymentsByCondition(ctx, req)
}

func (p *PaymentService) UpdatePaymentStatus(ctx context.Context, id uuid.UUID, status domain.PaymentStatus) error {
	return p.repo.UpdatePaymentStatus(ctx, id, status)
}

func (p *PaymentService) UpdatePayment(ctx context.Context, payment *domain.Payment) error {
	return p.repo.UpdatePayment(ctx, payment)
}

func (p *PaymentService) SoftDeletePayment(ctx context.Context, ID uuid.UUID) error {
	return p.repo.SoftDeletePayment(ctx, ID)
}

func (p *PaymentService) DeletePayment(ctx context.Context, id uuid.UUID) error {
	return p.repo.DeletePayment(ctx, id)
}

func (p *PaymentService) UpdatePaymentStatusByPaymentIntentId(ctx context.Context, paymentIntentId string, status domain.PaymentStatus) error {
	return p.repo.UpdatePaymentStatusByPaymentIntentId(ctx, paymentIntentId, status)
}

func (p *PaymentService) GetPaymentByPaymentIntentId(ctx context.Context, paymentIntentId string) (*domain.Payment, error) {
	return p.repo.GetPaymentByPaymentIntentId(ctx, paymentIntentId)
}

func (p *PaymentService) MakePaymentRefund(ctx context.Context, req *domain.MakePaymentRefundRequest) error {
	if req == nil || req.PaymentIntentId == "" || p.booking == nil {
		return repository.ErrInvalidFinancialIdentity
	}
	payment, err := p.repo.GetPaymentByPaymentIntentId(ctx, req.PaymentIntentId)
	if err != nil {
		return err
	}
	if payment.BookingId == nil || int(payment.Price) != req.Amount {
		return repository.ErrFinancialConflict
	}
	financial, err := p.repo.GetFinancial(ctx, payment.ID)
	if err != nil {
		return err
	}
	if financial.Generation != 1 || financial.ProviderOutcome != "collected" {
		return repository.ErrFinancialConflict
	}
	view, err := p.readEligibleBooking(ctx, *payment.BookingId, 1)
	if err != nil {
		return err
	}
	decision, err := uuid.Parse(view.DecisionId)
	if err != nil {
		return repository.ErrFinancialConflict
	}
	_, err = p.RequestPaymentResolution(ctx, payment.ID, *payment.BookingId, 1, decision, "legacy full refund acceptance")
	return err
}

func (p *PaymentService) GetPaymentsByBookingIds(ctx context.Context, bookingIds []uuid.UUID) ([]domain.Payment, error) {
	return p.repo.GetPaymentsByBookingIds(ctx, bookingIds)
}
