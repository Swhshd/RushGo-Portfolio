package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	sagapkg "github.com/ticketbox/pkg/saga"
	pkgtyped "github.com/ticketbox/pkg/typed"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/registry"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
)

type SagaService struct {
	logger           *zap.Logger
	repo             repository.SagaRepositoryInterface
	sagaStepRegistry *registry.SagaStepRegistry
	bookingClient    bookingv1.BookingServiceClient
	paymentClient    paymentv1.PaymentServiceClient
	eventClient      eventv1.EventServiceClient
	metrics          *observability.Metrics
	reliabilityHook  *reliabilityhook.Controller
}

// ConfigureReliabilityHook installs private, opt-in Saga replay barriers.
// A disabled controller leaves ordinary recovery behavior unchanged.
func (s *SagaService) ConfigureReliabilityHook(hook *reliabilityhook.Controller) error {
	if s == nil || hook == nil {
		return errors.New("missing Saga reliability hook")
	}
	s.reliabilityHook = hook
	return nil
}

func NewSagaService(logger *zap.Logger, repo repository.SagaRepositoryInterface, steps *registry.SagaStepRegistry, b bookingv1.BookingServiceClient, p paymentv1.PaymentServiceClient, e eventv1.EventServiceClient) *SagaService {
	return NewSagaServiceWithMetrics(logger, repo, steps, b, p, e, nil)
}
func NewSagaServiceWithMetrics(logger *zap.Logger, repo repository.SagaRepositoryInterface, steps *registry.SagaStepRegistry, b bookingv1.BookingServiceClient, p paymentv1.PaymentServiceClient, e eventv1.EventServiceClient, metrics *observability.Metrics) *SagaService {
	return &SagaService{logger: logger, repo: repo, sagaStepRegistry: steps, bookingClient: b, paymentClient: p, eventClient: e, metrics: metrics}
}

type CreateSagaRequest struct {
	Name      string
	BookingId uuid.UUID
	Steps     []domain.SagaStep
}
type UpdateSagaRequest struct {
	Id               uuid.UUID
	Name             string
	Status           domain.SagaStatus
	CurrentStepIndex int
}

func (s *SagaService) CreateSaga(ctx context.Context, req *CreateSagaRequest) (*domain.Saga, error) {
	if req == nil {
		return nil, repository.ErrInvalidRecoveryInput
	}
	v := &domain.Saga{ID: uuid.New(), BookingID: req.BookingId, Name: req.Name, Steps: req.Steps, Status: domain.SAGA_PENDING}
	if err := s.repo.Create(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}
func (s *SagaService) UpdateSaga(ctx context.Context, req *UpdateSagaRequest) error {
	if req == nil {
		return repository.ErrInvalidRecoveryInput
	}
	return s.repo.UpdateSaga(ctx, &domain.Saga{ID: req.Id, Name: req.Name, Status: req.Status, CurrentStepIndex: req.CurrentStepIndex}, nil)
}
func (s *SagaService) GetSagaById(ctx context.Context, id uuid.UUID) (*domain.Saga, error) {
	return s.repo.GetSagaById(ctx, id)
}
func (s *SagaService) GetSagaByBookingId(ctx context.Context, id uuid.UUID) (*domain.Saga, error) {
	return s.repo.GetSagaByBookingId(ctx, id)
}

func (s *SagaService) VerifyTicketIssuanceSaga(ctx context.Context, sagaID, bookingID, paymentID, eventID uuid.UUID, generation int64) (bool, string, error) {
	if sagaID == uuid.Nil || bookingID == uuid.Nil || paymentID == uuid.Nil || eventID == uuid.Nil || generation != 1 {
		return false, "request_identity_invalid", repository.ErrInvalidRecoveryInput
	}
	v, err := s.repo.GetSagaById(ctx, sagaID)
	if errors.Is(err, repository.ErrNotFound) {
		return false, "saga_not_found", nil
	}
	if err != nil {
		return false, "saga_authority_unavailable", err
	}
	if v == nil || !v.RecoveryManaged || v.Input == nil || v.ID != sagaID || v.BookingID != bookingID || v.PaymentID != paymentID ||
		v.Generation != generation || v.Input.BookingID != bookingID || v.Input.EventID != eventID || v.Status != domain.SAGA_COMPLETED ||
		v.Direction != "forward" || v.CurrentStepIndex != 5 || len(v.Steps) != 5 {
		return false, "saga_identity_or_state_mismatch", nil
	}
	for _, step := range v.Steps {
		if step.ExecuteState != "completed" || step.CompensateState != "pending" {
			return false, "saga_graph_not_final", nil
		}
	}
	return true, "managed_saga_completed", nil
}

// Deprecated registration adapters retain source compatibility. Managed replay
// attaches typed original-step handlers directly and cannot invoke these paths.
func (s *SagaService) UpdateBatchSeatStatus(ctx context.Context, req *eventv1.UpdateBatchSeatStatusRequest) error {
	_, err := s.eventClient.UpdateBatchSeatStatus(ctx, req)
	return err
}
func (s *SagaService) ReservedOrCompensateBatchSeats(ctx context.Context, req *eventv1.ReservedOrCompensateBatchSeatsReq) error {
	_, err := s.eventClient.ReservedOrCompensateBatchSeats(ctx, req)
	return err
}
func (s *SagaService) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	return s.paymentClient.CreatePayment(ctx, req)
}
func (s *SagaService) RefundPayment(ctx context.Context, req *paymentv1.MakeRefundPaymentReq) error {
	_, err := s.paymentClient.MakeRefundPayment(ctx, req)
	return err
}
func (s *SagaService) UpdateBookingStatus(ctx context.Context, req *bookingv1.UpdateBookingStatusByIdReq) error {
	_, err := s.bookingClient.UpdateBookingStatusById(ctx, req)
	return err
}
func (s *SagaService) UpdateAvailableSeatNumber(ctx context.Context, req *eventv1.UpdateTicketAvailabilityRequest) error {
	_, err := s.eventClient.UpdateTicketAvailability(ctx, req)
	return err
}

type StartOrderSagaRequest struct {
	BookingId             string
	SeatIds               []string
	UserId                string
	TotalCents            int
	EventID               string
	LifecycleGeneration   int64
	BookingVersion        int32
	ReservationDeadline   *time.Time
	BookingMode, Currency string
}
type StartOrderSagaResponse struct{ Status, Message, PaymentClientSecret, SagaId string }

func paymentClientSecretOrEmpty(response *paymentv1.CreatePaymentResponse) string {
	if response == nil {
		return ""
	}
	return response.PaymentIntentClientSecret
}

func (s *SagaService) startInput(ctx context.Context, req *StartOrderSagaRequest) (domain.SagaInput, *bookingv1.LifecycleDisposition, error) {
	var in domain.SagaInput
	if req == nil {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	var err error
	if in.BookingID, err = uuid.Parse(req.BookingId); err != nil || in.BookingID == uuid.Nil {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	if in.UserID, err = uuid.Parse(req.UserId); err != nil {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	if in.EventID, err = uuid.Parse(req.EventID); err != nil {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	in.Generation = 1
	in.AmountCents = int64(req.TotalCents)
	b, err := s.bookingClient.GetLifecycleDisposition(ctx, &bookingv1.LifecycleRequest{BookingId: in.BookingID.String(), Generation: 1})
	if err != nil {
		return in, nil, err
	}
	if b == nil || b.Booking == nil || b.ReservationDeadline == nil || b.ReservationDeadline.CheckValid() != nil {
		return in, nil, repository.ErrRecoveryAttention
	}
	in.Currency = b.Currency
	in.BookingMode = b.BookingMode
	in.ReservationDeadline = b.ReservationDeadline.AsTime()
	for _, raw := range req.SeatIds {
		id, err := uuid.Parse(raw)
		if err != nil {
			return in, nil, repository.ErrInvalidRecoveryInput
		}
		in.SeatIDs = append(in.SeatIDs, id)
	}
	in, _, err = domain.CanonicalInput(in)
	if err != nil {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	// All additive fields are required together. Zero generation is the explicit
	// compatibility adapter, which fills only from the authoritative Booking row.
	if req.LifecycleGeneration != 0 {
		if req.LifecycleGeneration != 1 || req.BookingVersion < 1 || req.BookingVersion > b.Version || req.ReservationDeadline == nil || !req.ReservationDeadline.Equal(in.ReservationDeadline) || req.Currency != in.Currency || req.BookingMode != in.BookingMode {
			return in, nil, repository.ErrRecoveryConflict
		}
	} else if req.BookingVersion != 0 || req.ReservationDeadline != nil || req.Currency != "" || req.BookingMode != "" {
		return in, nil, repository.ErrInvalidRecoveryInput
	}
	b, err = s.bookingAuthority(ctx, in)
	return in, b, err
}

func initialSteps() []domain.SagaStep {
	names := []sagapkg.SagaStepName{sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED, sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER}
	steps := make([]domain.SagaStep, len(names))
	for i, name := range names {
		steps[i] = domain.SagaStep{ID: uuid.New(), Name: string(name), Order: int16(i), Status: domain.SAGA_STEP_PENDING, ShouldPauseForPayment: i == 1}
	}
	return steps
}
func (s *SagaService) InitializeSagaHandler(ctx context.Context, req *StartOrderSagaRequest) (*registry.SagaHandler, error) {
	in, _, err := s.startInput(ctx, req)
	if err != nil {
		return nil, err
	}
	r, err := s.replayRepository()
	if err != nil {
		return nil, err
	}
	v, _, err := r.CreateOrGetSaga(ctx, in, initialSteps())
	if err != nil {
		return nil, err
	}
	return s.reBuildSagaHandler(ctx, v, nil)
}
func (s *SagaService) StartOrderSaga(ctx context.Context, req *StartOrderSagaRequest) (*StartOrderSagaResponse, error) {
	in, b, err := s.startInput(ctx, req)
	if err != nil {
		return nil, err
	}
	r, err := s.replayRepository()
	if err != nil {
		return nil, err
	}
	v, _, err := r.CreateOrGetSaga(ctx, in, initialSteps())
	if err != nil {
		return nil, err
	}
	if b.Phase == "TERMINAL" {
		return s.SettleOrderSaga(ctx, in.BookingID, 1, b.DecisionId, "", "start_terminal")
	}
	c, err := r.ClaimSaga(ctx, v.ID, 30*time.Second)
	if err != nil {
		return nil, err
	}
	if c != nil {
		executionErr, renewalErr := runClaimWithHeartbeat(
			ctx, *c, 30*time.Second, DefaultRecoveryConfig().RemoteTimeout,
			func(interval time.Duration) (<-chan time.Time, func()) {
				t := time.NewTicker(interval)
				return t.C, t.Stop
			}, r.RenewClaim,
			func(workCtx context.Context) error { return s.ExecuteClaim(workCtx, *c) })
		err = errors.Join(executionErr, renewalErr)
		if err != nil && !errors.Is(err, errFinancialWait) {
			// The durable result remains discoverable; return its current recovery state.
			s.logger.Warn("Saga start retained recovery responsibility")
		}
	}
	v, err = s.repo.GetSagaById(ctx, v.ID)
	if err != nil {
		return nil, err
	}
	response := &StartOrderSagaResponse{SagaId: v.ID.String(), Status: string(v.Status)}
	if v.Direction == "forward" && v.CurrentStepIndex >= 1 && v.CurrentStepIndex < len(v.Steps) && (v.Status == domain.SAGA_WAIT_FOR_PAYMENT || v.Status == domain.SAGA_PROCESSING) {
		p, err := s.paymentClient.CreatePayment(ctx, &paymentv1.CreatePaymentRequest{BookingId: in.BookingID.String(), UserId: in.UserID.String(), PaymentGeneration: 1, Price: int32(in.AmountCents), Currency: in.Currency})
		if err != nil {
			return response, nil
		}
		if p != nil && p.CreateState == "create_ready" {
			response.PaymentClientSecret = p.PaymentIntentClientSecret
			response.Status = "success"
		}
	}
	return response, nil
}

// Both Kafka labels only wake trusted Payment lookup; neither labels nor object
// status fields can create collection, failure, confirmation, or refund facts.
func paymentWakeID(raw json.RawMessage) (string, error) {
	var event pkgtyped.PaymentEvent
	if len(raw) == 0 || json.Unmarshal(raw, &event) != nil || event.Data == nil {
		return "", repository.ErrInvalidRecoveryInput
	}
	object := event.Data.Raw
	if len(object) == 0 && event.Data.Object != nil {
		var err error
		object, err = json.Marshal(event.Data.Object)
		if err != nil {
			return "", repository.ErrInvalidRecoveryInput
		}
	}
	var normalized struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	if json.Unmarshal(object, &normalized) != nil || !strings.HasPrefix(normalized.ID, "pi_") || len(normalized.ID) < 4 || len(normalized.ID) > 255 || (normalized.Object != "" && normalized.Object != "payment_intent") {
		return "", repository.ErrInvalidRecoveryInput
	}
	return normalized.ID, nil
}
func (s *SagaService) wakePayment(ctx context.Context, raw json.RawMessage) error {
	intent, err := paymentWakeID(raw)
	if err != nil {
		return err
	}
	return s.ContinueSagaAfterPaymentSuccess(ctx, intent)
}
func (s *SagaService) HandleSagaAferPaymentSuccess(ctx context.Context, raw json.RawMessage) error {
	return s.wakePayment(ctx, raw)
}
func (s *SagaService) HandleSagaAfterPaymentFailure(ctx context.Context, raw json.RawMessage) error {
	return s.wakePayment(ctx, raw)
}
func (s *SagaService) ContinueSagaAfterPaymentSuccess(ctx context.Context, intent string) error {
	p, err := s.paymentClient.GetPaymentByPaymentIntentId(ctx, &paymentv1.GetPaymentByIntentIdReq{PaymentIntentId: intent})
	if err != nil {
		return err
	}
	if p == nil || p.PaymentIntentId != intent {
		return repository.ErrRecoveryConflict
	}
	booking, err := uuid.Parse(p.BookingId)
	if err != nil || booking == uuid.Nil {
		return repository.ErrRecoveryConflict
	}
	// The legacy correlation read carries no financial projection. Settlement
	// independently reloads current financial truth by immutable generation.
	_, err = s.SettleOrderSaga(ctx, booking, 1, "", p.Id, "payment_receipt")
	return err
}

func (s *SagaService) ExpireOrderSaga(ctx context.Context, booking uuid.UUID) (bool, string, error) {
	v, err := s.repo.GetSagaByBookingId(ctx, booking)
	if err != nil {
		return false, "", err
	}
	if v.Input == nil {
		return false, "", repository.ErrRecoveryAttention
	}
	b, err := s.bookingAuthority(ctx, *v.Input)
	if err != nil {
		return false, "", err
	}
	if b.Phase == "OPEN" {
		b, err = s.decide(ctx, *v.Input, b, "expire")
		if err != nil {
			return false, "", err
		}
	}
	if b.Booking.Status != "EXPIRED" {
		return false, string(v.Status), nil
	}
	res, err := s.SettleOrderSaga(ctx, booking, 1, b.DecisionId, "", "expire_adapter")
	if err != nil {
		return false, "", err
	}
	return true, res.Status, nil
}
func (s *SagaService) CompensateOrderSaga(ctx context.Context, id uuid.UUID) (*StartOrderSagaResponse, error) {
	v, err := s.repo.GetSagaById(ctx, id)
	if err != nil {
		return nil, err
	}
	if v.Input == nil {
		return nil, repository.ErrRecoveryAttention
	}
	b, err := s.bookingAuthority(ctx, *v.Input)
	if err != nil {
		return nil, err
	}
	if b.Phase != "TERMINAL" || b.Booking.Status == "CONFIRMED" {
		return nil, fmt.Errorf("compensation requires winning terminal Booking decision: %w", repository.ErrRecoveryConflict)
	}
	return s.SettleOrderSaga(ctx, v.BookingID, 1, b.DecisionId, "", "compensate_adapter")
}
