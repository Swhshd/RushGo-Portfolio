package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/admission"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
)

var (
	ErrTicketAuthorityUnknown = errors.New("ticket authority evidence unknown")
	ErrTicketNotEligible      = errors.New("ticket is not eligible for an entry credential")
	ErrTicketAdmissionUnknown = errors.New("ticket admission coordination unavailable")
)

type TicketAuthority interface {
	CheckTicketAuthorities(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate) (domain.TicketAuthorityEvidence, error)
}

type TicketIssuanceService struct {
	store     repository.TicketIssuanceRepository
	authority TicketAuthority
}

func NewTicketIssuanceService(store repository.TicketIssuanceRepository, authority TicketAuthority) *TicketIssuanceService {
	return &TicketIssuanceService{store: store, authority: authority}
}

func (s *TicketIssuanceService) Process(ctx context.Context, event domain.TicketIssuanceEvent) (err error) {
	stage := "validate_event"
	defer func() { err = domain.WrapIssuanceFailure(stage, err) }()
	if err := event.Validate(); err != nil {
		return fmt.Errorf("%w: %v", domain.ErrInvalidTicketIssuanceEvent, err)
	}
	stage = "dependencies"
	if s == nil || s.store == nil || s.authority == nil {
		return errors.New("ticket issuance dependencies unavailable")
	}
	stage = "lookup_receipt"
	replayed, err := s.store.LookupTicketIssuanceReceipt(ctx, event)
	if err != nil {
		return err
	}
	if replayed {
		return nil
	}
	stage = "load_candidate"
	candidate, err := s.store.LoadTicketIssuanceCandidate(ctx, event)
	if err != nil {
		return err
	}
	stage = "candidate_identity"
	if candidate.BookingID != event.BookingID {
		return repository.ErrTicketIdentityConflict
	}
	// Do local shape validation before the network calls so malformed or legacy
	// bookings cannot trigger provider queries or mint tickets.
	localDecision := domain.ValidateTicketIssuance(event, candidate, domain.TicketAuthorityEvidence{})
	if localDecision.ReasonCode != "payment_evidence_not_current_and_complete" && localDecision.ReasonCode != "authoritative_booking_or_event_state_denied" {
		if !localDecision.Eligible {
			stage = "apply_denial"
			return s.store.ApplyTicketIssuance(ctx, event, candidate, localDecision, domain.TicketAuthorityEvidence{})
		}
	}

	stage = "authority"
	evidence, err := s.authority.CheckTicketAuthorities(ctx, event, candidate)
	if err != nil {
		return err
	}
	decision := domain.ValidateTicketIssuance(event, candidate, evidence)
	if !decision.Eligible && evidence.ReasonCode != "" {
		decision.ReasonCode = evidence.ReasonCode
	}
	if !decision.Eligible && evidence.SagaEligible && evidence.EventEligible && !evidence.PaymentDecisionKnown {
		return ErrTicketAuthorityUnknown
	}
	stage = "apply_transaction"
	return s.store.ApplyTicketIssuance(ctx, event, candidate, decision, evidence)
}

type GRPCTicketAuthority struct {
	event   eventv1.EventServiceClient
	saga    sagav1.SagaOrchestratorServiceClient
	payment paymentv1.PaymentServiceClient
}

func NewGRPCTicketAuthority(event eventv1.EventServiceClient, saga sagav1.SagaOrchestratorServiceClient, payment paymentv1.PaymentServiceClient) *GRPCTicketAuthority {
	return &GRPCTicketAuthority{event: event, saga: saga, payment: payment}
}

func (a *GRPCTicketAuthority) CheckTicketAuthorities(ctx context.Context, event domain.TicketIssuanceEvent, candidate domain.TicketIssuanceCandidate) (evidence domain.TicketAuthorityEvidence, err error) {
	stage := "authority_dependencies"
	defer func() { err = domain.WrapIssuanceFailure(stage, err) }()
	evidence = domain.TicketAuthorityEvidence{DisputeState: domain.EvidenceUnknown, RefundState: domain.RefundUnknown}
	if a == nil || a.event == nil || a.saga == nil || a.payment == nil {
		return evidence, ErrTicketAuthorityUnknown
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	stage = "saga_authority"
	sagaResponse, err := a.saga.VerifyTicketIssuanceSaga(ctx, &sagav1.VerifyTicketIssuanceSagaRequest{
		SagaId: event.SagaID.String(), BookingId: event.BookingID.String(), PaymentId: event.PaymentID.String(), EventId: event.EventID.String(), Generation: event.Generation,
	})
	if err != nil {
		return evidence, fmt.Errorf("saga ticket authority unavailable: %w", err)
	}
	if sagaResponse == nil || sagaResponse.State == sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_UNKNOWN {
		return evidence, ErrTicketAuthorityUnknown
	}
	if sagaResponse.State == sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_DENIED {
		evidence.ReasonCode = "saga_not_eligible"
		return evidence, nil
	}
	if sagaResponse.State != sagav1.TicketIssuanceSagaState_TICKET_ISSUANCE_SAGA_STATE_ELIGIBLE {
		return evidence, ErrTicketAuthorityUnknown
	}
	evidence.SagaEligible = true

	seatIDs := make([]string, 0)
	for _, item := range candidate.Items {
		for _, id := range item.SeatIDs {
			seatIDs = append(seatIDs, id.String())
		}
	}
	stage = "event_authority"
	eventResponse, err := a.event.VerifyBookingTicketEligibility(ctx, &eventv1.VerifyBookingTicketEligibilityRequest{
		BookingId: event.BookingID.String(), EventId: event.EventID.String(), Generation: event.Generation, SeatIds: seatIDs,
	})
	if err != nil {
		return evidence, fmt.Errorf("event ticket authority unavailable: %w", err)
	}
	if eventResponse == nil || eventResponse.State == eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_UNKNOWN {
		return evidence, ErrTicketAuthorityUnknown
	}
	if eventResponse.State == eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_DENIED {
		evidence.ReasonCode = "event_seats_not_eligible"
		return evidence, nil
	}
	if eventResponse.State != eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE {
		return evidence, ErrTicketAuthorityUnknown
	}
	evidence.EventEligible = true

	paymentQueryStartedAt := time.Now()
	evidence.PaymentQueryStartedAt = paymentQueryStartedAt
	evidence.PaymentQueryWallStartedAt = paymentQueryStartedAt.Round(0)
	stage = "payment_authority"
	paymentResponse, err := a.payment.CheckAdmissionFinancialEligibility(ctx, &paymentv1.AdmissionFinancialEligibilityRequest{
		PaymentId: event.PaymentID.String(), BookingId: event.BookingID.String(), Generation: event.Generation,
	})
	if err != nil {
		return evidence, fmt.Errorf("payment ticket authority unavailable: %w", err)
	}
	if paymentResponse == nil || paymentResponse.EligibilityState == paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_UNKNOWN {
		reason := "missing_response"
		if paymentResponse != nil {
			reason = paymentResponse.ReasonCode
		}
		return evidence, &domain.IssuanceFailure{Stage: "payment_authority", Reason: reason, Cause: ErrTicketAuthorityUnknown}
	}
	if paymentResponse.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE &&
		paymentResponse.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_DENIED {
		return evidence, ErrTicketAuthorityUnknown
	}
	evidence.PaymentDecisionKnown = true
	evidence.PaymentEligible = paymentResponse.EligibilityState == paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE
	switch paymentResponse.DisputeState {
	case paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE:
		evidence.DisputeState = domain.DisputeNoDispute
	case paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_DISPUTE_PRESENT:
		evidence.DisputeState = domain.DisputePresent
	}
	switch paymentResponse.RefundState {
	case paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND:
		evidence.RefundState = domain.RefundNoRefund
	case paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_REFUND_PRESENT:
		evidence.RefundState = domain.RefundPresent
	}
	if paymentResponse.ObservedAt != nil {
		evidence.ObservedAt = paymentResponse.ObservedAt.AsTime().UTC()
	}
	evidence.Source, evidence.ReasonCode = paymentResponse.Source, paymentResponse.ReasonCode
	if evidence.PaymentEligible && (evidence.DisputeState != domain.DisputeNoDispute || evidence.RefundState != domain.RefundNoRefund || evidence.ObservedAt.IsZero() || !domain.TicketAuthoritySourceAllowed(evidence.Source)) {
		return domain.TicketAuthorityEvidence{}, ErrTicketAuthorityUnknown
	}
	return evidence, nil
}

func parseTicketUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errors.New("invalid ticket identity")
	}
	return id, nil
}

type TicketManager struct {
	store     repository.TicketRepository
	authority TicketAuthority
	admission admission.Coordinator
	now       func() time.Time
	random    io.Reader
}

func NewTicketManager(store repository.TicketRepository, authority TicketAuthority) *TicketManager {
	return &TicketManager{store: store, authority: authority, now: time.Now, random: rand.Reader}
}

func (s *TicketManager) SetAdmissionCoordinator(coordinator admission.Coordinator) {
	if s != nil {
		s.admission = coordinator
	}
}

func newOpaqueTicketCredential(random io.Reader) (string, []byte, error) {
	if random == nil {
		random = rand.Reader
	}
	secret := make([]byte, 32)
	if _, err := io.ReadFull(random, secret); err != nil {
		return "", nil, fmt.Errorf("generate Ticket credential: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(secret)
	digest := sha256.Sum256([]byte(token))
	return token, digest[:], nil
}

func (s *TicketManager) ListOwnedTickets(ctx context.Context, userID uuid.UUID) ([]domain.Ticket, error) {
	if s == nil || s.store == nil || userID == uuid.Nil {
		return nil, repository.ErrTicketOwnerDenied
	}
	return s.store.ListOwnedTickets(ctx, userID)
}

func (s *TicketManager) RotateCredential(ctx context.Context, userID, ticketID uuid.UUID, idempotencyKey string) (domain.TicketCredential, error) {
	if s == nil || s.store == nil || userID == uuid.Nil || ticketID == uuid.Nil {
		return domain.TicketCredential{}, repository.ErrTicketOwnerDenied
	}
	if !repository.ValidTicketIdempotencyKey(idempotencyKey) {
		return domain.TicketCredential{}, repository.ErrTicketIdempotencyKeyInvalid
	}
	fingerprint := ticketCredentialRotationFingerprint(ticketID)
	prior, found, err := s.store.LookupTicketCredentialRotation(ctx, userID, ticketID, idempotencyKey, fingerprint)
	if err != nil {
		return domain.TicketCredential{}, err
	}
	if found {
		if prior.Outcome != domain.TicketCredentialRotationAlreadyApplied || prior.CredentialVersion < 1 || prior.ExpiresAt.IsZero() {
			return domain.TicketCredential{}, repository.ErrTicketIdempotencyInProgress
		}
		return domain.TicketCredential{TicketID: ticketID, ExpiresAt: prior.ExpiresAt, CredentialVersion: prior.CredentialVersion, Outcome: domain.TicketCredentialRotationAlreadyApplied}, nil
	}
	if s.authority == nil {
		return domain.TicketCredential{}, ErrTicketAuthorityUnknown
	}
	ticket, candidate, err := s.store.LoadTicketForCredential(ctx, userID, ticketID)
	if err != nil {
		return domain.TicketCredential{}, err
	}
	if ticket.Status != domain.TicketIssued || ticket.BookingStatus != domain.StatusConfirmed || candidate.Status != domain.StatusConfirmed ||
		candidate.UserID != userID || !ticketMatchesBooking(ticket, candidate) {
		return domain.TicketCredential{}, ErrTicketNotEligible
	}
	authorityEvent := domain.TicketIssuanceEvent{
		ID:         uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:wallet-credential:"+ticket.ID.String())),
		SagaID:     candidate.SagaID,
		BookingID:  candidate.BookingID,
		PaymentID:  candidate.PaymentID,
		EventID:    candidate.EventID,
		Generation: candidate.Generation,
	}
	evidence, err := s.authority.CheckTicketAuthorities(ctx, authorityEvent, candidate)
	if err != nil {
		return domain.TicketCredential{}, fmt.Errorf("check Ticket credential authority: %w", err)
	}
	decision := domain.ValidateTicketIssuance(authorityEvent, candidate, evidence)
	if evidence.SagaEligible && evidence.EventEligible && (!evidence.PaymentDecisionKnown || evidence.DisputeState == domain.EvidenceUnknown || evidence.RefundState == domain.RefundUnknown || evidence.ObservedAt.IsZero() || !domain.TicketAuthoritySourceAllowed(evidence.Source)) {
		return domain.TicketCredential{}, ErrTicketAuthorityUnknown
	}
	if !decision.Eligible {
		return domain.TicketCredential{}, ErrTicketNotEligible
	}
	token, digest, err := newOpaqueTicketCredential(s.random)
	if err != nil {
		return domain.TicketCredential{}, err
	}
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	expiry := now.UTC().Add(10 * time.Minute)
	rotation, err := s.store.RotateTicketCredential(ctx, domain.TicketCredentialRotationCommand{
		UserID: userID, TicketID: ticketID, IdempotencyKey: idempotencyKey,
		RequestFingerprint: fingerprint, CredentialDigest: digest, ExpiresAt: expiry,
	})
	if err != nil {
		return domain.TicketCredential{}, err
	}
	if rotation.Outcome == domain.TicketCredentialRotationAlreadyApplied {
		return domain.TicketCredential{TicketID: ticketID, ExpiresAt: rotation.ExpiresAt, CredentialVersion: rotation.CredentialVersion, Outcome: rotation.Outcome}, nil
	}
	if rotation.Outcome != domain.TicketCredentialRotationRotated || rotation.CredentialVersion < 1 || rotation.ExpiresAt.IsZero() {
		return domain.TicketCredential{}, fmt.Errorf("invalid Ticket credential rotation result")
	}
	return domain.TicketCredential{TicketID: ticketID, Token: token, ExpiresAt: rotation.ExpiresAt, CredentialVersion: rotation.CredentialVersion, Outcome: rotation.Outcome}, nil
}

func ticketCredentialRotationFingerprint(ticketID uuid.UUID) []byte {
	input := append([]byte("ROTATE_CREDENTIAL\x00"), ticketID[:]...)
	fingerprint := sha256.Sum256(input)
	return fingerprint[:]
}

func (s *TicketManager) SetScannerAssignment(ctx context.Context, eventID, staffID, actorID uuid.UUID, active bool) (domain.EventScannerAssignment, error) {
	if s == nil || s.store == nil {
		return domain.EventScannerAssignment{}, errors.New("Ticket store unavailable")
	}
	return s.store.SetScannerAssignment(ctx, eventID, staffID, actorID, active)
}

func (s *TicketManager) ListScannerAssignments(ctx context.Context, eventID uuid.UUID) ([]domain.EventScannerAssignment, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("Ticket store unavailable")
	}
	return s.store.ListScannerAssignments(ctx, eventID)
}

func (s *TicketManager) CheckIn(ctx context.Context, eventID, staffID uuid.UUID, credential, idempotencyKey string) (domain.TicketScanResult, error) {
	if s == nil || s.store == nil || s.authority == nil || eventID == uuid.Nil || staffID == uuid.Nil {
		return domain.TicketScanResult{}, ErrTicketAuthorityUnknown
	}
	if len(idempotencyKey) < 1 || len(idempotencyKey) > 128 {
		return domain.TicketScanResult{}, fmt.Errorf("invalid Ticket scan idempotency key")
	}
	raw, err := base64.RawURLEncoding.DecodeString(credential)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != credential {
		return domain.TicketScanResult{}, repository.ErrTicketCredentialMissing
	}
	digest := sha256.Sum256([]byte(credential))
	fingerprint := ticketScanRequestFingerprint(eventID, credential)
	keyDigest := sha256.Sum256([]byte(idempotencyKey))
	replayed, found, err := s.store.LookupTicketScanRequest(ctx, staffID, eventID, idempotencyKey, fingerprint[:])
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	if found {
		replayed.Replayed = true
		if admissionActive, admissionStore, modeErr := s.admissionMode(ctx); modeErr == nil && admissionActive && admissionStore != nil && s.admission != nil {
			if identity, exists, lookupErr := admissionStore.LookupAdmissionIntent(ctx, staffID, keyDigest, fingerprint[:], eventID); lookupErr == nil && exists {
				s.reconcileAdmissionReplay(ctx, identity)
			}
		}
		return replayed, nil
	}
	admissionActive, admissionStore, err := s.admissionMode(ctx)
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	scan, err := s.store.LoadTicketScanCandidate(ctx, digest[:])
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	command := domain.TicketScanCommand{EventID: eventID, StaffUserID: staffID, TicketID: scan.Ticket.ID, CredentialDigest: digest[:], RequestFingerprint: fingerprint[:], IdempotencyKey: idempotencyKey}
	deny := func(reason string) (domain.TicketScanResult, error) {
		command.Decision = domain.TicketIssuanceDecision{ReasonCode: reason}
		return s.store.CommitTicketCheckIn(ctx, command)
	}
	if scan.Ticket.EventID != eventID {
		return deny("event_mismatch")
	}
	if scan.Ticket.Status == domain.TicketRevoked {
		return deny("ticket_revoked")
	}
	if scan.Booking.Status != domain.StatusConfirmed || !scan.Booking.Managed || scan.Booking.Generation != 1 {
		return deny("booking_not_eligible")
	}
	if !scan.CredentialExpiresAt.After(s.currentTime()) {
		return deny("credential_expired")
	}
	if !ticketMatchesBooking(scan.Ticket, scan.Booking) {
		return deny("ticket_seat_binding_mismatch")
	}
	if scan.BookingRefundDecisionID != nil {
		return deny("refund_approved")
	}
	issuanceEvent := domain.TicketIssuanceEvent{ID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:check-in:"+scan.Ticket.ID.String())), SagaID: scan.Booking.SagaID, BookingID: scan.Booking.BookingID,
		PaymentID: scan.Booking.PaymentID, EventID: scan.Booking.EventID, Generation: scan.Booking.Generation}
	evidence, err := s.authority.CheckTicketAuthorities(ctx, issuanceEvent, scan.Booking)
	if err != nil {
		return domain.TicketScanResult{}, err
	}
	decision := domain.ValidateTicketIssuance(issuanceEvent, scan.Booking, evidence)
	if !decision.Eligible && evidence.SagaEligible && evidence.EventEligible && !evidence.PaymentDecisionKnown {
		return domain.TicketScanResult{}, ErrTicketAuthorityUnknown
	}
	if !decision.Eligible && evidence.ReasonCode != "" {
		decision.ReasonCode = evidence.ReasonCode
	}
	command.Decision, command.Evidence = decision, evidence
	if decision.Eligible && admissionActive {
		if s.admission == nil || admissionStore == nil {
			return domain.TicketScanResult{}, ErrTicketAdmissionUnknown
		}
		identity := admission.Identity{
			RequestID: admission.RequestIDFor(staffID, keyDigest), PaymentID: scan.Booking.PaymentID,
			BookingID: scan.Booking.BookingID, TicketID: scan.Ticket.ID, EventID: scan.Booking.EventID,
			UserID: scan.Booking.UserID, ActorID: staffID, Generation: scan.Booking.Generation,
			KeyDigest: keyDigest, Fingerprint: fingerprint,
		}
		if err = identity.Validate(); err != nil {
			return domain.TicketScanResult{}, fmt.Errorf("validate Ticket admission identity: %w", err)
		}
		if err = admissionStore.EnsureAdmissionIntent(ctx, identity); err != nil {
			switch {
			case errors.Is(err, repository.ErrAdmissionAlreadyCheckedIn):
				return deny("already_checked_in")
			case errors.Is(err, repository.ErrAdmissionTicketRevoked):
				return deny("ticket_revoked")
			case errors.Is(err, repository.ErrAdmissionBookingIneligible):
				return deny("booking_not_eligible")
			case errors.Is(err, repository.ErrAdmissionRefundApproved):
				command.Decision = domain.TicketIssuanceDecision{ReasonCode: "refund_approved"}
				command.Evidence = domain.TicketAuthorityEvidence{DisputeState: domain.EvidenceUnknown, RefundState: domain.RefundUnknown, Source: "booking_refund_fence"}
				return s.store.CommitTicketCheckIn(ctx, command)
			}
			return domain.TicketScanResult{}, fmt.Errorf("persist Ticket admission intent: %w", err)
		}
		permit, prepareErr := s.admission.PrepareAdmission(ctx, identity)
		if prepareErr != nil {
			return domain.TicketScanResult{}, fmt.Errorf("prepare Payment admission permit: %w", prepareErr)
		}
		if err = permit.ValidateFor(identity); err != nil {
			return domain.TicketScanResult{}, fmt.Errorf("validate Payment admission permit: %w", err)
		}
		if permit.State != admission.Pending {
			return domain.TicketScanResult{}, repository.ErrAdmissionTerminal
		}
		if err = admissionStore.AttachAdmissionPermit(ctx, permit); err != nil {
			return domain.TicketScanResult{}, fmt.Errorf("attach Payment admission permit: %w", err)
		}
		command.Admission = &permit
		result, commitErr := s.store.CommitTicketCheckIn(ctx, command)
		if commitErr != nil {
			return domain.TicketScanResult{}, commitErr
		}
		// Reconciliation is deliberately best-effort. Booking's intent and
		// outcome are durable; Payment recovery owns any lost acknowledgement.
		_, _ = s.admission.ReconcileAdmission(ctx, identity, permit.Token, permit.Fence)
		return result, nil
	}
	return s.store.CommitTicketCheckIn(ctx, command)
}

func ticketScanRequestFingerprint(eventID uuid.UUID, credential string) [32]byte {
	digest := sha256.Sum256([]byte(credential))
	input := make([]byte, 0, 48)
	input = append(input, eventID[:]...)
	input = append(input, digest[:]...)
	return sha256.Sum256(input)
}

func (s *TicketManager) admissionMode(ctx context.Context) (bool, repository.TicketAdmissionRepository, error) {
	store, ok := s.store.(repository.TicketAdmissionRepository)
	if !ok {
		if s.admission != nil {
			return false, nil, ErrTicketAdmissionUnknown
		}
		return false, nil, nil
	}
	active, err := store.AdmissionEnabled(ctx)
	if err != nil {
		return false, store, fmt.Errorf("read Booking admission mode: %w", err)
	}
	return active, store, nil
}

func (s *TicketManager) reconcileAdmissionReplay(ctx context.Context, identity admission.Identity) {
	permit, err := s.admission.GetAdmission(ctx, identity)
	if err != nil || permit.ValidateFor(identity) != nil {
		return
	}
	_, _ = s.admission.ReconcileAdmission(ctx, identity, permit.Token, permit.Fence)
}

func (s *TicketManager) currentTime() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func ticketMatchesBooking(ticket domain.Ticket, booking domain.TicketIssuanceCandidate) bool {
	if ticket.ID == uuid.Nil || ticket.BookingID != booking.BookingID || ticket.EventID != booking.EventID || ticket.SeatID == uuid.Nil || ticket.TicketTierID == uuid.Nil {
		return false
	}
	for _, item := range booking.Items {
		if item.ID != ticket.BookingItemID || item.TicketTierID != ticket.TicketTierID {
			continue
		}
		for _, seat := range item.SeatIDs {
			if seat == ticket.SeatID {
				return true
			}
		}
	}
	return false
}
