package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/admission"
)

type ticketIssuanceStoreFake struct {
	candidate      domain.TicketIssuanceCandidate
	applyCalls     int
	decision       domain.TicketIssuanceDecision
	receiptExists  bool
	receiptErr     error
	receiptLookups int
	candidateLoads int
}

func (s *ticketIssuanceStoreFake) LoadTicketIssuanceCandidate(context.Context, domain.TicketIssuanceEvent) (domain.TicketIssuanceCandidate, error) {
	s.candidateLoads++
	return s.candidate, nil
}
func (s *ticketIssuanceStoreFake) LookupTicketIssuanceReceipt(context.Context, domain.TicketIssuanceEvent) (bool, error) {
	s.receiptLookups++
	return s.receiptExists, s.receiptErr
}
func (s *ticketIssuanceStoreFake) ApplyTicketIssuance(_ context.Context, _ domain.TicketIssuanceEvent, _ domain.TicketIssuanceCandidate, decision domain.TicketIssuanceDecision, _ domain.TicketAuthorityEvidence) error {
	s.applyCalls++
	s.decision = decision
	return nil
}

type ticketAuthorityFake struct {
	evidence  domain.TicketAuthorityEvidence
	err       error
	calls     int
	afterRead func()
}

func (a *ticketAuthorityFake) CheckTicketAuthorities(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate) (domain.TicketAuthorityEvidence, error) {
	a.calls++
	if a.afterRead != nil {
		a.afterRead()
	}
	return a.evidence, a.err
}

type ticketManagerStoreFake struct {
	scan                 domain.TicketScanCandidate
	assigned             bool
	commitCalls          int
	scanLoadCalls        int
	scanLookupCalls      int
	scanLookupFound      bool
	scanLookupErr        error
	scanLookupResult     domain.TicketScanResult
	command              domain.TicketScanCommand
	result               domain.TicketScanResult
	ownerTickets         []domain.Ticket
	credentialTicket     domain.Ticket
	credentialBooking    domain.TicketIssuanceCandidate
	credentialLoadErr    error
	rotationDigest       []byte
	rotationKey          string
	rotationFingerprint  []byte
	rotationCalls        int
	rotationLookupCalls  int
	rotationLookupResult domain.TicketCredentialRotationResult
	rotationLookupFound  bool
	rotationLookupErr    error
	rotationResult       domain.TicketCredentialRotationResult
	rotationErr          error
	assignment           domain.EventScannerAssignment
	beforeCheckInCommit  func(domain.TicketScanCommand)
	admissionActive      bool
	admissionTrace       *[]string
	admissionIdentity    admission.Identity
	admissionPermit      admission.Permit
	admissionIntentErr   error
	admissionAttachErr   error
	admissionLookupID    admission.Identity
	admissionLookupFound bool
}

func (s *ticketManagerStoreFake) LoadTicketIssuanceCandidate(context.Context, domain.TicketIssuanceEvent) (domain.TicketIssuanceCandidate, error) {
	return s.scan.Booking, nil
}
func (*ticketManagerStoreFake) LookupTicketIssuanceReceipt(context.Context, domain.TicketIssuanceEvent) (bool, error) {
	return false, nil
}
func (*ticketManagerStoreFake) ApplyTicketIssuance(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate, domain.TicketIssuanceDecision, domain.TicketAuthorityEvidence) error {
	return nil
}
func (s *ticketManagerStoreFake) ListOwnedTickets(context.Context, uuid.UUID) ([]domain.Ticket, error) {
	return s.ownerTickets, nil
}
func (s *ticketManagerStoreFake) LookupTicketCredentialRotation(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ string, _ []byte) (domain.TicketCredentialRotationResult, bool, error) {
	s.rotationLookupCalls++
	return s.rotationLookupResult, s.rotationLookupFound, s.rotationLookupErr
}
func (s *ticketManagerStoreFake) RotateTicketCredential(_ context.Context, command domain.TicketCredentialRotationCommand) (domain.TicketCredentialRotationResult, error) {
	s.rotationCalls++
	s.rotationDigest = append([]byte(nil), command.CredentialDigest...)
	s.rotationKey = command.IdempotencyKey
	s.rotationFingerprint = append([]byte(nil), command.RequestFingerprint...)
	if s.rotationErr != nil {
		return domain.TicketCredentialRotationResult{}, s.rotationErr
	}
	if s.rotationResult.Outcome == "" {
		return domain.TicketCredentialRotationResult{Outcome: domain.TicketCredentialRotationRotated, CredentialVersion: 2, ExpiresAt: command.ExpiresAt}, nil
	}
	return s.rotationResult, nil
}
func (s *ticketManagerStoreFake) LoadTicketForCredential(context.Context, uuid.UUID, uuid.UUID) (domain.Ticket, domain.TicketIssuanceCandidate, error) {
	return s.credentialTicket, s.credentialBooking, s.credentialLoadErr
}
func (s *ticketManagerStoreFake) IsScannerAssigned(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return s.assigned, nil
}
func (s *ticketManagerStoreFake) SetScannerAssignment(_ context.Context, eventID, staffID, _ uuid.UUID, active bool) (domain.EventScannerAssignment, error) {
	s.assignment = domain.EventScannerAssignment{EventID: eventID, StaffUserID: staffID, Active: active}
	return s.assignment, nil
}
func (*ticketManagerStoreFake) ListScannerAssignments(context.Context, uuid.UUID) ([]domain.EventScannerAssignment, error) {
	return nil, nil
}
func (s *ticketManagerStoreFake) LoadTicketScanCandidate(context.Context, []byte) (domain.TicketScanCandidate, error) {
	s.scanLoadCalls++
	return s.scan, nil
}
func (s *ticketManagerStoreFake) LookupTicketScanRequest(context.Context, uuid.UUID, uuid.UUID, string, []byte) (domain.TicketScanResult, bool, error) {
	s.scanLookupCalls++
	if !s.assigned {
		return domain.TicketScanResult{}, false, repository.ErrScannerNotAssigned
	}
	return s.scanLookupResult, s.scanLookupFound, s.scanLookupErr
}
func (s *ticketManagerStoreFake) CommitTicketCheckIn(_ context.Context, c domain.TicketScanCommand) (domain.TicketScanResult, error) {
	s.commitCalls++
	s.command = c
	if s.admissionTrace != nil {
		*s.admissionTrace = append(*s.admissionTrace, "commit")
	}
	if s.beforeCheckInCommit != nil {
		s.beforeCheckInCommit(c)
	}
	return s.result, nil
}
func (s *ticketManagerStoreFake) AdmissionEnabled(context.Context) (bool, error) {
	return s.admissionActive, nil
}
func (s *ticketManagerStoreFake) EnsureAdmissionIntent(_ context.Context, identity admission.Identity) error {
	if s.admissionTrace != nil {
		*s.admissionTrace = append(*s.admissionTrace, "ensure")
	}
	s.admissionIdentity = identity
	return s.admissionIntentErr
}
func (s *ticketManagerStoreFake) AttachAdmissionPermit(_ context.Context, permit admission.Permit) error {
	if s.admissionTrace != nil {
		*s.admissionTrace = append(*s.admissionTrace, "attach")
	}
	s.admissionPermit = permit
	return s.admissionAttachErr
}
func (s *ticketManagerStoreFake) ResolveAdmission(context.Context, admission.Identity, uuid.UUID, int64, bool) (admission.Outcome, error) {
	return admission.Outcome{}, nil
}
func (s *ticketManagerStoreFake) LookupAdmissionIntent(context.Context, uuid.UUID, [32]byte, []byte, uuid.UUID) (admission.Identity, bool, error) {
	return s.admissionLookupID, s.admissionLookupFound, nil
}

type ticketAdmissionCoordinatorFake struct {
	permit         admission.Permit
	prepareCalls   int
	getCalls       int
	reconcileCalls int
	prepareErr     error
	getErr         error
	reconcileErr   error
	trace          *[]string
}

func (c *ticketAdmissionCoordinatorFake) PrepareAdmission(_ context.Context, identity admission.Identity) (admission.Permit, error) {
	c.prepareCalls++
	if c.trace != nil {
		*c.trace = append(*c.trace, "prepare")
	}
	if c.prepareErr != nil {
		return admission.Permit{}, c.prepareErr
	}
	if c.permit.Identity == (admission.Identity{}) {
		c.permit.Identity = identity
	}
	return c.permit, nil
}
func (c *ticketAdmissionCoordinatorFake) GetAdmission(context.Context, admission.Identity) (admission.Permit, error) {
	c.getCalls++
	if c.getErr != nil {
		return admission.Permit{}, c.getErr
	}
	return c.permit, nil
}
func (c *ticketAdmissionCoordinatorFake) ReconcileAdmission(context.Context, admission.Identity, uuid.UUID, int64) (admission.Permit, error) {
	c.reconcileCalls++
	return c.permit, c.reconcileErr
}

func validTicketIssuanceFixture() (domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate, domain.TicketAuthorityEvidence) {
	bookingID, eventID, sagaID, paymentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	event := domain.TicketIssuanceEvent{ID: uuid.New(), SagaID: sagaID, BookingID: bookingID, PaymentID: paymentID, EventID: eventID, Generation: 1}
	candidate := domain.TicketIssuanceCandidate{BookingID: bookingID, UserID: uuid.New(), EventID: eventID, SagaID: sagaID, PaymentID: paymentID,
		Status: domain.StatusConfirmed, Managed: true, Generation: 1, TotalAmountCents: 2000, Currency: "usd",
		Items: []domain.BookingItem{{ID: uuid.New(), BookingID: bookingID, TicketTierID: uuid.New(), Quantity: 1, SeatIDs: []uuid.UUID{uuid.New()}}}}
	queryStartedAt := time.Now()
	evidence := domain.TicketAuthorityEvidence{SagaEligible: true, PaymentEligible: true, PaymentDecisionKnown: true, EventEligible: true, DisputeState: domain.DisputeNoDispute,
		RefundState: domain.RefundNoRefund, Source: "stripe_test_api", ObservedAt: time.Now().UTC(), PaymentQueryStartedAt: queryStartedAt, PaymentQueryWallStartedAt: queryStartedAt.Round(0)}
	return event, candidate, evidence
}

func TestRotateCredentialFailsClosedWhenCurrentAuthorityIsUnknown(t *testing.T) {
	_, candidate, _ := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusConfirmed}
	store := &ticketManagerStoreFake{credentialTicket: ticket, credentialBooking: candidate}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	manager := NewTicketManager(store, authority)
	_, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "unknown-authority-key")
	if !errors.Is(err, ErrTicketAuthorityUnknown) {
		t.Fatalf("RotateCredential error=%v, want unknown authority", err)
	}
	if authority.calls != 1 || len(store.rotationDigest) != 0 {
		t.Fatalf("unknown authority minted a credential: calls=%d digest length=%d", authority.calls, len(store.rotationDigest))
	}
}

func TestRotateCredentialReplayReturnsAlreadyAppliedWithoutCredentialOrAuthorityCall(t *testing.T) {
	_, candidate, _ := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusConfirmed}
	expiresAt := time.Now().UTC().Add(5 * time.Minute)
	store := &ticketManagerStoreFake{rotationLookupFound: true, rotationLookupResult: domain.TicketCredentialRotationResult{
		Outcome: domain.TicketCredentialRotationAlreadyApplied, CredentialVersion: 7, ExpiresAt: expiresAt,
	}}
	manager := NewTicketManager(store, nil)
	credential, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "same-operation-key")
	if err != nil {
		t.Fatal(err)
	}
	if credential.Outcome != domain.TicketCredentialRotationAlreadyApplied || credential.Token != "" || credential.CredentialVersion != 7 || !credential.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("idempotent replay=%+v, want completed outcome without a recoverable credential", credential)
	}
	if store.rotationLookupCalls != 1 || store.rotationCalls != 0 {
		t.Fatalf("replay performed unexpected work: lookups=%d rotations=%d", store.rotationLookupCalls, store.rotationCalls)
	}
}

func TestRotateCredentialDoesNotMintWhenPaymentAuthorityIsUnknown(t *testing.T) {
	_, candidate, _ := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusConfirmed}
	store := &ticketManagerStoreFake{credentialTicket: ticket, credentialBooking: candidate}
	authority := &ticketAuthorityFake{evidence: domain.TicketAuthorityEvidence{SagaEligible: true, EventEligible: true, DisputeState: domain.EvidenceUnknown, RefundState: domain.RefundUnknown}}
	manager := NewTicketManager(store, authority)
	_, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "unknown-evidence-key")
	if !errors.Is(err, ErrTicketAuthorityUnknown) {
		t.Fatalf("RotateCredential error=%v, want unknown payment authority", err)
	}
	if authority.calls != 1 || len(store.rotationDigest) != 0 {
		t.Fatalf("unknown payment authority minted a credential: calls=%d digest length=%d", authority.calls, len(store.rotationDigest))
	}
}

func TestRotateCredentialHonorsBuildScopedAuthoritySource(t *testing.T) {
	_, candidate, evidence := validTicketIssuanceFixture()
	evidence.Source = "SIMULATED_PROVIDER_AUTHORITY"
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusConfirmed}
	store := &ticketManagerStoreFake{credentialTicket: ticket, credentialBooking: candidate}
	authority := &ticketAuthorityFake{evidence: evidence}
	manager := NewTicketManager(store, authority)
	credential, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "simulated-source-build-boundary")
	if domain.TicketAuthoritySourceAllowed(evidence.Source) {
		if err != nil || credential.Token == "" || len(store.rotationDigest) != 32 {
			t.Fatalf("build-authorized test source did not rotate credential: credential=%+v error=%v", credential, err)
		}
		return
	}
	if err == nil || store.rotationCalls != 0 || len(store.rotationDigest) != 0 {
		t.Fatalf("production build accepted simulated authority: credential=%+v error=%v rotations=%d", credential, err, store.rotationCalls)
	}
}

func TestRotateCredentialRejectsBookingThatIsNoLongerConfirmed(t *testing.T) {
	_, candidate, evidence := validTicketIssuanceFixture()
	candidate.Status = domain.StatusCancelled
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusCancelled}
	store := &ticketManagerStoreFake{credentialTicket: ticket, credentialBooking: candidate}
	authority := &ticketAuthorityFake{evidence: evidence}
	manager := NewTicketManager(store, authority)
	_, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "cancelled-booking-key")
	if err == nil {
		t.Fatal("RotateCredential succeeded for a cancelled Booking")
	}
	if authority.calls != 0 || len(store.rotationDigest) != 0 {
		t.Fatalf("cancelled Booking reached authority or minted a credential: calls=%d digest length=%d", authority.calls, len(store.rotationDigest))
	}
}

func TestTicketIssuanceRetriesUnknownAuthorityWithoutReceipt(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	store := &ticketIssuanceStoreFake{candidate: candidate}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	service := NewTicketIssuanceService(store, authority)
	if err := service.Process(context.Background(), event); !errors.Is(err, ErrTicketAuthorityUnknown) {
		t.Fatalf("Process error = %v, want retryable unknown authority", err)
	}
	if store.applyCalls != 0 {
		t.Fatalf("unknown authority wrote %d receipts", store.applyCalls)
	}
}

func TestTicketIssuanceReplaysStoredReceiptBeforeAuthorityUnavailable(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	store := &ticketIssuanceStoreFake{candidate: candidate, receiptExists: true}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	service := NewTicketIssuanceService(store, authority)
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatalf("duplicate event with stored receipt should complete without authority: %v", err)
	}
	if store.receiptLookups != 1 || store.candidateLoads != 0 || store.applyCalls != 0 || authority.calls != 0 {
		t.Fatalf("stored receipt replay performed new work: lookups=%d candidateLoads=%d apply=%d authority=%d", store.receiptLookups, store.candidateLoads, store.applyCalls, authority.calls)
	}
}

func TestTicketIssuanceDoesNotBypassConflictingReceipt(t *testing.T) {
	event, _, _ := validTicketIssuanceFixture()
	store := &ticketIssuanceStoreFake{receiptErr: repository.ErrTicketIdentityConflict}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	service := NewTicketIssuanceService(store, authority)
	if err := service.Process(context.Background(), event); !errors.Is(err, repository.ErrTicketIdentityConflict) {
		t.Fatalf("conflicting receipt error=%v, want identity conflict", err)
	}
	if store.candidateLoads != 0 || store.applyCalls != 0 || authority.calls != 0 {
		t.Fatalf("conflicting receipt reached downstream work: candidateLoads=%d apply=%d authority=%d", store.candidateLoads, store.applyCalls, authority.calls)
	}
}

func TestTicketIssuancePersistsDefinitiveDisputeDenial(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	evidence.PaymentEligible = false
	evidence.DisputeState = domain.DisputePresent
	evidence.ReasonCode = "provider_dispute_present"
	store := &ticketIssuanceStoreFake{candidate: candidate}
	authority := &ticketAuthorityFake{evidence: evidence}
	service := NewTicketIssuanceService(store, authority)
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if store.applyCalls != 1 || store.decision.Eligible || store.decision.ReasonCode == "" {
		t.Fatalf("expected durable denial decision, calls=%d decision=%+v", store.applyCalls, store.decision)
	}
}

func TestTicketIssuanceCreatesOnlyAfterAllAuthoritiesClear(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	store := &ticketIssuanceStoreFake{candidate: candidate}
	authority := &ticketAuthorityFake{evidence: evidence}
	service := NewTicketIssuanceService(store, authority)
	if err := service.Process(context.Background(), event); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if store.applyCalls != 1 || !store.decision.Eligible {
		t.Fatalf("expected eligible issuance, calls=%d decision=%+v", store.applyCalls, store.decision)
	}
}

func TestOpaqueTicketCredentialUsesThirtyTwoRandomBytesAndOnlyDigestStorage(t *testing.T) {
	token, digest, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		t.Fatalf("credential encoding/size invalid: %d %v", len(raw), err)
	}
	want := sha256.Sum256([]byte(token))
	if string(digest) != string(want[:]) {
		t.Fatal("credential digest does not match the token")
	}
	second, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	if second == token {
		t.Fatal("CSPRNG returned duplicate credentials")
	}
}

func TestTicketCheckInUnknownAuthorityDoesNotPersistOutcome(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID, SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	store := &ticketManagerStoreFake{assigned: true, scan: domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)}}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	manager := NewTicketManager(store, authority)
	token, _, _ := newOpaqueTicketCredential(nil)
	if _, err := manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "scan-key"); !errors.Is(err, ErrTicketAuthorityUnknown) {
		t.Fatalf("error=%v, want unknown authority", err)
	}
	if store.commitCalls != 0 {
		t.Fatalf("unknown authority persisted %d outcomes", store.commitCalls)
	}
}

func TestTicketCheckInReplaysStoredOutcomeBeforeAuthorityUnavailable(t *testing.T) {
	event, _, _ := validTicketIssuanceFixture()
	store := &ticketManagerStoreFake{
		assigned:         true,
		scanLookupFound:  true,
		scanLookupResult: domain.TicketScanResult{ResultCode: "accepted", ReasonCode: "checked_in", EvidenceSource: "stripe_test_api"},
	}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	manager := NewTicketManager(store, authority)
	token, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "scan-key")
	if err != nil || !got.Replayed || got.ResultCode != "accepted" {
		t.Fatalf("stored check-in replay=%+v error=%v, want original accepted outcome", got, err)
	}
	if store.scanLookupCalls != 1 || store.scanLoadCalls != 0 || store.commitCalls != 0 || authority.calls != 0 {
		t.Fatalf("stored check-in replay performed new work: lookups=%d candidateLoads=%d commit=%d authority=%d", store.scanLookupCalls, store.scanLoadCalls, store.commitCalls, authority.calls)
	}
}

func TestTicketCheckInDoesNotBypassConflictingIdempotencyKey(t *testing.T) {
	event, _, _ := validTicketIssuanceFixture()
	store := &ticketManagerStoreFake{assigned: true, scanLookupErr: repository.ErrTicketIdempotencyConflict}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	manager := NewTicketManager(store, authority)
	token, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "scan-key")
	if !errors.Is(err, repository.ErrTicketIdempotencyConflict) {
		t.Fatalf("idempotency conflict error=%v, want conflict", err)
	}
	if store.scanLoadCalls != 0 || store.commitCalls != 0 || authority.calls != 0 {
		t.Fatalf("conflicting key reached downstream work: candidateLoads=%d commit=%d authority=%d", store.scanLoadCalls, store.commitCalls, authority.calls)
	}
}

func TestTicketCheckInDoesNotReplayForRevokedScanner(t *testing.T) {
	event, _, _ := validTicketIssuanceFixture()
	store := &ticketManagerStoreFake{scanLookupFound: true, scanLookupResult: domain.TicketScanResult{ResultCode: "accepted"}}
	manager := NewTicketManager(store, &ticketAuthorityFake{})
	token, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "scan-key"); !errors.Is(err, repository.ErrScannerNotAssigned) {
		t.Fatalf("revoked scanner replay error=%v, want scanner authorization denial", err)
	}
	if store.scanLoadCalls != 0 || store.commitCalls != 0 {
		t.Fatalf("revoked scanner reached candidate or write path: loads=%d commits=%d", store.scanLoadCalls, store.commitCalls)
	}
}

func TestTicketCheckInPersistsDefinitiveDisputeDenial(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID, SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	store := &ticketManagerStoreFake{assigned: true, scan: domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)}}
	evidence.PaymentEligible = false
	evidence.DisputeState = domain.DisputePresent
	evidence.ReasonCode = "provider_dispute_present"
	authority := &ticketAuthorityFake{evidence: evidence}
	manager := NewTicketManager(store, authority)
	token, _, _ := newOpaqueTicketCredential(nil)
	if _, err := manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "scan-key"); err != nil {
		t.Fatal(err)
	}
	if store.commitCalls != 1 || store.command.Decision.Eligible || store.command.Decision.ReasonCode != "provider_dispute_present" {
		t.Fatalf("unexpected persisted decision: %+v", store.command.Decision)
	}
}

func TestTicketCheckInUsesAdmissionPermitWhenModeIsActive(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: event.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	trace := make([]string, 0, 4)
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true, admissionTrace: &trace,
		scan:   domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)},
		result: domain.TicketScanResult{ResultCode: "accepted", ReasonCode: "checked_in"}}
	coordinator := &ticketAdmissionCoordinatorFake{trace: &trace, permit: admission.Permit{Token: uuid.New(), Fence: 17, State: admission.Pending}}
	manager := NewTicketManager(store, &ticketAuthorityFake{evidence: evidence})
	manager.SetAdmissionCoordinator(coordinator)
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	actorID := uuid.New()
	result, err := manager.CheckIn(context.Background(), event.EventID, actorID, credential, "admission-key")
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "accepted" || store.command.Admission == nil {
		t.Fatalf("admission check-in result=%+v permit=%+v, want accepted with attached permit", result, store.command.Admission)
	}
	if coordinator.prepareCalls != 1 || store.admissionIdentity.Validate() != nil || store.admissionIdentity.ActorID != actorID ||
		store.admissionIdentity.UserID != candidate.UserID || store.admissionIdentity.PaymentID != candidate.PaymentID ||
		store.admissionIdentity.BookingID != candidate.BookingID || store.admissionIdentity.TicketID != ticket.ID || store.admissionIdentity.EventID != event.EventID {
		t.Fatalf("admission identity or preparation mismatch: identity=%+v prepare calls=%d", store.admissionIdentity, coordinator.prepareCalls)
	}
	if store.admissionPermit.Token != coordinator.permit.Token || store.command.Admission.Token != coordinator.permit.Token || store.command.Admission.Fence != 17 {
		t.Fatalf("attached permit mismatch: stored=%+v command=%+v", store.admissionPermit, store.command.Admission)
	}
	wantTrace := []string{"ensure", "prepare", "attach", "commit"}
	if len(trace) != len(wantTrace) {
		t.Fatalf("admission flow order=%v, want %v", trace, wantTrace)
	}
	for i := range wantTrace {
		if trace[i] != wantTrace[i] {
			t.Fatalf("admission flow order=%v, want %v", trace, wantTrace)
		}
	}
}

func TestTicketCheckInFailsClosedWithoutCoordinatorWhenAdmissionIsActive(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: event.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true,
		scan: domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)}}
	manager := NewTicketManager(store, &ticketAuthorityFake{evidence: evidence})
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.CheckIn(context.Background(), event.EventID, uuid.New(), credential, "no-coordinator"); err == nil {
		t.Fatal("active admission mode accepted a scan without a coordinator")
	}
	if store.commitCalls != 0 {
		t.Fatalf("active admission mode fell back to legacy commit path: commits=%d", store.commitCalls)
	}
}

func TestTicketCheckInRefundFenceRacePersistsDeniedScanWithoutPermitPreparation(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	actorID := uuid.New()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID,
		EventID: event.EventID, SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	trace := make([]string, 0, 3)
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true, admissionTrace: &trace,
		scan:               domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)},
		admissionIntentErr: repository.ErrAdmissionRefundApproved,
		result:             domain.TicketScanResult{ResultCode: "denied", ReasonCode: "refund_approved"}}
	authority := &ticketAuthorityFake{evidence: evidence}
	coordinator := &ticketAdmissionCoordinatorFake{trace: &trace}
	manager := NewTicketManager(store, authority)
	manager.SetAdmissionCoordinator(coordinator)
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := manager.CheckIn(context.Background(), event.EventID, actorID, credential, "refund-fence-race")
	if err != nil || got.ResultCode != "denied" || got.ReasonCode != "refund_approved" {
		t.Fatalf("refund-fence race result=%+v error=%v, want durable refund_approved denial", got, err)
	}
	if store.commitCalls != 1 || store.command.Decision.Eligible || store.command.Decision.ReasonCode != "refund_approved" || store.command.Admission != nil {
		t.Fatalf("refund-fence race did not persist a no-permit denial: commits=%d command=%+v", store.commitCalls, store.command)
	}
	if coordinator.prepareCalls != 0 || len(trace) != 2 || trace[0] != "ensure" || trace[1] != "commit" {
		t.Fatalf("refund-fence race prepared or attached a permit: prepares=%d trace=%v", coordinator.prepareCalls, trace)
	}
}

func TestTicketCheckInKnownRefundBlockSkipsAuthoritiesAndPersistsDenial(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	actorID := uuid.New()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID,
		EventID: event.EventID, SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	decisionID := uuid.New()
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true,
		scan:   domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute), BookingRefundDecisionID: &decisionID},
		result: domain.TicketScanResult{ResultCode: "denied", ReasonCode: "refund_approved"}}
	authority := &ticketAuthorityFake{err: ErrTicketAuthorityUnknown}
	coordinator := &ticketAdmissionCoordinatorFake{}
	manager := NewTicketManager(store, authority)
	manager.SetAdmissionCoordinator(coordinator)
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := manager.CheckIn(context.Background(), event.EventID, actorID, credential, "refund-already-approved")
	if err != nil || got.ResultCode != "denied" || got.ReasonCode != "refund_approved" {
		t.Fatalf("refund-blocked scan=%+v error=%v, want refund_approved denial", got, err)
	}
	if authority.calls != 0 || coordinator.prepareCalls != 0 || store.commitCalls != 1 {
		t.Fatalf("refund-blocked scan called upstream or skipped durable denial: authority=%d prepare=%d commit=%d", authority.calls, coordinator.prepareCalls, store.commitCalls)
	}
	if store.command.Decision.Eligible || store.command.Decision.ReasonCode != "refund_approved" || store.command.Admission != nil {
		t.Fatalf("refund-blocked scan command=%+v, want denied without Admission", store.command)
	}
}

func TestTicketCheckInLeavesIntentPendingWhenPermitPreparationIsUnknown(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: event.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	trace := make([]string, 0, 4)
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true, admissionTrace: &trace,
		scan: domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)}}
	coordinator := &ticketAdmissionCoordinatorFake{trace: &trace, prepareErr: context.DeadlineExceeded}
	manager := NewTicketManager(store, &ticketAuthorityFake{evidence: evidence})
	manager.SetAdmissionCoordinator(coordinator)
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.CheckIn(context.Background(), event.EventID, uuid.New(), credential, "unknown-permit")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown permit preparation error=%v, want deadline exceeded", err)
	}
	if store.admissionIdentity.Validate() != nil || store.commitCalls != 0 || store.admissionPermit.Token != uuid.Nil {
		t.Fatalf("unknown permit preparation skipped intent or committed: identity=%+v commits=%d permit=%+v", store.admissionIdentity, store.commitCalls, store.admissionPermit)
	}
	wantTrace := []string{"ensure", "prepare"}
	if len(trace) != len(wantTrace) || trace[0] != wantTrace[0] || trace[1] != wantTrace[1] {
		t.Fatalf("unknown permit preparation trace=%v, want %v", trace, wantTrace)
	}
}

func TestTicketCheckInReplayAttemptsAdmissionReconciliationAndReturnsStoredResult(t *testing.T) {
	event, candidate, _ := validTicketIssuanceFixture()
	actorID := uuid.New()
	ticketID := uuid.New()
	keyDigest := sha256.Sum256([]byte("replay-admission-key"))
	credential, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := ticketScanRequestFingerprint(event.EventID, credential)
	identity := admission.Identity{RequestID: admission.RequestIDFor(actorID, keyDigest), PaymentID: candidate.PaymentID, BookingID: candidate.BookingID,
		TicketID: ticketID, EventID: event.EventID, UserID: candidate.UserID, ActorID: actorID, Generation: 1, KeyDigest: keyDigest, Fingerprint: fingerprint}
	coordinator := &ticketAdmissionCoordinatorFake{permit: admission.Permit{Identity: identity, Token: uuid.New(), Fence: 21, State: admission.Committed}, reconcileErr: context.DeadlineExceeded}
	store := &ticketManagerStoreFake{admissionActive: true, assigned: true, scanLookupFound: true,
		scanLookupResult:     domain.TicketScanResult{ResultCode: "accepted", ReasonCode: "checked_in"},
		admissionLookupFound: true, admissionLookupID: identity}
	manager := NewTicketManager(store, &ticketAuthorityFake{err: ErrTicketAuthorityUnknown})
	manager.SetAdmissionCoordinator(coordinator)
	got, err := manager.CheckIn(context.Background(), event.EventID, actorID, credential, "replay-admission-key")
	if err != nil || !got.Replayed || got.ResultCode != "accepted" {
		t.Fatalf("replayed result=%+v error=%v, want stored accepted result", got, err)
	}
	if coordinator.getCalls != 1 || coordinator.reconcileCalls != 1 || coordinator.prepareCalls != 0 || store.commitCalls != 0 || store.scanLoadCalls != 0 {
		t.Fatalf("replay recovery calls get=%d reconcile=%d prepare=%d commit=%d candidate=%d", coordinator.getCalls, coordinator.reconcileCalls, coordinator.prepareCalls, store.commitCalls, store.scanLoadCalls)
	}
}

func TestTicketCheckInRecordsPostObservationDisputeAsResidualWindow(t *testing.T) {
	event, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: event.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued}
	store := &ticketManagerStoreFake{
		assigned: true,
		scan:     domain.TicketScanCandidate{Ticket: ticket, Booking: candidate, CredentialExpiresAt: time.Now().Add(time.Minute)},
		result:   domain.TicketScanResult{ResultCode: "accepted", ReasonCode: "checked_in"},
	}
	providerState := "no_dispute"
	store.beforeCheckInCommit = func(command domain.TicketScanCommand) {
		if providerState != "dispute_present" || command.Evidence.DisputeState != domain.DisputeNoDispute {
			t.Fatalf("fixture did not reach the post-observation residual window: provider=%q evidence=%q", providerState, command.Evidence.DisputeState)
		}
	}
	authority := &ticketAuthorityFake{evidence: evidence, afterRead: func() { providerState = "dispute_present" }}
	manager := NewTicketManager(store, authority)
	token, _, err := newOpaqueTicketCredential(nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.CheckIn(context.Background(), event.EventID, uuid.New(), token, "post-observation-change")
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "accepted" || store.commitCalls != 1 {
		t.Fatalf("bounded read-time policy did not expose the documented residual window: result=%+v commits=%d", result, store.commitCalls)
	}
}

func TestTicketCredentialRotationPersistsOnlyDigestAndExpiresInTenMinutes(t *testing.T) {
	_, candidate, evidence := validTicketIssuanceFixture()
	ticket := domain.Ticket{ID: uuid.New(), BookingID: candidate.BookingID, BookingItemID: candidate.Items[0].ID, EventID: candidate.EventID,
		SeatID: candidate.Items[0].SeatIDs[0], TicketTierID: candidate.Items[0].TicketTierID, Status: domain.TicketIssued, BookingStatus: domain.StatusConfirmed}
	store := &ticketManagerStoreFake{credentialTicket: ticket, credentialBooking: candidate}
	authority := &ticketAuthorityFake{evidence: evidence}
	manager := NewTicketManager(store, authority)
	manager.now = func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) }
	credential, err := manager.RotateCredential(context.Background(), candidate.UserID, ticket.ID, "rotation-key")
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(credential.Token))
	if string(store.rotationDigest) != string(want[:]) {
		t.Fatal("repository did not receive only token digest")
	}
	if credential.ExpiresAt.Sub(manager.now()) != 10*time.Minute {
		t.Fatalf("credential TTL=%s", credential.ExpiresAt.Sub(manager.now()))
	}
}
