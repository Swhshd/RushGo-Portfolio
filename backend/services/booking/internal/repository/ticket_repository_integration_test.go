package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

type ticketRepositoryFixture struct {
	bookingID, bookingEventID, ticketEventID uuid.UUID
	userID, staffID, ticketID                uuid.UUID
	sagaID, paymentID                        uuid.UUID
	digest                                   []byte
	checkedInAt                              *time.Time
}

func TestLoadTicketScanCandidateBindsBookingEventIdentityIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, false)
	candidate, err := repo.LoadTicketScanCandidate(ctx, f.digest)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Booking.EventID != f.bookingEventID || candidate.Ticket.EventID != f.ticketEventID {
		t.Fatalf("Booking/Ticket event identities collapsed: booking=%s ticket=%s want booking=%s ticket=%s", candidate.Booking.EventID, candidate.Ticket.EventID, f.bookingEventID, f.ticketEventID)
	}
}

func TestCommitTicketCheckInRejectsTicketBoundToDifferentBookingEventIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, false)
	now := time.Now().UTC()
	command := acceptedTicketScanFixture(f, now)
	_, err := repo.CommitTicketCheckIn(context.Background(), command)
	// Identity mismatches are rejected before claiming an idempotency key or
	// writing a scan result; they must never reach the mutation decision.
	if !errors.Is(err, ErrTicketIdentityConflict) {
		t.Fatalf("mismatched Booking/Event binding error=%v, want identity conflict", err)
	}
	var scanRequests, operationKeys int
	if err = p.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM ticket_scan_requests), (SELECT count(*) FROM ticket_operation_idempotency)`).Scan(&scanRequests, &operationKeys); err != nil || scanRequests != 0 || operationKeys != 0 {
		t.Fatalf("rejected identity consumed scan state: requests=%d keys=%d error=%v", scanRequests, operationKeys, err)
	}
	var status string
	if err = p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) {
		t.Fatalf("mismatched Event changed Ticket state to %s", status)
	}
}

func TestActiveAdmissionModeRejectsLegacyTicketCheckInUpdateIntegration(t *testing.T) {
	p, _ := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	_, err := p.Exec(context.Background(), `UPDATE tickets SET status='CHECKED_IN',checked_in_at=clock_timestamp() WHERE id=$1`, f.ticketID)
	if err == nil {
		t.Fatal("active admission mode allowed an unlinked legacy Ticket check-in")
	}
	var status string
	if err = p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) {
		t.Fatalf("legacy update changed Ticket status to %q", status)
	}
}

func TestBookingAdmissionActivationWaitsForLegacyTicketTransactionIntegration(t *testing.T) {
	p, _ := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	ctx := context.Background()
	legacyTx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyTx.Rollback(ctx)
	if _, err = legacyTx.Exec(ctx, `UPDATE tickets SET status='CHECKED_IN',checked_in_at=clock_timestamp() WHERE id=$1`, f.ticketID); err != nil {
		t.Fatal(err)
	}
	activationTx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = activationTx.Exec(ctx, `SET LOCAL lock_timeout='150ms'`); err != nil {
		activationTx.Rollback(ctx)
		t.Fatal(err)
	}
	_, err = activationTx.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`)
	activationTx.Rollback(ctx)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		legacyTx.Rollback(ctx)
		t.Fatalf("activation did not wait on the legacy transaction's shared mode lock: %v", err)
	}
	if err = legacyTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	var active bool
	if err = p.QueryRow(ctx, `SELECT active FROM booking_admission_mode WHERE singleton=TRUE`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if !active {
		t.Fatal("Booking admission activation did not complete after the old transaction committed")
	}
}

func TestAdmissionIntentAtomicallyCommitsWithTicketCheckInIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(f, command.StaffUserID, command.IdempotencyKey, command.RequestFingerprint)
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 3, State: admission.Pending}
	if err := repo.EnsureAdmissionIntent(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachAdmissionPermit(context.Background(), permit); err != nil {
		t.Fatal(err)
	}
	command.Admission = &permit
	result, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "accepted" {
		t.Fatalf("coordinated check-in result=%+v, want accepted", result)
	}
	var status string
	var requestID uuid.UUID
	var intentState, intentResult string
	var intentToken uuid.UUID
	var intentFence int64
	if err := p.QueryRow(context.Background(), `SELECT status,admission_request_id FROM tickets WHERE id=$1`, f.ticketID).Scan(&status, &requestID); err != nil {
		t.Fatal(err)
	}
	if err := p.QueryRow(context.Background(), `SELECT state,result_code,permit_token,permit_fence FROM ticket_admission_intents WHERE request_id=$1`, identity.RequestID).Scan(&intentState, &intentResult, &intentToken, &intentFence); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketCheckedIn) || requestID != identity.RequestID || intentState != string(admission.Committed) || intentResult != "accepted" || intentToken != permit.Token || intentFence != permit.Fence {
		t.Fatalf("ticket/intent not committed atomically: ticket=%q/%s intent=%q/%q/%s/%d", status, requestID, intentState, intentResult, intentToken, intentFence)
	}
}

func TestAdmissionAbortTombstonePreventsLaterCheckInIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(f, command.StaffUserID, command.IdempotencyKey, command.RequestFingerprint)
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 9, State: admission.Pending}
	if _, err := repo.ResolveAdmission(context.Background(), identity, permit.Token, permit.Fence, true); !errors.Is(err, admission.ErrNotFound) {
		t.Fatalf("missing intent abort result=%v, want not found without a release proof", err)
	}
	if err := repo.EnsureAdmissionIntent(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	prePermit, err := repo.ResolveAdmission(context.Background(), identity, uuid.Nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if prePermit.Identity != identity || prePermit.State != admission.Pending || prePermit.Token != uuid.Nil || prePermit.Fence != 0 {
		t.Fatalf("pre-permit proof=%+v, want pending identity only", prePermit)
	}
	outcome, err := repo.ResolveAdmission(context.Background(), identity, permit.Token, permit.Fence, true)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.State != admission.Aborted || outcome.Token != permit.Token || outcome.Fence != permit.Fence {
		t.Fatalf("abort outcome=%+v, want durable abort for exact permit", outcome)
	}
	if err = repo.AttachAdmissionPermit(context.Background(), permit); !errors.Is(err, ErrAdmissionTerminal) {
		t.Fatalf("late attach after lost-grant abort=%v, want terminal tombstone", err)
	}
	command.Admission = &permit
	if _, err = repo.CommitTicketCheckIn(context.Background(), command); err == nil {
		t.Fatal("Ticket check-in consumed an intent after the abort tombstone")
	}
	var status string
	var auditCount int
	if err = p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) || auditCount != 0 {
		t.Fatalf("aborted admission mutated ticket: status=%q audit rows=%d", status, auditCount)
	}
}

func TestAdmissionAbortLockWinsConcurrentCheckInIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(f, command.StaffUserID, command.IdempotencyKey, command.RequestFingerprint)
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 13, State: admission.Pending}
	if err := repo.EnsureAdmissionIntent(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if err := repo.AttachAdmissionPermit(context.Background(), permit); err != nil {
		t.Fatal(err)
	}
	command.Admission = &permit
	ctx := context.Background()
	abortTx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer abortTx.Rollback(ctx)
	var lockedRequest uuid.UUID
	if err = abortTx.QueryRow(ctx, `SELECT request_id FROM ticket_admission_intents WHERE request_id=$1 FOR UPDATE`, identity.RequestID).Scan(&lockedRequest); err != nil {
		t.Fatal(err)
	}
	checkInResult := make(chan error, 1)
	go func() {
		_, checkInErr := repo.CommitTicketCheckIn(ctx, command)
		checkInResult <- checkInErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var waiting bool
		if err = p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query ILIKE '%ticket_admission_intents%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("check-in did not block on the pending admission intent lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = abortTx.Exec(ctx, `UPDATE ticket_admission_intents SET state='ABORTED',updated_at=clock_timestamp() WHERE request_id=$1 AND state='PENDING'`, identity.RequestID); err != nil {
		t.Fatal(err)
	}
	if err = abortTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-checkInResult; !errors.Is(err, ErrAdmissionTerminal) {
		t.Fatalf("check-in after concurrent abort error=%v, want terminal intent", err)
	}
	var status string
	var auditCount int
	if err = p.QueryRow(ctx, `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err = p.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) || auditCount != 0 {
		t.Fatalf("aborted concurrent check-in changed Ticket: status=%q audits=%d", status, auditCount)
	}
}

func admissionIdentityForTicketFixture(f ticketRepositoryFixture, actorID uuid.UUID, key string, fingerprint []byte) admission.Identity {
	keyDigest := sha256.Sum256([]byte(key))
	var requestFingerprint [32]byte
	copy(requestFingerprint[:], fingerprint)
	return admission.Identity{RequestID: admission.RequestIDFor(actorID, keyDigest), PaymentID: f.paymentID, BookingID: f.bookingID,
		TicketID: f.ticketID, EventID: f.ticketEventID, UserID: f.userID, ActorID: actorID, Generation: 1, KeyDigest: keyDigest, Fingerprint: requestFingerprint}
}

func TestCommitTicketCheckInRejectsStalePaymentEvidenceIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC().Add(-domain.MaxPaymentEvidenceAge-time.Second))
	result, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "denied" || result.ReasonCode != "payment_evidence_stale" {
		t.Fatalf("stale Payment evidence result=%+v, want denied payment_evidence_stale", result)
	}
	var status string
	if err = p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) {
		t.Fatalf("stale evidence changed Ticket state to %s", status)
	}
	var auditCount int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("stale evidence wrote %d accepted check-in audits, want 0", auditCount)
	}
}

func TestCommitTicketCheckInPaymentEvidenceFreshnessMatrixIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	for _, tc := range []struct {
		name       string
		age        time.Duration
		missing    bool
		wantAccept bool
	}{
		{name: "fresh", age: 0, wantAccept: true},
		{name: "five seconds", age: 5 * time.Second, wantAccept: true},
		{name: "near ten second limit", age: 9 * time.Second, wantAccept: true},
		{name: "over ten second limit", age: domain.MaxPaymentEvidenceAge + time.Second},
		{name: "missing observed_at", missing: true},
		{name: "future observed_at", age: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
			observedAt := time.Now().UTC().Add(-tc.age)
			if tc.missing {
				observedAt = time.Time{}
			}
			command := acceptedTicketScanFixture(f, observedAt)
			result, err := repo.CommitTicketCheckIn(context.Background(), command)
			if tc.missing && err == nil {
				t.Fatal("missing Payment observed_at was accepted")
			}
			if !tc.missing && err != nil {
				t.Fatal(err)
			}
			wantStatus, wantAudits := string(domain.TicketIssued), 0
			if tc.wantAccept {
				if result.ResultCode != "accepted" || result.ReasonCode != "checked_in" {
					t.Fatalf("fresh evidence result=%+v, want accepted", result)
				}
				wantStatus, wantAudits = string(domain.TicketCheckedIn), 1
			} else if !tc.missing && (result.ResultCode != "denied" || result.ReasonCode != "payment_evidence_stale") {
				t.Fatalf("non-fresh evidence result=%+v, want denied payment_evidence_stale", result)
			}
			var status string
			if err := p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != wantStatus {
				t.Fatalf("Ticket status=%s, want %s", status, wantStatus)
			}
			var auditCount int
			if err := p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1 AND result_code='accepted'`, f.ticketID).Scan(&auditCount); err != nil {
				t.Fatal(err)
			}
			if auditCount != wantAudits {
				t.Fatalf("accepted check-in audit rows=%d, want %d", auditCount, wantAudits)
			}
		})
	}
}

func TestCommitTicketCheckInRejectsStaleEvidenceDespitePaymentClockSkewIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	command.Evidence.PaymentQueryStartedAt = time.Now().Add(-domain.MaxPaymentEvidenceAge - time.Second)
	result, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "denied" || result.ReasonCode != "payment_evidence_stale" {
		t.Fatalf("stale evidence masked by a fast Payment clock result=%+v, want denied payment_evidence_stale", result)
	}
}

func TestCommitTicketCheckInRejectsStaleWallElapsedDespiteShortMonotonicElapsedIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	queryStartedAt := time.Now()
	command.Evidence.PaymentQueryStartedAt = queryStartedAt.Add(-2 * time.Second)
	command.Evidence.PaymentQueryWallStartedAt = queryStartedAt.Round(0).Add(-61 * time.Second)
	result, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "denied" || result.ReasonCode != "payment_evidence_stale" {
		t.Fatalf("stale wall evidence masked by a short monotonic interval result=%+v, want denied payment_evidence_stale", result)
	}
	var status string
	if err = p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.TicketIssued) {
		t.Fatalf("stale wall evidence changed Ticket state to %s", status)
	}
	var auditCount int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("stale wall evidence wrote %d accepted check-in audits, want 0", auditCount)
	}
}

func TestCommitTicketCheckInReturnsOriginalCheckInTimeForDuplicateCredentialScanIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	checkedInAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	f := seedTicketRepositoryRows(t, p, domain.TicketCheckedIn, &checkedInAt, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	command.IdempotencyKey = "second-scan"
	fingerprint := sha256.Sum256([]byte("second-scan"))
	command.RequestFingerprint = fingerprint[:]
	result, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCode != "already_checked_in" || result.CheckedInAt == nil || !result.CheckedInAt.Equal(checkedInAt) {
		t.Fatalf("duplicate scan lost original check-in time: %+v, want %s", result, checkedInAt)
	}
}

func TestApplyTicketIssuanceReplayCreatesOneTicketAndOneReceiptIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f, item := seedIssuanceBooking(t, p)
	event := domain.TicketIssuanceEvent{ID: uuid.New(), SagaID: f.sagaID, BookingID: f.bookingID, PaymentID: f.paymentID, EventID: f.bookingEventID, Generation: 1}
	candidate, err := repo.LoadTicketIssuanceCandidate(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.SagaID != f.sagaID || candidate.PaymentID != f.paymentID || len(candidate.Items) != 1 || candidate.Items[0].ID != item {
		t.Fatalf("loaded issuance candidate lost authoritative identity: %+v", candidate)
	}
	evidence := domain.TicketAuthorityEvidence{SagaEligible: true, EventEligible: true, PaymentDecisionKnown: true, PaymentEligible: true,
		DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund, ObservedAt: time.Now().UTC(), Source: "stripe_test_api"}
	decision := domain.ValidateTicketIssuance(event, candidate, evidence)
	if !decision.Eligible {
		t.Fatalf("fixture did not meet issuance contract: %+v", decision)
	}
	for range 2 {
		if err := repo.ApplyTicketIssuance(context.Background(), event, candidate, decision, evidence); err != nil {
			t.Fatal(err)
		}
	}
	var ticketCount, receiptCount int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM tickets WHERE booking_id=$1`, f.bookingID).Scan(&ticketCount); err != nil {
		t.Fatal(err)
	}
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_issuance_receipts WHERE booking_id=$1 AND outcome='issued'`, f.bookingID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if ticketCount != 1 || receiptCount != 1 {
		t.Fatalf("duplicate issuance produced tickets=%d receipts=%d, want one each", ticketCount, receiptCount)
	}
	found, err := repo.LookupTicketIssuanceReceipt(context.Background(), event)
	if err != nil || !found {
		t.Fatalf("issuance receipt preflight found=%v error=%v, want stored receipt", found, err)
	}
	conflicting := event
	conflicting.PaymentID = uuid.New()
	if _, err = repo.LookupTicketIssuanceReceipt(context.Background(), conflicting); err != ErrTicketIdentityConflict {
		t.Fatalf("conflicting issuance preflight error=%v, want identity conflict", err)
	}
}

func TestTicketCredentialRotationReplacesDigestAndEnforcesOwnerIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	newDigest := sha256.Sum256([]byte("rotated fixture credential"))
	if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, "rotation-owner-key", "rotated fixture credential")); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LoadTicketScanCandidate(context.Background(), f.digest); err != ErrTicketCredentialMissing {
		t.Fatalf("previous digest remained usable: %v", err)
	}
	if _, err := repo.LoadTicketScanCandidate(context.Background(), newDigest[:]); err != nil {
		t.Fatalf("new digest not active: %v", err)
	}
	if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(uuid.New(), f.ticketID, "rotation-owner-denied", "owner denied fixture")); err != ErrTicketOwnerDenied {
		t.Fatalf("non-owner credential rotation error=%v, want owner denial", err)
	}
}

func TestConcurrentTicketCredentialRotationsLeaveExactlyOneCurrentCredentialIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	const rotations = 8
	digests := make([][]byte, rotations)
	tokens := make([]string, rotations)
	for i := range rotations {
		tokens[i] = fmt.Sprintf("parallel rotation credential %d", i)
		digest := sha256.Sum256([]byte(tokens[i]))
		digests[i] = append([]byte(nil), digest[:]...)
	}
	start := make(chan struct{})
	errs := make(chan error, rotations)
	var wg sync.WaitGroup
	for i := range rotations {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, fmt.Sprintf("parallel-rotation-%d", i), tokens[i]))
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent credential rotation: %v", err)
		}
	}
	var currentDigest []byte
	var version int64
	var credentialCount int
	if err := p.QueryRow(context.Background(), `SELECT token_digest,credential_version,(SELECT count(*) FROM ticket_credentials WHERE ticket_id=$1) FROM ticket_credentials WHERE ticket_id=$1`, f.ticketID).Scan(&currentDigest, &version, &credentialCount); err != nil {
		t.Fatal(err)
	}
	if credentialCount != 1 || version != rotations+1 {
		t.Fatalf("rotations left credential rows=%d version=%d, want 1 row and version %d", credentialCount, version, rotations+1)
	}
	matched := false
	for _, digest := range digests {
		if string(currentDigest) == string(digest) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatal("current credential digest is not one of the committed rotation requests")
	}
	if _, err := repo.LoadTicketScanCandidate(context.Background(), f.digest); err != ErrTicketCredentialMissing {
		t.Fatalf("original credential remained usable after concurrent rotations: %v", err)
	}
	for i, digest := range digests {
		_, err := repo.LoadTicketScanCandidate(context.Background(), digest)
		if string(currentDigest) == string(digest) {
			if err != nil {
				t.Fatalf("winning credential %d is not usable: %v", i, err)
			}
		} else if err != ErrTicketCredentialMissing {
			t.Fatalf("superseded credential %d remained usable: %v", i, err)
		}
	}
	var ticketStatus string
	if err := p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&ticketStatus); err != nil {
		t.Fatal(err)
	}
	if ticketStatus != string(domain.TicketIssued) {
		t.Fatalf("credential rotation changed Ticket status to %s", ticketStatus)
	}
	t.Logf("different-key concurrency: requests=%d ROTATED=%d current_credential_rows=%d database_version=%d exactly_one_digest_current=true original_digest_invalid=true", rotations, rotations, credentialCount, version)
}

func TestTicketCredentialRotationRejectsIneligibleTicketOrBookingIntegration(t *testing.T) {
	cases := []struct {
		name          string
		ticketStatus  domain.TicketStatus
		bookingStatus domain.BookingStatus
	}{
		{name: "checked in ticket", ticketStatus: domain.TicketCheckedIn, bookingStatus: domain.StatusConfirmed},
		{name: "revoked ticket", ticketStatus: domain.TicketRevoked, bookingStatus: domain.StatusConfirmed},
		{name: "cancelled booking", ticketStatus: domain.TicketIssued, bookingStatus: domain.StatusCancelled},
		{name: "failed booking", ticketStatus: domain.TicketIssued, bookingStatus: domain.StatusFailed},
		{name: "expired booking", ticketStatus: domain.TicketIssued, bookingStatus: domain.StatusExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, repo := newBookingOutboxIntegrationFixture(t)
			var checkedInAt *time.Time
			if tc.ticketStatus == domain.TicketCheckedIn {
				value := time.Now().UTC().Truncate(time.Microsecond)
				checkedInAt = &value
			}
			f := seedTicketRepositoryRows(t, p, tc.ticketStatus, checkedInAt, true)
			if tc.bookingStatus != domain.StatusConfirmed {
				if _, err := p.Exec(context.Background(), `UPDATE bookings SET status=$2 WHERE id=$1`, f.bookingID, string(tc.bookingStatus)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, "ineligible-rotation-key", "must not replace credential")); err == nil {
				t.Fatal("ineligible Ticket/Booking allowed credential rotation")
			}
			var storedDigest []byte
			var storedTicketStatus, storedBookingStatus string
			if err := p.QueryRow(context.Background(), `SELECT c.token_digest,t.status,b.status FROM ticket_credentials c JOIN tickets t ON t.id=c.ticket_id JOIN bookings b ON b.id=t.booking_id WHERE c.ticket_id=$1`, f.ticketID).Scan(&storedDigest, &storedTicketStatus, &storedBookingStatus); err != nil {
				t.Fatal(err)
			}
			if string(storedDigest) != string(f.digest) || storedTicketStatus != string(tc.ticketStatus) || storedBookingStatus != string(tc.bookingStatus) {
				t.Fatalf("rejected rotation changed state: digest_replaced=%v ticket=%s booking=%s", string(storedDigest) != string(f.digest), storedTicketStatus, storedBookingStatus)
			}
		})
	}
}

func TestListOwnedTicketsIncludesCurrentBookingStatusIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `UPDATE bookings SET status='CANCELLED' WHERE id=$1`, f.bookingID); err != nil {
		t.Fatal(err)
	}
	tickets, err := repo.ListOwnedTickets(context.Background(), f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tickets) != 1 || tickets[0].Status != domain.TicketIssued || tickets[0].BookingStatus != domain.StatusCancelled {
		t.Fatalf("Wallet projection=%+v, want persisted ISSUED and current CANCELLED Booking", tickets)
	}
}

func TestScannerAssignmentGrantRevokeIsEventScopedAndAuditedIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	staffID, actorID := uuid.New(), uuid.New()
	if _, err := repo.SetScannerAssignment(context.Background(), f.bookingEventID, staffID, actorID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetScannerAssignment(context.Background(), f.bookingEventID, staffID, actorID, true); err != nil {
		t.Fatal(err)
	}
	assigned, err := repo.IsScannerAssigned(context.Background(), f.bookingEventID, staffID)
	if err != nil || !assigned {
		t.Fatalf("granted staff not active: assigned=%v error=%v", assigned, err)
	}
	if _, err = repo.SetScannerAssignment(context.Background(), f.bookingEventID, staffID, actorID, false); err != nil {
		t.Fatal(err)
	}
	assigned, err = repo.IsScannerAssigned(context.Background(), f.bookingEventID, staffID)
	if err != nil || assigned {
		t.Fatalf("revoked staff still active: assigned=%v error=%v", assigned, err)
	}
	var auditCount int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM event_scanner_assignment_audit WHERE event_id=$1 AND staff_user_id=$2`, f.bookingEventID, staffID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 2 {
		t.Fatalf("idempotent grant/revoke audit rows=%d, want grant and revoke only", auditCount)
	}
	otherEvent := uuid.New()
	assigned, err = repo.IsScannerAssigned(context.Background(), otherEvent, staffID)
	if err != nil || assigned {
		t.Fatalf("assignment leaked to another Event: assigned=%v error=%v", assigned, err)
	}
}

func TestScannerAssignmentRevokeSerializesWithConcurrentFirstGrantIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	staffID, actorID := uuid.New(), uuid.New()
	ctx := context.Background()
	if _, err := p.Exec(ctx, `CREATE FUNCTION ticket_test_delay_assignment_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `CREATE TRIGGER ticket_test_delay_assignment_insert BEFORE INSERT ON event_scanner_assignments
		FOR EACH ROW EXECUTE FUNCTION ticket_test_delay_assignment_insert()`); err != nil {
		t.Fatal(err)
	}
	grantDone := make(chan error, 1)
	go func() {
		_, err := repo.SetScannerAssignment(ctx, f.bookingEventID, staffID, actorID, true)
		grantDone <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	grantPaused := false
	for time.Now().Before(deadline) {
		var waiting int
		if err := p.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND wait_event='PgSleep' AND query ILIKE '%INSERT INTO event_scanner_assignments%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			grantPaused = true
			break
		}
		select {
		case err := <-grantDone:
			t.Fatalf("first grant completed before controlled pause: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !grantPaused {
		err := <-grantDone
		t.Fatalf("did not observe first grant at delayed insert boundary: %v", err)
	}
	revokeDone := make(chan error, 1)
	go func() {
		_, err := repo.SetScannerAssignment(ctx, f.bookingEventID, staffID, actorID, false)
		revokeDone <- err
	}()
	if err := <-revokeDone; err != nil {
		t.Fatalf("concurrent revoke: %v", err)
	}
	if err := <-grantDone; err != nil {
		t.Fatalf("concurrent grant: %v", err)
	}
	assigned, err := repo.IsScannerAssigned(ctx, f.bookingEventID, staffID)
	if err != nil || assigned {
		t.Fatalf("grant-then-revoke race left scanner active: assigned=%v error=%v", assigned, err)
	}
	var auditCount int
	if err = p.QueryRow(ctx, `SELECT count(*) FROM event_scanner_assignment_audit WHERE event_id=$1 AND staff_user_id=$2`, f.bookingEventID, staffID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 2 {
		t.Fatalf("grant-then-revoke race audit rows=%d, want grant and revoke", auditCount)
	}
}

func TestConcurrentTicketCheckInAcceptsOnceAndKeepsReplayOutcomeIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	const scanners = 8
	type outcome struct {
		result domain.TicketScanResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, scanners)
	for i := range scanners {
		go func(i int) {
			<-start
			command := acceptedTicketScanFixture(f, time.Now().UTC())
			command.IdempotencyKey = fmt.Sprintf("parallel-%d", i)
			fingerprint := sha256.Sum256([]byte(command.IdempotencyKey))
			command.RequestFingerprint = fingerprint[:]
			result, err := repo.CommitTicketCheckIn(context.Background(), command)
			results <- outcome{result: result, err: err}
		}(i)
	}
	close(start)
	accepted, already := 0, 0
	for i := 0; i < scanners; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		switch got.result.ResultCode {
		case "accepted":
			accepted++
		case "already_checked_in":
			already++
			if got.result.CheckedInAt == nil {
				t.Fatal("already checked-in response omitted original check-in time")
			}
		default:
			t.Fatalf("unexpected concurrent scan result: %+v", got.result)
		}
	}
	if accepted != 1 || already != scanners-1 {
		t.Fatalf("concurrent check-ins accepted=%d already=%d, want 1 and %d", accepted, already, scanners-1)
	}
	var auditCount int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("concurrent check-in audit count=%d, want one", auditCount)
	}
	t.Logf("check-in concurrency: requests=%d accepted=%d already_checked_in=%d check-in_audit_rows=%d", scanners, accepted, already, auditCount)
}

func TestTicketScanIdempotencyReturnsOriginalAndRejectsKeyReuseIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	// Exercise precision PostgreSQL cannot retain in timestamptz.
	command.Evidence.ObservedAt = command.Evidence.ObservedAt.Truncate(time.Microsecond).Add(789 * time.Nanosecond)
	first, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil || first.ResultCode != "accepted" || first.CheckedInAt == nil {
		t.Fatalf("first check-in=%+v error=%v", first, err)
	}
	replayed, err := repo.CommitTicketCheckIn(context.Background(), command)
	if err != nil || !replayed.Replayed || replayed.ResultCode != first.ResultCode || replayed.CheckedInAt == nil || !replayed.CheckedInAt.Equal(*first.CheckedInAt) {
		t.Fatalf("idempotent replay=%+v error=%v, want original accepted result", replayed, err)
	}
	preflight, found, err := repo.LookupTicketScanRequest(context.Background(), command.StaffUserID, command.EventID, command.IdempotencyKey, command.RequestFingerprint)
	if err != nil || !found || !preflight.Replayed || preflight.ResultCode != first.ResultCode || preflight.CheckedInAt == nil || !preflight.CheckedInAt.Equal(*first.CheckedInAt) {
		t.Fatalf("idempotent preflight=%+v found=%v error=%v, want original accepted result", preflight, found, err)
	}
	conflicting := command
	for name, got := range map[string]domain.TicketScanResult{"first": first, "replay": replayed, "preflight": preflight} {
		if got.ObservedAt == nil || !got.ObservedAt.Equal(command.Evidence.ObservedAt.Truncate(time.Microsecond)) {
			t.Fatalf("%s observed_at=%v, want persisted microsecond timestamp", name, got.ObservedAt)
		}
		if got.ReasonCode != first.ReasonCode || got.EvidenceSource != first.EvidenceSource {
			t.Fatalf("%s changed persisted response metadata", name)
		}
	}
	fingerprint := sha256.Sum256([]byte("different request under same idempotency key"))
	conflicting.RequestFingerprint = fingerprint[:]
	if _, err = repo.CommitTicketCheckIn(context.Background(), conflicting); err != ErrTicketIdempotencyConflict {
		t.Fatalf("idempotency key reuse error=%v, want conflict", err)
	}
	if _, _, err = repo.LookupTicketScanRequest(context.Background(), conflicting.StaffUserID, conflicting.EventID, conflicting.IdempotencyKey, conflicting.RequestFingerprint); err != ErrTicketIdempotencyConflict {
		t.Fatalf("idempotency preflight key reuse error=%v, want conflict", err)
	}
	if _, err = repo.SetScannerAssignment(context.Background(), command.EventID, command.StaffUserID, uuid.New(), false); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.LookupTicketScanRequest(context.Background(), command.StaffUserID, command.EventID, command.IdempotencyKey, command.RequestFingerprint); err != ErrScannerNotAssigned {
		t.Fatalf("revoked scanner preflight error=%v, want scanner authorization denial", err)
	}
}

func seedIssuanceBooking(t *testing.T, p *pgxpool.Pool) (ticketRepositoryFixture, uuid.UUID) {
	t.Helper()
	f := ticketRepositoryFixture{bookingID: uuid.New(), bookingEventID: uuid.New(), ticketEventID: uuid.New(), userID: uuid.New(), sagaID: uuid.New(), paymentID: uuid.New()}
	itemID, tierID, seatID := uuid.New(), uuid.New(), uuid.New()
	if _, err := p.Exec(context.Background(), `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',725,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+interval '1 hour',$5)`, f.bookingID, f.userID, f.bookingEventID, uuid.New(), f.sagaID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,1,725,$4)`, itemID, f.bookingID, tierID, []uuid.UUID{seatID}); err != nil {
		t.Fatal(err)
	}
	return f, itemID
}

func seedTicketRepositoryRows(t *testing.T, p *pgxpool.Pool, status domain.TicketStatus, checkedInAt *time.Time, sameEvent bool) ticketRepositoryFixture {
	t.Helper()
	ctx := context.Background()
	f := ticketRepositoryFixture{bookingID: uuid.New(), bookingEventID: uuid.New(), ticketEventID: uuid.New(), userID: uuid.New(), staffID: uuid.New(), ticketID: uuid.New(), sagaID: uuid.New(), paymentID: uuid.New()}
	if sameEvent {
		f.ticketEventID = f.bookingEventID
	}
	if _, err := p.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',725,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+interval '1 hour',$5)`, f.bookingID, f.userID, f.bookingEventID, uuid.New(), f.sagaID); err != nil {
		t.Fatal(err)
	}
	itemID, tierID, seatID := uuid.New(), uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,1,725,$4)`, itemID, f.bookingID, tierID, []uuid.UUID{seatID}); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("fixture credential:" + f.ticketID.String()))
	f.digest = digest[:]
	if _, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status,checked_in_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10)`, f.ticketID, f.bookingID, itemID, f.sagaID, f.paymentID, f.ticketEventID, tierID, seatID, string(status), checkedInAt); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO ticket_credentials(ticket_id,token_digest,expires_at,credential_version) VALUES($1,$2,clock_timestamp()+interval '5 minutes',1)`, f.ticketID, f.digest); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at) VALUES($1,$2,true,$3,clock_timestamp())`, f.ticketEventID, f.staffID, uuid.New()); err != nil {
		t.Fatal(err)
	}
	f.checkedInAt = checkedInAt
	return f
}

func acceptedTicketScanFixture(f ticketRepositoryFixture, observedAt time.Time) domain.TicketScanCommand {
	fingerprint := sha256.Sum256([]byte("ticket scan request"))
	queryStartedAt := time.Now()
	return domain.TicketScanCommand{
		EventID: f.ticketEventID, StaffUserID: f.staffID, TicketID: f.ticketID,
		CredentialDigest: f.digest, RequestFingerprint: fingerprint[:], IdempotencyKey: "scan-1",
		Decision: domain.TicketIssuanceDecision{Eligible: true},
		Evidence: domain.TicketAuthorityEvidence{SagaEligible: true, EventEligible: true, PaymentDecisionKnown: true, PaymentEligible: true, DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund, ObservedAt: observedAt, PaymentQueryStartedAt: queryStartedAt, PaymentQueryWallStartedAt: queryStartedAt.Round(0), Source: "stripe_test_api"},
	}
}
