package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

func beginRefundTimeAttempt(t *testing.T, r *PostgresBookingRepository, f unusedRefundFixture) domain.RefundApprovalAttempt {
	t.Helper()
	attempt, err := r.BeginUnusedRefundApproval(context.Background(), domain.BeginUnusedRefundApprovalCommand{
		AttemptID: f.decision.DecisionID, RequestID: f.request.RequestID, EventID: f.bookingEventID, ApproverID: f.approver,
		Reason: f.decision.Reason, KeyDigest: f.decision.KeyDigest, Fingerprint: f.decision.Fingerprint,
	})
	if err != nil {
		t.Fatalf("begin durable refund approval attempt: %v", err)
	}
	return attempt
}

func TestRefundTimePrestartQualificationAllowsLaterBookingCommitIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	if attempt.State != domain.RefundApprovalAttemptPending || attempt.AttemptID != f.decision.DecisionID || attempt.ApproverID != f.approver ||
		attempt.KeyDigest != f.decision.KeyDigest || attempt.Fingerprint != f.decision.Fingerprint || attempt.Reason != f.decision.Reason {
		t.Fatalf("persisted attempt=%+v, want pending full stable binding", attempt)
	}

	// Production qualification comes from Event PostgreSQL. Use a database
	// timestamp here too, so exact evidence comparisons use its stored precision
	// rather than comparing a nanosecond Go clock value with a microsecond value.
	var startsAt time.Time
	if err := p.QueryRow(context.Background(), `SELECT clock_timestamp() - interval '1 minute'`).Scan(&startsAt); err != nil {
		t.Fatal(err)
	}
	qualifiedAt := startsAt.Add(-time.Second)
	command := f.decision
	command.DecisionID = attempt.AttemptID
	command.QualificationAttemptID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{EventID: f.bookingEventID, StartsAt: startsAt, AuthorityVersion: "4",
		QualificationAttemptID: attempt.AttemptID, EventQualifiedAt: qualifiedAt}
	approved, err := r.FinalizeUnusedRefundApproval(context.Background(), command)
	if err != nil || approved.Status != domain.RefundRequestApproved {
		t.Fatalf("post-start Booking commit result=%+v error=%v, want approved from pre-start Event qualification", approved, err)
	}
	if approved.QualificationAttemptID != attempt.AttemptID || !approved.EventQualifiedAt.Equal(qualifiedAt) {
		t.Fatalf("approved request qualification proof=%+v, want attempt %s at %s", approved, attempt.AttemptID, qualifiedAt)
	}
	decision, err := r.GetApprovedUnusedRefundDecision(context.Background(), f.bookingID, attempt.AttemptID)
	if err != nil || decision.QualificationAttemptID != attempt.AttemptID || !decision.EventQualifiedAt.Equal(qualifiedAt) {
		t.Fatalf("approved decision proof=%+v error=%v", decision, err)
	}
	persisted, err := r.GetUnusedRefundApprovalAttempt(context.Background(), attempt.AttemptID)
	if err != nil || persisted.State != domain.RefundApprovalAttemptApproved || !persisted.EventQualifiedAt.Equal(qualifiedAt) || persisted.ReleaseAcknowledged {
		t.Fatalf("terminal approval attempt=%+v error=%v, want approved and awaiting Event release", persisted, err)
	}
	if err = r.AcknowledgeUnusedRefundAttemptRelease(context.Background(), attempt.AttemptID, string(domain.RefundApprovalAttemptApproved)); err != nil {
		t.Fatalf("acknowledge Event release: %v", err)
	}
	items, err := r.ListUnusedRefundApprovalAttemptsForRecovery(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.AttemptID == attempt.AttemptID {
			t.Fatal("released terminal attempt remained in recovery list")
		}
	}
}

func TestRefundTimeSameKeyFinalRetryReusesDecisionAndOutboxIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	startsAt := time.Now().UTC().Add(time.Hour)
	qualifiedAt := time.Now().UTC()
	command := f.decision
	command.DecisionID = attempt.AttemptID
	command.QualificationAttemptID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{EventID: f.bookingEventID, StartsAt: startsAt, AuthorityVersion: "8",
		QualificationAttemptID: attempt.AttemptID, EventQualifiedAt: qualifiedAt}
	approved, err := r.FinalizeUnusedRefundApproval(context.Background(), command)
	if err != nil || approved.Status != domain.RefundRequestApproved {
		t.Fatalf("initial coordinated approval=%+v error=%v", approved, err)
	}

	retry := f.decision
	retry.DecisionID = attempt.AttemptID
	retry.Event = domain.RefundEventSnapshot{}
	retry.Payment = domain.UnusedRefundPaymentSnapshot{}
	replayed, err := r.DecideUnusedRefund(context.Background(), retry)
	if err != nil || replayed.Status != domain.RefundRequestApproved || replayed.DecisionID != approved.DecisionID || replayed.DecisionID != attempt.AttemptID {
		t.Fatalf("same-key final retry=%+v error=%v, want the original approved decision %s", replayed, err, attempt.AttemptID)
	}
	var decisionRows, outboxRows int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM booking_refund_decisions WHERE request_id=$1 AND decision_id=$2`, f.request.RequestID, attempt.AttemptID).Scan(&decisionRows); err != nil || decisionRows != 1 {
		t.Fatalf("same-key retry decision rows=%d error=%v, want one", decisionRows, err)
	}
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE id=$1 AND event_type=$2`, attempt.AttemptID, domain.TypeUnusedTicketRefundApprovedV1).Scan(&outboxRows); err != nil || outboxRows != 1 {
		t.Fatalf("same-key retry outbox rows=%d error=%v, want one", outboxRows, err)
	}
}

func TestRefundTimeLateFinalizeCannotReviveAbortedAttemptIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	aborted, err := r.AbortUnusedRefundApprovalAttempt(context.Background(), attempt.AttemptID, "request_withdrawn")
	if err != nil || aborted.State != domain.RefundApprovalAttemptAborted {
		t.Fatalf("abort attempt=%+v error=%v", aborted, err)
	}
	startsAt := time.Now().UTC().Add(time.Hour)
	command := f.decision
	command.DecisionID = attempt.AttemptID
	command.QualificationAttemptID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{EventID: f.bookingEventID, StartsAt: startsAt, AuthorityVersion: "5",
		QualificationAttemptID: attempt.AttemptID, EventQualifiedAt: startsAt.Add(-time.Second)}
	if _, err = r.FinalizeUnusedRefundApproval(context.Background(), command); !errors.Is(err, ErrRefundApprovalAttemptFinal) {
		t.Fatalf("late finalization error=%v, want terminal-attempt rejection", err)
	}
	var bookingDecision *uuid.UUID
	if err = p.QueryRow(context.Background(), `SELECT refund_decision_id FROM bookings WHERE id=$1`, f.bookingID).Scan(&bookingDecision); err != nil || bookingDecision != nil {
		t.Fatalf("late Acquire revived aborted attempt: booking decision=%v error=%v", bookingDecision, err)
	}
	var decisions int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM booking_refund_decisions WHERE request_id=$1`, f.request.RequestID).Scan(&decisions); err != nil || decisions != 0 {
		t.Fatalf("late finalize wrote %d decision rows error=%v", decisions, err)
	}
}

func TestRefundTimePermissionRevocationAbortsAttemptWithoutApprovalIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	if _, err := r.SetEventRefundApprover(context.Background(), f.bookingEventID, f.approver, uuid.New(), false); err != nil {
		t.Fatalf("revoke approval authority: %v", err)
	}
	startsAt := time.Now().UTC().Add(time.Hour)
	command := f.decision
	command.DecisionID = attempt.AttemptID
	command.QualificationAttemptID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{EventID: f.bookingEventID, StartsAt: startsAt, AuthorityVersion: "6",
		QualificationAttemptID: attempt.AttemptID, EventQualifiedAt: startsAt.Add(-time.Second)}
	if _, err := r.FinalizeUnusedRefundApproval(context.Background(), command); !errors.Is(err, ErrRefundRequestApproverDenied) {
		t.Fatalf("finalize after permission revocation error=%v, want approver denial", err)
	}
	persisted, err := r.GetUnusedRefundApprovalAttempt(context.Background(), attempt.AttemptID)
	if err != nil || persisted.State != domain.RefundApprovalAttemptAborted {
		t.Fatalf("revoked approval attempt=%+v error=%v, want durable ABORTED state", persisted, err)
	}
	var bookingDecision *uuid.UUID
	if err = p.QueryRow(context.Background(), `SELECT refund_decision_id FROM bookings WHERE id=$1`, f.bookingID).Scan(&bookingDecision); err != nil || bookingDecision != nil {
		t.Fatalf("revoked approver created Booking block %v error=%v", bookingDecision, err)
	}
	var decisions int
	if err = p.QueryRow(context.Background(), `SELECT count(*) FROM booking_refund_decisions WHERE request_id=$1`, f.request.RequestID).Scan(&decisions); err != nil || decisions != 0 {
		t.Fatalf("revoked approver wrote %d decision rows error=%v", decisions, err)
	}
}

func TestRefundTimeWithdrawalAtomicallyAbortsPendingAttemptIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	_, err := r.WithdrawUnusedRefund(context.Background(), domain.WithdrawUnusedRefundCommand{
		RequestID: f.request.RequestID, BuyerID: f.userID, KeyDigest: sha256.Sum256([]byte("withdraw")), Fingerprint: sha256.Sum256([]byte("withdraw-fingerprint")),
	})
	if err != nil {
		t.Fatalf("withdraw request with pending attempt: %v", err)
	}
	persisted, err := r.GetUnusedRefundApprovalAttempt(context.Background(), attempt.AttemptID)
	if err != nil || persisted.State != domain.RefundApprovalAttemptAborted {
		t.Fatalf("withdraw left attempt=%+v error=%v, want durable ABORTED tombstone", persisted, err)
	}
	request, err := r.GetUnusedRefundByRequest(context.Background(), f.userID, f.request.RequestID)
	if err != nil || request.Status != domain.RefundRequestWithdrawn {
		t.Fatalf("withdraw request=%+v error=%v", request, err)
	}
}

func TestRefundTimeTerminalAttemptReplayKeepsSingleRequestIdentityIntegration(t *testing.T) {
	_, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, r.pool, r)
	activateUnusedRefund(t, r.pool)
	attempt := beginRefundTimeAttempt(t, r, f)
	if _, err := r.AbortUnusedRefundApprovalAttempt(context.Background(), attempt.AttemptID, "test_abort"); err != nil {
		t.Fatalf("abort approval attempt: %v", err)
	}
	retry := domain.BeginUnusedRefundApprovalCommand{AttemptID: uuid.New(), RequestID: f.request.RequestID, EventID: f.bookingEventID, ApproverID: f.approver,
		Reason: f.decision.Reason, KeyDigest: f.decision.KeyDigest, Fingerprint: f.decision.Fingerprint}
	replayed, err := r.BeginUnusedRefundApproval(context.Background(), retry)
	if err != nil || replayed.AttemptID != attempt.AttemptID || replayed.State != domain.RefundApprovalAttemptAborted {
		t.Fatalf("terminal replay=%+v error=%v, want same ABORTED attempt", replayed, err)
	}
	retry.KeyDigest = sha256.Sum256([]byte("different-key"))
	retry.Fingerprint = sha256.Sum256([]byte("different-approval-content"))
	if _, err = r.BeginUnusedRefundApproval(context.Background(), retry); !errors.Is(err, ErrRefundApprovalAttemptConflict) {
		t.Fatalf("different approval binding reused request attempt: %v", err)
	}
}

func TestRefundTimeRecoveryFairnessDoesNotStarveTerminalReleaseIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	activateUnusedRefund(t, p)
	pendingFixture := seedUnusedRefund(t, p, r)
	terminalFixture := seedUnusedRefund(t, p, r)
	pendingAttempt := beginRefundTimeAttempt(t, r, pendingFixture)
	terminalAttempt := beginRefundTimeAttempt(t, r, terminalFixture)
	if _, err := r.AbortUnusedRefundApprovalAttempt(context.Background(), terminalAttempt.AttemptID, "test_abort"); err != nil {
		t.Fatalf("persist terminal release work: %v", err)
	}
	first, err := r.ListUnusedRefundApprovalAttemptsForRecovery(context.Background(), 1)
	if err != nil || len(first) != 1 || first[0].AttemptID != pendingAttempt.AttemptID {
		t.Fatalf("first fair recovery batch=%+v error=%v, want oldest pending attempt", first, err)
	}
	second, err := r.ListUnusedRefundApprovalAttemptsForRecovery(context.Background(), 1)
	if err != nil || len(second) != 1 || second[0].AttemptID != terminalAttempt.AttemptID {
		t.Fatalf("second fair recovery batch=%+v error=%v, want terminal release work after pending touch", second, err)
	}
}

func TestRefundTimePendingAdmissionPersistsWaitForExistingAttemptIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	attempt := beginRefundTimeAttempt(t, r, f)
	ctx := context.Background()
	scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
	if err := r.EnsureAdmissionIntent(ctx, identity); err != nil {
		t.Fatal(err)
	}
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 1, State: admission.Pending}
	if err := r.AttachAdmissionPermit(ctx, permit); err != nil {
		t.Fatal(err)
	}
	command := f.decision
	command.DecisionID = attempt.AttemptID
	command.Event = domain.RefundEventSnapshot{}
	command.Payment = domain.UnusedRefundPaymentSnapshot{}
	waiting, err := r.DecideUnusedRefund(ctx, command)
	if err != nil || waiting.Status != domain.RefundRequestWaitingCheckInResolution || waiting.QualificationAttemptID != attempt.AttemptID {
		t.Fatalf("pending Admission result=%+v error=%v, want persisted WAITING on same attempt", waiting, err)
	}
	persistedAttempt, err := r.GetUnusedRefundApprovalAttempt(ctx, attempt.AttemptID)
	if err != nil || persistedAttempt.State != domain.RefundApprovalAttemptPending {
		t.Fatalf("pending Admission changed attempt=%+v error=%v, want PENDING", persistedAttempt, err)
	}
	var bookingDecision *uuid.UUID
	if err = p.QueryRow(ctx, `SELECT refund_decision_id FROM bookings WHERE id=$1`, f.bookingID).Scan(&bookingDecision); err != nil || bookingDecision != nil {
		t.Fatalf("pending Admission created Booking refund block %v error=%v", bookingDecision, err)
	}
	var commands int
	if err = p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type=$1`, domain.TypeUnusedTicketRefundApprovedV1).Scan(&commands); err != nil || commands != 0 {
		t.Fatalf("pending Admission emitted %d refund commands error=%v", commands, err)
	}
}
