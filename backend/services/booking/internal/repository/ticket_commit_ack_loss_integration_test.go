//go:build recoverycomponents

package repository

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
)

func TestBookingAdmissionCommitAckLossIntegration(t *testing.T) {
	observerPool, observerRepo := newBookingOutboxIntegrationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var synchronousCommit, fsync string
	if err := observerPool.QueryRow(ctx, `SELECT current_setting('synchronous_commit'), current_setting('fsync')`).Scan(&synchronousCommit, &fsync); err != nil {
		t.Fatal("read isolated PostgreSQL commit durability settings:", err)
	}
	if strings.EqualFold(synchronousCommit, "off") || !strings.EqualFold(fsync, "on") {
		t.Fatalf("BLOCKED: commit acknowledgment loss requires synchronous_commit other than off and fsync=on; got %q/%q", synchronousCommit, fsync)
	}

	dropper := admissiontest.NewCommitAckLoss()
	proxyConfig, err := dropper.SingleConnectionPoolConfig(observerPool.Config())
	if err != nil {
		t.Fatal("configure test-only one-connection COMMIT acknowledgment dropper:", err)
	}
	targetPool, err := pgxpool.NewWithConfig(ctx, proxyConfig)
	if err != nil {
		t.Fatal("create isolated Booking repository pool with test-only COMMIT acknowledgment dropper:", err)
	}
	t.Cleanup(targetPool.Close)
	targetRepo := NewPostgresBookingRepository(targetPool)

	fixture := seedTicketRepositoryRows(t, observerPool, domain.TicketIssued, nil, true)
	if _, err := observerPool.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal("activate isolated Booking admission mode:", err)
	}
	command := acceptedTicketScanFixture(fixture, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(fixture, command.StaffUserID, command.IdempotencyKey, command.RequestFingerprint)
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 37, State: admission.Pending}
	if err := observerRepo.EnsureAdmissionIntent(ctx, identity); err != nil {
		t.Fatal("persist Booking admission intent:", err)
	}
	if err := observerRepo.AttachAdmissionPermit(ctx, permit); err != nil {
		t.Fatal("attach test Payment permit:", err)
	}
	// Refresh the evidence timestamps after setup transactions, then arm at the
	// direct repository boundary so setup COMMITs cannot consume the fault.
	command = acceptedTicketScanFixture(fixture, time.Now().UTC())
	command.Admission = &permit
	if !dropper.Arm() {
		t.Fatal("could not arm one-shot COMMIT acknowledgment drop")
	}
	_, commitErr := targetRepo.CommitTicketCheckIn(ctx, command)
	if commitErr == nil {
		t.Fatal("check-in caller received success after its COMMIT completion was dropped")
	}
	if got := dropper.DroppedCommitCount(); got != 1 {
		t.Fatalf("dropped PostgreSQL COMMIT completion frames=%d; want exactly one", got)
	}

	var ticketState string
	var linkedRequest uuid.UUID
	if err := observerPool.QueryRow(ctx, `SELECT status,admission_request_id FROM tickets WHERE id=$1`, fixture.ticketID).Scan(&ticketState, &linkedRequest); err != nil {
		t.Fatal("independent pool could not observe committed Ticket:", err)
	}
	var intentState string
	var storedToken uuid.UUID
	var storedFence int64
	if err := observerPool.QueryRow(ctx, `SELECT state,permit_token,permit_fence FROM ticket_admission_intents WHERE request_id=$1`, identity.RequestID).Scan(&intentState, &storedToken, &storedFence); err != nil {
		t.Fatal("independent pool could not observe committed Booking intent:", err)
	}
	audits, receipts, completed := bookingCommitAckFactCounts(t, ctx, observerPool, fixture.ticketID)
	if ticketState != string(domain.TicketCheckedIn) || linkedRequest != identity.RequestID || intentState != string(admission.Committed) || storedToken != permit.Token || storedFence != permit.Fence || audits != 1 || receipts != 1 || completed != 1 {
		t.Fatalf("independent committed state ticket=%q/%s intent=%q/%s/%d audit=%d receipt=%d completed=%d", ticketState, linkedRequest, intentState, storedToken, storedFence, audits, receipts, completed)
	}

	outcome, err := observerRepo.ResolveAdmission(ctx, identity, permit.Token, permit.Fence, true)
	if err != nil || outcome.State != admission.Committed || outcome.Token != permit.Token || outcome.Fence != permit.Fence {
		t.Fatalf("abort reconciliation after committed check-in=%+v err=%v; want original committed outcome", outcome, err)
	}
	replay, err := targetRepo.CommitTicketCheckIn(ctx, command)
	if err != nil || !replay.Replayed || replay.ResultCode != "accepted" {
		t.Fatalf("same-command check-in replay=%+v err=%v; want accepted replay", replay, err)
	}
	auditsAfter, receiptsAfter, completedAfter := bookingCommitAckFactCounts(t, ctx, observerPool, fixture.ticketID)
	if got := dropper.DroppedCommitCount(); got != 1 {
		t.Fatalf("COMMIT completion drops after replay=%d; want exactly one", got)
	}
	if auditsAfter != audits || receiptsAfter != receipts || completedAfter != completed {
		t.Fatalf("replay changed durable facts: before=%d/%d/%d after=%d/%d/%d", audits, receipts, completed, auditsAfter, receiptsAfter, completedAfter)
	}
	if auditsAfter != 1 || receiptsAfter != 1 || completedAfter != 1 {
		t.Fatalf("durable check-in facts after replay=%d/%d/%d; want exactly one audit, receipt, and idempotency record", auditsAfter, receiptsAfter, completedAfter)
	}

	writeBookingCommitAckLossEvidence(t, struct {
		Status                     string `json:"status"`
		DroppedCommitFrames        int    `json:"dropped_commit_frames"`
		CallerSawError             bool   `json:"caller_saw_error"`
		TicketState                string `json:"ticket_state"`
		IntentState                string `json:"intent_state"`
		CheckInAuditRows           int    `json:"check_in_audit_rows"`
		ScanReceiptRows            int    `json:"scan_receipt_rows"`
		CompletedIdempotencyRows   int    `json:"completed_idempotency_rows"`
		AbortResolveReturnedCommit bool   `json:"abort_resolve_returned_committed"`
		SameCommandReplayed        bool   `json:"same_command_replayed"`
		ReplayKeptSingleAudit      bool   `json:"replay_kept_single_audit_and_receipt"`
		DurabilitySettings         string `json:"durability_settings"`
	}{
		Status:              "OBSERVED_BY_TEST_SOURCE; execution status is the captured go test result",
		DroppedCommitFrames: int(dropper.DroppedCommitCount()), CallerSawError: true,
		TicketState: ticketState, IntentState: intentState,
		CheckInAuditRows: audits, ScanReceiptRows: receipts, CompletedIdempotencyRows: completed,
		AbortResolveReturnedCommit: outcome.State == admission.Committed, SameCommandReplayed: replay.Replayed,
		ReplayKeptSingleAudit: auditsAfter == 1 && receiptsAfter == 1 && completedAfter == 1,
		DurabilitySettings:    synchronousCommit + "/fsync=" + fsync,
	})
}

func bookingCommitAckFactCounts(t testing.TB, ctx context.Context, pool *pgxpool.Pool, ticketID uuid.UUID) (audits, receipts, completed int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, ticketID).Scan(&audits); err != nil {
		t.Fatal("read Booking check-in audit count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_scan_requests WHERE ticket_id=$1`, ticketID).Scan(&receipts); err != nil {
		t.Fatal("read Booking scan receipt count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_operation_idempotency WHERE ticket_id=$1 AND operation='CHECK_IN' AND completed_at IS NOT NULL`, ticketID).Scan(&completed); err != nil {
		t.Fatal("read Booking completed idempotency count:", err)
	}
	return audits, receipts, completed
}

func writeBookingCommitAckLossEvidence(t testing.TB, evidence any) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal("create Booking COMMIT acknowledgment evidence directory:", err)
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal("encode Booking COMMIT acknowledgment evidence:", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "booking-commit-ack-loss-evidence.json"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal("write Booking COMMIT acknowledgment evidence:", err)
	}
}
