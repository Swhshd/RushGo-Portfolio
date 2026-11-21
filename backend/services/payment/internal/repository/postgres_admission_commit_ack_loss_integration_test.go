//go:build recoverycomponents

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
)

func TestPaymentAdmissionCommitAckLossIntegration(t *testing.T) {
	observerPool, fixtureCtx, observerRepo := newRefundComponentRepository(t)
	ctx, cancel := context.WithTimeout(fixtureCtx, 45*time.Second)
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
		t.Fatal("create isolated Payment repository pool with test-only COMMIT acknowledgment dropper:", err)
	}
	t.Cleanup(targetPool.Close)
	targetRepo := NewPostgresPaymentRepository(targetPool)

	op := seedPaymentAdmissionEligible(t, ctx, observerPool, observerRepo)
	if err := observerRepo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal("activate isolated Payment admission mode:", err)
	}
	identity := paymentAdmissionIdentity(op)
	if !dropper.Arm() {
		t.Fatal("could not arm one-shot COMMIT acknowledgment drop")
	}
	_, grantErr := targetRepo.GrantAdmission(ctx, identity, paymentAdmissionEvidence())
	if grantErr == nil || !errors.Is(grantErr, admission.ErrUnavailable) {
		t.Fatalf("grant caller error=%v; want unknown outcome reported as unavailable", grantErr)
	}
	if got := dropper.DroppedCommitCount(); got != 1 {
		t.Fatalf("dropped PostgreSQL COMMIT completion frames=%d; want exactly one", got)
	}

	permitBeforeRetry, err := observerRepo.GetAdmission(ctx, identity)
	if err != nil {
		t.Fatal("independent pool could not observe committed Payment permit:", err)
	}
	if permitBeforeRetry.State != admission.Pending || permitBeforeRetry.Token == uuid.Nil || permitBeforeRetry.Fence <= 0 {
		t.Fatalf("independently observed permit=%+v; want one durable pending token/fence", permitBeforeRetry)
	}
	var permitRows int
	if err := observerPool.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID).Scan(&permitRows); err != nil || permitRows != 1 {
		t.Fatalf("independent permit row count=%d err=%v; want exactly one", permitRows, err)
	}

	if err := requireAdmissionTestRefund(t, ctx, observerRepo, op); err != nil {
		t.Fatal("create isolated refund obligation:", err)
	}
	claims, err := observerRepo.ClaimFinancialWork(ctx, 1, 30*time.Second)
	if err != nil || len(claims) != 0 {
		t.Fatalf("refund claims while the permit is pending=%+v err=%v; want none", claims, err)
	}
	assertAdmissionRefundBookkeeping(t, ctx, observerPool, op.PaymentID, 0, nil)
	prepared := paymentAdmissionPreparedAudit(t, ctx, observerRepo, op)
	var guardErr *pgconn.PgError
	if err := observerRepo.AppendRefundAudit(ctx, prepared); !errors.As(err, &guardErr) ||
		guardErr.Code != "P0001" || guardErr.Message != "refund preparation waits for pending payment admission" {
		t.Fatalf("PREPARED error=%v; want the pending-admission SQL guard", err)
	}
	assertAdmissionPreparedCount(t, ctx, observerRepo, op.PaymentID, 0)

	permitRetry, err := targetRepo.GrantAdmission(ctx, identity, paymentAdmissionEvidence())
	if err != nil || permitRetry != permitBeforeRetry {
		t.Fatalf("same-identity grant retry=%+v err=%v; want original permit %+v", permitRetry, err, permitBeforeRetry)
	}
	permitAfterRetry, err := observerRepo.GetAdmission(ctx, identity)
	if err != nil || permitAfterRetry != permitBeforeRetry {
		t.Fatalf("independent permit after retry=%+v err=%v; want unchanged %+v", permitAfterRetry, err, permitBeforeRetry)
	}
	if err := observerPool.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE request_id=$1`, identity.RequestID).Scan(&permitRows); err != nil || permitRows != 1 {
		t.Fatalf("independent permit rows after retry=%d err=%v; want exactly one", permitRows, err)
	}
	if got := dropper.DroppedCommitCount(); got != 1 {
		t.Fatalf("COMMIT completion drops after retry=%d; want exactly one", got)
	}

	writePaymentCommitAckLossEvidence(t, struct {
		Status                   string `json:"status"`
		DroppedCommitFrames      int    `json:"dropped_commit_frames"`
		CallerSawError           bool   `json:"caller_saw_error"`
		PermitState              string `json:"permit_state"`
		PermitRows               int    `json:"permit_rows"`
		RetryReturnedOriginal    bool   `json:"retry_returned_original_token_and_fence"`
		RefundClaimsWhilePending int    `json:"refund_claims_while_pending"`
		PreparedRejected         bool   `json:"refund_prepared_rejected"`
		DurabilitySettings       string `json:"durability_settings"`
	}{
		Status:              "OBSERVED_BY_TEST_SOURCE; execution status is the captured go test result",
		DroppedCommitFrames: int(dropper.DroppedCommitCount()), CallerSawError: true,
		PermitState: string(permitAfterRetry.State), PermitRows: permitRows,
		RetryReturnedOriginal: permitRetry == permitBeforeRetry, RefundClaimsWhilePending: len(claims),
		PreparedRejected: true, DurabilitySettings: synchronousCommit + "/fsync=" + fsync,
	})
}

func writePaymentCommitAckLossEvidence(t testing.TB, evidence any) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal("create Payment COMMIT acknowledgment evidence directory:", err)
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal("encode Payment COMMIT acknowledgment evidence:", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "payment-commit-ack-loss-evidence.json"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal("write Payment COMMIT acknowledgment evidence:", err)
	}
}
