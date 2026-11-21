//go:build admission_integration

package grpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	bookingCrashChildEnv  = "RUSHGO_ADMISSION_BOOKING_CRASH_PROCESS"
	bookingCrashConfigEnv = "RUSHGO_ADMISSION_BOOKING_CRASH_CONFIG"
	bookingCrashChildOn   = "1"
	bookingCrashReadyLine = "ADMISSION_BOOKING_READY "
	bookingCrashBefore    = "before_commit"
	bookingCrashAfter     = "after_commit_before_reconcile"
)

type bookingCrashConfig struct {
	Version           int    `json:"version"`
	DatabaseURL       string `json:"database_url"`
	ListenAddress     string `json:"listen_address"`
	BookingServerCert string `json:"booking_server_cert"`
	BookingServerKey  string `json:"booking_server_key"`
	PaymentServerName string `json:"payment_server_name"`
	BookingClientCert string `json:"booking_client_cert"`
	BookingClientKey  string `json:"booking_client_key"`
	CAFile            string `json:"ca_file"`
	EventID           string `json:"event_id"`
	StaffID           string `json:"staff_id"`
	Credential        string `json:"credential"`
	IdempotencyKey    string `json:"idempotency_key"`
	CommitBoundary    string `json:"commit_boundary"`
}

type bookingCrashReady struct {
	GRPCAddress    string `json:"grpc_address"`
	ControlAddress string `json:"control_address"`
}

type bookingCrashControlResponse struct {
	OK          bool       `json:"ok"`
	Boundary    string     `json:"boundary,omitempty"`
	ResultCode  string     `json:"result_code,omitempty"`
	ReasonCode  string     `json:"reason_code,omitempty"`
	Replayed    bool       `json:"replayed,omitempty"`
	CheckedInAt *time.Time `json:"checked_in_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type bookingCrashAttachRequest struct {
	PaymentAddress string `json:"payment_address"`
}

type bookingCrashBarrierRepository struct {
	repository.TicketRepository
	repository.TicketAdmissionRepository
	boundary string
	reached  chan string
	release  chan struct{}
}

func (r *bookingCrashBarrierRepository) CommitTicketCheckIn(ctx context.Context, command domain.TicketScanCommand) (domain.TicketScanResult, error) {
	if command.Admission != nil && r.boundary == bookingCrashBefore {
		if err := r.waitAtBoundary(ctx, bookingCrashBefore); err != nil {
			return domain.TicketScanResult{}, err
		}
	}
	result, err := r.TicketRepository.CommitTicketCheckIn(ctx, command)
	if err == nil && result.ResultCode == "accepted" && command.Admission != nil && r.boundary == bookingCrashAfter {
		if err := r.waitAtBoundary(ctx, bookingCrashAfter); err != nil {
			return domain.TicketScanResult{}, err
		}
	}
	return result, err
}

func (r *bookingCrashBarrierRepository) waitAtBoundary(ctx context.Context, boundary string) error {
	select {
	case r.reached <- boundary:
	default:
	}
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type bookingCrashProcess struct {
	command *exec.Cmd
	ready   bookingCrashReady
	done    chan struct{}
	waitErr error
	client  *http.Client
}

type bookingCrashKillResult struct {
	BoundaryDetail string
	ProcessExit    string
	RequestClosed  bool
}

// TestAdmissionBookingProcessCrashRecovery kills the actual Booking process
// at two different points around the real Ticket check-in repository call.
// Payment remains a separately running process over the real mTLS RPC.
type bookingCrashCaseEvidence struct {
	Scenario                         string `json:"scenario"`
	KillBoundary                     string `json:"kill_boundary"`
	BoundaryDetail                   string `json:"boundary_detail"`
	BookingProcessKilled             bool   `json:"booking_process_killed"`
	BookingProcessExit               string `json:"booking_process_exit"`
	BlockedCheckInRequestClosed      bool   `json:"blocked_checkin_request_closed"`
	PaymentStateWhileBookingDown     string `json:"payment_state_while_booking_down"`
	BookingStateWhileBookingDown     string `json:"booking_state_while_booking_down"`
	TicketStateWhileBookingDown      string `json:"ticket_state_while_booking_down"`
	ScanAuditWhileBookingDown        int    `json:"scan_audit_while_booking_down"`
	ScanReceiptWhileBookingDown      int    `json:"scan_receipt_while_booking_down"`
	ScanIdempotencyWhileBookingDown  int    `json:"scan_idempotency_while_booking_down"`
	UnavailableRecoveryHTTPStatus    int    `json:"unavailable_recovery_http_status"`
	UnavailableRecoveryProcessed     int    `json:"unavailable_recovery_processed"`
	UnavailableRecoveryReturnedError bool   `json:"unavailable_recovery_returned_error"`
	RefundStateWhileBookingDown      string `json:"refund_state_while_booking_down"`
	RefundClaimedWhileBookingDown    int    `json:"refund_claimed_while_booking_down"`
	RefundPreparedWhileBookingDown   int    `json:"refund_prepared_while_booking_down"`
	RefundCallsWhileBookingDown      int64  `json:"refund_calls_while_booking_down"`
	PaymentStateAfterRecovery        string `json:"payment_state_after_recovery"`
	BookingStateAfterRecovery        string `json:"booking_state_after_recovery"`
	TicketStateAfterRecovery         string `json:"ticket_state_after_recovery"`
	ScanAuditAfterRecovery           int    `json:"scan_audit_after_recovery"`
	ScanReceiptAfterRecovery         int    `json:"scan_receipt_after_recovery"`
	ScanIdempotencyAfterRecovery     int    `json:"scan_idempotency_after_recovery"`
	RefundStateAfterRecovery         string `json:"refund_state_after_recovery"`
	RefundPreparedAfterRecovery      int    `json:"refund_prepared_after_recovery"`
	RefundCallsAfterRecovery         int64  `json:"refund_calls_after_recovery"`
	IdempotentReplayAccepted         bool   `json:"idempotent_replay_accepted"`
}

// TestAdmissionBookingProcessCrashRecovery checks the two remaining durable
// crash windows with a real Booking OS child process, Payment helper process,
// isolated PostgreSQL schemas and internal mTLS RPC.
func TestAdmissionBookingProcessCrashRecovery(t *testing.T) {
	admissiontest.RequireOptIn(t)
	bookingDSN := os.Getenv("BOOKING_TEST_DATABASE_URL")
	paymentDSN := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	admissiontest.VerifyBookingPostgres(t, bookingDSN)
	admissiontest.VerifyRefundComponentPostgres(t, paymentDSN, os.Getenv(dualDBOwnerManifestEnv))
	admissiontest.RequireDistinctPostgresEndpoints(t, bookingDSN, paymentDSN)
	paymentBinary := strings.TrimSpace(os.Getenv(dualDBPaymentBinaryEnv))
	if paymentBinary == "" {
		t.Fatal("BLOCKED: ADMISSION_PAYMENT_TEST_BINARY must point to the independently compiled Payment gRPC test binary")
	}
	if info, err := os.Stat(paymentBinary); err != nil || info.IsDir() {
		t.Fatal("BLOCKED: independently compiled Payment test binary is missing")
	}

	cases := make([]bookingCrashCaseEvidence, 0, 2)
	beforePass := t.Run("before_commit", func(t *testing.T) {
		cases = append(cases, runAdmissionBookingCrashCase(t, bookingDSN, paymentDSN, paymentBinary,
			bookingCrashBefore, 0, "dualdb-crash-before-commit"))
	})
	afterPass := t.Run("after_commit_before_reconcile", func(t *testing.T) {
		cases = append(cases, runAdmissionBookingCrashCase(t, bookingDSN, paymentDSN, paymentBinary,
			bookingCrashAfter, 1, "dualdb-crash-after-commit"))
	})
	if beforePass && afterPass {
		writeBookingCrashEvidence(t, cases)
	}
}

func runAdmissionBookingCrashCase(t *testing.T, bookingDSN, paymentDSN, paymentBinary, boundary string, ticketIndex int, key string) bookingCrashCaseEvidence {
	t.Helper()
	bookingSchema := admissiontest.OpenMigrationSchema(t, bookingDSN, "../../migrations", 12)
	paymentSchema := admissiontest.OpenMigrationSchema(t, paymentDSN, "../../../payment/migrations", 12)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := seedDualDBBookingRows(t, bookingSchema.Pool)

	ca := admissiontest.NewPKI(t, "RushGo admission Booking crash integration CA")
	certDir := t.TempDir()
	caPath := admissiontest.WriteCA(t, certDir, ca.CAPEM)
	bookingServerLeaf := ca.NewLeaf(t, dualDBBookingServer, grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageServerAuth)
	bookingClientLeaf := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)
	paymentServerLeaf := ca.NewLeaf(t, dualDBPaymentServer, grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageServerAuth)
	paymentClientLeaf := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageClientAuth)
	bookingServerCert, bookingServerKey := admissiontest.WriteLeaf(t, certDir, "crash-booking-server", bookingServerLeaf)
	bookingClientCert, bookingClientKey := admissiontest.WriteLeaf(t, certDir, "crash-booking-client", bookingClientLeaf)
	paymentServerCert, paymentServerKey := admissiontest.WriteLeaf(t, certDir, "crash-payment-server", paymentServerLeaf)
	paymentClientCert, paymentClientKey := admissiontest.WriteLeaf(t, certDir, "crash-payment-client", paymentClientLeaf)

	bookingConfig := bookingCrashConfig{
		Version: 1, DatabaseURL: bookingSchema.DSN, ListenAddress: "127.0.0.1:0",
		BookingServerCert: bookingServerCert, BookingServerKey: bookingServerKey,
		PaymentServerName: dualDBPaymentServer,
		BookingClientCert: bookingClientCert, BookingClientKey: bookingClientKey, CAFile: caPath,
		EventID: fixture.EventID.String(), StaffID: fixture.StaffID.String(),
		Credential: fixture.Credentials[ticketIndex], IdempotencyKey: key, CommitBoundary: boundary,
	}
	bookingConfigPath := filepath.Join(certDir, "booking-process.json")
	var bookingProcess *bookingCrashProcess
	var paymentProcess *dualDBPaymentProcess
	t.Cleanup(func() {
		if bookingProcess != nil {
			bookingProcess.stop(t)
		}
		if paymentProcess != nil {
			paymentProcess.stop(t)
		}
	})
	bookingProcess = startBookingCrashProcess(t, bookingConfigPath, bookingConfig)
	bookingConfig.ListenAddress = bookingProcess.ready.GRPCAddress

	paymentConfig := dualDBPaymentConfig{
		Version: 1, DatabaseURL: paymentSchema.DSN,
		BookingAddress: bookingProcess.ready.GRPCAddress, BookingServerName: dualDBBookingServer,
		BookingClientCert: paymentClientCert, BookingClientKey: paymentClientKey,
		PaymentServerCert: paymentServerCert, PaymentServerKey: paymentServerKey, CAFile: caPath,
		PaymentID: fixture.PaymentID.String(), BookingID: fixture.BookingID.String(), UserID: fixture.UserID.String(),
		AmountCents: 2175, PaymentIntentID: "pi_booking_crash_integration_local", ProviderAccount: dualDBProviderAccount,
		SeedFinancial: true,
	}
	paymentConfigPath := filepath.Join(certDir, "crash-payment-process.json")
	startPayment := func() {
		paymentProcess = startDualDBPaymentProcess(t, paymentBinary, paymentConfigPath, paymentConfig)
		bookingProcess.attachPayment(t, paymentProcess.ready.GRPCAddress)
	}
	startPayment()
	if _, err := bookingSchema.Pool.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal("activate Booking crash-test admission guard:", err)
	}
	if response := paymentProcess.post(t, "/v1/activate"); !response.OK {
		t.Fatal("activate Payment crash-test admission guard")
	}

	identity := dualDBIdentity(fixture, ticketIndex, key)
	kill := bookingProcess.startCheckInThenKill(t, boundary)
	evidence := bookingCrashCaseEvidence{
		Scenario: key, KillBoundary: boundary, BookingProcessKilled: true, BoundaryDetail: kill.BoundaryDetail,
		BookingProcessExit: kill.ProcessExit, BlockedCheckInRequestClosed: kill.RequestClosed,
	}
	evidence.PaymentStateWhileBookingDown = readDualDBPermit(t, paymentSchema.Pool, identity.RequestID).State
	intentWhileDown := readDualDBIntent(t, bookingSchema.Pool, identity.RequestID)
	evidence.BookingStateWhileBookingDown = intentWhileDown.State
	evidence.TicketStateWhileBookingDown = readDualDBTicketStateValue(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	evidence.ScanAuditWhileBookingDown, evidence.ScanReceiptWhileBookingDown, evidence.ScanIdempotencyWhileBookingDown = dualDBScanFactCounts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	if boundary == bookingCrashBefore {
		// This barrier fires at CommitTicketCheckIn entry, before the method
		// starts its SQL transaction. It proves safe ABORT recovery from an
		// attached permit before the check-in write begins, not rollback of a
		// transaction interrupted during COMMIT.
		if evidence.PaymentStateWhileBookingDown != string(admission.Pending) || intentWhileDown.State != string(admission.Pending) || intentWhileDown.Token == uuid.Nil {
			t.Fatalf("pre-commit crash state: Payment=%s Booking=%+v", evidence.PaymentStateWhileBookingDown, intentWhileDown)
		}
		assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], "ISSUED")
		assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], 0, 0)
	} else {
		// Here the real repository returned a successful accepted result. The
		// decorator blocks before returning to TicketManager, so Reconcile has
		// not been sent when the OS process is killed.
		if evidence.PaymentStateWhileBookingDown != string(admission.Pending) || intentWhileDown.State != string(admission.Committed) ||
			intentWhileDown.Token == uuid.Nil || evidence.TicketStateWhileBookingDown != "CHECKED_IN" ||
			evidence.ScanAuditWhileBookingDown != 1 || evidence.ScanReceiptWhileBookingDown != 1 || evidence.ScanIdempotencyWhileBookingDown != 1 {
			t.Fatalf("post-commit/pre-reconcile crash state: Payment=%s Booking=%+v Ticket=%s audit/receipt=%d/%d",
				evidence.PaymentStateWhileBookingDown, intentWhileDown, evidence.TicketStateWhileBookingDown,
				evidence.ScanAuditWhileBookingDown, evidence.ScanReceiptWhileBookingDown)
		}
	}
	if _, err := paymentSchema.Pool.Exec(ctx, `UPDATE payment_admission_permits SET next_attempt_at=clock_timestamp()-INTERVAL '1 second'
		WHERE request_id=$1 AND state='PENDING'`, identity.RequestID); err != nil {
		t.Fatal("make only the owned pending permit due while Booking is down:", err)
	}
	statusCode, unavailableRecovery, err := postDualDBPaymentControl(paymentProcess, "/v1/recover-once")
	if err != nil || statusCode >= 200 && statusCode < 300 || unavailableRecovery.Processed != 1 || unavailableRecovery.Error == "" {
		t.Fatalf("Payment recovery while Booking is unreachable status=%d result=%+v err=%v; want one deferred pending permit", statusCode, unavailableRecovery, err)
	}
	evidence.UnavailableRecoveryHTTPStatus = statusCode
	evidence.UnavailableRecoveryProcessed = unavailableRecovery.Processed
	evidence.UnavailableRecoveryReturnedError = true
	if state := readDualDBPermit(t, paymentSchema.Pool, identity.RequestID).State; state != string(admission.Pending) {
		t.Fatalf("Payment recovery released a permit while Booking was unreachable: state=%s", state)
	}
	intentStateWhileUnavailable := string(admission.Pending)
	if boundary == bookingCrashAfter {
		intentStateWhileUnavailable = string(admission.Committed)
	}
	if state := readDualDBIntent(t, bookingSchema.Pool, identity.RequestID).State; state != intentStateWhileUnavailable {
		t.Fatalf("Booking intent changed while its process was unreachable: state=%s want=%s", state, intentStateWhileUnavailable)
	}

	// Create a real Payment refund obligation while Booking is unavailable.
	// The SQL claim query must hold the refund behind this PENDING permit.
	if !paymentProcess.post(t, "/v1/require-refund").OK {
		t.Fatal("create refund obligation during Booking outage")
	}
	refundWait := paymentProcess.post(t, "/v1/refund-once")
	if !refundWait.OK || refundWait.Processed != 0 || refundWait.RefundCalls != 0 {
		t.Fatalf("refund escaped the pending permit while Booking was unavailable: %+v", refundWait)
	}
	assertPaymentPreparedCount(t, paymentSchema.Pool, fixture.PaymentID, 0)
	evidence.RefundStateWhileBookingDown = readDualDBRefundState(t, ctx, paymentSchema.Pool, fixture.PaymentID)
	if evidence.RefundStateWhileBookingDown != "pending" {
		t.Fatalf("refund state while Booking was unavailable=%s, want pending", evidence.RefundStateWhileBookingDown)
	}
	evidence.RefundClaimedWhileBookingDown = refundWait.Processed
	evidence.RefundPreparedWhileBookingDown = readDualDBPreparedCount(t, ctx, paymentSchema.Pool, fixture.PaymentID)
	evidence.RefundCallsWhileBookingDown = paymentProcess.get(t, "/v1/provider-count").RefundCalls
	if evidence.RefundPreparedWhileBookingDown != 0 || evidence.RefundCallsWhileBookingDown != 0 {
		t.Fatalf("refund produced a side effect during Booking outage: prepared=%d provider_calls=%d",
			evidence.RefundPreparedWhileBookingDown, evidence.RefundCallsWhileBookingDown)
	}

	bookingConfig.CommitBoundary = ""
	bookingProcess = startBookingCrashProcess(t, bookingConfigPath, bookingConfig)
	paymentProcess.stop(t)
	paymentConfig.SeedFinancial = false
	startPayment()
	forceDualDBAdmissionDue(t, ctx, paymentSchema.Pool, identity.RequestID)
	recovery := paymentProcess.post(t, "/v1/recover-once")
	if !recovery.OK || recovery.Processed != 1 {
		t.Fatalf("recovery after restarting both services=%+v; want one durable permit processed", recovery)
	}
	permit := readDualDBPermit(t, paymentSchema.Pool, identity.RequestID)
	intent := readDualDBIntent(t, bookingSchema.Pool, identity.RequestID)
	if boundary == bookingCrashBefore {
		if permit.State != string(admission.Aborted) || intent.State != string(admission.Aborted) || permit.Token != intent.Token || permit.Fence != intent.Fence {
			t.Fatalf("pre-commit crash did not recover to matching ABORTED tombstones: Payment=%+v Booking=%+v", permit, intent)
		}
	} else if permit.State != string(admission.Committed) || intent.State != string(admission.Committed) || permit.Token != intent.Token || permit.Fence != intent.Fence {
		t.Fatalf("post-commit crash did not recover to matching COMMITTED state: Payment=%+v Booking=%+v", permit, intent)
	}
	evidence.PaymentStateAfterRecovery = permit.State
	evidence.BookingStateAfterRecovery = intent.State
	evidence.TicketStateAfterRecovery = readDualDBTicketStateValue(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	evidence.ScanAuditAfterRecovery, evidence.ScanReceiptAfterRecovery, evidence.ScanIdempotencyAfterRecovery = dualDBScanFactCounts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	if boundary == bookingCrashBefore {
		assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], "ISSUED")
		assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], 0, 0)
	} else {
		assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], "CHECKED_IN")
		assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], 1, 1)
	}

	refundDone := paymentProcess.post(t, "/v1/refund-once")
	if !refundDone.OK || refundDone.Processed != 1 || refundDone.RefundCalls != 1 {
		t.Fatalf("refund after durable Booking recovery=%+v; want one local provider call", refundDone)
	}
	evidence.RefundStateAfterRecovery = readDualDBRefundState(t, ctx, paymentSchema.Pool, fixture.PaymentID)
	evidence.RefundPreparedAfterRecovery = readDualDBPreparedCount(t, ctx, paymentSchema.Pool, fixture.PaymentID)
	evidence.RefundCallsAfterRecovery = paymentProcess.get(t, "/v1/provider-count").RefundCalls
	if evidence.RefundStateAfterRecovery != "succeeded" || evidence.RefundPreparedAfterRecovery != 1 || evidence.RefundCallsAfterRecovery != 1 {
		t.Fatalf("refund did not resume after Booking recovery: state=%s prepared=%d provider_calls=%d",
			evidence.RefundStateAfterRecovery, evidence.RefundPreparedAfterRecovery, evidence.RefundCallsAfterRecovery)
	}
	if boundary == bookingCrashAfter {
		replay, err := bookingProcess.request("POST", "/v1/check-in", nil)
		evidence.IdempotentReplayAccepted = err == nil && replay.OK && replay.ResultCode == "accepted" && replay.Replayed
		if !evidence.IdempotentReplayAccepted {
			t.Fatalf("post-recovery TicketManager replay=%+v err=%v; want original accepted operation", replay, err)
		}
	}
	evidence.TicketStateAfterRecovery = readDualDBTicketStateValue(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	evidence.ScanAuditAfterRecovery, evidence.ScanReceiptAfterRecovery, evidence.ScanIdempotencyAfterRecovery = dualDBScanFactCounts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex])
	if boundary == bookingCrashBefore {
		assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], "ISSUED")
		assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], 0, 0)
	} else {
		assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], "CHECKED_IN")
		assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[ticketIndex], 1, 1)
	}
	if responseCount := readDualDBRefundResponseCount(t, ctx, paymentSchema.Pool, fixture.PaymentID); responseCount != 1 {
		t.Fatalf("refund RESPONSE audit count=%d, want one", responseCount)
	}
	t.Logf("ADMISSION_BOUNDARY scenario=%s killed=%t child_exit=%s unavailable_recovery=%d refund_prepared_while_down=%d refund_calls_while_down=%d recovered_payment=%s ticket_audits=%d",
		evidence.Scenario, evidence.BookingProcessKilled, evidence.BookingProcessExit, evidence.UnavailableRecoveryHTTPStatus,
		evidence.RefundPreparedWhileBookingDown, evidence.RefundCallsWhileBookingDown, evidence.PaymentStateAfterRecovery, evidence.ScanAuditAfterRecovery)
	return evidence
}

func writeBookingCrashEvidence(t testing.TB, cases []bookingCrashCaseEvidence) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal("create Booking crash evidence directory:", err)
	}
	evidence := struct {
		Status     string                     `json:"status"`
		Cases      []bookingCrashCaseEvidence `json:"cases"`
		NotCovered []string                   `json:"not_covered"`
	}{
		Status: "OBSERVED_BY_TEST_SOURCE; execution status must come from go test",
		Cases:  cases,
		NotCovered: []string{
			"before_commit kills Booking before CommitTicketCheckIn starts its SQL transaction; this is not an in-flight COMMIT rollback or lost-COMMIT-ack test",
			"Provider operations use the existing local Payment test double; no external Provider request is sent",
		},
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal("encode Booking crash evidence:", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "admission-booking-crash-evidence.json"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal("write Booking crash evidence:", err)
	}
}
func TestAdmissionBookingCrashProcess(t *testing.T) {
	if os.Getenv(bookingCrashChildEnv) != bookingCrashChildOn {
		t.Skip("Booking admission crash process is started only by the dual-PostgreSQL harness")
	}
	if os.Getenv(admissiontest.OptInEnv) != admissiontest.OptInValue {
		t.Fatal("BLOCKED: Booking process is missing the dual-PostgreSQL test-only opt-in")
	}
	configPath := os.Getenv(bookingCrashConfigEnv)
	if configPath == "" {
		t.Fatal("BLOCKED: Booking process config is required")
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("read Booking crash-process config")
	}
	var config bookingCrashConfig
	if err := json.Unmarshal(contents, &config); err != nil || config.Version != 1 || config.DatabaseURL == "" ||
		config.ListenAddress == "" || config.PaymentServerName == "" ||
		config.EventID == "" || config.StaffID == "" || config.Credential == "" || config.IdempotencyKey == "" ||
		(config.CommitBoundary != "" && config.CommitBoundary != bookingCrashBefore && config.CommitBoundary != bookingCrashAfter) {
		t.Fatal("BLOCKED: Booking crash-process config is invalid")
	}
	eventID, err := uuid.Parse(config.EventID)
	if err != nil || eventID == uuid.Nil {
		t.Fatal("BLOCKED: Booking event identity is invalid")
	}
	staffID, err := uuid.Parse(config.StaffID)
	if err != nil || staffID == uuid.Nil {
		t.Fatal("BLOCKED: Booking scanner identity is invalid")
	}
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		startupCancel()
		t.Fatal("parse isolated Booking database config")
	}
	poolConfig.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(startupCtx, poolConfig)
	if err != nil {
		startupCancel()
		t.Fatal("connect isolated Booking PostgreSQL")
	}
	if err := pool.Ping(startupCtx); err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("ping isolated Booking PostgreSQL")
	}
	repo := repository.NewPostgresBookingRepository(pool)
	barrierRepo := &bookingCrashBarrierRepository{
		TicketRepository: repo, TicketAdmissionRepository: repo, boundary: config.CommitBoundary,
		reached: make(chan string, 1), release: make(chan struct{}),
	}
	manager := service.NewTicketManager(barrierRepo, dualDBEvidenceAuthority{})
	var managerMu sync.RWMutex
	var paymentConn *grpc.ClientConn

	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral test CA")
	}
	bookingServerCert, err := os.ReadFile(config.BookingServerCert)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Booking server certificate")
	}
	bookingServerKey, err := os.ReadFile(config.BookingServerKey)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("read ephemeral Booking server key")
	}
	serverCredentials, err := grpcsecurity.NewServerCredentials(bookingServerCert, bookingServerKey, caPEM, []string{grpcsecurity.ServiceIdentityPayment})
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("create Booking mTLS admission credentials")
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		pool.Close()
		startupCancel()
		t.Fatal("listen for isolated Booking admission RPC")
	}
	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener.Close()
		pool.Close()
		startupCancel()
		t.Fatal("listen for loopback-only Booking test control")
	}
	server := grpc.NewServer(grpc.Creds(serverCredentials))
	admissionv1.RegisterBookingAdmissionAuthorityServer(server, NewAdmissionServer(repo))
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	stop := make(chan struct{})
	var stopOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/attach-payment", func(w http.ResponseWriter, r *http.Request) {
		var request bookingCrashAttachRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil || request.PaymentAddress == "" {
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "invalid Payment address"}, http.StatusBadRequest)
			return
		}
		clientCert, err := os.ReadFile(config.BookingClientCert)
		if err != nil {
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "Booking client certificate unavailable"}, http.StatusInternalServerError)
			return
		}
		clientKey, err := os.ReadFile(config.BookingClientKey)
		if err != nil {
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "Booking client key unavailable"}, http.StatusInternalServerError)
			return
		}
		credentials, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, caPEM, config.PaymentServerName, grpcsecurity.ServiceIdentityBooking)
		if err != nil {
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "Booking-to-Payment mTLS credentials unavailable"}, http.StatusInternalServerError)
			return
		}
		conn, err := grpc.NewClient(request.PaymentAddress, grpc.WithTransportCredentials(credentials))
		if err == nil {
			conn.Connect()
			readyCtx, readyCancel := context.WithTimeout(r.Context(), 5*time.Second)
			err = waitBookingCrashGRPCReady(readyCtx, conn)
			readyCancel()
		}
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "Payment admission gRPC endpoint did not become ready"}, http.StatusServiceUnavailable)
			return
		}
		managerMu.Lock()
		old := paymentConn
		paymentConn = conn
		manager.SetAdmissionCoordinator(admission.NewCoordinatorClient(conn))
		managerMu.Unlock()
		if old != nil {
			_ = old.Close()
		}
		writeBookingCrashControl(w, bookingCrashControlResponse{OK: true}, http.StatusOK)
	})
	mux.HandleFunc("POST /v1/check-in", func(w http.ResponseWriter, r *http.Request) {
		managerMu.RLock()
		result, err := manager.CheckIn(r.Context(), eventID, staffID, config.Credential, config.IdempotencyKey)
		managerMu.RUnlock()
		if err != nil {
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "TicketManager check-in returned an error"}, http.StatusConflict)
			return
		}
		writeBookingCrashControl(w, bookingCrashControlResponse{
			OK: true, ResultCode: result.ResultCode, ReasonCode: result.ReasonCode,
			Replayed: result.Replayed, CheckedInAt: result.CheckedInAt,
		}, http.StatusOK)
	})
	mux.HandleFunc("GET /v1/wait-boundary", func(w http.ResponseWriter, r *http.Request) {
		select {
		case boundary := <-barrierRepo.reached:
			writeBookingCrashControl(w, bookingCrashControlResponse{OK: true, Boundary: boundary}, http.StatusOK)
		case <-r.Context().Done():
			writeBookingCrashControl(w, bookingCrashControlResponse{Error: "commit boundary wait was canceled"}, http.StatusRequestTimeout)
		}
	})
	mux.HandleFunc("POST /v1/shutdown", func(w http.ResponseWriter, _ *http.Request) {
		writeBookingCrashControl(w, bookingCrashControlResponse{OK: true}, http.StatusAccepted)
		go stopOnce.Do(func() {
			close(stop)
			server.GracefulStop()
		})
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpServer.Serve(controlListener) }()
	startupCancel()
	ready := bookingCrashReady{GRPCAddress: listener.Addr().String(), ControlAddress: controlListener.Addr().String()}
	encoded, err := json.Marshal(ready)
	if err != nil {
		server.Stop()
		_ = httpServer.Close()
		_ = listener.Close()
		_ = controlListener.Close()
		pool.Close()
		t.Fatal("encode Booking crash-process readiness")
	}
	if _, err := os.Stdout.Write(append(append([]byte(bookingCrashReadyLine), encoded...), '\n')); err != nil {
		server.Stop()
		_ = httpServer.Close()
		_ = listener.Close()
		_ = controlListener.Close()
		pool.Close()
		t.Fatal("publish Booking crash-process readiness")
	}
	select {
	case <-stop:
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = httpServer.Shutdown(shutdownCtx)
		shutdownCancel()
		_ = listener.Close()
		_ = controlListener.Close()
	case err := <-serveErr:
		_ = httpServer.Close()
		_ = controlListener.Close()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Fatalf("Booking admission gRPC process stopped: %v", err)
		}
	case err := <-httpErr:
		server.Stop()
		_ = listener.Close()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Booking test control process stopped: %v", err)
		}
	}
	managerMu.Lock()
	if paymentConn != nil {
		_ = paymentConn.Close()
	}
	managerMu.Unlock()
	pool.Close()
}

func startBookingCrashProcess(t testing.TB, configPath string, config bookingCrashConfig) *bookingCrashProcess {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal("encode Booking crash-process config:", err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write Booking crash-process config:", err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal("locate current Booking gRPC test binary:", err)
	}
	command := exec.Command(binary, "-test.v", "-test.run=^TestAdmissionBookingCrashProcess$")
	command.Env = append(os.Environ(),
		admissiontest.OptInEnv+"="+admissiontest.OptInValue,
		bookingCrashChildEnv+"="+bookingCrashChildOn,
		bookingCrashConfigEnv+"="+configPath,
	)
	logDirectory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if logDirectory == "" {
		logDirectory = filepath.Dir(configPath)
	}
	if err := os.MkdirAll(logDirectory, 0o700); err != nil {
		t.Fatal("create Booking process log directory:", err)
	}
	logPath := filepath.Join(logDirectory, "booking-process-"+uuid.NewString()+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal("create Booking process log:", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = logFile.Close()
		t.Fatal("open Booking crash-process stdout:", err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = logFile.Close()
		t.Fatal("open Booking crash-process stderr:", err)
	}
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal("start independent Booking TicketManager process:", err)
	}
	process := &bookingCrashProcess{command: command, done: make(chan struct{}), client: &http.Client{Timeout: 12 * time.Second}}
	var logReaders sync.WaitGroup
	logReaders.Add(2)
	go func() {
		logReaders.Wait()
		process.waitErr = command.Wait()
		_ = logFile.Close()
		close(process.done)
	}()
	go func() {
		defer logReaders.Done()
		_, _ = io.Copy(logFile, stderr)
	}()
	ready := make(chan bookingCrashReady, 1)
	readErr := make(chan error, 1)
	go func() {
		defer logReaders.Done()
		published := false
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = logFile.Write(append([]byte(line), '\n'))
			if published || !strings.HasPrefix(line, bookingCrashReadyLine) {
				continue
			}
			var value bookingCrashReady
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, bookingCrashReadyLine)), &value); err != nil {
				readErr <- errors.New("Booking crash process published invalid readiness JSON")
				return
			}
			published = true
			ready <- value
		}
		if err := scanner.Err(); err != nil {
			readErr <- errors.New("read Booking crash-process output")
			return
		}
		readErr <- errors.New("Booking crash process exited before readiness")
	}()
	select {
	case value := <-ready:
		if value.GRPCAddress == "" || value.ControlAddress == "" {
			_ = command.Process.Kill()
			<-process.done
			t.Fatal("Booking crash process published an empty listener address")
		}
		process.ready = value
		return process
	case err := <-readErr:
		select {
		case <-process.done:
			t.Fatalf("%v (exit=%v)", err, process.waitErr)
		case <-time.After(2 * time.Second):
			_ = command.Process.Kill()
			<-process.done
			t.Fatal(err)
		}
	case <-process.done:
		t.Fatalf("Booking crash process exited before readiness: %v", process.waitErr)
	case <-time.After(20 * time.Second):
		_ = command.Process.Kill()
		<-process.done
		t.Fatal("Booking crash-process readiness deadline exceeded")
	}
	return nil
}

func (p *bookingCrashProcess) attachPayment(t testing.TB, address string) {
	t.Helper()
	response, err := p.request("POST", "/v1/attach-payment", bookingCrashAttachRequest{PaymentAddress: address})
	if err != nil || !response.OK {
		t.Fatalf("attach Booking TicketManager to real Payment admission RPC: response=%+v err=%v", response, err)
	}
}

func (p *bookingCrashProcess) startCheckInThenKill(t testing.TB, wantBoundary string) bookingCrashKillResult {
	t.Helper()
	requestErr := make(chan error, 1)
	go func() {
		_, err := p.request("POST", "/v1/check-in", nil)
		requestErr <- err
	}()
	boundary, err := p.request("GET", "/v1/wait-boundary", nil)
	if err != nil || !boundary.OK || boundary.Boundary != wantBoundary {
		t.Fatalf("Booking check-in did not reach %s boundary: response=%+v err=%v", wantBoundary, boundary, err)
	}
	p.kill(t)
	select {
	case err := <-requestErr:
		if err == nil {
			t.Fatal("blocked Booking check-in unexpectedly returned before its process was killed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("killed Booking check-in request did not finish")
	}
	detail := "attached permit; process killed before entering CommitTicketCheckIn; SQL transaction had not started"
	if wantBoundary == bookingCrashAfter {
		detail = "CommitTicketCheckIn returned accepted; process killed before TicketManager sent ReconcileAdmission"
	}
	if p.waitErr == nil {
		t.Fatal("Booking child exited successfully after an OS kill; expected a killed-process exit status")
	}
	return bookingCrashKillResult{BoundaryDetail: detail, ProcessExit: p.waitErr.Error(), RequestClosed: true}
}

func (p *bookingCrashProcess) request(method, path string, body any) (bookingCrashControlResponse, error) {
	var result bookingCrashControlResponse
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return result, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, "http://"+p.ready.ControlAddress+path, reader)
	if err != nil {
		return result, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := p.client.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return result, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, errors.New("Booking loopback control returned a non-success status")
	}
	return result, nil
}

func (p *bookingCrashProcess) stop(t testing.TB) {
	t.Helper()
	if p == nil || p.command == nil || p.command.Process == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_, _ = p.request("POST", "/v1/shutdown", nil)
	select {
	case <-p.done:
	case <-time.After(6 * time.Second):
		_ = p.command.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Error("Booking crash-process helper did not stop after kill")
		}
	}
}

func (p *bookingCrashProcess) kill(t testing.TB) {
	t.Helper()
	if p == nil || p.command == nil || p.command.Process == nil {
		t.Fatal("cannot kill missing Booking crash process")
	}
	select {
	case <-p.done:
		t.Fatal("Booking process exited before the requested commit boundary")
	default:
	}
	if err := p.command.Process.Kill(); err != nil {
		t.Fatalf("kill Booking OS process at %s boundary: %v", p.ready.GRPCAddress, err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("killed Booking process did not exit")
	}
}

func postDualDBPaymentControl(p *dualDBPaymentProcess, path string) (int, dualDBControlResponse, error) {
	var result dualDBControlResponse
	response, err := p.client.Post("http://"+p.ready.ControlAddress+path, "application/json", bytes.NewReader(nil))
	if err != nil {
		return 0, result, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return response.StatusCode, result, err
	}
	return response.StatusCode, result, nil
}

func forceDualDBAdmissionDue(t testing.TB, ctx context.Context, pool *pgxpool.Pool, requestID uuid.UUID) {
	t.Helper()
	tag, err := pool.Exec(ctx, `UPDATE payment_admission_permits SET next_attempt_at=clock_timestamp()-INTERVAL '1 second'
		WHERE request_id=$1 AND state='PENDING'`, requestID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("make only the owned pending permit due for recovery: rows=%d err=%v", tag.RowsAffected(), err)
	}
}

func waitBookingCrashGRPCReady(ctx context.Context, conn *grpc.ClientConn) error {
	for conn.GetState() != connectivity.Ready {
		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
	return nil
}

func readDualDBTicketStateValue(t testing.TB, pool *pgxpool.Pool, ticketID uuid.UUID) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var state string
	if err := pool.QueryRow(ctx, `SELECT status FROM tickets WHERE id=$1`, ticketID).Scan(&state); err != nil {
		t.Fatal("read crash-window Ticket state:", err)
	}
	return state
}

func dualDBScanFactCounts(t testing.TB, pool *pgxpool.Pool, ticketID uuid.UUID) (audits, receipts, completed int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, ticketID).Scan(&audits); err != nil {
		t.Fatal("read crash-window check-in audit count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_scan_requests WHERE ticket_id=$1`, ticketID).Scan(&receipts); err != nil {
		t.Fatal("read crash-window scan receipt count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_operation_idempotency WHERE ticket_id=$1 AND operation='CHECK_IN' AND completed_at IS NOT NULL`, ticketID).Scan(&completed); err != nil {
		t.Fatal("read crash-window idempotency count:", err)
	}
	return audits, receipts, completed
}

func readDualDBRefundState(t testing.TB, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, paymentID).Scan(&state); err != nil {
		t.Fatal("read crash-window Payment refund state:", err)
	}
	return state
}

func readDualDBPreparedCount(t testing.TB, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='PREPARED'`, paymentID).Scan(&count); err != nil {
		t.Fatal("read crash-window PREPARED audit count:", err)
	}
	return count
}

func readDualDBRefundResponseCount(t testing.TB, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='RESPONSE'`, paymentID).Scan(&count); err != nil {
		t.Fatal("read crash-window RESPONSE audit count:", err)
	}
	return count
}

func writeBookingCrashControl(w http.ResponseWriter, response bookingCrashControlResponse, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(response)
}
